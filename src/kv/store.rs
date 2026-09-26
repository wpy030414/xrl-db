//! 键值状态机。
//!
//! 纯内存 `HashMap` + 惰性过期。
//!
//! # 两条不可动摇的约束
//!
//! 1. **所有写入口只接受 [`WriteOp`]，不接受 `Command`。** `WriteOp` 不含任何相对时间，
//!    从类型层面杜绝了「apply 时读时钟」这种破坏确定性的写法。
//! 2. **所有方法都显式接收 `now_ms`，绝不读系统时钟。** 时间由调用方给出——
//!    单机模式下是当前时刻，集群模式下由 leader 写进日志。状态机本身对此无感。
//!
//! 这两条让同一个状态机既能单机使用，又能被 Raft 复制，无需任何改动。

use std::collections::HashMap;

use bytes::Bytes;

use super::glob;
use super::op::{TimestampMs, WriteOp};
use crate::protocol::{Reply, SetCondition};

/// 一条键值记录。
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    /// 值。
    value: Bytes,
    /// 绝对过期时刻；`None` 表示永不过期。
    expire_at: Option<TimestampMs>,
}

impl Entry {
    /// 判断在 `now_ms` 时刻是否已过期。
    ///
    /// 用 `>=` 而非 `>`：恰好到期的瞬间即视为过期。
    fn is_expired(&self, now_ms: TimestampMs) -> bool {
        matches!(self.expire_at, Some(at) if now_ms >= at)
    }
}

/// 键值状态机。
#[derive(Debug, Default)]
pub struct Store {
    entries: HashMap<Bytes, Entry>,
}

impl Store {
    /// 创建一个空的状态机。
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前记录的键数量。
    ///
    /// **不是** `DBSIZE` 的语义——后者会排除已过期的键，因此以 [`Store::dbsize`] 为准。
    /// 这个方法用于内存观测。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 状态机是否为空（含已过期但尚未回收的记录）。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // ---------------------------------------------------------------- 写入口

    /// 应用一条写操作，返回应回给客户端的响应。
    ///
    /// 这是**唯一的写入口**。它的返回类型是 [`Reply`]，因为状态机正是决定
    /// 「客户端应当看到什么」的地方——把这一步拆成两段只会产生无谓的转换层。
    pub fn apply(&mut self, op: &WriteOp, now_ms: TimestampMs) -> Reply {
        match op {
            WriteOp::Set {
                key,
                value,
                expire_at,
                condition,
            } => {
                // 条件检查发生在 apply 时，只取决于当时的状态，因此结果仍然确定。
                // 注意：已过期的键在此视为不存在，这与 Redis 语义一致。
                let exists = self.get_live(key, now_ms).is_some();
                let allowed = match condition {
                    SetCondition::Always => true,
                    SetCondition::IfAbsent => !exists,
                    SetCondition::IfPresent => exists,
                };

                if !allowed {
                    // NX/XX 条件不满足时返回空值，客户端的 SET 命令据此返回 nil
                    return Reply::Null;
                }

                self.entries.insert(
                    key.clone(),
                    Entry {
                        value: value.clone(),
                        expire_at: *expire_at,
                    },
                );
                Reply::ok()
            }

            WriteOp::Del { keys } => {
                let mut removed = 0i64;
                for key in keys {
                    // 已过期的键视为不存在，不应计入删除数
                    if self.get_live(key, now_ms).is_some() {
                        self.entries.remove(key);
                        removed += 1;
                    }
                }
                Reply::Integer(removed)
            }

            WriteOp::MSet { pairs } => {
                for (key, value) in pairs {
                    // MSET 会清除原有的 TTL——这与 Redis 一致
                    self.entries.insert(
                        key.clone(),
                        Entry {
                            value: value.clone(),
                            expire_at: None,
                        },
                    );
                }
                Reply::ok()
            }

            WriteOp::Append { key, value } => {
                let existing = self
                    .get_live(key, now_ms)
                    .map(|entry| (entry.value.clone(), entry.expire_at));

                let (mut buffer, expire_at) = match existing {
                    // APPEND 保留原有的 TTL
                    Some((current, expire_at)) => (current.to_vec(), expire_at),
                    None => (Vec::new(), None),
                };

                buffer.extend_from_slice(value);
                let length = buffer.len();
                self.entries.insert(
                    key.clone(),
                    Entry {
                        value: Bytes::from(buffer),
                        expire_at,
                    },
                );

                Reply::Integer(length as i64)
            }

            WriteOp::IncrBy { key, delta } => {
                let existing = self
                    .get_live(key, now_ms)
                    .map(|entry| (entry.value.clone(), entry.expire_at));

                let current = match &existing {
                    Some((value, _)) => match parse_integer(value) {
                        Some(number) => number,
                        None => {
                            return Reply::error("ERR value is not an integer or out of range");
                        }
                    },
                    None => 0,
                };

                let Some(updated) = current.checked_add(*delta) else {
                    return Reply::error("ERR increment or decrement would overflow");
                };

                // INCR 保留原有的 TTL
                let expire_at = existing.and_then(|(_, expire_at)| expire_at);
                self.entries.insert(
                    key.clone(),
                    Entry {
                        value: Bytes::from(updated.to_string()),
                        expire_at,
                    },
                );

                Reply::Integer(updated)
            }

            WriteOp::ExpireAt { key, at } => {
                let Some(entry) = self.get_live(key, now_ms).cloned() else {
                    // 键不存在：无法设置过期时间
                    return Reply::Integer(0);
                };

                if *at <= now_ms {
                    // 过期时刻已到，直接删除——这正是 `EXPIRE key -1` 的语义
                    self.entries.remove(key);
                    return Reply::Integer(1);
                }

                self.entries.insert(
                    key.clone(),
                    Entry {
                        expire_at: Some(*at),
                        ..entry
                    },
                );
                Reply::Integer(1)
            }

            WriteOp::Persist { key } => {
                let Some(entry) = self.get_live(key, now_ms).cloned() else {
                    return Reply::Integer(0);
                };

                // 本来就没有过期时间，视作未改动
                if entry.expire_at.is_none() {
                    return Reply::Integer(0);
                }

                self.entries.insert(
                    key.clone(),
                    Entry {
                        expire_at: None,
                        ..entry
                    },
                );
                Reply::Integer(1)
            }

            WriteOp::FlushDb => {
                self.entries.clear();
                Reply::ok()
            }
        }
    }

    // ---------------------------------------------------------------- 读入口

    /// `GET key`
    pub fn get(&self, key: &Bytes, now_ms: TimestampMs) -> Reply {
        match self.get_live(key, now_ms) {
            Some(entry) => Reply::Bulk(entry.value.clone()),
            None => Reply::Null,
        }
    }

    /// `EXISTS key [key ...]`
    ///
    /// 重复的键会被重复计数，这与 Redis 一致（`EXISTS a a` 在 `a` 存在时返回 2）。
    pub fn exists(&self, keys: &[Bytes], now_ms: TimestampMs) -> Reply {
        let count = keys
            .iter()
            .filter(|key| self.get_live(key, now_ms).is_some())
            .count();
        Reply::Integer(count as i64)
    }

    /// `MGET key [key ...]`
    pub fn mget(&self, keys: &[Bytes], now_ms: TimestampMs) -> Reply {
        Reply::Array(
            keys.iter()
                .map(|key| match self.get_live(key, now_ms) {
                    Some(entry) => Reply::Bulk(entry.value.clone()),
                    None => Reply::Null,
                })
                .collect(),
        )
    }

    /// `STRLEN key`
    pub fn strlen(&self, key: &Bytes, now_ms: TimestampMs) -> Reply {
        match self.get_live(key, now_ms) {
            Some(entry) => Reply::Integer(entry.value.len() as i64),
            None => Reply::Integer(0),
        }
    }

    /// `TTL key`
    ///
    /// 返回值遵循 Redis 约定：`-2` 表示键不存在，`-1` 表示存在但无过期时间，
    /// 其余为剩余秒数。
    pub fn ttl(&self, key: &Bytes, now_ms: TimestampMs) -> Reply {
        let Some(entry) = self.get_live(key, now_ms) else {
            return Reply::Integer(-2);
        };

        match entry.expire_at {
            None => Reply::Integer(-1),
            Some(at) => {
                let remaining_ms = at.saturating_sub(now_ms);
                // 四舍五入到秒——否则刚设置的 60 秒 TTL 会立刻显示成 59，
                // 这是 Redis 客户端的常见困惑点
                Reply::Integer(((remaining_ms + 500) / 1000) as i64)
            }
        }
    }

    /// `KEYS pattern`
    ///
    /// 结果按键排序。Redis 本身不保证顺序，但排序让行为可预测、便于测试，
    /// 且代价（`O(n log n)`）在 `KEYS` 这种本就 `O(n)` 的命令上可以接受。
    pub fn keys(&self, pattern: &Bytes, now_ms: TimestampMs) -> Reply {
        let mut matched: Vec<&Bytes> = self
            .entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired(now_ms))
            .map(|(key, _)| key)
            .filter(|key| glob::matches(pattern, key))
            .collect();

        matched.sort();

        Reply::Array(
            matched
                .into_iter()
                .map(|key| Reply::Bulk(key.clone()))
                .collect(),
        )
    }

    /// `SCAN cursor [MATCH pattern] [COUNT count]`
    ///
    /// # 关于游标的实现
    ///
    /// Redis 的游标是对哈希表桶的遍历位置，语义相当特殊。这里采用的是**排序后下标**
    /// 的方案——把全部键排序，游标即下标。
    ///
    /// 这个实现满足 SCAN 的核心保证（一次完整遍历能取到遍历期间始终存在的所有键），
    /// 但**不满足** Redis 的「保证不会返回重复元素」这一点：遍历期间若有键被删除，
    /// 后续元素会前移，可能造成重复。对绝大多数客户端用法没有影响，但如实记录于此。
    pub fn scan(
        &self,
        cursor: u64,
        pattern: Option<&Bytes>,
        count: Option<u64>,
        now_ms: TimestampMs,
    ) -> Reply {
        let mut all: Vec<&Bytes> = self
            .entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired(now_ms))
            .map(|(key, _)| key)
            .filter(|key| match pattern {
                Some(pattern) => glob::matches(pattern, key),
                None => true,
            })
            .collect();

        all.sort();

        let start = (cursor as usize).min(all.len());
        let count = count.unwrap_or(10).max(1) as usize;
        let end = start.saturating_add(count).min(all.len());

        let page: Vec<Reply> = all[start..end]
            .iter()
            .map(|key| Reply::Bulk((*key).clone()))
            .collect();

        // 游标 0 表示遍历结束
        let next_cursor = if end >= all.len() { 0 } else { end as u64 };

        Reply::Array(vec![
            Reply::Bulk(Bytes::from(next_cursor.to_string())),
            Reply::Array(page),
        ])
    }

    /// `TYPE key`
    ///
    /// 目前只支持 String 类型，因此不是 `string` 就是 `none`。
    pub fn type_of(&self, key: &Bytes, now_ms: TimestampMs) -> Reply {
        match self.get_live(key, now_ms) {
            Some(_) => Reply::Simple("string".to_string()),
            None => Reply::Simple("none".to_string()),
        }
    }

    /// `DBSIZE`
    ///
    /// 排除已过期的键——即使用户尚未访问过它们。
    pub fn dbsize(&self, now_ms: TimestampMs) -> Reply {
        let count = self
            .entries
            .values()
            .filter(|entry| !entry.is_expired(now_ms))
            .count();
        Reply::Integer(count as i64)
    }

    // ---------------------------------------------------------------- 内部

    /// 取出一条**未过期**的记录。
    ///
    /// 惰性过期的全部实现就是这一个方法：所有读取路径都经过它，
    /// 因此已过期的键在任何地方都不会被观察到。
    fn get_live(&self, key: &Bytes, now_ms: TimestampMs) -> Option<&Entry> {
        self.entries
            .get(key)
            .filter(|entry| !entry.is_expired(now_ms))
    }
}

/// 按 Redis 语义把字节串解析为整数。
///
/// 与 Redis 的 `string2ll` 有一处细微差异：本实现接受前导 `+`（如 `+5`）与前导零
/// （如 `007`），而 Redis 会拒绝它们。这在实践中极少出现，且拒绝还是接受都属于
/// 「未定义行为」的范畴，故不做额外处理，仅在此记录。
fn parse_integer(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: TimestampMs = 1_700_000_000_000;
    const SECOND: TimestampMs = 1_000;

    fn key(name: &str) -> Bytes {
        Bytes::copy_from_slice(name.as_bytes())
    }

    fn set(name: &str, value: &str) -> WriteOp {
        WriteOp::Set {
            key: key(name),
            value: Bytes::copy_from_slice(value.as_bytes()),
            expire_at: None,
            condition: SetCondition::Always,
        }
    }

    /// 取出块字符串的值，便于断言
    fn bulk_of(reply: &Reply) -> Option<&[u8]> {
        match reply {
            Reply::Bulk(data) => Some(data),
            _ => None,
        }
    }

    #[test]
    fn set_then_get() {
        let mut store = Store::new();
        assert_eq!(store.apply(&set("foo", "bar"), NOW), Reply::ok());

        assert_eq!(bulk_of(&store.get(&key("foo"), NOW)), Some(&b"bar"[..]));
    }

    #[test]
    fn get_missing_key_returns_null() {
        let store = Store::new();
        assert_eq!(store.get(&key("nope"), NOW), Reply::Null);
        assert_eq!(store.strlen(&key("nope"), NOW), Reply::Integer(0));
        assert_eq!(
            store.type_of(&key("nope"), NOW),
            Reply::Simple("none".to_string())
        );
    }

    #[test]
    fn set_if_absent_respects_existing_key() {
        let mut store = Store::new();

        let nx = WriteOp::Set {
            key: key("k"),
            value: Bytes::from_static(b"first"),
            expire_at: None,
            condition: SetCondition::IfAbsent,
        };

        assert_eq!(store.apply(&nx, NOW), Reply::ok(), "键不存在时应写入成功");
        assert_eq!(
            store.apply(&nx, NOW),
            Reply::Null,
            "键已存在时 NX 应失败并返回空值"
        );
        assert_eq!(bulk_of(&store.get(&key("k"), NOW)), Some(&b"first"[..]));
    }

    #[test]
    fn set_if_present_requires_existing_key() {
        let mut store = Store::new();

        let xx = WriteOp::Set {
            key: key("k"),
            value: Bytes::from_static(b"v"),
            expire_at: None,
            condition: SetCondition::IfPresent,
        };

        assert_eq!(store.apply(&xx, NOW), Reply::Null, "键不存在时 XX 应失败");

        store.apply(&set("k", "old"), NOW);
        assert_eq!(store.apply(&xx, NOW), Reply::ok(), "键存在时 XX 应成功");
    }

    #[test]
    fn expired_key_is_invisible_everywhere() {
        let mut store = Store::new();
        store.apply(
            &WriteOp::Set {
                key: key("temp"),
                value: Bytes::from_static(b"v"),
                expire_at: Some(NOW + SECOND),
                condition: SetCondition::Always,
            },
            NOW,
        );

        // 到期前可见
        assert_eq!(bulk_of(&store.get(&key("temp"), NOW)), Some(&b"v"[..]));
        assert_eq!(store.dbsize(NOW), Reply::Integer(1));

        // 到期瞬间即不可见——这是惰性过期的全部含义
        let after = NOW + SECOND;
        assert_eq!(store.get(&key("temp"), after), Reply::Null);
        assert_eq!(store.exists(&[key("temp")], after), Reply::Integer(0));
        assert_eq!(store.dbsize(after), Reply::Integer(0));
        assert_eq!(
            store.type_of(&key("temp"), after),
            Reply::Simple("none".to_string())
        );
    }

    #[test]
    fn ttl_follows_redis_conventions() {
        let mut store = Store::new();

        // 键不存在
        assert_eq!(store.ttl(&key("nope"), NOW), Reply::Integer(-2));

        // 键存在但无过期时间
        store.apply(&set("permanent", "v"), NOW);
        assert_eq!(store.ttl(&key("permanent"), NOW), Reply::Integer(-1));

        // 恰好 60 秒——四舍五入后应为 60 而非 59
        store.apply(
            &WriteOp::Set {
                key: key("temp"),
                value: Bytes::from_static(b"v"),
                expire_at: Some(NOW + 60 * SECOND),
                condition: SetCondition::Always,
            },
            NOW,
        );
        assert_eq!(store.ttl(&key("temp"), NOW), Reply::Integer(60));
        assert_eq!(
            store.ttl(&key("temp"), NOW + 30 * SECOND),
            Reply::Integer(30)
        );
        // 已过期则视作不存在
        assert_eq!(
            store.ttl(&key("temp"), NOW + 61 * SECOND),
            Reply::Integer(-2)
        );
    }

    #[test]
    fn expire_and_persist_toggle_ttl() {
        let mut store = Store::new();
        store.apply(&set("k", "v"), NOW);

        let expire = WriteOp::ExpireAt {
            key: key("k"),
            at: NOW + 100 * SECOND,
        };
        assert_eq!(store.apply(&expire, NOW), Reply::Integer(1));
        assert_eq!(store.ttl(&key("k"), NOW), Reply::Integer(100));

        assert_eq!(
            store.apply(&WriteOp::Persist { key: key("k") }, NOW),
            Reply::Integer(1)
        );
        assert_eq!(store.ttl(&key("k"), NOW), Reply::Integer(-1));

        // 再次 PERSIST 无实际改动，应返回 0
        assert_eq!(
            store.apply(&WriteOp::Persist { key: key("k") }, NOW),
            Reply::Integer(0)
        );

        // 对不存在的键操作返回 0
        assert_eq!(
            store.apply(&WriteOp::Persist { key: key("nope") }, NOW),
            Reply::Integer(0)
        );
        assert_eq!(
            store.apply(
                &WriteOp::ExpireAt {
                    key: key("nope"),
                    at: NOW + SECOND
                },
                NOW
            ),
            Reply::Integer(0)
        );
    }

    #[test]
    fn expire_in_the_past_deletes_the_key() {
        let mut store = Store::new();
        store.apply(&set("k", "v"), NOW);

        // `EXPIRE key -1` 的等价形式
        let result = store.apply(
            &WriteOp::ExpireAt {
                key: key("k"),
                at: 0,
            },
            NOW,
        );
        assert_eq!(result, Reply::Integer(1), "应报告删除成功");
        assert_eq!(store.get(&key("k"), NOW), Reply::Null, "键应已被删除");
    }

    #[test]
    fn del_counts_only_live_keys() {
        let mut store = Store::new();
        store.apply(&set("a", "1"), NOW);
        store.apply(&set("b", "2"), NOW);
        // c 已过期，删除它不应计入
        store.apply(
            &WriteOp::Set {
                key: key("c"),
                value: Bytes::from_static(b"3"),
                expire_at: Some(NOW + SECOND),
                condition: SetCondition::Always,
            },
            NOW,
        );

        let later = NOW + 2 * SECOND;
        assert_eq!(
            store.apply(
                &WriteOp::Del {
                    keys: vec![key("a"), key("b"), key("c")]
                },
                later
            ),
            Reply::Integer(2),
            "已过期的 c 不应计入删除数"
        );
        assert_eq!(store.dbsize(later), Reply::Integer(0));
    }

    #[test]
    fn exists_counts_duplicates() {
        // 与 Redis 一致：`EXISTS a a` 在 a 存在时返回 2
        let mut store = Store::new();
        store.apply(&set("a", "1"), NOW);

        assert_eq!(
            store.exists(&[key("a"), key("a"), key("nope")], NOW),
            Reply::Integer(2)
        );
    }

    #[test]
    fn mget_preserves_positions_with_nulls() {
        let mut store = Store::new();
        store.apply(&set("a", "1"), NOW);
        store.apply(&set("c", "3"), NOW);

        let reply = store.mget(&[key("a"), key("b"), key("c")], NOW);
        assert_eq!(
            reply,
            Reply::Array(vec![
                Reply::Bulk(Bytes::from_static(b"1")),
                // 缺失的键在结果中占位为 null，不能跳过——否则客户端无法对齐
                Reply::Null,
                Reply::Bulk(Bytes::from_static(b"3")),
            ])
        );
    }

    #[test]
    fn append_concatenates_and_preserves_ttl() {
        let mut store = Store::new();

        // 对不存在的键 APPEND 等价于 SET，返回新长度
        assert_eq!(
            store.apply(
                &WriteOp::Append {
                    key: key("k"),
                    value: Bytes::from_static(b"Hello")
                },
                NOW
            ),
            Reply::Integer(5)
        );
        assert_eq!(
            store.apply(
                &WriteOp::Append {
                    key: key("k"),
                    value: Bytes::from_static(b" World")
                },
                NOW
            ),
            Reply::Integer(11)
        );
        assert_eq!(
            bulk_of(&store.get(&key("k"), NOW)),
            Some(&b"Hello World"[..])
        );

        // APPEND 不应清除 TTL
        store.apply(
            &WriteOp::ExpireAt {
                key: key("k"),
                at: NOW + 50 * SECOND,
            },
            NOW,
        );
        store.apply(
            &WriteOp::Append {
                key: key("k"),
                value: Bytes::from_static(b"!"),
            },
            NOW,
        );
        assert_eq!(
            store.ttl(&key("k"), NOW),
            Reply::Integer(50),
            "APPEND 应保留 TTL"
        );
    }

    #[test]
    fn incr_by_creates_missing_key_and_preserves_ttl() {
        let mut store = Store::new();

        // 不存在的键按 0 起算
        assert_eq!(
            store.apply(
                &WriteOp::IncrBy {
                    key: key("counter"),
                    delta: 1
                },
                NOW
            ),
            Reply::Integer(1)
        );
        assert_eq!(
            store.apply(
                &WriteOp::IncrBy {
                    key: key("counter"),
                    delta: 41
                },
                NOW
            ),
            Reply::Integer(42)
        );
        assert_eq!(
            store.apply(
                &WriteOp::IncrBy {
                    key: key("counter"),
                    delta: -50
                },
                NOW
            ),
            Reply::Integer(-8)
        );

        // INCR 不应清除 TTL
        store.apply(
            &WriteOp::ExpireAt {
                key: key("counter"),
                at: NOW + 30 * SECOND,
            },
            NOW,
        );
        store.apply(
            &WriteOp::IncrBy {
                key: key("counter"),
                delta: 1,
            },
            NOW,
        );
        assert_eq!(store.ttl(&key("counter"), NOW), Reply::Integer(30));
    }

    #[test]
    fn incr_rejects_non_integer_value() {
        let mut store = Store::new();
        store.apply(&set("k", "not-a-number"), NOW);

        let reply = store.apply(
            &WriteOp::IncrBy {
                key: key("k"),
                delta: 1,
            },
            NOW,
        );

        assert!(
            matches!(&reply, Reply::Error(message) if message.contains("not an integer")),
            "应返回 Redis 标准的整数错误，实际为：{reply:?}"
        );
        // 失败不应修改原值
        assert_eq!(
            bulk_of(&store.get(&key("k"), NOW)),
            Some(&b"not-a-number"[..])
        );
    }

    #[test]
    fn incr_detects_overflow() {
        let mut store = Store::new();
        store.apply(&set("k", &i64::MAX.to_string()), NOW);

        let reply = store.apply(
            &WriteOp::IncrBy {
                key: key("k"),
                delta: 1,
            },
            NOW,
        );

        assert!(
            matches!(&reply, Reply::Error(message) if message.contains("overflow")),
            "应报告溢出，实际为：{reply:?}"
        );
    }

    #[test]
    fn mset_overwrites_and_clears_ttl() {
        let mut store = Store::new();
        store.apply(&set("a", "old"), NOW);
        store.apply(
            &WriteOp::ExpireAt {
                key: key("a"),
                at: NOW + SECOND,
            },
            NOW,
        );

        store.apply(
            &WriteOp::MSet {
                pairs: vec![
                    (key("a"), Bytes::from_static(b"new")),
                    (key("b"), Bytes::from_static(b"2")),
                ],
            },
            NOW,
        );

        assert_eq!(bulk_of(&store.get(&key("a"), NOW)), Some(&b"new"[..]));
        // MSET 会清除原有 TTL
        assert_eq!(store.ttl(&key("a"), NOW), Reply::Integer(-1));
        assert_eq!(bulk_of(&store.get(&key("b"), NOW)), Some(&b"2"[..]));
    }

    #[test]
    fn keys_matches_glob_pattern() {
        let mut store = Store::new();
        for name in ["user:1", "user:2", "session:1", "other"] {
            store.apply(&set(name, "v"), NOW);
        }

        let reply = store.keys(&Bytes::from_static(b"user:*"), NOW);
        assert_eq!(
            reply,
            Reply::Array(vec![
                Reply::Bulk(Bytes::from_static(b"user:1")),
                Reply::Bulk(Bytes::from_static(b"user:2")),
            ]),
            "结果应被排序以保证可预测"
        );

        // 已过期的键不应出现在结果里
        store.apply(
            &WriteOp::Set {
                key: key("user:3"),
                value: Bytes::from_static(b"v"),
                expire_at: Some(NOW + SECOND),
                condition: SetCondition::Always,
            },
            NOW,
        );
        assert_eq!(
            store.keys(&Bytes::from_static(b"user:*"), NOW + 2 * SECOND),
            Reply::Array(vec![
                Reply::Bulk(Bytes::from_static(b"user:1")),
                Reply::Bulk(Bytes::from_static(b"user:2")),
            ])
        );
    }

    #[test]
    fn scan_iterates_all_keys_then_terminates() {
        let mut store = Store::new();
        for index in 0..25 {
            store.apply(&set(&format!("k{index:02}"), "v"), NOW);
        }

        let mut collected: Vec<Bytes> = Vec::new();
        let mut cursor = 0u64;
        let mut rounds = 0;

        loop {
            rounds += 1;
            assert!(rounds < 100, "游标未能收敛，可能是死循环");

            let Reply::Array(outer) = store.scan(cursor, None, Some(10), NOW) else {
                panic!("SCAN 应返回两元素数组");
            };
            let Reply::Bulk(next) = &outer[0] else {
                panic!("第一个元素应是游标");
            };
            let Reply::Array(page) = &outer[1] else {
                panic!("第二个元素应是键列表");
            };

            for item in page {
                if let Reply::Bulk(data) = item {
                    collected.push(data.clone());
                }
            }

            cursor = std::str::from_utf8(next)
                .expect("游标应是 ASCII")
                .parse()
                .expect("游标应是数字");
            if cursor == 0 {
                break;
            }
        }

        assert_eq!(collected.len(), 25, "一次完整遍历应取到全部键");
    }

    #[test]
    fn scan_honors_pattern() {
        let mut store = Store::new();
        for name in ["user:1", "user:2", "other"] {
            store.apply(&set(name, "v"), NOW);
        }

        let Reply::Array(outer) =
            store.scan(0, Some(&Bytes::from_static(b"user:*")), Some(100), NOW)
        else {
            panic!("SCAN 应返回两元素数组");
        };
        let Reply::Array(page) = &outer[1] else {
            panic!("第二个元素应是键列表");
        };

        assert_eq!(page.len(), 2);
    }

    #[test]
    fn flushdb_removes_everything() {
        let mut store = Store::new();
        store.apply(&set("a", "1"), NOW);
        store.apply(&set("b", "2"), NOW);
        assert_eq!(store.dbsize(NOW), Reply::Integer(2));

        assert_eq!(store.apply(&WriteOp::FlushDb, NOW), Reply::ok());
        assert_eq!(store.dbsize(NOW), Reply::Integer(0));
        assert!(store.is_empty());
    }

    #[test]
    fn apply_is_deterministic_for_the_same_input_sequence() {
        // 这是整个设计的目标：同一串操作 + 同一串时刻 = 完全相同的结果。
        // 单机测试无法覆盖多副本，但能锁定「状态机不读系统时钟」这一点。
        let script: Vec<WriteOp> = vec![
            set("a", "1"),
            set("b", "2"),
            WriteOp::IncrBy {
                key: key("a"),
                delta: 10,
            },
            WriteOp::ExpireAt {
                key: key("b"),
                at: NOW + 100 * SECOND,
            },
            WriteOp::Append {
                key: key("a"),
                value: Bytes::from_static(b"!"),
            },
        ];

        let run = || {
            let mut store = Store::new();
            let mut replies = Vec::new();
            for op in &script {
                replies.push(store.apply(op, NOW));
            }
            (
                replies,
                store.get(&key("a"), NOW),
                store.ttl(&key("b"), NOW),
                store.dbsize(NOW),
            )
        };

        assert_eq!(run(), run(), "相同输入必须产生相同输出");
    }
}
