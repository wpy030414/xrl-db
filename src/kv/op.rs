//! 可复制的写操作。
//!
//! # 为什么不能直接复制 `Command`
//!
//! Raft 要求状态机的 apply **必须是确定性的**：同一份日志条目在各个副本上重放，
//! 必须得到完全相同的结果。
//!
//! 而 `Command::Set { expire: Some(Seconds(60)) }` 这种**相对时间**破坏了确定性——
//! 如果两个副本在重放时各自去读当前时钟，算出的过期时刻就会不同，副本之间于是产生分歧。
//! 这是分布式状态机最常见也最隐蔽的一类 bug：它不会立刻暴露，只会在某次快照或重启后
//! 表现为「两个副本的数据不一样」。
//!
//! 解决办法是：**由 leader 在把命令写入日志之前，就把相对时间解析为绝对时刻**。
//! 日志里存的是绝对时刻，所有副本重放时读到的都是同一个值，结果自然一致。
//!
//! 因此有了 [`WriteOp`]——它是 `Command` 中「写操作」部分的**确定性表示**。
//! 状态机的所有写入口都只接受 `WriteOp`，不接受 `Command`，从类型层面杜绝了
//! 误用相对时间的可能。

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::protocol::{Command, CommandError, SetCondition};

/// 绝对时刻，单位毫秒（Unix 纪元）。
pub type TimestampMs = u64;

/// 一条可复制的写操作。
///
/// 与 [`Command`] 的关键区别：**所有时间都是绝对时刻**，不含任何需要读取时钟才能
/// 解释的相对量。
///
/// 需要序列化是因为它会被写入 Raft 日志——**这正是「相对时间必须提前解析」的原因**：
/// 日志一旦写下就不再改变，各副本读到的必定是同一个绝对时刻。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteOp {
    /// 写入一个键。
    ///
    /// `condition` 为 `IfAbsent`（NX）或 `IfPresent`（XX）时需要先检查键是否存在，
    /// 这个检查发生在 apply 时，因此结果只取决于当时的状态，仍然确定。
    Set {
        key: Bytes,
        value: Bytes,
        /// 绝对过期时刻；`None` 表示永不过期
        expire_at: Option<TimestampMs>,
        condition: SetCondition,
    },
    /// 删除若干键。
    Del { keys: Vec<Bytes> },
    /// 批量写入。
    MSet { pairs: Vec<(Bytes, Bytes)> },
    /// 追加到键的现有值之后。
    Append { key: Bytes, value: Bytes },
    /// 对键的整数值做增量。`Incr` / `Decr` / `IncrBy` / `DecrBy` 都归一到这一种。
    IncrBy { key: Bytes, delta: i64 },
    /// 把键的过期时刻设为绝对时刻。
    ExpireAt { key: Bytes, at: TimestampMs },
    /// 移除键的过期时间，使其永不过期。
    Persist { key: Bytes },
    /// 清空整个数据库。
    FlushDb,
}

impl WriteOp {
    /// 把一条客户端命令转换为可复制的写操作。
    ///
    /// **相对时间在此处解析为绝对时刻**——这是整个确定性保证的关键一步，
    /// 必须在命令进入 Raft 日志之前完成。
    ///
    /// # 返回值
    ///
    /// - `Ok(Some(op))` — 这是一条写命令
    /// - `Ok(None)` — 这是一条读命令，不产生日志条目
    /// - `Err(e)` — 写命令本身不合法（如过期时间为负）
    pub fn from_command(
        command: &Command,
        now_ms: TimestampMs,
    ) -> Result<Option<Self>, CommandError> {
        let op = match command {
            Command::Set {
                key,
                value,
                expire,
                condition,
            } => WriteOp::Set {
                key: key.clone(),
                value: value.clone(),
                expire_at: resolve_expire(expire.as_ref(), now_ms)?,
                condition: *condition,
            },

            Command::Del { keys } => WriteOp::Del { keys: keys.clone() },

            Command::MSet { pairs } => WriteOp::MSet {
                pairs: pairs.clone(),
            },

            Command::Append { key, value } => WriteOp::Append {
                key: key.clone(),
                value: value.clone(),
            },

            Command::Incr { key } => WriteOp::IncrBy {
                key: key.clone(),
                delta: 1,
            },
            Command::Decr { key } => WriteOp::IncrBy {
                key: key.clone(),
                delta: -1,
            },
            Command::IncrBy { key, delta } => WriteOp::IncrBy {
                key: key.clone(),
                delta: *delta,
            },
            // DECRBY 的语义是「减去」，因此取反
            Command::DecrBy { key, delta } => WriteOp::IncrBy {
                key: key.clone(),
                delta: delta
                    .checked_neg()
                    .ok_or_else(|| CommandError::InvalidArgument {
                        name: "decrby".to_string(),
                        detail: "decrement would overflow".to_string(),
                    })?,
            },

            Command::Expire { key, seconds } => {
                // Redis 对非正数秒数的处理等价于「立即过期」，即删除该键。
                // 这里解析为纪元时刻 0，由状态机在 apply 时删除它。
                let at = if *seconds <= 0 {
                    0
                } else {
                    now_ms.saturating_add((*seconds as u64).saturating_mul(1000))
                };
                WriteOp::ExpireAt {
                    key: key.clone(),
                    at,
                }
            }

            Command::Persist { key } => WriteOp::Persist { key: key.clone() },

            Command::FlushDb => WriteOp::FlushDb,

            // 其余都是读命令或连接/管理命令，不产生日志条目
            _ => return Ok(None),
        };

        Ok(Some(op))
    }
}

/// 把 `SET` 的过期时间设定解析为绝对时刻。
///
/// 相对量（`EX` / `PX`）加上当前时刻，绝对量（`EXAT` / `PXAT`）直接换算单位。
fn resolve_expire(
    expire: Option<&crate::protocol::SetExpire>,
    now_ms: TimestampMs,
) -> Result<Option<TimestampMs>, CommandError> {
    use crate::protocol::SetExpire;

    let invalid = |detail: &str| CommandError::InvalidArgument {
        name: "set".to_string(),
        detail: detail.to_string(),
    };

    let at = match expire {
        None => return Ok(None),

        Some(SetExpire::Seconds(seconds)) => {
            if *seconds <= 0 {
                return Err(invalid("invalid expire time, must be positive"));
            }
            now_ms.saturating_add((*seconds as u64).saturating_mul(1000))
        }
        Some(SetExpire::Milliseconds(millis)) => {
            if *millis <= 0 {
                return Err(invalid("invalid expire time, must be positive"));
            }
            now_ms.saturating_add(*millis as u64)
        }
        Some(SetExpire::SecondsAt(timestamp)) => {
            if *timestamp <= 0 {
                return Err(invalid("invalid expire time, must be positive"));
            }
            (*timestamp as u64).saturating_mul(1000)
        }
        Some(SetExpire::MillisecondsAt(timestamp)) => {
            if *timestamp <= 0 {
                return Err(invalid("invalid expire time, must be positive"));
            }
            *timestamp as u64
        }
    };

    Ok(Some(at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Command, SetExpire};

    const NOW: TimestampMs = 1_700_000_000_000;

    fn set_with(expire: Option<SetExpire>) -> Command {
        Command::Set {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            expire,
            condition: SetCondition::Always,
        }
    }

    /// 取出写操作里的过期时刻，便于断言
    fn expire_of(op: WriteOp) -> Option<TimestampMs> {
        match op {
            WriteOp::Set { expire_at, .. } => expire_at,
            other => panic!("期望 Set，实际为 {other:?}"),
        }
    }

    #[test]
    fn set_without_expire_is_persistent() {
        let op = WriteOp::from_command(&set_with(None), NOW)
            .expect("应能转换")
            .expect("应是写操作");

        assert_eq!(expire_of(op), None);
    }

    #[test]
    fn relative_expiry_is_resolved_against_now() {
        // 这是确定性保证的核心：相对时间必须在进入日志前变成绝对时刻，
        // 否则各副本重放时会因时钟不同而算出不同的过期时刻。
        let op = WriteOp::from_command(&set_with(Some(SetExpire::Seconds(60))), NOW)
            .expect("应能转换")
            .expect("应是写操作");
        assert_eq!(expire_of(op), Some(NOW + 60_000));

        let op = WriteOp::from_command(&set_with(Some(SetExpire::Milliseconds(1500))), NOW)
            .expect("应能转换")
            .expect("应是写操作");
        assert_eq!(expire_of(op), Some(NOW + 1_500));
    }

    #[test]
    fn absolute_expiry_ignores_now() {
        // EXAT / PXAT 本就是绝对时刻，不应受当前时间影响
        let target_seconds = 1_800_000_000i64;

        let op = WriteOp::from_command(&set_with(Some(SetExpire::SecondsAt(target_seconds))), NOW)
            .expect("应能转换")
            .expect("应是写操作");
        assert_eq!(expire_of(op), Some(target_seconds as u64 * 1000));

        let op = WriteOp::from_command(
            &set_with(Some(SetExpire::MillisecondsAt(target_seconds * 1000))),
            NOW,
        )
        .expect("应能转换")
        .expect("应是写操作");
        assert_eq!(expire_of(op), Some(target_seconds as u64 * 1000));
    }

    #[test]
    fn same_command_at_different_times_yields_different_absolute_expiry() {
        // 反过来确认：相对时间的解析**确实**依赖当前时刻。
        // 正因为如此，这个解析必须发生在 leader 上、且只发生一次。
        let first = WriteOp::from_command(&set_with(Some(SetExpire::Seconds(10))), NOW)
            .expect("应能转换")
            .expect("应是写操作");
        let second = WriteOp::from_command(&set_with(Some(SetExpire::Seconds(10))), NOW + 5000)
            .expect("应能转换")
            .expect("应是写操作");

        assert_ne!(expire_of(first), expire_of(second));
    }

    #[test]
    fn rejects_non_positive_expiry() {
        for bad in [
            SetExpire::Seconds(0),
            SetExpire::Seconds(-1),
            SetExpire::Milliseconds(0),
            SetExpire::SecondsAt(0),
            SetExpire::MillisecondsAt(-5),
        ] {
            let err = WriteOp::from_command(&set_with(Some(bad)), NOW)
                .expect_err("非正数过期时间应被拒绝");
            assert!(
                err.to_string().contains("invalid expire time"),
                "错误信息应说明过期时间非法，实际为：{err}"
            );
        }
    }

    #[test]
    fn read_commands_produce_no_write_op() {
        // 读命令不应产生日志条目
        let reads = [
            Command::Get {
                key: Bytes::from_static(b"k"),
            },
            Command::Exists {
                keys: vec![Bytes::from_static(b"k")],
            },
            Command::Ttl {
                key: Bytes::from_static(b"k"),
            },
            Command::Dbsize,
            Command::Ping { message: None },
            Command::Info { section: None },
        ];

        for command in reads {
            assert_eq!(
                WriteOp::from_command(&command, NOW).expect("不应报错"),
                None,
                "{command:?} 不应产生写操作"
            );
        }
    }

    #[test]
    fn arithmetic_commands_normalize_to_incr_by() {
        let key = Bytes::from_static(b"counter");

        assert_eq!(
            WriteOp::from_command(&Command::Incr { key: key.clone() }, NOW).expect("应能转换"),
            Some(WriteOp::IncrBy {
                key: key.clone(),
                delta: 1
            })
        );
        assert_eq!(
            WriteOp::from_command(&Command::Decr { key: key.clone() }, NOW).expect("应能转换"),
            Some(WriteOp::IncrBy {
                key: key.clone(),
                delta: -1
            })
        );
        // DECRBY 的语义是减去 delta，因此取反
        assert_eq!(
            WriteOp::from_command(
                &Command::DecrBy {
                    key: key.clone(),
                    delta: 7
                },
                NOW
            )
            .expect("应能转换"),
            Some(WriteOp::IncrBy { key, delta: -7 })
        );
    }

    #[test]
    fn expire_with_non_positive_seconds_means_immediate_deletion() {
        // Redis 的语义：`EXPIRE key -1` 等价于删除该键
        let op = WriteOp::from_command(
            &Command::Expire {
                key: Bytes::from_static(b"k"),
                seconds: -1,
            },
            NOW,
        )
        .expect("应能转换")
        .expect("应是写操作");

        assert_eq!(
            op,
            WriteOp::ExpireAt {
                key: Bytes::from_static(b"k"),
                at: 0
            }
        );
    }

    #[test]
    fn huge_expiry_saturates_instead_of_overflowing() {
        // 溢出会引发 panic（debug）或产生错误结果（release），必须饱和处理
        let op = WriteOp::from_command(&set_with(Some(SetExpire::Seconds(i64::MAX))), NOW)
            .expect("应能转换")
            .expect("应是写操作");

        assert_eq!(expire_of(op), Some(u64::MAX));
    }
}
