//! Raft 状态机与快照。
//!
//! # 一个决定正确性的设计选择
//!
//! openraft 会在启动时调用 [`applied_state`](RaftStateMachine::applied_state) 来了解
//! 「状态机已经处理到哪一条日志」，然后从那里开始重放。这意味着一条硬性约束：
//!
//! > **状态机的内存内容，必须与它报告的 `applied_state` 严格对应。**
//!
//! 反例很直观：假设我们把「已应用到第 100 条」持久化了，但状态机内容只在内存里。
//! 进程崩溃重启后，内存是空的，我们却报告「已应用到第 100 条」——openraft 于是从
//! 第 101 条开始重放，前 100 条的效果全部丢失。
//!
//! 反过来，如果报告得太保守（比如总说「还没开始」），openraft 会重放已经应用过的
//! 条目——`INCR` 这类**非幂等**操作会被执行两次，数据同样出错。
//!
//! # 因此采用的方案
//!
//! **状态机内容只随快照持久化，且 `applied_state` 与快照严格同步**：
//!
//! - 运行期间：内容在内存，`applied_state` 在内存，两者一起前进
//! - 启动时：从 redb 载入最近一次快照，`applied_state` 即该快照的日志位置
//! - openraft 随后从快照位置之后开始重放日志，重新构建内存状态
//!
//! 这里的要点是：我们**从不**单独持久化 `applied_state`。它要么与快照一起落盘，
//! 要么就只活在内存里。两者永不脱节，上面那个反例也就不可能发生。

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use openraft::impls::BasicNode;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use redb::{Database, Durability, ReadableDatabase, TableDefinition};

use super::{NodeId, TypeConfig, decode, encode};
use crate::kv::Store;
use crate::protocol::Reply;

/// 快照表：`"meta"` → 序列化的 `SnapshotMeta`，`"data"` → 序列化的状态机内容。
///
/// 只保留最近一次快照——openraft 也只需要最近的那一个。
const SNAPSHOT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_snapshot");

/// 快照元数据的键。
const KEY_SNAPSHOT_META: &str = "meta";
/// 快照内容的键。
const KEY_SNAPSHOT_DATA: &str = "data";

/// 状态机的内部状态。用 `Mutex` 包起来是为了让快照构建器能安全地共享它。
struct Inner {
    /// 键值状态机。语义全部来自 [`crate::kv`]，这里只负责驱动它。
    store: Store,
    /// 已应用到哪一条日志。
    last_applied: Option<LogId<NodeId>>,
    /// 最近一次成员变更。
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// Raft 状态机。
#[derive(Clone)]
pub struct StateMachine {
    inner: Arc<Mutex<Inner>>,
    db: Arc<Database>,
}

/// 快照构建器。
pub struct SnapshotBuilder {
    inner: Arc<Mutex<Inner>>,
    db: Arc<Database>,
}

impl StateMachine {
    /// 在给定的 redb 数据库上创建状态机。
    ///
    /// 会尝试载入最近一次快照——**这正是重启后状态得以恢复的入口**。
    pub fn new(db: Arc<Database>) -> Result<Self, StorageError<NodeId>> {
        // 先确保表存在，让后续读取路径不必处理「表不存在」
        let write_txn = db.begin_write().map_err(write_error)?;
        {
            write_txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
        }
        write_txn.commit().map_err(write_error)?;

        let (store, last_applied, last_membership) = load_snapshot(&db)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                store,
                last_applied,
                last_membership,
            })),
            db,
        })
    }

    /// 取内部状态的锁。
    ///
    /// 锁中毒只可能发生在持锁线程 panic 时。状态机的操作不会 panic，
    /// 因此这里更可能是无关的 panic 波及——恢复数据继续服务比让数据库倒下更合适。
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 当前记录数（含已过期但尚未回收的），用于观测。
    pub fn len(&self) -> usize {
        self.lock().store.len()
    }

    /// 状态机是否为空（含已过期但尚未回收的记录）。
    pub fn is_empty(&self) -> bool {
        self.lock().store.is_empty()
    }
}

/// 从快照恢复出来的状态：`(状态机, 已应用位置, 成员配置)`。
type LoadedSnapshot = (
    Store,
    Option<LogId<NodeId>>,
    StoredMembership<NodeId, BasicNode>,
);

/// 从 redb 载入最近一次快照。
///
/// 返回 `(状态机, 已应用位置, 成员配置)`。没有快照时返回一个空状态机。
fn load_snapshot(db: &Database) -> Result<LoadedSnapshot, StorageError<NodeId>> {
    let read_txn = db.begin_read().map_err(read_error)?;
    let table = read_txn
        .open_table(SNAPSHOT_TABLE)
        .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?;

    let Some(meta_raw) = table
        .get(KEY_SNAPSHOT_META)
        .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?
    else {
        // 没有快照：全新的状态机，什么都没应用过
        return Ok((Store::new(), None, StoredMembership::default()));
    };

    let meta: SnapshotMeta<NodeId, BasicNode> = decode(meta_raw.value())?;

    let data_raw = table
        .get(KEY_SNAPSHOT_DATA)
        .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?
        .ok_or_else(|| {
            // 元数据在但内容不在——快照被写坏了一半。这是不可恢复的，
            // 必须报错而不是假装状态机是空的（那会导致静默丢数据）
            io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "快照元数据存在但内容缺失，存储已损坏",
                ),
            )
        })?;

    let entries = decode::<Vec<crate::kv::SnapshotEntry>>(data_raw.value())?;
    let mut store = Store::new();
    store.restore(entries);

    Ok((store, meta.last_log_id, meta.last_membership))
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (entries, last_applied, last_membership) = {
            let inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                // 导出**全部**记录（含已过期的），让快照成为「已应用日志」的纯函数，
                // 不依赖生成快照那一刻的时钟。已过期的记录在读取路径上依然不可见。
                inner.store.export(),
                inner.last_applied,
                inner.last_membership.clone(),
            )
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id: format!(
                "{}-{}",
                last_applied.map(|id| id.leader_id.term).unwrap_or_default(),
                last_applied.map(|id| id.index).unwrap_or_default()
            ),
        };

        let data = encode(&entries)?;

        // 快照必须落盘：它是重启后恢复状态的唯一依据
        let mut write_txn = self.db.begin_write().map_err(write_error)?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
        {
            let mut table = write_txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
            let meta_raw = encode(&meta)?;
            table
                .insert(KEY_SNAPSHOT_META, meta_raw.as_slice())
                .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
            table
                .insert(KEY_SNAPSHOT_DATA, data.as_slice())
                .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
        }
        write_txn.commit().map_err(write_error)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Reply>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // 时间在这里被读一次，然后作为参数传给状态机——状态机自己不读时钟。
        // 这与单机模式下的调用方式完全一致，状态机因此无需任何改动就知道
        // 自己是在被 Raft 驱动。
        let now_ms = crate::kv::now_ms();

        let mut inner = self.lock();
        let mut replies = Vec::new();

        for entry in entries {
            // 无论条目是什么类型，都必须推进 applied_state——它记录的是
            // 「处理到哪一条」，而不是「处理了几条写入」
            inner.last_applied = Some(entry.log_id);

            let reply = match entry.payload {
                EntryPayload::Blank => {
                    // 心跳或领导权确认产生的空条目，没有客户端在等响应
                    Reply::Null
                }
                EntryPayload::Normal(op) => inner.store.apply(&op, now_ms),
                EntryPayload::Membership(membership) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    // 成员变更是内部操作，同样没有客户端在等响应
                    Reply::Null
                }
            };

            replies.push(reply);
        }

        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            inner: Arc::clone(&self.inner),
            db: Arc::clone(&self.db),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        // 返回一块空缓冲，openraft 会把收到的快照字节写进去
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let entries = decode::<Vec<crate::kv::SnapshotEntry>>(&data)?;

        // 先把快照写入 redb，再更新内存。顺序很重要：
        // 若中途崩溃，内存状态不会被更新，重启后会重新载入旧快照并重放日志，
        // 结果仍然一致。反过来先更新内存再落盘，崩溃就会造成两者脱节。
        let mut write_txn = self.db.begin_write().map_err(write_error)?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
        {
            let mut table = write_txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
            let meta_raw = encode(meta)?;
            table
                .insert(KEY_SNAPSHOT_META, meta_raw.as_slice())
                .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
            table
                .insert(KEY_SNAPSHOT_DATA, data.as_slice())
                .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
        }
        write_txn.commit().map_err(write_error)?;

        // 落盘成功后才替换内存状态
        let mut inner = self.lock();
        inner.store = Store::new();
        inner.store.restore(entries);
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let read_txn = self.db.begin_read().map_err(read_error)?;
        let table = read_txn
            .open_table(SNAPSHOT_TABLE)
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?;

        let Some(meta_raw) = table
            .get(KEY_SNAPSHOT_META)
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?
        else {
            return Ok(None);
        };
        let Some(data_raw) = table
            .get(KEY_SNAPSHOT_DATA)
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?
        else {
            return Ok(None);
        };

        let meta: SnapshotMeta<NodeId, BasicNode> = decode(meta_raw.value())?;

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data_raw.value().to_vec())),
        }))
    }
}

// ============================================================== 错误转换辅助

/// 读事务开启失败。
///
/// 写成泛型是因为 redb 在不同环节返回不同的错误类型（`TransactionError`、
/// `CommitError`、`TableError`…），而这里只关心「哪一类操作失败」。
fn read_error<E>(error: E) -> StorageError<NodeId>
where
    E: std::error::Error + 'static,
{
    io_error(ErrorSubject::Store, ErrorVerb::Read, error)
}

/// 写事务开启或提交失败。
fn write_error<E>(error: E) -> StorageError<NodeId>
where
    E: std::error::Error + 'static,
{
    io_error(ErrorSubject::Store, ErrorVerb::Write, error)
}

/// 把 redb 的错误包装成 openraft 的存储 IO 错误。
///
/// 同 `log_store` 中的同名函数：`AnyError` 只有 `From<&E>`，必须显式取引用。
fn io_error<E>(subject: ErrorSubject<NodeId>, verb: ErrorVerb, source: E) -> StorageError<NodeId>
where
    E: std::error::Error + 'static,
{
    StorageError::from(StorageIOError::new(
        subject,
        verb,
        openraft::AnyError::new(&source),
    ))
}
