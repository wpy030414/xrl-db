//! Raft 日志与投票的持久化（基于 redb）。
//!
//! # 持久性是「不丢数据」的最后一道防线
//!
//! Raft 的正确性建立在一条前提上：**一个节点在向客户端确认写入之后，即使立刻断电，
//! 重启后也必须还记得这条写入**。因此本模块的每一次写事务都使用
//! [`Durability::Immediate`]，确保 `commit()` 返回时数据已 fsync 到稳定存储。
//!
//! 这是整个项目里唯一不能为了性能而让步的地方。
//!
//! # 关于日志下标
//!
//! redb 的 `Key for u64` **不比较原始字节**，而是重写 `compare()` 解码后做数值比较。
//! 因此虽然内部存储是小端序，`range()` 仍然按数值顺序返回——这正是 Raft 所要求的
//! 「日志必须连续且有序」。

use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, OptionalSend, StorageError, StorageIOError, Vote,
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

use super::{NodeId, TypeConfig, decode, encode};

/// 日志表：日志下标 → 序列化后的日志条目。
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");

/// 元数据表：固定键 → 序列化值。存放投票、已提交位置、已清除位置。
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

/// 元数据键：最后一次投票。
const KEY_VOTE: &str = "vote";
/// 元数据键：最后一次已提交的日志 ID。
const KEY_COMMITTED: &str = "committed";
/// 元数据键：最后一次被清除的日志 ID。
const KEY_LAST_PURGED: &str = "last_purged";

/// Raft 日志存储。
///
/// 内部只是一个 `Arc<Database>`——redb 自己负责并发控制与持久化，我们不需要再加锁。
#[derive(Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    /// 在给定的 redb 数据库上创建日志存储。
    ///
    /// redb 的表是「首次写入时创建」的，读一个不存在的表会报错。因此这里先做一次
    /// 空写事务把两张表建出来，后续所有读取路径就不必再处理「表不存在」这个分支。
    pub fn new(db: Arc<Database>) -> Result<Self, StorageError<NodeId>> {
        let write_txn = db.begin_write().map_err(write_error)?;
        {
            write_txn
                .open_table(LOG_TABLE)
                .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
            write_txn
                .open_table(META_TABLE)
                .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        }
        write_txn.commit().map_err(write_error)?;

        Ok(Self { db })
    }

    /// 读取一个元数据项。
    fn read_meta<T>(&self, key: &str) -> Result<Option<T>, StorageError<NodeId>>
    where
        T: serde::de::DeserializeOwned,
    {
        let read_txn = self.db.begin_read().map_err(read_error)?;
        let table = read_txn
            .open_table(META_TABLE)
            .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Read, error))?;

        match table
            .get(key)
            .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Read, error))?
        {
            Some(value) => Ok(Some(decode(value.value())?)),
            None => Ok(None),
        }
    }

    /// 写入一个元数据项。
    fn write_meta<T>(&self, key: &str, value: Option<&T>) -> Result<(), StorageError<NodeId>>
    where
        T: serde::Serialize,
    {
        let mut write_txn = self.db.begin_write().map_err(write_error)?;
        // 元数据（投票、已提交位置）决定了节点重启后的行为，必须真正落盘
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Write, error))?;

        {
            let mut table = write_txn
                .open_table(META_TABLE)
                .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Write, error))?;

            match value {
                Some(value) => {
                    let encoded = encode(value)?;
                    table
                        .insert(key, encoded.as_slice())
                        .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
                }
                // `None` 表示清除该项
                None => {
                    table
                        .remove(key)
                        .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Delete, error))?;
                }
            }
        }

        write_txn.commit().map_err(write_error)
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let read_txn = self.db.begin_read().map_err(read_error)?;
        let table = read_txn
            .open_table(LOG_TABLE)
            .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;

        let mut entries = Vec::new();
        let iter = table
            .range(range)
            .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;

        for item in iter {
            let (_, value) =
                item.map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
            entries.push(decode::<Entry<TypeConfig>>(value.value())?);
        }

        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged: Option<LogId<NodeId>> = self.read_meta(KEY_LAST_PURGED)?;

        let read_txn = self.db.begin_read().map_err(read_error)?;
        let table = read_txn
            .open_table(LOG_TABLE)
            .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;

        // 最后一条日志。若日志已被全部清除，则退回到「最后一次清除的位置」——
        // 这正是 Raft 判断「本节点日志进度」的依据。
        let last_log_id = match table
            .last()
            .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Read, error))?
        {
            Some((_, value)) => Some(decode::<Entry<TypeConfig>>(value.value())?.log_id),
            // `LogId` 实现了 `Copy`，不需要也不应该在这里克隆
            None => last_purged,
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.write_meta(KEY_VOTE, Some(vote))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.read_meta(KEY_VOTE)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.write_meta(KEY_COMMITTED, committed.as_ref())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        self.read_meta(KEY_COMMITTED)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut write_txn = self.db.begin_write().map_err(write_error)?;

        // 这一行是「不丢数据」的物理落点：commit 返回前数据必须已经 fsync 完成
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;

        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;

            for entry in entries {
                let encoded = encode(&entry)?;
                table
                    .insert(entry.log_id.index, encoded.as_slice())
                    .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
            }
        }

        match write_txn.commit() {
            Ok(()) => {
                // 只有在确实落盘之后才通知 openraft 这批日志已持久化。
                // 顺序反了就等于承诺了一件没做到的事。
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
                Err(write_error(error))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // openraft 的语义是 **inclusive**：删除 from `log_id.index` **起**（含）的全部日志。
        // 差一处理错误会造成日志出现空洞，而 Raft 只看最后一条日志 ID 就认为日志连续，
        // 于是静默地产生不一致。
        let mut write_txn = self.db.begin_write().map_err(write_error)?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;

        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
            table
                .retain_in(log_id.index.., |_index: u64, _value: &[u8]| false)
                .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
        }

        write_txn.commit().map_err(write_error)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // 同样是 inclusive：删除 **到** `log_id.index` 为止（含）的全部日志，
        // 并把清除位置记下来——重启后要靠它判断「本节点最少拥有哪些日志」。
        let mut write_txn = self.db.begin_write().map_err(write_error)?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;

        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
            table
                .retain_in(..=log_id.index, |_index: u64, _value: &[u8]| false)
                .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;

            let mut meta = write_txn
                .open_table(META_TABLE)
                .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
            let encoded = encode(&log_id)?;
            meta.insert(KEY_LAST_PURGED, encoded.as_slice())
                .map_err(|error| io_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        }

        write_txn.commit().map_err(write_error)
    }
}

// ============================================================== 错误转换辅助
//
// openraft 的错误类型是它自己的，这里把 redb 的错误统一转过去。
// 分开写而不是用 `?` + From，是因为 openraft 需要知道**出错的是哪一类操作**，
// 才能在日志里给出有意义的诊断。

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
/// 注意 `AnyError` 只提供 `From<&E>` 而没有 `From<E>`，因此这里显式用
/// [`AnyError::new`](openraft::AnyError::new) 取引用构造，而不是依赖 `Into`——
/// 后者对 redb 的错误类型并不成立。
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
