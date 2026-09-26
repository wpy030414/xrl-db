//! Raft 共识层。
//!
//! 基于 openraft 实现（选型依据见 `docs/DECISIONS.md` 的 ADR-004）。
//!
//! # 我们只需要实现五件事
//!
//! openraft 把选举、心跳、日志复制、快照传输的状态机全部收进了库里。留给使用者的
//! 接口面只有：
//!
//! | 接口 | 职责 | 位置 |
//! |---|---|---|
//! | `RaftLogStorage` | 日志与投票的持久化 | [`log_store`] |
//! | `RaftLogReader` | 日志读取 | [`log_store`] |
//! | `RaftStateMachine` | 把已提交的日志应用到键值状态机 | [`state_machine`] |
//! | `RaftSnapshotBuilder` | 生成快照 | [`state_machine`] |
//! | `RaftNetwork` / `RaftNetworkFactory` | 节点间 RPC | [`network`] |
//!
//! # 正确性从哪来
//!
//! Raft 最危险的失败模式不是「跑不起来」，而是「跑得起来但偶发静默丢数据」——这类
//! 问题在单元测试里几乎不可见。
//!
//! 因此本层的正确性**不靠我们自己判断**，而是靠 openraft 自带的协议一致性测试套件
//! `openraft::testing::Suite::test_all`：跑通即证明存储实现满足 Raft 协议对存储的
//! 全部要求。这是选择 openraft 而非手写 Raft 的核心理由。

pub mod log_store;
pub mod network;
pub mod rpc;
pub mod state_machine;
pub mod types;

pub use types::TypeConfig;

use openraft::{ErrorSubject, ErrorVerb, StorageError, StorageIOError};

/// 本项目的节点标识类型。
///
/// 单独起个别名是因为它在 openraft 的每个接口里都要出现，写全称会让签名难以阅读。
pub type NodeId = u64;

/// 把值序列化为字节。
///
/// 用 postcard 而非 JSON：日志条目里装的是键值，而 `Bytes` 在 JSON 中会退化成
/// 「每个字节一个数字」的数组，体积会膨胀数倍。
pub(crate) fn encode<T>(value: &T) -> Result<Vec<u8>, StorageError<NodeId>>
where
    T: serde::Serialize,
{
    postcard::to_allocvec(value).map_err(|error| {
        StorageError::from(StorageIOError::new(
            ErrorSubject::Store,
            ErrorVerb::Write,
            openraft::AnyError::new(&error),
        ))
    })
}

/// 从字节反序列化。
pub(crate) fn decode<T>(bytes: &[u8]) -> Result<T, StorageError<NodeId>>
where
    T: serde::de::DeserializeOwned,
{
    postcard::from_bytes(bytes).map_err(|error| {
        StorageError::from(StorageIOError::new(
            ErrorSubject::Store,
            ErrorVerb::Read,
            openraft::AnyError::new(&error),
        ))
    })
}
