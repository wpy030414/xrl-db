//! Raft 类型配置。
//!
//! [`declare_raft_types!`](openraft::declare_raft_types) 一次性声明共识层用到的全部类型。
//! 这里最关键的是前两个，其余沿用 openraft 的默认值（`NodeId = u64`、`Node = BasicNode`、
//! `SnapshotData = Cursor<Vec<u8>>`、运行时为 tokio 等）。
//!
//! # `D` 为什么是 `WriteOp` 而不是 `Command`
//!
//! `D` 是**进入 Raft 日志的应用数据**。日志一旦写下就不再改变，各副本读到的必须是
//! 同一个值。
//!
//! `Command` 含相对时间（如 `SET k v EX 60` 的 `EX 60`），若把它写进日志，各副本在
//! 重放时会各自读时钟、算出不同的过期时刻，于是产生分歧。用 `WriteOp` 从类型层面
//! 消除了这种可能——它里面根本没有相对时间这种形态。
//!
//! 相对时间到绝对时刻的转换发生在 [`crate::backend`]，也就是「把命令交给共识层之前」
//! 的那一步。

use std::io::Cursor;

use crate::kv::WriteOp;
use crate::protocol::Reply;

openraft::declare_raft_types!(
    /// 本项目的 Raft 类型配置。
    pub TypeConfig:
        /// 写入日志的应用数据。**不含相对时间**。
        D = WriteOp,
        /// 状态机 apply 之后返回给客户端的响应。
        R = Reply,
);
