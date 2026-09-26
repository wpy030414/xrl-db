//! XRLDB —— 兼容 Redis RESP3 协议、基于 Raft 提供强一致性的分布式键值数据库。
//!
//! 项目的立足点：**保留 Redis 的全部生态兼容性，同时把一致性做到 Raft 级别**。
//! 客户端可以直接使用任何现成的 Redis 客户端，无需改造。
//!
//! 当前处于原型开发阶段，架构为「单 Raft 组」。详见 `docs/PRD.md` 与
//! `docs/ARCHITECTURE.md`。

// openraft 的存储 trait 把返回类型固定为 `StorageError<NodeId>`，而这个类型有 224 字节。
// 我们无法通过装箱来缩小它——那会改变 trait 签名，导致无法实现这些 trait。
// 这是第三方 API 契约带来的、无法在本地消除的开销，因此在全 crate 范围内豁免该 lint。
#![allow(clippy::result_large_err)]

pub mod backend;
pub mod config;
pub mod error;
pub mod kv;
pub mod node;
pub mod protocol;
pub mod raft;
pub mod server;

/// 本服务的版本号，取自 `Cargo.toml`。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
