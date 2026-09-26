//! XRLDB —— 兼容 Redis RESP3 协议、基于 Raft 提供强一致性的分布式键值数据库。
//!
//! 项目的立足点：**保留 Redis 的全部生态兼容性，同时把一致性做到 Raft 级别**。
//! 客户端可以直接使用任何现成的 Redis 客户端，无需改造。
//!
//! 当前处于原型开发阶段，架构为「单 Raft 组」。详见 `docs/PRD.md` 与
//! `docs/ARCHITECTURE.md`。

pub mod config;
pub mod error;
pub mod protocol;
