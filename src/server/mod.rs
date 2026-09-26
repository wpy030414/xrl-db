//! 网络服务层。
//!
//! 职责边界很明确：**只关心「连接」与「字节」，不关心「命令的语义」**。
//! 命令语义全部委托给 [`crate::backend::Backend`]。
//!
//! - [`listener`] — 监听端口、接受连接、优雅停止
//! - [`session`] — 服务单条连接，直到对方断开或帧层面出错

pub mod listener;
pub mod session;

pub use listener::{serve, serve_with};
