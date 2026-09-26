//! 全局统一错误类型。
//!
//! 全项目共用一个 [`Error`] 枚举，各层通过 `From` 实现向上转换。
//! 这样做的代价是所有错误变体集中在一个文件，但换来的是：
//! 错误类型可被枚举、可被穷举匹配，调用方不会因为 `Box<dyn Error>`
//! 而失去对「可能失败的方式」的完整认知。

use std::fmt;
use std::path::PathBuf;

/// 全项目统一错误类型。
#[derive(Debug)]
pub enum Error {
    /// 配置文件读取失败（文件不存在、权限不足等）。
    ConfigRead {
        /// 出错的配置文件路径。
        path: PathBuf,
        /// 底层 IO 错误。
        source: std::io::Error,
    },

    /// TOML 配置文件的语法或类型错误。
    ConfigToml {
        /// 出错的配置文件路径。
        path: PathBuf,
        /// 底层解析错误，其 `Display` 已包含行列信息。
        source: toml::de::Error,
    },

    /// redis.conf 风格配置的行式解析错误。
    ConfigLine {
        /// 出错的配置文件路径。
        path: PathBuf,
        /// 行号，从 1 开始。
        line: usize,
        /// 人类可读的错误说明。
        message: String,
    },

    /// 配置在语义上不合法——语法正确，但各项组合起来说不通。
    ConfigInvalid(String),

    /// 监听地址绑定失败（端口被占用、地址不可用、权限不足等）。
    Bind {
        /// 尝试绑定的地址。
        addr: std::net::SocketAddr,
        /// 底层 IO 错误。
        source: std::io::Error,
    },

    /// 节点启动或运行失败。
    ///
    /// 包装 [`crate::node::NodeError`] 而不是把它拍平成字符串，是为了保留
    /// openraft / redb 的原始错误类型，便于定位问题。
    ///
    /// 装箱是必需的：`NodeError` 里又有一个 `InvalidConfig(Error)` 变体，
    /// 直接内联会让两个类型互相包含、大小无限。
    Node(Box<crate::node::NodeError>),

    /// 组建集群时，某个配置里写明的节点始终无法纳入。
    ///
    /// 单独作为一个变体而不是塞进 [`Error::Node`]，是因为它描述的**不是**本节点的
    /// 故障，而是集群拓扑没能按配置建成——排查方向完全不同。
    Bootstrap {
        /// 未能纳入的节点 ID。
        id: u64,
        /// 该节点在配置里的地址。
        addr: String,
        /// 失败原因。
        source: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ConfigRead { path, source } => {
                write!(f, "读取配置文件 {} 失败：{source}", path.display())
            }
            Error::ConfigToml { path, source } => {
                write!(f, "解析 TOML 配置文件 {} 失败：{source}", path.display())
            }
            Error::ConfigLine {
                path,
                line,
                message,
            } => {
                write!(
                    f,
                    "解析配置文件 {} 第 {line} 行失败：{message}",
                    path.display()
                )
            }
            Error::ConfigInvalid(message) => {
                write!(f, "配置校验失败：{message}")
            }
            Error::Bind { addr, source } => {
                write!(f, "监听地址 {addr} 绑定失败：{source}")
            }
            // NodeError 自己已经带了完整的上下文，直接透传
            Error::Node(source) => write!(f, "{source}"),
            Error::Bootstrap { id, addr, source } => write!(
                f,
                "无法把节点 {id}（{addr}）纳入集群：{source}。\
                 该节点没有启动、地址写错了，或者它监听的不是这个地址。\
                 集群未按配置建成，本节点已停止启动"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ConfigRead { source, .. } => Some(source),
            Error::ConfigToml { source, .. } => Some(source),
            Error::Bind { source, .. } => Some(source),
            Error::Node(source) => Some(source),
            // 无底层错误可追溯的信息性变体
            Error::ConfigLine { .. } | Error::ConfigInvalid(_) | Error::Bootstrap { .. } => None,
        }
    }
}

/// 本项目的统一 `Result` 别名。
pub type Result<T> = std::result::Result<T, Error>;
