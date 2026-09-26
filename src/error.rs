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
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ConfigRead { source, .. } => Some(source),
            Error::ConfigToml { source, .. } => Some(source),
            Error::Bind { source, .. } => Some(source),
            // 无底层错误可追溯的信息性变体
            Error::ConfigLine { .. } | Error::ConfigInvalid(_) => None,
        }
    }
}

/// 本项目的统一 `Result` 别名。
pub type Result<T> = std::result::Result<T, Error>;
