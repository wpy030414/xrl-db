//! 配置加载与校验。
//!
//! 支持两种配置文件格式，解析后归一到同一个强类型 [`Config`]：
//!
//! - **TOML**（`.toml` 扩展名）：原生格式。集群拓扑这类嵌套结构用它表达最自然，
//!   且与 serde 无缝衔接、类型安全。
//! - **redis.conf 风格**（其他扩展名）：行式 `key value`。让沿用 Redis 的运维
//!   可以直接复用现有配置文件，迁移成本接近于零。
//!
//! 覆盖优先级：**CLI 参数 > 环境变量 > 配置文件 > 内置默认值**。
//! 本模块只负责「配置文件 → `Config`」这一段，CLI 与环境变量的覆盖在 `main.rs` 完成。
//!
//! # 用法
//!
//! ```no_run
//! use std::path::Path;
//! use xrl_db::config::Config;
//!
//! # fn main() -> xrl_db::error::Result<()> {
//! let cfg = Config::from_file(Path::new("xrldb.toml"))?.resolve()?;
//! println!("节点 {} 监听于 {}", cfg.node.id, cfg.node.listen);
//! # Ok(())
//! # }
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// 节点标识，在集群内必须唯一。
pub type NodeId = u64;

/// 未指定端口时使用的默认端口。
pub const DEFAULT_PORT: u16 = 7001;

/// 节点间 RPC 端口相对客户端端口的偏移量。
///
/// 沿用 Redis Cluster「集群总线端口」的同一规则（客户端端口 + 10000），
/// 熟悉 Redis 的运维不需要额外记忆。好处是集群配置里每个节点**只需要写一个地址**，
/// 节点间通信的地址由它推导而来，不会出现两者写得不一致的情况。
pub const RPC_PORT_OFFSET: u16 = 10_000;

/// 完整配置。
///
/// 四个分节分别对应：节点自身、集群拓扑、存储、Raft 调参。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// 节点自身的标识与监听地址。
    pub node: NodeConfig,
    /// 集群拓扑。
    pub cluster: ClusterConfig,
    /// 存储相关配置。
    pub storage: StorageConfig,
    /// Raft 共识调参。
    pub raft: RaftConfig,
}

/// 节点自身的标识与监听地址。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// 节点在集群内的唯一标识。
    pub id: NodeId,
    /// 客户端与节点间通信的监听地址。
    ///
    /// 默认只监听回环地址。对外暴露必须显式修改此项——这是一道有意的门槛。
    pub listen: SocketAddr,
}

/// 集群拓扑。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    /// 是否启用集群模式。
    ///
    /// 为 `false` 时按单节点运行；为 `true` 时 `peers` 必须包含本节点。
    pub enabled: bool,
    /// 集群内全部节点，**必须包含本节点自己**。
    pub peers: Vec<Peer>,
}

/// 集群中的一个节点。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    /// 节点标识。
    pub id: NodeId,
    /// 该节点的地址，用于节点间 Raft RPC。
    pub addr: SocketAddr,
}

/// 存储相关配置。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// 数据目录。
    ///
    /// 未指定时按节点 ID 自动推导为 `./data/node-{id}`。这个默认值是有意的：
    /// **redb 是单进程的**（文件锁），同一台机器上的多个节点绝不能共用一个文件，
    /// 按节点 ID 分目录可以避免这个坑。
    pub path: Option<PathBuf>,
}

/// Raft 共识调参。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RaftConfig {
    /// 选举超时（毫秒）。follower 超过此时长未收到心跳即发起选举。
    pub election_timeout_ms: u64,
    /// 心跳间隔（毫秒）。leader 按此频率向 follower 发送心跳。
    pub heartbeat_interval_ms: u64,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: 1,
            // 默认只监听回环地址，对外暴露须显式配置
            listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_PORT),
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            // 默认比值为 3，与 openraft 的默认配置一致
            election_timeout_ms: 300,
            heartbeat_interval_ms: 100,
        }
    }
}

impl Config {
    /// 从文件加载配置。
    ///
    /// 按扩展名分派解析器：`.toml` 走 TOML，其他一律按 redis.conf 风格解析。
    /// 本方法**只解析，不做校验**——在此之后调用 [`Config::resolve`] 完成默认值
    /// 推导与校验，返回可直接使用的配置。
    ///
    /// # 错误
    ///
    /// - 文件不存在或无法读取 → [`Error::ConfigRead`]
    /// - TOML 语法/类型错误 → [`Error::ConfigToml`]
    /// - redis.conf 风格的行式错误（含不支持的配置项）→ [`Error::ConfigLine`]
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|source| Error::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;

        if is_toml(path) {
            toml::from_str(&text).map_err(|source| Error::ConfigToml {
                path: path.to_path_buf(),
                source,
            })
        } else {
            parse_line_config(&text, path)
        }
    }

    /// 完成默认值推导与语义校验，返回可直接使用的配置。
    ///
    /// 这是使用配置的**唯一推荐入口**——它把「推导」与「校验」绑在一起，
    /// 调用方无法只做其中一半而漏掉另一半。
    pub fn resolve(mut self) -> Result<Self> {
        self.finalize();
        self.validate()?;
        Ok(self)
    }

    /// 推导未显式指定的默认值。
    ///
    /// 目前只处理数据目录：未指定时按节点 ID 推导，避免多节点共用一个 redb 文件。
    pub fn finalize(&mut self) {
        if self.storage.path.is_none() {
            self.storage.path = Some(default_storage_path(self.node.id));
        }
    }

    /// 语义校验——语法正确但组合起来说不通的情况。
    ///
    /// # 错误
    ///
    /// 任何一条不满足都返回 [`Error::ConfigInvalid`]，消息中说明具体原因。
    pub fn validate(&self) -> Result<()> {
        // 心跳必须显著快于选举超时。若两者相等或心跳更慢，follower 会在收到心跳前
        // 就超时发起选举，集群将陷入持续选举而无法稳定产生 leader。
        if self.raft.heartbeat_interval_ms == 0 {
            return Err(Error::ConfigInvalid(
                "raft.heartbeat_interval_ms 不能为 0".to_string(),
            ));
        }
        if self.raft.election_timeout_ms < self.raft.heartbeat_interval_ms * 2 {
            return Err(Error::ConfigInvalid(format!(
                "raft.election_timeout_ms（{}）必须至少是 heartbeat_interval_ms（{}）的 2 倍，\
                 否则 follower 会在收到心跳前超时，导致集群无法稳定选出 leader",
                self.raft.election_timeout_ms, self.raft.heartbeat_interval_ms
            )));
        }

        if !self.cluster.enabled {
            return Ok(());
        }

        if self.cluster.peers.is_empty() {
            return Err(Error::ConfigInvalid(
                "cluster.enabled 为 true 但 cluster.peers 为空；\
                 启用集群时必须列出全部节点（含本节点）"
                    .to_string(),
            ));
        }

        // 本节点必须在 peers 中，否则它无法参与共识
        if !self.cluster.peers.iter().any(|p| p.id == self.node.id) {
            return Err(Error::ConfigInvalid(format!(
                "本节点 ID（{}）未出现在 cluster.peers 中；\
                 peers 必须包含本节点自己，否则本节点无法参与共识",
                self.node.id
            )));
        }

        check_unique(
            self.cluster.peers.iter().map(|p| p.id),
            "cluster.peers 中存在重复的节点 ID",
        )?;
        check_unique(
            self.cluster.peers.iter().map(|p| p.addr),
            "cluster.peers 中存在重复的地址",
        )?;

        // 每个节点的 RPC 端口都必须能推导出来，否则那个节点永远连不上
        self.rpc_listen()?;
        for peer in &self.cluster.peers {
            rpc_addr_of(peer.addr)?;
        }

        Ok(())
    }

    /// 数据目录。
    ///
    /// 经 [`Config::resolve`] 之后一定已填充；此方法对未经 resolve 的配置也能给出
    /// 合理答案，且不会 panic。
    pub fn storage_path(&self) -> PathBuf {
        self.storage
            .path
            .clone()
            .unwrap_or_else(|| default_storage_path(self.node.id))
    }

    /// 本节点用于节点间 RPC 的监听地址。
    ///
    /// # 错误
    ///
    /// 端口加上 [`RPC_PORT_OFFSET`] 后若超出 `u16` 范围则报错——这种配置若被放行，
    /// 推导出的地址会静默回绕到一个小端口上，症状是节点之间怎么都连不上，
    /// 极难排查。
    pub fn rpc_listen(&self) -> Result<SocketAddr> {
        rpc_addr_of(self.node.listen)
    }

    /// 取指定节点的 RPC 地址。
    ///
    /// 节点不存在或地址推导失败时返回 `None`。
    pub fn peer_rpc_addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.cluster
            .peers
            .iter()
            .find(|peer| peer.id == id)
            .and_then(|peer| rpc_addr_of(peer.addr).ok())
    }
}

/// 由客户端地址推导节点间 RPC 地址。
fn rpc_addr_of(client: SocketAddr) -> Result<SocketAddr> {
    client
        .port()
        .checked_add(RPC_PORT_OFFSET)
        .map(|port| SocketAddr::new(client.ip(), port))
        .ok_or_else(|| {
            Error::ConfigInvalid(format!(
                "监听地址 {client} 的端口加上 {RPC_PORT_OFFSET} 后超出了端口范围；\
                 请改用小于 {} 的端口，否则节点间无法通信",
                u16::MAX - RPC_PORT_OFFSET
            ))
        })
}

/// 按节点 ID 推导默认数据目录。
fn default_storage_path(node_id: NodeId) -> PathBuf {
    PathBuf::from(format!("./data/node-{node_id}"))
}

/// 判断配置文件是否应按 TOML 解析。
fn is_toml(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
}

/// 检查迭代器中是否存在重复项，`what` 用于描述冲突的是什么。
fn check_unique<T, I>(items: I, what: &str) -> Result<()>
where
    T: std::hash::Hash + Eq + std::fmt::Debug,
    I: IntoIterator<Item = T>,
{
    let mut seen = std::collections::HashSet::new();
    for item in items {
        // 先用引用检查并构造错误信息，确认无冲突后再把值的所有权交给集合，
        // 否则 item 已被移入 HashSet，后续就无法再格式化它了
        if seen.contains(&item) {
            return Err(Error::ConfigInvalid(format!("{what}：{item:?}")));
        }
        seen.insert(item);
    }
    Ok(())
}

/// 解析 redis.conf 风格的行式配置。
///
/// 格式为每行一条 `配置项 值...`，`#` 起始的内容视为注释，空行忽略。
///
/// 支持的配置项是本项目**自有语义的一个子集**（见 `docs/PRD.md`）。遇到不认识的
/// 配置项会直接报错而非静默忽略——静默忽略一次配置是运维事故的经典成因：
/// 你以为什么都配好了，其实没有。
fn parse_line_config(text: &str, path: &Path) -> Result<Config> {
    let mut config = Config::default();

    // redis.conf 把绑定地址与端口分成两项，这里先分别收集，最后合并成监听地址
    let mut bind: Option<IpAddr> = None;
    let mut port: Option<u16> = None;

    for (idx, raw_line) in text.lines().enumerate() {
        let line_no = idx + 1;
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }

        let mut tokens = line.split_whitespace();
        // 上面已确认行非空，因此 split 至少产出一个 token
        let key = tokens.next().unwrap_or_default();
        let values: Vec<&str> = tokens.collect();
        let first = values.first().copied();

        match key {
            "bind" => {
                let raw = require(first, key, path, line_no)?;
                // Redis 允许在地址前加 `-` 表示「绑定失败不致命」，这里只关心地址本身
                let addr = raw.trim_start_matches('-');
                bind = Some(addr.parse::<IpAddr>().map_err(|e| Error::ConfigLine {
                    path: path.to_path_buf(),
                    line: line_no,
                    message: format!("bind 的地址 `{addr}` 无法解析：{e}"),
                })?);
            }
            "port" => {
                let raw = require(first, key, path, line_no)?;
                port = Some(raw.parse::<u16>().map_err(|e| Error::ConfigLine {
                    path: path.to_path_buf(),
                    line: line_no,
                    message: format!("port 的值 `{raw}` 不是合法端口号：{e}"),
                })?);
            }
            "dir" => {
                let raw = require(first, key, path, line_no)?;
                config.storage.path = Some(PathBuf::from(raw));
            }
            "cluster-enabled" => {
                config.cluster.enabled = parse_yes_no(first, key, path, line_no)?;
            }
            "cluster-node-id" => {
                config.node.id = parse_u64(first, key, path, line_no)?;
            }
            "cluster-peer" => {
                if values.len() < 2 {
                    return Err(Error::ConfigLine {
                        path: path.to_path_buf(),
                        line: line_no,
                        message: "cluster-peer 需要两个参数：<节点ID> <地址>".to_string(),
                    });
                }
                let id = parse_u64(Some(values[0]), "cluster-peer 的节点 ID", path, line_no)?;
                let addr = values[1]
                    .parse::<SocketAddr>()
                    .map_err(|e| Error::ConfigLine {
                        path: path.to_path_buf(),
                        line: line_no,
                        message: format!("cluster-peer 的地址 `{}` 无法解析：{e}", values[1]),
                    })?;
                config.cluster.peers.push(Peer { id, addr });
            }
            "cluster-election-timeout-ms" => {
                config.raft.election_timeout_ms = parse_u64(first, key, path, line_no)?;
            }
            "cluster-heartbeat-interval-ms" => {
                config.raft.heartbeat_interval_ms = parse_u64(first, key, path, line_no)?;
            }
            other => {
                return Err(Error::ConfigLine {
                    path: path.to_path_buf(),
                    line: line_no,
                    message: format!(
                        "不支持的配置项 `{other}`。本项目兼容的是 redis.conf 的一个子集，\
                         支持的项见 docs/PRD.md；若该项对 XRLDB 无意义，注释掉即可"
                    ),
                });
            }
        }
    }

    // 只有至少指定了其中之一时才覆盖默认监听地址，否则保留默认值
    if bind.is_some() || port.is_some() {
        let ip = bind.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let p = port.unwrap_or(DEFAULT_PORT);
        config.node.listen = SocketAddr::new(ip, p);
    }

    Ok(config)
}

/// 去掉行内注释并返回剩余部分。
///
/// redis.conf 风格中 `#` 起始即为注释。这里不做「引号内的 `#` 不算注释」的复杂处理——
/// 本项目的配置项都不需要在值里包含 `#`，为此引入引号解析规则得不偿失。
fn strip_comment(line: &str) -> &str {
    match line.find('#') {
        Some(idx) => &line[..idx],
        None => line,
    }
}

/// 取出必需的单个值，缺失时报错。
fn require<'a>(value: Option<&'a str>, key: &str, path: &Path, line_no: usize) -> Result<&'a str> {
    value.ok_or_else(|| Error::ConfigLine {
        path: path.to_path_buf(),
        line: line_no,
        message: format!("配置项 `{key}` 缺少值"),
    })
}

/// 解析 `yes` / `no` 形式的布尔值。
fn parse_yes_no(value: Option<&str>, key: &str, path: &Path, line_no: usize) -> Result<bool> {
    let build_err = |message: String| Error::ConfigLine {
        path: path.to_path_buf(),
        line: line_no,
        message,
    };

    match value {
        Some("yes" | "true" | "1") => Ok(true),
        Some("no" | "false" | "0") => Ok(false),
        Some(other) => Err(build_err(format!(
            "配置项 `{key}` 期望 yes/no，得到 `{other}`"
        ))),
        None => Err(build_err(format!("配置项 `{key}` 缺少值，期望 yes 或 no"))),
    }
}

/// 解析无符号整数。
fn parse_u64(value: Option<&str>, key: &str, path: &Path, line_no: usize) -> Result<u64> {
    let raw = require(value, key, path, line_no)?;
    raw.parse::<u64>().map_err(|e| Error::ConfigLine {
        path: path.to_path_buf(),
        line: line_no,
        message: format!("配置项 `{key}` 的值 `{raw}` 不是非负整数：{e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一份完整的 TOML 配置，用于多个测试复用
    const SAMPLE_TOML: &str = r#"
[node]
id = 2
listen = "127.0.0.1:7002"

[cluster]
enabled = true
peers = [
  { id = 1, addr = "127.0.0.1:7001" },
  { id = 2, addr = "127.0.0.1:7002" },
  { id = 3, addr = "127.0.0.1:7003" },
]

[storage]
path = "./data/node2"

[raft]
election_timeout_ms = 300
heartbeat_interval_ms = 100
"#;

    /// 等价的 redis.conf 风格配置
    const SAMPLE_REDIS_CONF: &str = r#"
# XRLDB 示例配置
bind 127.0.0.1
port 7002

dir ./data/node2
cluster-enabled yes
cluster-node-id 2
cluster-peer 1 127.0.0.1:7001
cluster-peer 2 127.0.0.1:7002
cluster-peer 3 127.0.0.1:7003

cluster-election-timeout-ms 300
cluster-heartbeat-interval-ms 100
"#;

    /// 把字符串按 TOML 解析
    fn parse_toml(text: &str) -> Result<Config> {
        toml::from_str(text).map_err(|source| Error::ConfigToml {
            path: PathBuf::from("<test>"),
            source,
        })
    }

    #[test]
    fn toml_config_parses_all_sections() {
        let cfg = parse_toml(SAMPLE_TOML)
            .expect("应能解析")
            .resolve()
            .expect("应能校验");

        assert_eq!(cfg.node.id, 2);
        assert_eq!(cfg.node.listen.to_string(), "127.0.0.1:7002");
        assert!(cfg.cluster.enabled);
        assert_eq!(cfg.cluster.peers.len(), 3);
        assert_eq!(cfg.raft.election_timeout_ms, 300);
        assert_eq!(cfg.raft.heartbeat_interval_ms, 100);
    }

    #[test]
    fn toml_config_rejects_unknown_field() {
        // 拼错的配置项必须被拒绝，而不是被静默忽略
        let text = "[node]\nid = 1\nlistenn = \"127.0.0.1:7001\"\n";
        let err = parse_toml(text).expect_err("未知字段应导致解析失败");
        assert!(
            err.to_string().contains("listenn"),
            "错误信息应指出是哪个字段出错，实际为：{err}"
        );
    }

    #[test]
    fn both_formats_normalize_to_same_config() {
        // 这是「双格式」承诺的核心：两种写法必须归一到完全相同的配置
        let from_toml = parse_toml(SAMPLE_TOML)
            .expect("TOML 应能解析")
            .resolve()
            .expect("TOML 应能校验");
        let from_conf = parse_line_config(SAMPLE_REDIS_CONF, Path::new("redis.conf"))
            .expect("redis.conf 应能解析")
            .resolve()
            .expect("redis.conf 应能校验");

        assert_eq!(from_toml, from_conf);
    }

    #[test]
    fn line_config_ignores_comments_and_blank_lines() {
        let text = "\n# 只有注释\n\n   \nport 7009\n# 尾部注释\n";
        let cfg = parse_line_config(text, Path::new("t.conf")).expect("应能解析");

        assert_eq!(cfg.node.listen.port(), 7009);
        // 未指定 bind，应回落到回环地址
        assert!(cfg.node.listen.ip().is_loopback());
    }

    #[test]
    fn line_config_rejects_unknown_directive() {
        // 静默忽略未知配置项等同于制造运维事故，必须报错
        let text = "port 7001\nmaxmemory 2gb\n";
        let err = parse_line_config(text, Path::new("redis.conf")).expect_err("未知配置项应报错");

        let msg = err.to_string();
        assert!(
            msg.contains("maxmemory"),
            "应指出具体的配置项，实际为：{msg}"
        );
        assert!(msg.contains("第 2 行"), "应指出具体行号，实际为：{msg}");
    }

    #[test]
    fn default_storage_path_is_per_node() {
        // 默认数据目录必须按节点分开——redb 是单进程的，共用文件会直接打不开
        let cfg = Config::default().resolve().expect("默认配置应合法");

        assert_eq!(cfg.storage_path(), PathBuf::from("./data/node-1"));
    }

    #[test]
    fn heartbeat_must_be_slower_than_election_timeout() {
        let text = "[raft]\nelection_timeout_ms = 100\nheartbeat_interval_ms = 100\n";
        let err = parse_toml(text)
            .expect("语法应正确")
            .resolve()
            .expect_err("心跳与选举超时相等应被拒绝");

        assert!(
            err.to_string().contains("2 倍"),
            "错误信息应说明约束，实际为：{err}"
        );
    }

    #[test]
    fn cluster_requires_node_in_peers() {
        // 本节点不在 peers 里，它就无法参与共识——这是典型的配置错误
        let text = r#"
[cluster]
enabled = true
peers = [
  { id = 7, addr = "127.0.0.1:7007" },
]
"#;
        let err = parse_toml(text)
            .expect("语法应正确")
            .resolve()
            .expect_err("本节点不在 peers 中应被拒绝");

        assert!(
            err.to_string().contains("未出现在 cluster.peers 中"),
            "错误信息应说明原因，实际为：{err}"
        );
    }

    #[test]
    fn cluster_rejects_duplicate_peer_ids() {
        let text = r#"
[node]
id = 1
[cluster]
enabled = true
peers = [
  { id = 1, addr = "127.0.0.1:7001" },
  { id = 1, addr = "127.0.0.1:7002" },
]
"#;
        let err = parse_toml(text)
            .expect("语法应正确")
            .resolve()
            .expect_err("重复的节点 ID 应被拒绝");

        assert!(
            err.to_string().contains("重复的节点 ID"),
            "错误信息应说明原因，实际为：{err}"
        );
    }

    #[test]
    fn single_node_mode_needs_no_peers() {
        // 未启用集群时不要求 peers，单机模式必须开箱即用
        let cfg = Config::default().resolve().expect("单机默认配置应合法");

        assert!(!cfg.cluster.enabled);
        assert!(cfg.cluster.peers.is_empty());
    }
}
