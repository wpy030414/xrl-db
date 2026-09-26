//! 命令执行后端。
//!
//! 会话层把解析好的 [`Command`] 交给它，它返回 [`Reply`]。
//!
//! # 写路径：先共识，后可见
//!
//! 写命令在这里被转换成 [`WriteOp`]（相对时间在此解析为绝对时刻），然后提交给
//! [`Node::write`]。那个方法返回时，写入**已经存在于多数派节点上**——这就是
//! 「不丢数据」承诺的物理兑现点。
//!
//! # 读路径：先确认领导权，再读本地
//!
//! 读命令走 [`Node::read`]，它先通过多数派确认本节点仍是 leader、且状态机已追平，
//! 然后才读本地状态。少了这一步，已退位的旧 leader 会返回过期数据——那正是本项目
//! 要避免的事情。
//!
//! # 单机与集群没有两条代码路径
//!
//! 单节点同样运行完整的 Raft（只是成员只有一个）。这样「单机测试通过、一上集群
//! 就出问题」这类最难查的缺陷从根上就不存在。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::Config;
use crate::kv::{self, Store, WriteOp};
use crate::node::{Node, state_name};
use crate::protocol::{Command, Reply};

/// 本服务声明的 Redis 协议兼容版本。
///
/// 不少客户端会读取 `INFO` 里的 `redis_version` 来决定启用哪些特性，
/// 缺失这个字段可能导致它们行为异常。同类项目（KeyDB、Dragonfly、Valkey）也都
/// 提供该字段，这是兼容层的常规做法而非冒充——我们同时提供 `xrldb_version` 表明真实身份。
const REDIS_COMPAT_VERSION: &str = "7.4.0";

/// 命令执行后端。
pub struct Backend {
    /// 承载共识与状态机的节点。
    node: Arc<Node>,
    /// 节点配置，供 `INFO` / `CLUSTER` 等命令读取。
    config: Config,
    /// 已处理的命令总数，用于 `INFO` 的统计。
    commands_processed: AtomicU64,
    /// 当前连接数，由监听层维护。
    connected_clients: AtomicU64,
}

impl Backend {
    /// 创建一个后端。
    pub fn new(node: Arc<Node>, config: Config) -> Self {
        Self {
            node,
            config,
            commands_processed: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
        }
    }

    /// 执行一条命令，返回应回给客户端的响应。
    ///
    /// 本方法**不会失败**——所有错误都表达为 [`Reply::Error`]，因为「客户端发来
    /// 一条不合法的命令」或「本节点暂时不是 leader」都是正常现象，不应中断连接。
    pub async fn execute(&self, command: Command) -> Reply {
        self.commands_processed.fetch_add(1, Ordering::Relaxed);

        // 连接管理与集群状态类命令不碰数据，先处理掉
        match &command {
            Command::Ping { message } => {
                // 带参数的 PING 按 Redis 惯例回送该参数本身
                return match message {
                    Some(message) => Reply::Bulk(message.clone()),
                    None => Reply::Simple("PONG".to_string()),
                };
            }
            Command::Echo { message } => return Reply::Bulk(message.clone()),
            Command::Quit => return Reply::ok(),
            Command::Hello { version } => return self.hello_reply(*version),
            Command::Info { section } => return self.info_reply(section.as_deref()).await,
            Command::ClusterInfo => return self.cluster_info_reply(),
            Command::RaftLeader => return self.raft_leader_reply(),
            Command::RaftInfo => return self.raft_info_reply(),
            Command::RaftAddNode { .. } => {
                return Reply::error(
                    "ERR RAFT ADD-NODE requires an explicit membership change flow, which is not exposed over the client protocol yet",
                );
            }
            _ => {}
        }

        let now = kv::now_ms();

        // 写命令：先转换为确定性表示（相对时间 → 绝对时刻），再提交共识
        match WriteOp::from_command(&command, now) {
            Ok(Some(op)) => {
                return match self.node.write(op).await {
                    Ok(reply) => reply,
                    // 最常见的失败是「本节点不是 leader」。如实告知，客户端可据此改连。
                    Err(error) => Reply::error(format!("ERR {error}")),
                };
            }
            Ok(None) => {} // 读命令，继续往下走
            Err(error) => return Reply::error(error.to_string()),
        }

        // 读命令：线性一致读
        match self
            .node
            .read(|store| dispatch_read(store, &command, now))
            .await
        {
            Ok(reply) => reply,
            Err(error) => Reply::error(format!("ERR {error}")),
        }
    }

    /// 记录一个新连接建立。
    pub fn connection_opened(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一个连接关闭。
    pub fn connection_closed(&self) {
        // 用 saturating 语义避免计数在异常路径下下溢成一个巨大的数字
        let _ =
            self.connected_clients
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(1))
                });
    }

    /// 当前连接数。
    pub fn connected_clients(&self) -> u64 {
        self.connected_clients.load(Ordering::Relaxed)
    }

    // ------------------------------------------------------------ 内部

    /// `HELLO` 的回复。
    fn hello_reply(&self, requested: Option<u8>) -> Reply {
        let proto = requested.unwrap_or(2);
        Reply::Map(vec![
            (Reply::bulk("server"), Reply::bulk("xrldb")),
            (Reply::bulk("version"), Reply::bulk(crate::VERSION)),
            (Reply::bulk("proto"), Reply::Integer(proto as i64)),
            (
                Reply::bulk("id"),
                Reply::Integer(self.config.node.id as i64),
            ),
            (Reply::bulk("mode"), Reply::bulk(self.mode_name())),
            (Reply::bulk("role"), Reply::bulk(self.role_name())),
            (Reply::bulk("modules"), Reply::Array(vec![])),
        ])
    }

    /// 运行模式的可读名称。
    fn mode_name(&self) -> &'static str {
        if self.config.cluster.enabled {
            "cluster"
        } else {
            "standalone"
        }
    }

    /// 本节点在集群中的角色。
    fn role_name(&self) -> &'static str {
        if self.node.leader() == Some(self.config.node.id) {
            "master"
        } else {
            "replica"
        }
    }
    /// `CLUSTER INFO`
    ///
    /// 注意 `cluster_enabled` 恒为 `0`：Redis 客户端看到它为 `1` 会启用**槽位路由**
    /// 并对 `MOVED` 重定向做特殊处理，而本项目是单 Raft 组、不做分片，服务端会自行
    /// 把请求转到正确的位置。报 `0` 才能让客户端保持最简单的行为。
    /// 真实的集群状态由非标准的 `xrldb_*` 字段如实告知。
    fn cluster_info_reply(&self) -> Reply {
        let metrics = self.node.metrics();
        let body = format!(
            "# Cluster\r\n\
             cluster_enabled:0\r\n\
             xrldb_mode:{}\r\n\
             xrldb_node_id:{}\r\n\
             xrldb_state:{}\r\n\
             xrldb_leader:{}\r\n\
             xrldb_term:{}\r\n\
             xrldb_members:{}\r\n\
             xrldb_note:single Raft group, no sharding\r\n",
            self.mode_name(),
            self.config.node.id,
            state_name(metrics.state),
            metrics
                .current_leader
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            metrics.vote.leader_id().term,
            self.config.cluster.peers.len().max(1),
        );
        Reply::Bulk(bytes::Bytes::from(body))
    }

    /// `RAFT LEADER`
    fn raft_leader_reply(&self) -> Reply {
        match self.node.leader() {
            Some(id) => Reply::Integer(id as i64),
            // 集群尚未选出 leader。返回空值是诚实的做法，而不是编造一个答案。
            None => Reply::Null,
        }
    }

    /// `RAFT INFO`
    fn raft_info_reply(&self) -> Reply {
        let metrics = self.node.metrics();
        Reply::Map(vec![
            (Reply::bulk("mode"), Reply::bulk(self.mode_name())),
            (
                Reply::bulk("node_id"),
                Reply::Integer(self.config.node.id as i64),
            ),
            (Reply::bulk("state"), Reply::bulk(state_name(metrics.state))),
            (
                Reply::bulk("leader_id"),
                match metrics.current_leader {
                    Some(id) => Reply::Integer(id as i64),
                    None => Reply::Null,
                },
            ),
            (
                Reply::bulk("term"),
                Reply::Integer(metrics.vote.leader_id().term as i64),
            ),
            (
                Reply::bulk("last_log_index"),
                match metrics.last_log_index {
                    Some(index) => Reply::Integer(index as i64),
                    None => Reply::Null,
                },
            ),
            (
                Reply::bulk("last_applied"),
                match metrics.last_applied {
                    Some(id) => Reply::Integer(id.index as i64),
                    None => Reply::Null,
                },
            ),
            (
                Reply::bulk("members"),
                Reply::Integer(self.config.cluster.peers.len().max(1) as i64),
            ),
        ])
    }

    /// `INFO [section]`
    async fn info_reply(&self, section: Option<&[u8]>) -> Reply {
        let key_count = self
            .node
            .read(|store| store.dbsize(crate::kv::now_ms()))
            .await
            .ok()
            .and_then(|reply| match reply {
                Reply::Integer(count) => Some(count),
                _ => None,
            })
            .unwrap_or(0);

        let metrics = self.node.metrics();
        let section = section.map(|raw| String::from_utf8_lossy(raw).to_ascii_lowercase());
        let wants = |name: &str| section.as_deref().is_none_or(|s| s == name || s == "all");

        let mut out = String::new();

        if wants("server") {
            out.push_str(&format!(
                "# Server\r\n\
                 xrldb_version:{}\r\n\
                 redis_version:{}\r\n\
                 mode:{}\r\n\
                 os:{}\r\n\
                 arch_bits:{}\r\n\
                 process_id:{}\r\n",
                crate::VERSION,
                REDIS_COMPAT_VERSION,
                self.mode_name(),
                std::env::consts::OS,
                if cfg!(target_pointer_width = "64") {
                    64
                } else {
                    32
                },
                std::process::id(),
            ));
        }

        if wants("clients") {
            out.push_str(&format!(
                "\r\n# Clients\r\nconnected_clients:{}\r\n",
                self.connected_clients()
            ));
        }

        if wants("stats") {
            out.push_str(&format!(
                "\r\n# Stats\r\ntotal_commands_processed:{}\r\n",
                self.commands_processed.load(Ordering::Relaxed)
            ));
        }

        if wants("replication") {
            // 如实报告本节点在 Raft 中的角色与任期
            out.push_str(&format!(
                "\r\n# Replication\r\nrole:{}\r\nxrldb_state:{}\r\nxrldb_term:{}\r\n",
                self.role_name(),
                state_name(metrics.state),
                metrics.vote.leader_id().term,
            ));
        }

        if wants("cluster") {
            out.push_str(&format!(
                "\r\n# Cluster\r\ncluster_enabled:0\r\nxrldb_members:{}\r\n",
                self.config.cluster.peers.len().max(1)
            ));
        }

        if wants("keyspace") {
            // 状态机是内存副本，这里报告的是它在本地看到的记录数
            out.push_str(&format!(
                "\r\n# Keyspace\r\ndb0:keys={key_count},expires=0,avg_ttl=0\r\n"
            ));
        }

        // 安全提示：ADR-009 明确首版不做认证，必须让运维看得见这件事
        out.push_str(
            "\r\n# Security\r\n\
             warning:no password is set, do not expose this instance to an untrusted network\r\n\
             warning:listening address is whatever you configured; the default is 127.0.0.1\r\n",
        );

        Reply::Bulk(bytes::Bytes::from(out))
    }
}

/// 读命令的分发。
///
/// 单独抽成自由函数，是因为它需要在一个闭包里访问状态机——
/// [`Node::read`] 的签名正是「给一个只在只读状态下运行的函数」。
fn dispatch_read(store: &Store, command: &Command, now: kv::TimestampMs) -> Reply {
    match command {
        Command::Get { key } => store.get(key, now),
        Command::Exists { keys } => store.exists(keys, now),
        Command::MGet { keys } => store.mget(keys, now),
        Command::Strlen { key } => store.strlen(key, now),
        Command::Ttl { key } => store.ttl(key, now),
        Command::Keys { pattern } => store.keys(pattern, now),
        Command::Scan {
            cursor,
            pattern,
            count,
        } => store.scan(*cursor, pattern.as_ref(), *count, now),
        Command::Type { key } => store.type_of(key, now),
        Command::Dbsize => store.dbsize(now),

        // 上面已经处理过的命令不应走到这里；写命令也不会（它们在前面就返回了）
        other => Reply::error(format!(
            "ERR internal error: unhandled read command {other:?}"
        )),
    }
}
