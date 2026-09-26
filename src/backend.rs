//! 命令执行后端。
//!
//! 会话层把解析好的 [`Command`] 交给它，它返回 [`Reply`]。
//!
//! # 这一层为什么单独存在
//!
//! 当前实现直接作用于本地状态机。接入 Raft 之后，这个结构体会变成
//! 「把写命令提交到共识层并等待多数派确认」——但**会话层的调用方式完全不变**。
//! 把「命令如何被执行」与「连接如何被服务」分开，是为了让共识层的引入
//! 不波及任何一行网络代码。

use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::Config;
use crate::kv::{self, Store, WriteOp};
use crate::protocol::{Command, Reply};

/// 本服务声明的 Redis 协议兼容版本。
///
/// 不少客户端会读取 `INFO` 里的 `redis_version` 来决定启用哪些特性，
/// 缺失这个字段可能导致它们行为异常。同类项目（KeyDB、Dragonfly、Valkey）也都
/// 提供该字段，这是兼容层的常规做法而非冒充——我们同时提供 `xrldb_version` 表明真实身份。
const REDIS_COMPAT_VERSION: &str = "7.4.0";

/// 命令执行后端。
pub struct Backend {
    /// 键值状态机。
    ///
    /// 用 `RwLock` 而非 `Mutex`：读命令（GET/MGET/KEYS…）之间可以并发，
    /// 只有写命令需要独占。这是本服务读写性能差异的主要来源。
    store: RwLock<Store>,
    /// 节点配置。
    config: Config,
    /// 已处理的命令总数，用于 `INFO` 的统计。
    commands_processed: AtomicU64,
    /// 当前连接数，由监听层维护。
    connected_clients: AtomicU64,
}

impl Backend {
    /// 按配置创建一个后端。
    pub fn new(config: Config) -> Self {
        Self {
            store: RwLock::new(Store::new()),
            config,
            commands_processed: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
        }
    }

    /// 执行一条命令，返回应回给客户端的响应。
    ///
    /// 本方法**不会失败**——所有错误都表达为 [`Reply::Error`]，因为「客户端发来
    /// 一条不合法的命令」是正常现象，不应中断连接。
    pub fn execute(&self, command: Command) -> Reply {
        self.commands_processed.fetch_add(1, Ordering::Relaxed);
        let now = kv::now_ms();

        // 写命令：先转换为确定性表示——相对时间在这一步变成绝对时刻。
        // 这正是状态机能够被 Raft 复制的前提，详见 kv::op 的模块说明。
        match WriteOp::from_command(&command, now) {
            Ok(Some(op)) => {
                let mut store = self.store_mut();
                return store.apply(&op, now);
            }
            Ok(None) => {} // 读命令，继续往下走
            Err(error) => return Reply::error(error.to_string()),
        }

        let store = self.store_ref();
        match &command {
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

            Command::Ping { message } => match message {
                // 带参数的 PING 按 Redis 惯例回送该参数本身
                Some(message) => Reply::Bulk(message.clone()),
                None => Reply::Simple("PONG".to_string()),
            },
            Command::Echo { message } => Reply::Bulk(message.clone()),

            Command::Info { section } => self.info_reply(now, section.as_deref()),
            Command::ClusterInfo => self.cluster_info_reply(),
            Command::RaftLeader => self.raft_leader_reply(),
            Command::RaftInfo => self.raft_info_reply(),

            Command::RaftAddNode { .. } => Reply::error(
                "ERR RAFT ADD-NODE requires an active consensus layer, which is not enabled yet",
            ),

            // HELLO 的方言切换由会话层负责，这里只负责生成回复
            Command::Hello { version } => self.hello_reply(*version),
            Command::Quit => Reply::ok(),

            // 写命令在上面已经处理并返回，不应走到这里
            other => Reply::error(format!("ERR internal error: unhandled command {other:?}")),
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

    /// 取读锁。
    ///
    /// 锁中毒只可能发生在持锁线程 panic 时。状态机的所有方法都不会 panic，
    /// 因此这里遇到中毒更可能是无关的 panic 波及——**恢复数据继续服务**
    /// 比让整个数据库倒下更合适。
    fn store_ref(&self) -> std::sync::RwLockReadGuard<'_, Store> {
        self.store
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 取写锁，理由同上。
    fn store_mut(&self) -> std::sync::RwLockWriteGuard<'_, Store> {
        self.store
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

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
            (Reply::bulk("role"), Reply::bulk("master")),
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

    /// `CLUSTER INFO`
    ///
    /// 注意 `cluster_enabled` 恒为 `0`：Redis 客户端看到它为 `1` 会启用**槽位路由**
    /// 并对 `MOVED` 重定向做特殊处理，而本项目当前是单 Raft 组、不做分片，
    /// 服务端会自行把请求转到 leader。报 `0` 才能让客户端保持最简单的行为。
    /// 真实的集群拓扑由非标准的 `xrldb_*` 字段如实告知。
    fn cluster_info_reply(&self) -> Reply {
        let body = format!(
            "# Cluster\r\n\
             cluster_enabled:0\r\n\
             xrldb_mode:{}\r\n\
             xrldb_members:{}\r\n\
             xrldb_note:single Raft group, no sharding\r\n",
            self.mode_name(),
            self.config.cluster.peers.len()
        );
        Reply::Bulk(bytes::Bytes::from(body))
    }

    /// `RAFT LEADER`
    fn raft_leader_reply(&self) -> Reply {
        if self.config.cluster.enabled {
            // 共识层尚未接入，此刻无法得知 leader。返回空值是诚实的做法，
            // 而不是编造一个答案。
            Reply::Null
        } else {
            // 单机模式下本节点就是唯一节点
            Reply::Integer(self.config.node.id as i64)
        }
    }

    /// `RAFT INFO`
    fn raft_info_reply(&self) -> Reply {
        let (mode, leader) = if self.config.cluster.enabled {
            ("raft", Reply::Null)
        } else {
            ("standalone", Reply::Integer(self.config.node.id as i64))
        };

        Reply::Map(vec![
            (Reply::bulk("mode"), Reply::bulk(mode)),
            (
                Reply::bulk("node_id"),
                Reply::Integer(self.config.node.id as i64),
            ),
            (Reply::bulk("leader_id"), leader),
            (
                Reply::bulk("members"),
                Reply::Integer(self.config.cluster.peers.len().max(1) as i64),
            ),
            // consensus_active 明确告知共识层是否已生效。当前恒为 0，
            // 接入 Raft 后由真实状态决定。
            (Reply::bulk("consensus_active"), Reply::Integer(0)),
        ])
    }

    /// `INFO [section]`
    fn info_reply(&self, now: kv::TimestampMs, section: Option<&[u8]>) -> Reply {
        let store = self.store_ref();
        let key_count = match store.dbsize(now) {
            Reply::Integer(count) => count,
            _ => 0,
        };
        drop(store);

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
            // 共识层尚未接入，如实报告为单节点主库
            out.push_str("\r\n# Replication\r\nrole:master\r\nconnected_slaves:0\r\n");
        }

        if wants("cluster") {
            out.push_str(&format!(
                "\r\n# Cluster\r\ncluster_enabled:0\r\nxrldb_members:{}\r\n",
                self.config.cluster.peers.len()
            ));
        }

        if wants("keyspace") {
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
