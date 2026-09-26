//! 节点组装：把存储、共识、网络拼装成一个可运行的节点。
//!
//! # 这是唯一知道「所有组件如何接线」的地方
//!
//! 其他各层都只与自己的邻居打交道：
//!
//! - [`crate::kv`] 不知道 Raft 的存在
//! - [`crate::protocol`] 不知道 Raft 的存在
//! - [`crate::raft::log_store`] 与 [`crate::raft::state_machine`] 不知道网络的存在
//! - [`crate::raft::network`] 与 [`crate::raft::rpc`] 不知道存储的存在
//!
//! 拼装全部发生在这里。这样任何一个组件都能被单独测试，也使得「先做单机可用版本、
//! 再接入共识层」这条实施路径成为可能。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, InitializeError, RaftError};
use openraft::impls::BasicNode;
use openraft::{ChangeMembers, Config as RaftConfig, Raft, RaftMetrics, StorageError};
use redb::Database;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::kv::{Store, WriteOp};
use crate::protocol::Reply;
use crate::raft::log_store::LogStore;
use crate::raft::network::NetworkFactory;
use crate::raft::state_machine::StateMachine;
use crate::raft::{NodeId, TypeConfig, rpc};

/// 节点运行中可能出现的错误。
///
/// 单独定义而不用 [`crate::error::Error`]，是因为这里所有的失败来源都是外部库的
/// 具体类型（openraft、redb、tokio），把它们统统拍平成字符串会丢掉诊断信息。
#[derive(Debug)]
pub enum NodeError {
    /// 准备工作目录失败。
    PrepareStorage {
        /// 出错的路径。
        path: std::path::PathBuf,
        /// 底层错误。
        source: std::io::Error,
    },

    /// 打开存储失败。
    OpenStorage {
        /// 出错的路径。
        path: std::path::PathBuf,
        /// 底层错误。
        source: redb::DatabaseError,
    },

    /// 初始化存储层失败。
    Storage(StorageError<NodeId>),

    /// 节点间 RPC 端口绑定失败。
    BindRpc {
        /// 尝试绑定的地址。
        addr: std::net::SocketAddr,
        /// 底层错误。
        source: std::io::Error,
    },

    /// 配置在启动阶段被发现不合法。
    ///
    /// 正常情况下 [`crate::config::Config::resolve`] 已经拦下了所有非法配置，
    /// 走到这里说明有人绕过了那道校验。
    InvalidConfig(crate::error::Error),

    /// Raft 实例启动失败。
    Startup(Fatal<NodeId>),

    /// 写请求被拒绝（不是 leader、超时等）。
    Write(RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>),

    /// 读请求未能确认领导权。
    ///
    /// 线性一致读要求先确认本节点仍是 leader 且状态机已追平；这个错误表示该前提
    /// 未能满足。客户端应当改连 leader 后重试。
    Read(RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>),

    /// 集群初始化失败。
    Initialize(RaftError<NodeId, InitializeError<NodeId, BasicNode>>),

    /// 成员变更失败。
    Membership(RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>),
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::PrepareStorage { path, source } => {
                write!(f, "无法准备工作目录 {}：{source}", path.display())
            }
            NodeError::OpenStorage { path, source } => {
                write!(f, "无法打开存储 {}：{source}", path.display())
            }
            NodeError::Storage(source) => write!(f, "初始化存储层失败：{source}"),
            NodeError::BindRpc { addr, source } => {
                write!(f, "节点间通信地址 {addr} 绑定失败：{source}")
            }
            NodeError::InvalidConfig(source) => write!(f, "{source}"),
            NodeError::Startup(source) => write!(f, "Raft 实例启动失败：{source}"),
            NodeError::Write(source) => write!(f, "写请求未能提交：{source}"),
            NodeError::Read(source) => write!(f, "读请求未能确认领导权：{source}"),
            NodeError::Initialize(source) => write!(f, "集群初始化失败：{source}"),
            NodeError::Membership(source) => write!(f, "成员变更失败：{source}"),
        }
    }
}

impl std::error::Error for NodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NodeError::PrepareStorage { source, .. } => Some(source),
            NodeError::OpenStorage { source, .. } => Some(source),
            NodeError::Storage(source) => Some(source),
            NodeError::BindRpc { source, .. } => Some(source),
            NodeError::InvalidConfig(source) => Some(source),
            NodeError::Startup(source) => Some(source),
            NodeError::Write(source) => Some(source),
            NodeError::Read(source) => Some(source),
            NodeError::Initialize(source) => Some(source),
            NodeError::Membership(source) => Some(source),
        }
    }
}

/// 一个正在运行的节点。
pub struct Node {
    id: NodeId,
    raft: Raft<TypeConfig>,
    rpc_addr: std::net::SocketAddr,
    /// 读路径用的状态机句柄。
    ///
    /// 状态机被交给了 Raft 独占，但它自身是可克隆的——节点保留一份，
    /// 便能在确认领导权之后直接读本地状态，而不必经过共识。
    state_machine: StateMachine,
}

impl Node {
    /// 按配置启动一个节点。
    ///
    /// 启动完成后，节点会立即参与选举与日志复制——但**集群的成员配置还没建立**，
    /// 需要另外调用 [`Node::initialize`] 或 [`Node::join`]。
    pub async fn start(config: Config) -> Result<Self, NodeError> {
        // ---- 存储 ----
        let dir = config.storage_path();
        std::fs::create_dir_all(&dir).map_err(|source| NodeError::PrepareStorage {
            path: dir.clone(),
            source,
        })?;

        // 每个节点一个独立的 redb 文件。redb 是单进程的，共用文件会让后启动的
        // 节点直接打不开——这正是默认数据目录按节点 ID 分开的原因。
        let db_path = dir.join("raft.redb");
        let db = Arc::new(
            Database::create(&db_path).map_err(|source| NodeError::OpenStorage {
                path: db_path.clone(),
                source,
            })?,
        );

        let log_store = LogStore::new(Arc::clone(&db)).map_err(NodeError::Storage)?;
        let state_machine = StateMachine::new(db).map_err(NodeError::Storage)?;

        // 保留一份句柄给读路径——状态机一旦交给 Raft 就取不回来了
        let read_handle = state_machine.clone();

        // ---- 网络 ----
        let rpc_addr = config.rpc_listen().map_err(NodeError::InvalidConfig)?;

        let listener = TcpListener::bind(rpc_addr)
            .await
            .map_err(|source| NodeError::BindRpc {
                addr: rpc_addr,
                source,
            })?;

        // ---- 共识 ----
        let raft_config = build_raft_config(&config);
        let network = NetworkFactory::new(&config);

        let raft = Raft::new(
            config.node.id,
            Arc::new(raft_config),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(NodeError::Startup)?;

        // RPC 服务在后台运行，随进程结束而结束
        let rpc_raft = raft.clone();
        tokio::spawn(async move {
            if let Err(error) = rpc::serve(listener, rpc_raft).await {
                eprintln!("节点间 RPC 服务已停止：{error}");
            }
        });

        Ok(Self {
            id: config.node.id,
            raft,
            rpc_addr,
            state_machine: read_handle,
        })
    }

    /// 以**单节点集群**的形式启动并自举。
    ///
    /// 单节点同样运行完整的 Raft，只是成员只有一个。这样「单机」与「集群」之间
    /// 只有成员数量之差，不存在两套代码路径——也就不会出现「单机测试全过、
    /// 一上集群就出问题」这类最难排查的缺陷。
    pub async fn start_single(config: Config) -> Result<Self, NodeError> {
        let node = Self::start(config).await?;

        let mut members = BTreeMap::new();
        members.insert(node.id, BasicNode::new(node.rpc_addr.to_string()));
        node.initialize(members).await?;

        Ok(node)
    }

    /// 本节点的 ID。
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// 节点间通信地址。
    pub fn rpc_addr(&self) -> std::net::SocketAddr {
        self.rpc_addr
    }

    /// 取得 Raft 实例的句柄，供上层直接调用。
    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    /// 提交一条写操作，等待多数派确认后返回结果。
    ///
    /// 这个方法返回时，写入**已经存在于多数派节点上**——这正是「不丢数据」
    /// 承诺的兑现点。
    pub async fn write(&self, op: WriteOp) -> Result<Reply, NodeError> {
        let response = self.raft.client_write(op).await.map_err(NodeError::Write)?;
        Ok(response.data)
    }

    /// 执行一次线性一致读。
    ///
    /// 先通过 [`Raft::ensure_linearizable`] 向多数派确认本节点仍是 leader、且状态机
    /// 已追平到该时刻，**然后**才读本地状态机。少了这一步，从库或已退位的旧 leader
    /// 就会返回过期数据——那正是本项目承诺要避免的事情。
    ///
    /// 这一步的代价是一次心跳往返，比读操作本身贵，但换来的是「读到的绝不是旧值」。
    pub async fn read<F, T>(&self, read: F) -> Result<T, NodeError>
    where
        F: FnOnce(&Store) -> T,
    {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(NodeError::Read)?;
        Ok(self.state_machine.read(read))
    }

    /// 当前的 Raft 指标，用于观测与等待集群状态。
    pub fn metrics(&self) -> RaftMetrics<NodeId, BasicNode> {
        self.raft.metrics().borrow().clone()
    }

    /// 当前的 leader；集群尚未选出 leader 时为 `None`。
    pub fn leader(&self) -> Option<NodeId> {
        self.metrics().current_leader
    }

    /// 等待集群选出 leader，超时则返回 `None`。
    ///
    /// 选举是异步过程，启动后立刻写入可能撞上「还没有 leader」。运维脚本与测试
    /// 都需要一个明确的等待点，而不是靠固定 `sleep` 碰运气。
    pub async fn wait_for_leader(&self, timeout: std::time::Duration) -> Option<NodeId> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if let Some(leader) = self.leader() {
                return Some(leader);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// 等待本节点的状态机追平到指定位置，超时则返回 `false`。
    ///
    /// 用于「确认数据已经落到某个节点上」——比如在故障转移测试中确认继任者
    /// 已经拥有了全部日志。
    pub async fn wait_for_applied(&self, index: u64, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let applied = self.metrics().last_applied.map(|id| id.index).unwrap_or(0);
            if applied >= index {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// 用给定的成员集合初始化集群。
    ///
    /// **只应对一个全新的集群调用一次**。对已初始化的集群重复调用会返回错误——
    /// 这是保护而非缺陷：成员变更应当走 [`Node::change_membership`]，
    /// 重新初始化会丢弃既有日志。
    pub async fn initialize(&self, members: BTreeMap<NodeId, BasicNode>) -> Result<(), NodeError> {
        self.raft
            .initialize(members)
            .await
            .map_err(NodeError::Initialize)
    }

    /// 把一个节点作为**学习者**加入集群。
    ///
    /// 先让它追平日志，再通过 [`Node::change_membership`] 转为投票成员——
    /// 直接加入投票成员会在它还没追上数据时就参与多数派计算，损害可用性。
    pub async fn add_learner(&self, id: NodeId, node: BasicNode) -> Result<(), NodeError> {
        self.raft
            .add_learner(id, node, true)
            .await
            .map_err(NodeError::Membership)?;
        Ok(())
    }

    /// 变更集群的投票成员集合。
    ///
    /// 传入的每个节点都**必须已经通过 [`Node::add_learner`] 加入**——否则 Raft
    /// 不知道它们的地址，无从与之通信。先加学习者再转正式成员，是为了让新节点
    /// 追赶日志期间不参与多数派计算：若它尚未追平就计入投票，集群反而更容易失去
    /// 可用性。
    pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<(), NodeError> {
        self.raft
            .change_membership(ChangeMembers::ReplaceAllVoters(voters), false)
            .await
            .map_err(NodeError::Membership)?;
        Ok(())
    }

    /// 关闭本节点。
    ///
    /// 取 `&self` 而非 `self`，是为了让调用方不必从 `Arc` 里解包——`Raft` 句柄
    /// 本身是可克隆的，底层的关闭动作并不需要独占所有权。
    pub async fn shutdown(&self) {
        if let Err(error) = self.raft.shutdown().await {
            eprintln!("节点 {} 关闭时出错：{error}", self.id);
        }
    }
}

/// 把 Raft 的服务器状态映射为纯文本。
///
/// `ServerState` 只实现了 `Debug`——直接 `{:?}` 会把 Rust 的调试格式暴露给
/// Redis 客户端。这里显式映射，保证输出是稳定的、可被脚本解析的文本。
pub fn state_name(state: openraft::ServerState) -> &'static str {
    use openraft::ServerState;
    match state {
        ServerState::Learner => "learner",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Leader => "leader",
        ServerState::Shutdown => "shutdown",
    }
}

/// 依据项目配置构造 openraft 的运行时配置。
fn build_raft_config(config: &Config) -> RaftConfig {
    RaftConfig {
        cluster_name: format!("xrl-db-{}", config.node.id),
        heartbeat_interval: config.raft.heartbeat_interval_ms,
        // 选举超时给一个区间：所有节点若用完全相同的超时，会在 leader 失联后
        // 同时发起选举、反复分裂选票而选不出新 leader。openraft 会在区间内随机
        // 取值来打破这种对称。
        election_timeout_min: config.raft.election_timeout_ms,
        election_timeout_max: config.raft.election_timeout_ms * 2,
        ..Default::default()
    }
}
