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

use openraft::error::{ClientWriteError, Fatal, InitializeError, RaftError};
use openraft::impls::BasicNode;
use openraft::{
    ChangeMembers, Config as RaftConfig, Raft, RaftMetrics, SnapshotPolicy, StorageError,
};
use redb::Database;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::config::Config;
use crate::kv::{Store, WriteOp};
use crate::protocol::Reply;
use crate::raft::forward::{ForwardError, Forwarder};
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

    /// 集群初始化失败。
    Initialize(RaftError<NodeId, InitializeError<NodeId, BasicNode>>),

    /// 成员变更失败。
    Membership(RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>),

    /// 请求转交主节点失败。
    ///
    /// 注意其中的 [`ForwardError::OutcomeUnknown`]：那表示一次写入**可能已经生效**，
    /// 调用方必须原样上报，绝不可当作「失败」来自动重试。
    Forward(ForwardError),

    /// 等待本地状态机追平主节点确认的读索引时超时。
    ///
    /// 出现它说明本节点落后于主节点太多（网络慢、或正在追赶快照）。**不能退化成
    /// 直接读本地**——那等于返回过期数据，正是本项目要消灭的东西。
    ReadLagging {
        /// 主节点确认的索引。
        index: u64,
    },
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
            NodeError::Initialize(source) => write!(f, "集群初始化失败：{source}"),
            NodeError::Membership(source) => write!(f, "成员变更失败：{source}"),
            NodeError::Forward(source) => write!(f, "{source}"),
            NodeError::ReadLagging { index } => write!(
                f,
                "本节点尚未追平到主节点确认的日志位置 {index}，\
                 为避免返回过期数据，本次读取已被拒绝；稍后重试即可"
            ),
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
            NodeError::Initialize(source) => Some(source),
            NodeError::Membership(source) => Some(source),
            NodeError::Forward(source) => Some(source),
            NodeError::ReadLagging { .. } => None,
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
    /// 把客户端请求转交给主节点。
    ///
    /// 有了它，客户端连到任何一个节点都能读写——这正是相对 Redis Cluster 的
    /// 额外卖点：Redis 需要客户端自己实现槽位路由，我们不需要。
    forwarder: Forwarder,
    /// 节点间 RPC 服务的停止信号。
    rpc_stop: watch::Sender<bool>,
    /// 节点间 RPC 服务的任务句柄。
    ///
    /// 用 `Mutex` 包一层只是为了让 [`Node::shutdown`] 能取 `&self` 的同时把句柄
    /// 拿走——关闭动作本身不需要独占整个节点。
    rpc_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
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

        // RPC 服务在后台运行，随进程结束而结束；但要留下停止信号与句柄，
        // 让「关闭这个节点」能真正把端口和 Raft 引用交还出来（见 Node::shutdown）。
        let (rpc_stop, rpc_stopped) = watch::channel(false);
        let rpc_raft = raft.clone();
        let rpc_task = tokio::spawn(async move {
            if let Err(error) = rpc::serve(listener, rpc_raft, rpc_stopped).await {
                eprintln!("节点间 RPC 服务已停止：{error}");
            }
        });

        let forwarder = Forwarder::new(config.node.id, raft.clone(), &config);

        Ok(Self {
            id: config.node.id,
            raft,
            rpc_addr,
            state_machine: read_handle,
            forwarder,
            rpc_stop,
            rpc_task: std::sync::Mutex::new(Some(rpc_task)),
        })
    }

    /// 以**单节点集群**的形式启动并自举。
    ///
    /// 单节点同样运行完整的 Raft，只是成员只有一个。这样「单机」与「集群」之间
    /// 只有成员数量之差，不存在两套代码路径——也就不会出现「单机测试全过、
    /// 一上集群就出问题」这类最难排查的缺陷。
    ///
    /// # 对已经初始化过的数据目录是幂等的
    ///
    /// 数据目录里已经有日志时，`initialize` 会被 openraft 拒绝——这是**保护**而非
    /// 缺陷：重新初始化会丢弃既有日志。但「重启一个既有节点」是最普通的运维操作，
    /// 若把它当成启动失败，那么**只要集群落过盘，它就再也起不来了**，而且
    /// 报错只说「不允许初始化」，与真正的原因相距甚远。
    ///
    /// 因此这里把「已经初始化过」单独识别出来，沿用它已有的成员配置继续启动。
    ///
    /// 返回时保证主节点已经选出：选举是异步的，不等它完成就会打印出
    /// 「尚未选出主节点」这种自相矛盾的启动信息。
    pub async fn start_single(config: Config) -> Result<Self, NodeError> {
        let node = Self::start(config).await?;

        let mut members = BTreeMap::new();
        members.insert(node.id, BasicNode::new(node.rpc_addr.to_string()));

        match node.initialize(members).await {
            Ok(()) => {}
            // 数据目录里已经有日志与成员配置——重启，不是首次启动。
            // 已有的成员配置就在磁盘上，openraft 自己会恢复它。
            Err(NodeError::Initialize(RaftError::APIError(InitializeError::NotAllowed(_)))) => {}
            Err(error) => return Err(error),
        }

        // 单节点集群的选举几乎是瞬时的，但仍然是异步的，必须显式等待。
        // 超时不作为启动失败——集群可能只是慢了一点，稍后会自行恢复。
        if node
            .wait_for_leader(SINGLE_NODE_ELECTION_TIMEOUT)
            .await
            .is_none()
        {
            eprintln!(
                "警告：节点 {} 在 {:?} 内未选出主节点，稍后会自行重试",
                node.id, SINGLE_NODE_ELECTION_TIMEOUT
            );
        }

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
    ///
    /// 本节点不是主节点时，操作会被转交给主节点执行（见 [`Forwarder`]），
    /// 调用方无需知道自己是连在哪个节点上。因此本方法只有一条代码路径，
    /// 「连主节点」与「连从节点」不会分叉成两种行为。
    ///
    /// # 错误
    ///
    /// 特别注意 [`ForwardError::OutcomeUnknown`]：它表示写入**可能已经生效**。
    /// 调用方必须把它如实上报给客户端，不能当作失败而自动重试——那会让一条
    /// `INCR` 被执行两次。
    pub async fn write(&self, op: WriteOp) -> Result<Reply, NodeError> {
        self.forwarder.write(op).await.map_err(NodeError::Forward)
    }

    /// 执行一次线性一致读。
    ///
    /// # 读为什么不必把请求转给主节点
    ///
    /// 线性一致读分两步。第一步是**确认一个足够新的日志位置**：
    ///
    /// - 本节点是主节点时，直接向多数派发一轮心跳确认自己的领导权
    ///   （openraft 的 `ensure_linearizable`）；
    /// - 本节点是**从**节点时，向主节点要一个它刚刚确认过的日志位置
    ///   （Raft 论文里的 ReadIndex）。
    ///
    /// 第二步是**等待本地状态机追平该位置**，然后读本地。
    ///
    /// 关键在于第二步读的是本地数据——数据不必从主节点搬过来，读取吞吐也不会
    /// 因为全部汇聚到主节点而受限。少了第一步，从节点或已退位的旧主节点就会返回
    /// 过期数据；少了第二步也同样会——状态机可能还没追上那个位置。
    ///
    /// # 错误
    ///
    /// 追平超时会返回 [`NodeError::ReadLagging`]。此时**绝不放行这次读取**：
    /// 宁可让客户端重试，也不返回一个可能已经过期的值。
    pub async fn read<F, T>(&self, read: F) -> Result<T, NodeError>
    where
        F: FnOnce(&Store) -> T,
    {
        let read_index = self
            .forwarder
            .read_index()
            .await
            .map_err(NodeError::Forward)?;

        // 日志为空时读索引为 None——此时没有任何已提交的数据，无需等待
        if let Some(log_id) = read_index
            && !self
                .wait_for_applied(log_id.index, READ_INDEX_TIMEOUT)
                .await
        {
            return Err(NodeError::ReadLagging {
                index: log_id.index,
            });
        }

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

    /// 把一个节点纳入集群：先作为学习者追平数据，再转为投票成员。
    ///
    /// 两步的顺序不能反。直接把它设为投票成员，就是让一个还没有任何数据的节点
    /// 立刻参与多数派计算——集群规模变大了，可用性却不升反降。
    ///
    /// **只应由主节点调用**。本节点不是主节点时，底层的调用会返回错误，错误信息
    /// 里带着真正的主节点是谁。
    ///
    /// 幂等：已经在该节点集合里时直接返回成功，不重复提交成员变更。
    pub async fn add_member(&self, id: NodeId, node: BasicNode) -> Result<(), NodeError> {
        let membership = self.metrics().membership_config;

        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        if voters.contains(&id) {
            return Ok(());
        }

        // 已经作为学习者存在时不必重复登记——`add_learner` 会重新走一遍
        // 「等待追平」的流程，在重试场景下白白拖延。
        let known = membership.nodes().any(|(node_id, _)| *node_id == id);
        if !known {
            self.add_learner(id, node).await?;
        }

        let mut next = voters;
        next.insert(id);
        self.change_membership(next).await
    }

    /// 关闭本节点。
    ///
    /// 取 `&self` 而非 `self`，是为了让调用方不必从 `Arc` 里解包——`Raft` 句柄
    /// 本身是可克隆的，底层的关闭动作并不需要独占所有权。
    ///
    /// # 关闭必须真的把东西交还出去
    ///
    /// 只调用 `Raft::shutdown` 是不够的：节点间 RPC 的服务循环持有监听器，也持有
    /// 一份 `Raft` 引用。它如果一直跑到进程结束，那么「已关闭」的节点会
    /// **仍然占着端口、仍然拖着整个共识实例和 redb 的文件锁**——
    /// 同一个进程里重启这个节点就会失败在「地址已被占用」或「数据库已被打开」上，
    /// 而错误信息指向的东西看起来完全正常。
    pub async fn shutdown(&self) {
        // 先让 RPC 服务交还监听器。顺序不能反：等 Raft 都关完了再停 RPC，
        // 那段窗口里服务循环可能正拿着一个已经失效的 Raft 去处理请求。
        let _ = self.rpc_stop.send(true);

        let handle = {
            let mut slot = self.rpc_task.lock().expect("RPC 任务句柄的锁不应被毒化");
            slot.take()
        };
        if let Some(handle) = handle {
            // 等它真正退出，而不是只看信号已经发出——端口是在它退出的那一刻才释放的
            let _ = handle.await;
        }

        if let Err(error) = self.raft.shutdown().await {
            eprintln!("节点 {} 关闭时出错：{error}", self.id);
        }
    }
}

/// 单节点集群选举主节点的等待上限。
///
/// 给得比选举超时宽松得多：单节点不需要和任何人通信，正常情况下是瞬时的；
/// 留出余量只是为了容忍调度延迟。
const SINGLE_NODE_ELECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// 等待本地状态机追平读索引的上限。
///
/// 正常情况下落后的从节点只需要毫秒级就能追上——条目已经是提交状态，复制流不会断。
/// 给到秒级是为了容忍「节点正在安装快照」这种确实需要一段时间的情况。
const READ_INDEX_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
    let raft = &config.raft;

    // 落后多少条日志就该改传快照而不是继续补日志。
    //
    // 取「两次快照之间的日志量 + 快照之外保留的日志量」：一个从节点若能靠现有日志
    // 追上，它落后的量必然落在这个范围内；超出它，说明它要的那段日志已经被截断了，
    // 只能给它整个快照。openraft 自己的默认值是 5000，这里取两者的较大值，
    // 免得把 tuning 过的参数反而调小了。
    let replication_lag_threshold = raft
        .snapshot_logs_since_last
        .saturating_add(raft.max_in_snapshot_log_to_keep)
        .max(DEFAULT_REPLICATION_LAG_THRESHOLD);

    RaftConfig {
        cluster_name: format!("xrl-db-{}", config.node.id),
        heartbeat_interval: raft.heartbeat_interval_ms,
        // 选举超时给一个区间：所有节点若用完全相同的超时，会在 leader 失联后
        // 同时发起选举、反复分裂选票而选不出新 leader。openraft 会在区间内随机
        // 取值来打破这种对称。
        election_timeout_min: raft.election_timeout_ms,
        election_timeout_max: raft.election_timeout_ms * 2,

        // ---- 快照与日志截断 ----
        //
        // 不配这一段，日志就会无限增长，磁盘迟早被吃满。截断的唯一依据是快照，
        // 因此「多久建一次快照」实际上就是「日志能占多大磁盘」。
        snapshot_policy: SnapshotPolicy::LogsSinceLast(raft.snapshot_logs_since_last),
        max_in_snapshot_log_to_keep: raft.max_in_snapshot_log_to_keep,
        // 每次最多删一条。批量删看起来更快，但删除发生在提交路径上——
        // 一次删一大批会让某次写入的延迟突然多出几十毫秒。平滑更重要。
        purge_batch_size: 1,
        replication_lag_threshold,

        ..Default::default()
    }
}

/// 落后多少条日志才改用快照追赶的默认阈值。
///
/// 与 openraft 的默认值保持一致。
const DEFAULT_REPLICATION_LAG_THRESHOLD: u64 = 5_000;
