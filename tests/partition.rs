//! 网络分区——**非对称**的那种。
//!
//! # 为什么「杀掉一个进程」不等于「网络分区」
//!
//! `tests/cluster.rs` 已经覆盖了进程死亡。那是**一种**故障，但不是全部：进程死了
//! 就那么死着，两边同时失去联系，而且对端立刻知道（`connect` 直接被拒）。真实的
//! 网络分区不是这样——它是**链路上丢包**：连接可能建立、请求可能已经发出、回音
//! 却永远不来；而且常常是**单向**的：A 能发给 B，B 发不回 A。
//!
//! 这两种形状对代码的要求不一样。对「连接被拒」，我们可以确定「请求根本没发出去」，
//! 于是放心重试；对「发出去了但没回音」，我们**什么都确定不了**——这正是
//! `src/raft/forward.rs` 里那张重试判据表存在的理由，也是本文件要验证的东西。
//!
//! # 怎么造出分区
//!
//! 在每一条**有向边**上放一个 TCP 闸门，把该方向的节点间流量引过去：
//!
//! ```text
//!   配置里：节点 i 认为节点 j 在 (闸门端口 - RPC_PORT_OFFSET)
//!   实际：  闸门监听该端口，把连接转给节点 j 真实的 RPC 端口
//!          —— 闸门关掉时，接受连接后**立刻丢掉**（而不是不监听）
//! ```
//!
//! 「接受之后丢掉」这个选择是刻意的，它决定了对端看到的是哪一种失败：
//! 不监听 → `connect` 被拒 → 「请求根本没发出去」→ 可以重试；
//! 接受后丢掉 → 「发出去了但没有回音」→ **真实的网络分区**，也是唯一需要
//! 验证「绝不能当成失败来重试」的那种情况。
//!
//! 六个方向的闸门各自独立，因此可以造出任意拓扑，包括完全单向的分区。
//!
//! # 两条测试分别盯住什么
//!
//! 1. **主节点被彻底孤立**：多数派必须选出新主节点继续服务，被孤立的主节点必须
//!    拒绝服务而不是拿本地数据糊弄；愈合之后，它在孤立期间那条**没能提交**的写入
//!    必须消失——未提交的日志被回滚，这是 Raft 的核心承诺之一。
//! 2. **从节点发不出去、但主节点发得进来**：这是纯粹的**非对称**故障。从节点上
//!    的转发必须及时失败且绝不谎报成功，同时它仍要能收到主节点推过来的日志。
//!    进程死亡造不出这个形状。

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::impls::BasicNode;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

mod common;

use bytes::Bytes;
use xrl_db::config::{Config, RPC_PORT_OFFSET};
use xrl_db::kv::WriteOp;
use xrl_db::node::Node;
use xrl_db::protocol::{Reply, SetCondition};

const CLUSTER_SIZE: u64 = 3;

/// 等待选举的上限。
const ELECTION_TIMEOUT: Duration = Duration::from_secs(15);

/// 等待一个被孤立的节点重新追上来的上限。
const CATCHUP_TIMEOUT: Duration = Duration::from_secs(30);

/// 等待孤立节点上的操作返回的上限。
///
/// 必须**大于** `LOCAL_TIMEOUT`（5 秒）：孤立的主节点上，一次写入会先在本地
/// 干等 5 秒才放弃。这里要证明的是「它终究会返回」，而不是「它多快返回」。
const ISOLATED_OP_TIMEOUT: Duration = Duration::from_secs(20);

/// 等待从节点上的转发失败的上限。
///
/// 转发本身是毫秒级的；给到 10 秒只是为了容忍测试机的调度抖动。
const FORWARD_OP_TIMEOUT: Duration = Duration::from_secs(10);

// ============================================================ 地址规划

/// 全拓扑的地址安排。
struct Plan {
    /// 各节点**真实**的客户端地址。节点自己的监听端口与 RPC 端口都从这里推导。
    real: Vec<(u64, SocketAddr)>,
    /// `(从, 到)` → 闸门监听的地址。
    gates: HashMap<(u64, u64), SocketAddr>,
}

impl Plan {
    fn real_addr(&self, id: u64) -> SocketAddr {
        self.real
            .iter()
            .find(|(peer_id, _)| *peer_id == id)
            .map(|(_, addr)| *addr)
            .expect("节点地址应存在")
    }
}

/// 端口是否可用。
fn port_free(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// 规划全套地址：`size` 个节点的真实端口，加上每条有向边一个闸门端口。
///
/// 一次规划全部端口而不是逐个去找，是因为它们之间必须互不冲突——冲突的症状是
/// 测试随机失败，而随机失败的测试会耗尽对整套测试的信任。
fn plan_topology(size: u64) -> Plan {
    'outer: loop {
        let mut used: BTreeSet<u16> = BTreeSet::new();
        let mut real = Vec::new();

        // 真实端口：客户端端口与（+10000 的）RPC 端口都要可用且未被占用
        for id in 1..=size {
            let Ok(probe) = std::net::TcpListener::bind("127.0.0.1:0") else {
                continue 'outer;
            };
            let port = probe.local_addr().expect("应能取得地址").port();
            drop(probe);

            let Some(rpc) = port.checked_add(RPC_PORT_OFFSET) else {
                continue 'outer;
            };
            if !port_free(port) || !port_free(rpc) {
                continue 'outer;
            }
            if used.contains(&port) || used.contains(&rpc) {
                continue 'outer;
            }

            used.insert(port);
            used.insert(rpc);
            real.push((id, SocketAddr::from(([127, 0, 0, 1], port))));
        }

        // 闸门端口：端口本身可用。
        //
        // 另外，配置里写的是「闸门端口 - RPC_PORT_OFFSET」，那个推导出来的端口
        // 虽然不会被绑定，但也不能和任何真实端口撞车——否则同一份配置里会出现
        // 两个相同的地址，语义就乱了。
        let mut gates = HashMap::new();
        for from in 1..=size {
            for to in 1..=size {
                if from == to {
                    continue;
                }

                let Ok(probe) = std::net::TcpListener::bind("127.0.0.1:0") else {
                    continue 'outer;
                };
                let port = probe.local_addr().expect("应能取得地址").port();
                drop(probe);

                let Some(derived) = port.checked_sub(RPC_PORT_OFFSET) else {
                    continue 'outer;
                };
                if !port_free(port) || used.contains(&port) || used.contains(&derived) {
                    continue 'outer;
                }

                used.insert(port);
                used.insert(derived);
                gates.insert((from, to), SocketAddr::from(([127, 0, 0, 1], port)));
            }
        }

        return Plan { real, gates };
    }
}

// ============================================================ 闸门

/// 一条有向边上的闸门。
struct Gate {
    blocked: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Gate {
    async fn start(addr: SocketAddr, target: SocketAddr) -> Self {
        let listener = TcpListener::bind(addr)
            .await
            .unwrap_or_else(|error| panic!("闸门应能绑定 {addr}：{error}"));

        let (blocked, receiver) = watch::channel(false);
        let task = tokio::spawn(pump(listener, target, receiver));

        Self { blocked, task }
    }

    fn set(&self, blocked: bool) {
        let _ = self.blocked.send(blocked);
    }
}

/// 读一次闸门状态。
///
/// 抽成函数是为了不让 `watch::Ref` 临时值跨过 `await`——那会借用冲突。
fn is_blocked(blocked: &watch::Receiver<bool>) -> bool {
    *blocked.borrow()
}

/// 闸门本体：接受连接、转给真实目标，并在闸门关闭时把它拆掉。
async fn pump(listener: TcpListener, target: SocketAddr, blocked: watch::Receiver<bool>) {
    loop {
        let Ok((mut inbound, _peer)) = listener.accept().await else {
            // 单次 accept 失败（比如瞬时耗尽文件描述符）不该终止整条闸门
            continue;
        };

        // 闸门关着：**接受之后立刻丢掉**，而不是不监听。
        //
        // 这个区别决定了被测代码看到的是哪一种失败，是本文件的关键设计：
        //   - 不监听 → 对端在 connect 阶段就被拒 →「请求根本没发出去」→ 重试是安全的
        //   - 接受后丢掉 →「发出去了但没有回音」→ 什么都确定不了
        // 后者才是真实网络分区的样子，也正是最需要验证「绝不能当成失败去重试」
        // 的那种情况。
        if is_blocked(&blocked) {
            drop(inbound);
            continue;
        }

        let mut blocked = blocked.clone();
        tokio::spawn(async move {
            let Ok(mut outbound) = TcpStream::connect(target).await else {
                return;
            };

            tokio::select! {
                _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}

                // 连接建立**之后**才切断：立刻把这条连接拆掉，模拟链路中断。
                // 已经在传的消息就丢在路上了，对端只会看到连接无声地断掉。
                _ = async {
                    while !is_blocked(&blocked) {
                        if blocked.changed().await.is_err() {
                            // 闸门本身没了，让这条连接正常活下去
                            std::future::pending::<()>().await;
                        }
                    }
                } => {}
            }
        });
    }
}

// ============================================================ 集群

/// 一个部分可控的三节点集群。
struct TestCluster {
    gates: HashMap<(u64, u64), Gate>,
    nodes: Vec<Option<Arc<Node>>>,
    plan: Plan,
    /// 数据目录必须留住，否则 `TempDir` 析构会连数据一起删掉。
    _dirs: Vec<tempfile::TempDir>,
}

impl TestCluster {
    async fn start() -> Self {
        let plan = plan_topology(CLUSTER_SIZE);

        let dirs: Vec<tempfile::TempDir> = (0..CLUSTER_SIZE)
            .map(|_| tempfile::tempdir().expect("应能创建临时目录"))
            .collect();

        // 先建闸门，再启节点：节点一起来就会去找对端，那时闸门必须已经在监听，
        // 否则第一次连接会被拒——那虽然是「可重试」的失败，但会让启动阶段
        // 多绕几圈，徒增测试时间。
        let mut gates = HashMap::new();
        for (&(from, to), &addr) in &plan.gates {
            let target = rpc_addr_of_client(plan.real_addr(to));
            gates.insert((from, to), Gate::start(addr, target).await);
        }

        let mut nodes = Vec::new();
        for (id, _) in &plan.real {
            let config = config_for(*id, &plan, dirs[(*id - 1) as usize].path().to_path_buf());
            let node = Node::start(config)
                .await
                .unwrap_or_else(|error| panic!("应能启动节点 {id}：{error}"));
            nodes.push(Some(Arc::new(node)));
        }

        let cluster = Self {
            gates,
            nodes,
            plan,
            _dirs: dirs,
        };
        cluster.bootstrap().await;
        cluster
    }

    /// 引导集群：先让 1 号成为单节点集群，再把其余节点作为学习者加入，最后转为正式成员。
    async fn bootstrap(&self) {
        let first = self.node(1);
        let members = BTreeMap::from([(1, BasicNode::new(self.plan.real_addr(1).to_string()))]);
        first.initialize(members).await.expect("应能初始化集群");

        for id in 2..=CLUSTER_SIZE {
            first
                .add_learner(id, BasicNode::new(self.plan.real_addr(id).to_string()))
                .await
                .expect("应能添加学习者");
        }

        first
            .change_membership(BTreeSet::from_iter(1..=CLUSTER_SIZE))
            .await
            .expect("应能变更成员集合");
    }

    fn node(&self, id: u64) -> &Arc<Node> {
        self.nodes[(id - 1) as usize]
            .as_ref()
            .unwrap_or_else(|| panic!("节点 {id} 已不存活"))
    }

    /// 切断 `from` → `to` 这一个方向。
    fn cut(&self, from: u64, to: u64) {
        self.gates
            .get(&(from, to))
            .unwrap_or_else(|| panic!("{from} → {to} 这条边应当存在"))
            .set(true);
    }

    /// 恢复 `from` → `to` 这一个方向。
    fn heal(&self, from: u64, to: u64) {
        self.gates
            .get(&(from, to))
            .unwrap_or_else(|| panic!("{from} → {to} 这条边应当存在"))
            .set(false);
    }

    /// 把一个节点和cluster其余部分**在两个方向上**都切断。
    fn isolate(&self, id: u64) {
        for other in 1..=CLUSTER_SIZE {
            if other == id {
                continue;
            }
            self.cut(id, other);
            self.cut(other, id);
        }
    }

    /// 恢复全部方向。
    fn heal_all(&self) {
        for gate in self.gates.values() {
            gate.set(false);
        }
    }

    /// 等待集群选出主节点（全部节点都活着时用）。
    async fn wait_for_leader(&self) -> u64 {
        self.wait_for_leader_among(&(1..=CLUSTER_SIZE).collect::<Vec<_>>())
            .await
            .expect("应能选出主节点")
    }

    /// 等待**存活的那一部分节点**选出主节点。
    ///
    /// 判定条件是「至少两个节点认可同一个主节点」——只有多数派认可的 leader 才
    /// 是权威的，采信单个节点的视图会在换届瞬间读到过期的答案。
    async fn wait_for_leader_among(&self, candidates: &[u64]) -> Option<u64> {
        let deadline = tokio::time::Instant::now() + ELECTION_TIMEOUT;

        loop {
            let mut votes = Vec::new();
            for id in candidates {
                let Some(node) = self.nodes[(*id - 1) as usize].as_ref() else {
                    continue;
                };
                if let Some(leader) = node.leader() {
                    votes.push(leader);
                }
            }

            if votes.len() >= 2
                && let Some(&leader) = votes.first()
                && votes.iter().all(|candidate| *candidate == leader)
                && candidates.contains(&leader)
            {
                return Some(leader);
            }

            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 写一个键，失败就 panic。
    async fn set(&self, id: u64, key: &str, value: &str) {
        self.node(id)
            .write(set_op(key, value))
            .await
            .unwrap_or_else(|error| panic!("往节点 {id} 写入 {key} 失败：{error}"));
    }

    /// 尝试写入，失败时把错误文本带出来而不是 panic。
    async fn try_set(&self, id: u64, key: &str, value: &str) -> Result<Reply, String> {
        self.node(id)
            .write(set_op(key, value))
            .await
            .map_err(|error| error.to_string())
    }

    /// 读一个键，失败就 panic。
    async fn get(&self, id: u64, key: &str) -> Reply {
        // 键的所有权要交给读闭包，因此报错信息里的名字单独留一份
        let label = key.to_string();
        let key = Bytes::copy_from_slice(key.as_bytes());

        self.node(id)
            .read(move |store| store.get(&key, xrl_db::kv::now_ms()))
            .await
            .unwrap_or_else(|error| panic!("在节点 {id} 上读取 {label:?} 失败：{error}"))
    }

    /// 尝试读取，失败时把错误文本带出来而不是 panic。
    async fn try_get(&self, id: u64, key: &str) -> Result<Reply, String> {
        let key = Bytes::copy_from_slice(key.as_bytes());
        self.node(id)
            .read(move |store| store.get(&key, xrl_db::kv::now_ms()))
            .await
            .map_err(|error| error.to_string())
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        // 闸门任务持有监听端口。测试跑完不收回，同一个进程里的后续测试就可能在
        // 端口分配上撞车——那会以「随机失败」的形式出现，极难定位。
        for gate in self.gates.values() {
            gate.task.abort();
        }
    }
}

/// 由客户端地址推导节点间 RPC 地址。
fn rpc_addr_of_client(addr: SocketAddr) -> SocketAddr {
    SocketAddr::from((addr.ip(), addr.port() + RPC_PORT_OFFSET))
}

/// 构造某个节点的配置：它自己用真实地址，对端一律指向闸门。
fn config_for(id: u64, plan: &Plan, data_dir: PathBuf) -> Config {
    let peers: Vec<(u64, SocketAddr)> = plan
        .real
        .iter()
        .map(|(peer_id, real)| {
            if *peer_id == id {
                (*peer_id, *real)
            } else {
                // 配置里要写的是客户端地址，RPC 地址由它推导（端口 +10000）。
                // 于是把闸门端口减去这个偏移，推导回来的正好是闸门。
                let gate = plan.gates[&(id, *peer_id)];
                (
                    *peer_id,
                    SocketAddr::from((gate.ip(), gate.port() - RPC_PORT_OFFSET)),
                )
            }
        })
        .collect();

    common::cluster_config(id, &peers, data_dir, common::test_raft_config())
}

/// 构造一个键的写入操作。
fn set_op(key: &str, value: &str) -> WriteOp {
    WriteOp::Set {
        key: Bytes::copy_from_slice(key.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
        expire_at: None,
        condition: SetCondition::Always,
    }
}

fn bulk(text: &str) -> Reply {
    Reply::Bulk(Bytes::copy_from_slice(text.as_bytes()))
}

// ==================================================================== 测试

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_leader_refuses_service_and_its_uncommitted_write_never_lands() {
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    // 先确认一切正常，留下一个「已经确认过」的写入
    cluster.set(leader, "before", "1").await;

    // ---- 把主节点在**两个方向**上彻底孤立 ----
    cluster.isolate(leader);

    // ---- 1) 被孤立的主节点必须拒绝写入 ----
    //
    // 它此刻可能还自认为是主节点。写入会先在本地干等一个永远不会到来的多数派确认，
    // 因此这里同时验证两件事：**最终会返回**，且**绝不谎报成功**。
    let outcome =
        tokio::time::timeout(ISOLATED_OP_TIMEOUT, cluster.try_set(leader, "doomed", "x")).await;

    match outcome {
        Err(_elapsed) => panic!(
            "被孤立的主节点在 {ISOLATED_OP_TIMEOUT:?} 内没有返回。\
             请求挂起比返回错误糟糕得多：调用方连该不该重试都无从判断。"
        ),
        Ok(Ok(reply)) => panic!(
            "被孤立的主节点竟然回复写入成功（{reply:?}）。\
             它不可能拿到多数派确认——这是脑裂。"
        ),
        Ok(Err(message)) => assert!(!message.is_empty(), "拒绝写入时必须说明原因"),
    }

    // ---- 2) 被孤立的主节点必须拒绝读 ----
    //
    // 它本地**有** "before" 这个键。若它就地返回，客户端就会拿到一个无法确认
    // 时效性的值——那正是本项目要消灭的东西。
    let read = tokio::time::timeout(ISOLATED_OP_TIMEOUT, cluster.try_get(leader, "before"))
        .await
        .expect("被孤立的主节点上，读也必须及时返回")
        .expect_err("被孤立的主节点返回了读结果——那个值可能已经过期");

    assert!(!read.is_empty(), "拒绝读取时必须说明原因");

    // ---- 3) 多数派必须选出新主节点，并且继续服务 ----
    let survivors: Vec<u64> = (1..=CLUSTER_SIZE).filter(|id| *id != leader).collect();
    let new_leader = cluster
        .wait_for_leader_among(&survivors)
        .await
        .unwrap_or_else(|| {
            panic!("多数派（{survivors:?}）没能在 {ELECTION_TIMEOUT:?} 内选出新主节点")
        });

    assert_ne!(new_leader, leader, "新主节点不可能是被孤立的那个");

    cluster.set(new_leader, "after", "2").await;
    assert_eq!(
        cluster.get(new_leader, "before").await,
        bulk("1"),
        "多数派把孤立之前**已经确认**的写入弄丢了"
    );

    // ---- 4) 愈合之后：它必须追上来，而那条没提交的写入必须消失 ----
    cluster.heal_all();

    let target = cluster
        .node(new_leader)
        .metrics()
        .last_log_index
        .unwrap_or(0);
    assert!(
        cluster
            .node(leader)
            .wait_for_applied(target, CATCHUP_TIMEOUT)
            .await,
        "愈合之后原主节点没能在 {CATCHUP_TIMEOUT:?} 内追上进度"
    );

    assert_eq!(
        cluster.get(leader, "after").await,
        bulk("2"),
        "愈合之后原主节点没有拿到新主节点的写入"
    );

    assert_eq!(
        cluster.get(leader, "doomed").await,
        Reply::Null,
        "那条在孤立期间**没能提交**的写入竟然生效了——未提交的日志没有被回滚。\
         这是 Raft 最核心的承诺之一：只有多数派确认过的条目才允许留下。"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_cut_off_one_way_still_receives_data_but_cannot_write() {
    // ★ 这条测试锁定的是一个进程死亡造不出来的故障形状 ★
    //
    // 切断的是**单向**：从节点 → 主节点。于是：
    //   - 从节点再也无法主动向主节点发起任何 RPC（投票、转发写入、要读索引）
    //   - 而主节点 → 从节点这条路是通的，而且它推过去的数据的回音走的是**同一条
    //     连接**，因此复制仍然正常
    //
    // 结果是一个看起来很矛盾的状态：从节点一直在收到最新的数据，却什么都写不了。
    // 非对称故障最容易暴露「想当然」的实现——比如把「我收到了主节点的心跳」误当作
    // 「主节点也听得到我」。
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    let follower = (1..=CLUSTER_SIZE)
        .find(|id| *id != leader)
        .expect("三节点集群里必然有从节点");

    cluster.set(leader, "before", "1").await;

    // ---- 只切断一个方向 ----
    cluster.cut(follower, leader);

    // ---- 1) 从节点上的写入必须及时失败，且绝不谎报成功 ----
    let outcome = tokio::time::timeout(
        FORWARD_OP_TIMEOUT,
        cluster.try_set(follower, "via-follower", "x"),
    )
    .await;

    match outcome {
        Err(_elapsed) => {
            panic!("从节点 {follower} 上的写入在 {FORWARD_OP_TIMEOUT:?} 内没有返回——请求挂住了")
        }
        Ok(Ok(reply)) => panic!(
            "从节点 {follower} 上的写入竟然成功了（{reply:?}），而它此刻联系不上主节点——\
             它不可能把请求送出去，更不可能拿到多数派确认"
        ),
        Ok(Err(message)) => assert!(
            !message.is_empty(),
            "转发失败时必须说明原因，实际是一条空错误"
        ),
    }

    // ---- 2) 但它仍然**收得到**主节点推过来的数据 ----
    //
    // 这是本条测试与「杀掉节点」最关键的区别：那个方向是通的。
    // 用 `last_applied` 判定，因为从节点此刻要不到读索引，没法通过读取来确认。
    for index in 0..10 {
        cluster.set(leader, &format!("pushed-{index}"), "v").await;
    }

    let target = cluster.node(leader).metrics().last_log_index.unwrap_or(0);
    assert!(
        cluster
            .node(follower)
            .wait_for_applied(target, CATCHUP_TIMEOUT)
            .await,
        "从节点 {follower} 没能跟上主节点的日志。\
         它发不出去是预期的，但**收**得到——主节点推过来的复制的回音走的是同一条\
         连接，不该被这个方向的分区影响。"
    );

    // ---- 3) 主节点这一侧完全不受影响 ----
    cluster.set(leader, "on-leader", "ok").await;
    assert_eq!(cluster.get(leader, "on-leader").await, bulk("ok"));

    // ---- 4) 愈合之后，从节点必须能重新写入 ----
    cluster.heal(follower, leader);

    cluster.set(follower, "after-heal", "ok").await;
    assert_eq!(
        cluster.get(leader, "after-heal").await,
        bulk("ok"),
        "愈合之后从节点的写入没有到达主节点"
    );

    // 那条没能转出去的写入，始终没有生效过
    assert_eq!(
        cluster.get(leader, "via-follower").await,
        Reply::Null,
        "从节点上失败的转发竟然在主节点上留下了数据"
    );
}
