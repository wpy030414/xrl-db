//! 快照**经过网络**装到一个落后的节点上。
//!
//! # 为什么这条路径需要单独盯住
//!
//! 「日志只能靠快照截断」已经由 `tests/snapshot.rs` 盯住了，但那只覆盖了**单机**：
//! 进程重启，从本地磁盘上的快照恢复。多节点场景下还有第二条路径——一个落后的节点
//! 向主节点要数据时，主节点手上已经没有它需要的日志了（那些早被截断），于是只能把
//! **整份快照通过 RPC 传过去**。
//!
//! 这条路径落在 `src/raft/state_machine.rs` 的 `install_snapshot` 与
//! `src/raft/network.rs` 的 `install_snapshot` 上。代码写好了，**但没有任何测试
//! 走到过它**：其余集群测试里的日志都很短，节点之间的追赶一律走日志复制。
//! 一条从没被执行过的代码路径，等于没有代码。
//!
//! # 怎么证明走的确实是快照那条路
//!
//! 「重启之后数据是对的」不足以证明什么——日志复制也能得到同样的结果。所以先把
//! 环境逼到「日志复制不可能」的地步：
//!
//! 1. 干掉一个从节点，记下它当时走到哪条日志（记作 `V`）；
//! 2. 继续写，直到主节点的 `purged` **超过** `V`。那之后主节点手上已经没有
//!    `V+1` 往后的日志了，能让这个节点追上的办法**只剩**传一份快照；
//! 3. 让它回来并追平。此时它的快照位置必须**超过 `V`**——它停机期间不可能自己
//!    产生更新的快照，而那段日志又已经不存在了。这个数字只可能来自一次经过网络
//!    传输的快照安装。
//!
//! 第 2 步是**断言 A**，它保证这个测试是有效的；第 3 步是**断言 B**，它保证测的
//! 是快照而不是日志。少了 A，测试可能悄悄退化成「又测了一遍日志复制」，而且不会
//! 有任何迹象；少了 B，它只是在断言「数据没丢」——那是别的测试的职责。

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::impls::BasicNode;

mod common;

use bytes::Bytes;
use xrl_db::config::{Config, RaftConfig};
use xrl_db::kv::WriteOp;
use xrl_db::node::Node;
use xrl_db::protocol::{Reply, SetCondition};

const CLUSTER_SIZE: u64 = 3;

/// 累计多少条日志触发一次快照。刻意取小——策略只决定快照来得早晚，不影响被测逻辑。
const SNAPSHOT_EVERY: u64 = 20;

/// 快照之外保留的日志条数。同样取小，让截断来得干脆。
const KEEP_LOGS: u64 = 4;

/// 干掉从节点之前写入的键数。
const KEYS_BEFORE_KILL: u64 = 120;

/// 干掉从节点之后写入的键数。必须足够多，让主节点的截断点越过牺牲者。
const KEYS_AFTER_KILL: u64 = 120;

/// 等待选举的上限。
const ELECTION_TIMEOUT: Duration = Duration::from_secs(15);

/// 等待重启的节点靠快照追平的上限。
///
/// 给得宽松：这里要证明的是「能追上」，而不是「多快追上」。
const CATCHUP_TIMEOUT: Duration = Duration::from_secs(30);

/// 一个跑在进程内的三节点集群，支持把节点关掉再原样拉起来。
struct TestCluster {
    nodes: Vec<Option<Arc<Node>>>,
    addrs: Vec<(u64, SocketAddr)>,
    /// 数据目录必须留住——`TempDir` 析构会连数据一起删掉，而「用一个空目录重启」
    /// 什么也验证不到。
    _dirs: Vec<tempfile::TempDir>,
    /// 重启时要靠它找回同一个端口、同一份数据目录、同一套 Raft 参数。
    configs: Vec<Config>,
}

impl TestCluster {
    async fn start() -> Self {
        let dirs: Vec<tempfile::TempDir> = (0..CLUSTER_SIZE)
            .map(|_| tempfile::tempdir().expect("应能创建临时目录"))
            .collect();

        let addrs = common::find_port_pairs(CLUSTER_SIZE);

        let raft = RaftConfig {
            snapshot_logs_since_last: SNAPSHOT_EVERY,
            max_in_snapshot_log_to_keep: KEEP_LOGS,
            ..common::test_raft_config()
        };

        let configs: Vec<Config> = (0..CLUSTER_SIZE)
            .map(|index| {
                common::cluster_config(
                    index + 1,
                    &addrs,
                    dirs[index as usize].path().to_path_buf(),
                    raft.clone(),
                )
            })
            .collect();

        let mut nodes = Vec::new();
        for config in &configs {
            // 用 `start` 而非 `start_single`：多节点集群必须显式引导，
            // 否则每个节点都会试图把自己变成单节点集群
            let node = Node::start(config.clone()).await.expect("应能启动节点");
            nodes.push(Some(Arc::new(node)));
        }

        let cluster = Self {
            nodes,
            addrs,
            _dirs: dirs,
            configs,
        };
        cluster.bootstrap().await;
        cluster
    }

    /// 引导集群：先让 1 号成为单节点集群，再把 2、3 号作为学习者加入，最后转为正式成员。
    ///
    /// 先做学习者让它们追赶，追上了再转正——直接让还没追平的节点参与投票，
    /// 会在它数据还不全的时候把它计入多数派。
    async fn bootstrap(&self) {
        let first = self.node(1);
        let members = BTreeMap::from([(1, BasicNode::new(self.addr_of(1).to_string()))]);
        first.initialize(members).await.expect("应能初始化集群");

        for id in 2..=CLUSTER_SIZE {
            first
                .add_learner(id, BasicNode::new(self.addr_of(id).to_string()))
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

    fn addr_of(&self, id: u64) -> SocketAddr {
        self.addrs
            .iter()
            .find(|(peer_id, _)| *peer_id == id)
            .map(|(_, addr)| *addr)
            .expect("节点地址应存在")
    }

    /// 关掉一个节点，并把它从存活集合里摘掉。
    ///
    /// 必须把 `Arc` 也放掉：redb 是单进程的（文件锁），只要还有一份 `Database`
    /// 的引用没被丢弃，重新打开同一份数据就会以「数据库已被打开」失败。
    async fn kill(&mut self, id: u64) {
        if let Some(node) = self.nodes[(id - 1) as usize].take() {
            node.shutdown().await;
            drop(node);
        }
    }

    /// 用**同一个数据目录**把节点重新拉起来。
    ///
    /// 它带着自己停机时的日志与快照回来，而集群已经往前走了很远。
    async fn restart(&mut self, id: u64) {
        let config = self.configs[(id - 1) as usize].clone();
        let node = Node::start(config)
            .await
            .unwrap_or_else(|error| panic!("重启节点 {id} 失败：{error}"));

        self.nodes[(id - 1) as usize] = Some(Arc::new(node));
    }

    /// 等待集群选出主节点，返回其编号。
    async fn wait_for_leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + ELECTION_TIMEOUT;

        loop {
            // 以多数派都认可的 leader 为准，避免采信某个节点尚未更新的视图
            let mut votes = Vec::new();
            for id in 1..=CLUSTER_SIZE {
                let Some(node) = self.nodes[(id - 1) as usize].as_ref() else {
                    continue;
                };
                if let Some(leader) = node.leader() {
                    votes.push(leader);
                }
            }

            if votes.len() >= 2
                && let Some(&leader) = votes.first()
                && votes.iter().all(|candidate| *candidate == leader)
            {
                return leader;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "等待 {ELECTION_TIMEOUT:?} 仍未选出获得多数派认可的主节点，当前选票：{votes:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 取一个**存活且不是主节点**的节点。
    fn follower_of(&self, leader: u64) -> u64 {
        (1..=CLUSTER_SIZE)
            .find(|id| *id != leader && self.nodes[(*id - 1) as usize].is_some())
            .expect("集群中必然存在存活的从节点")
    }

    /// 写一个键。
    async fn set(&self, id: u64, index: u64) {
        self.node(id)
            .write(set_op(index))
            .await
            .unwrap_or_else(|error| panic!("往节点 {id} 写入 key-{index} 失败：{error}"));
    }

    /// 读一个键。
    async fn get(&self, id: u64, index: u64) -> Reply {
        let key = Bytes::from(format!("key-{index}"));
        self.node(id)
            .read(move |store| store.get(&key, xrl_db::kv::now_ms()))
            .await
            .unwrap_or_else(|error| panic!("在节点 {id} 上读取 key-{index} 失败：{error}"))
    }
}

/// 一个键的取值——写入时用的就是它。
fn expected(index: u64) -> Reply {
    Reply::Bulk(Bytes::from(index.to_string()))
}

/// 构造一个键的写入操作。
fn set_op(index: u64) -> WriteOp {
    WriteOp::Set {
        key: Bytes::from(format!("key-{index}")),
        value: Bytes::from(index.to_string()),
        expire_at: None,
        condition: SetCondition::Always,
    }
}

/// 等某个节点的快照位置超过 `index`，返回最终位置。
///
/// 用轮询而不是读一次：快照安装完成后，`metrics().snapshot` 的更新可能与
/// 「状态机已经装好了」不在同一个瞬间。等一小会儿再断言，断言的仍然是同一件事，
/// 只是不会因为指标刷新的时机而随机失败。
async fn wait_for_snapshot_past(
    cluster: &TestCluster,
    id: u64,
    index: u64,
    timeout: Duration,
) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let current = cluster
            .node(id)
            .metrics()
            .snapshot
            .map(|log_id| log_id.index)
            .unwrap_or(0);

        if current > index {
            return current;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "等了 {timeout:?}，节点 {id} 的快照位置仍停在 {current}，没有超过 {index}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ==================================================================== 测试

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lagging_node_catches_up_from_a_snapshot_over_the_network() {
    let mut cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    // ---- 阶段一：让整个集群追平，并各自建过快照 ----
    for index in 0..KEYS_BEFORE_KILL {
        cluster.set(leader, index).await;
    }

    // ---- 挑一个从节点干掉，记下它当时走到哪条日志 ----
    let victim = cluster.follower_of(leader);
    let victim_last_log = cluster.node(victim).metrics().last_log_index.unwrap_or(0);
    cluster.kill(victim).await;

    // ---- 阶段二：继续写，直到主节点把日志截断到牺牲者够不着的地方 ----
    for index in KEYS_BEFORE_KILL..KEYS_BEFORE_KILL + KEYS_AFTER_KILL {
        cluster.set(leader, index).await;
    }

    let purged = cluster
        .node(leader)
        .metrics()
        .purged
        .map(|log_id| log_id.index)
        .unwrap_or(0);

    // ★ 断言 A：这个测试有效的前提 ★
    //
    // 截断点必须越过牺牲者停下的位置。否则它需要的日志还在主节点手上，追上它
    // 只需日志复制——测试会**静默地**退化成「又测了一遍日志复制」，不会有任何迹象。
    assert!(
        purged > victim_last_log,
        "主节点的日志只截断到 {purged}，而节点 {victim} 停在 {victim_last_log}：\
         它需要的日志还在，追上它只需日志复制，这个测试没能测到快照路径。\
         把 KEYS_AFTER_KILL（现在是 {KEYS_AFTER_KILL}）调大一些。"
    );

    // ---- 让它带着自己的旧日志回来 ----
    cluster.restart(victim).await;

    let current = cluster.wait_for_leader().await;
    let target = cluster.node(current).metrics().last_log_index.unwrap_or(0);

    assert!(
        cluster
            .node(victim)
            .wait_for_applied(target, CATCHUP_TIMEOUT)
            .await,
        "重启的节点 {victim} 没能在 {CATCHUP_TIMEOUT:?} 内追上日志位置 {target}"
    );

    // ★ 断言 B：追上它的那份快照，不是它自己能产生的 ★
    //
    // 它停机期间不可能产生更新的快照；而它需要的那段日志又已经被截断
    // （断言 A 保证）。所以这个数字只可能来自一次经过网络传输的快照安装。
    let snapshot_after =
        wait_for_snapshot_past(&cluster, victim, victim_last_log, CATCHUP_TIMEOUT).await;
    assert!(
        snapshot_after > victim_last_log,
        "节点 {victim} 的快照位置是 {snapshot_after}，没有超过它停机时的日志位置 \
         {victim_last_log}；而主节点已经截断到 {purged}"
    );

    // ---- 数据必须完整：靠快照装出来的状态机，要和写进去的一致 ----
    for index in 0..KEYS_BEFORE_KILL + KEYS_AFTER_KILL {
        assert_eq!(
            cluster.get(victim, index).await,
            expected(index),
            "key-{index} 在靠快照追上的节点上读不出来——传过来的快照不完整"
        );
    }

    // ---- 它必须重新成为一个能正常服务的成员 ----
    let follower_write = KEYS_BEFORE_KILL + KEYS_AFTER_KILL;
    let leader_now = cluster.wait_for_leader().await;
    cluster.set(leader_now, follower_write).await;
    assert_eq!(
        cluster.get(victim, follower_write).await,
        expected(follower_write),
        "追平之后新写入的键在节点 {victim} 上读不出来——它没有真正回到成员集合里"
    );
}
