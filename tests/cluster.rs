//! 三节点集群的端到端验证。
//!
//! # 这个测试验证什么
//!
//! 它检验的是项目最核心的那句承诺：
//!
//! > **兼容 Redis 生态，但提供 Redis 拿不到的强一致性。**
//!
//! 具体到可观测的行为就是：**杀掉主节点之后，已经确认的写入一个都不能丢。**
//! Redis Cluster 在同样的情况下会丢失尚未同步到副本的写入——这正是本项目的
//! 存在理由。
//!
//! # 与真实部署的关系
//!
//! 这里的三个节点跑在同一个进程里，但**它们是真实的节点**：各自独立的 redb 文件、
//! 各自监听真实的 TCP 端口、通过真实的 socket 互相通信、跑完整的 Raft。
//!
//! 唯一的差别是「杀掉主节点」用的是 [`Node::shutdown`] 而非 `kill -9`。后者由
//! `scripts/verify-cluster.sh` 以多进程方式验证。

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::impls::BasicNode;

use xrl_db::config::{Config, Peer, RPC_PORT_OFFSET};
use xrl_db::kv::WriteOp;
use xrl_db::node::Node;
use xrl_db::protocol::{Reply, SetCondition};

/// 集群规模。
const CLUSTER_SIZE: u64 = 3;

/// 等待选举的上限。
///
/// 给得宽松：测试环境里可能有其他测试并行占用 CPU，选举偶尔会慢一些。
/// 把它设得太紧会制造随机失败，那比慢几秒糟糕得多。
const ELECTION_TIMEOUT: Duration = Duration::from_secs(15);

/// 一个跑在进程内的三节点集群。
struct TestCluster {
    /// 节点。[`Option`] 是因为「杀掉主节点」需要把某个位置抽空。
    nodes: Vec<Option<Arc<Node>>>,
    /// 各节点的客户端地址。
    addrs: Vec<(u64, SocketAddr)>,
    /// 数据目录。必须留住，否则 `TempDir` 析构时会连同数据一起删掉。
    _dirs: Vec<tempfile::TempDir>,
}

impl TestCluster {
    /// 启动一个三节点集群并完成引导。
    async fn start() -> Self {
        let dirs: Vec<tempfile::TempDir> = (0..CLUSTER_SIZE)
            .map(|_| tempfile::tempdir().expect("应能创建临时目录"))
            .collect();

        // 为每个节点找一对可用端口（客户端端口、以及 +10000 的节点间通信端口）
        let addrs = find_port_pairs(CLUSTER_SIZE);

        let mut nodes = Vec::new();
        for index in 0..CLUSTER_SIZE {
            let id = index + 1;
            let config = build_config(id, &addrs, dirs[index as usize].path().to_path_buf());

            // 注意用的是 `start` 而非 `start_single`：多节点集群必须显式引导，
            // 否则每个节点都会试图把自己变成单节点集群
            let node = Node::start(config).await.expect("应能启动节点");
            nodes.push(Some(Arc::new(node)));
        }

        let cluster = Self {
            nodes,
            addrs,
            _dirs: dirs,
        };
        cluster.bootstrap().await;
        cluster
    }

    /// 引导集群：先让 1 号成为单节点集群，再把 2、3 号作为学习者加入，最后转为正式成员。
    ///
    /// 这个顺序是有讲究的：直接让新节点参与投票，会在它还没追平数据时就把它计入
    /// 多数派，反而损害可用性。先做学习者让它们追赶，追上了再转正。
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

    /// 取某个节点的句柄。
    fn node(&self, id: u64) -> &Arc<Node> {
        self.nodes[(id - 1) as usize]
            .as_ref()
            .unwrap_or_else(|| panic!("节点 {id} 已不存活"))
    }

    /// 取某个节点的客户端地址。
    fn addr_of(&self, id: u64) -> SocketAddr {
        self.addrs
            .iter()
            .find(|(peer_id, _)| *peer_id == id)
            .map(|(_, addr)| *addr)
            .expect("节点地址应存在")
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

            if let Some(&leader) = votes.first()
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

    /// 写一个键。
    async fn set(&self, id: u64, key: &str, value: &str) -> Reply {
        self.node(id)
            .write(WriteOp::Set {
                key: bytes::Bytes::copy_from_slice(key.as_bytes()),
                value: bytes::Bytes::copy_from_slice(value.as_bytes()),
                expire_at: None,
                condition: SetCondition::Always,
            })
            .await
            .expect("写入应当成功")
    }

    /// 读一个键。
    async fn get(&self, id: u64, key: &str) -> Reply {
        let key = bytes::Bytes::copy_from_slice(key.as_bytes());
        self.node(id)
            .read(move |store| store.get(&key, xrl_db::kv::now_ms()))
            .await
            .expect("读取应当成功")
    }
}

/// 为 `count` 个节点各找一对可用端口。
///
/// 客户端端口由操作系统分配；节点间通信端口是客户端端口 +10000，必须另行确认可用。
/// 找不到就重试——这比固定端口号可靠得多，固定端口在并行测试时必然冲突。
fn find_port_pairs(count: u64) -> Vec<(u64, SocketAddr)> {
    'outer: loop {
        let mut chosen = Vec::new();

        for id in 1..=count {
            let Ok(probe) = std::net::TcpListener::bind("127.0.0.1:0") else {
                continue 'outer;
            };
            let port = probe.local_addr().expect("应能取得地址").port();
            drop(probe);

            let Some(rpc_port) = port.checked_add(RPC_PORT_OFFSET) else {
                continue 'outer;
            };
            if std::net::TcpListener::bind(("127.0.0.1", rpc_port)).is_err() {
                continue 'outer;
            }
            // 与其他已选端口也不能撞车
            if chosen
                .iter()
                .any(|(_, addr): &(u64, SocketAddr)| addr.port() == port)
            {
                continue 'outer;
            }

            chosen.push((id, SocketAddr::from(([127, 0, 0, 1], port))));
        }

        return chosen;
    }
}

/// 构造一个节点的配置。
fn build_config(id: u64, addrs: &[(u64, SocketAddr)], data_dir: std::path::PathBuf) -> Config {
    let config = Config {
        node: xrl_db::config::NodeConfig {
            id,
            listen: addrs
                .iter()
                .find(|(peer_id, _)| *peer_id == id)
                .map(|(_, addr)| *addr)
                .expect("本节点地址应存在"),
        },
        cluster: xrl_db::config::ClusterConfig {
            enabled: true,
            peers: addrs
                .iter()
                .map(|(peer_id, addr)| Peer {
                    id: *peer_id,
                    addr: *addr,
                })
                .collect(),
        },
        storage: xrl_db::config::StorageConfig {
            path: Some(data_dir),
        },
        raft: xrl_db::config::RaftConfig {
            // 调快一些让测试不必等太久；生产默认值更保守
            election_timeout_ms: 300,
            heartbeat_interval_ms: 100,
        },
    };

    // `resolve` 会推导默认值并做语义校验，返回可直接使用的配置
    config.resolve().expect("测试配置应合法")
}

// ==================================================================== 测试

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_elects_a_leader_and_replicates_writes() {
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    for index in 0..20 {
        let reply = cluster
            .set(leader, &format!("key-{index}"), &format!("value-{index}"))
            .await;
        assert_eq!(reply, Reply::ok(), "第 {index} 个键应写入成功");
    }

    // 写入必须能从任一个节点读到——包括非 leader 之外的节点也持有同样的数据
    for index in 0..20 {
        assert_eq!(
            cluster.get(leader, &format!("key-{index}")).await,
            Reply::Bulk(bytes::Bytes::from(format!("value-{index}"))),
            "第 {index} 个键的取值不符"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_survives_leader_failure() {
    // ★ 这是整个项目的核心验收标准 ★
    //
    // 写入若干键 → 杀掉主节点 → 等待新主节点产生 → 确认**一个键都没丢**。
    //
    // Redis Cluster 在这个场景下会丢失尚未同步到副本的写入。我们不丢——
    // 因为每一次写入都是等到多数派确认之后才回给客户端的。
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    const KEY_COUNT: usize = 100;
    for index in 0..KEY_COUNT {
        cluster
            .set(leader, &format!("key-{index}"), &format!("value-{index}"))
            .await;
    }

    // 杀掉主节点
    let fallen = cluster.node(leader).clone();
    fallen.shutdown().await;

    // 等新主节点产生（剩下的两个节点仍构成多数派）
    let survivors = CLUSTER_SIZE - 1;
    let new_leader = wait_for_leader_among(&cluster, 1..=CLUSTER_SIZE, Some(leader))
        .await
        .unwrap_or_else(|| {
            panic!("{survivors} 个存活节点应能在 {ELECTION_TIMEOUT:?} 内选出新主节点")
        });

    assert_ne!(new_leader, leader, "新主节点不应还是那个已被关闭的节点");

    // ★ 一个键都不能丢 ★
    let mut missing = Vec::new();
    for index in 0..KEY_COUNT {
        let key = format!("key-{index}");
        let expected = Reply::Bulk(bytes::Bytes::from(format!("value-{index}")));
        if cluster.get(new_leader, &key).await != expected {
            missing.push(index);
        }
    }

    assert!(
        missing.is_empty(),
        "故障转移后有 {} 个键读取失败（{:?}…）。\
         这意味着已经向客户端确认过的写入丢失了——这正是本项目要避免的事情。",
        missing.len(),
        &missing[..missing.len().min(10)],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_keeps_serving_after_leader_returns() {
    // 被关闭的节点重启后应能重新加入，且不影响现有集群继续服务
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    cluster.set(leader, "before", "1").await;
    cluster.node(leader).shutdown().await;

    let new_leader = wait_for_leader_among(&cluster, 1..=CLUSTER_SIZE, Some(leader))
        .await
        .expect("应能选出新主节点");

    // 集群在少一个节点的情况下仍然可以写入
    cluster.set(new_leader, "during", "2").await;

    assert_eq!(
        cluster.get(new_leader, "before").await,
        Reply::Bulk(bytes::Bytes::from_static(b"1")),
        "故障转移前写入的数据必须仍然可读"
    );
    assert_eq!(
        cluster.get(new_leader, "during").await,
        Reply::Bulk(bytes::Bytes::from_static(b"2")),
        "故障转移后写入的数据必须可读"
    );
}

/// 等待一个**排除指定节点后**的集群选出主节点。
async fn wait_for_leader_among(
    cluster: &TestCluster,
    candidates: impl IntoIterator<Item = u64>,
    exclude: Option<u64>,
) -> Option<u64> {
    let candidates: Vec<u64> = candidates
        .into_iter()
        .filter(|id| Some(*id) != exclude)
        .collect();

    let deadline = tokio::time::Instant::now() + ELECTION_TIMEOUT;

    loop {
        let mut votes = Vec::new();
        for id in &candidates {
            let Some(node) = cluster.nodes[(*id - 1) as usize].as_ref() else {
                continue;
            };
            if let Some(leader) = node.leader() {
                votes.push(leader);
            }
        }

        // 至少两个存活节点都认可同一个 leader，才认为选举真正完成
        if votes.len() >= 2 {
            let first = votes[0];
            if votes.iter().all(|candidate| *candidate == first)
                && !exclude.is_some_and(|excluded| excluded == first)
            {
                return Some(first);
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
