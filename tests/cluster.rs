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

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::impls::BasicNode;

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
        let addrs = common::find_port_pairs(CLUSTER_SIZE);

        let mut nodes = Vec::new();
        for index in 0..CLUSTER_SIZE {
            let id = index + 1;
            let config = common::cluster_config(
                id,
                &addrs,
                dirs[index as usize].path().to_path_buf(),
                common::test_raft_config(),
            );

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

    /// 关掉一个节点，并把它从存活集合里摘掉。
    ///
    /// **必须摘掉**：只调用 `shutdown` 而保留那个 `Arc`，后续按「存活」筛选节点时
    /// 就会选中一个 Raft 已经停止的实例。症状是测试随机失败，且失败信息里出现
    /// 「本节点在处理请求时停止服务」——看起来像转发逻辑有 bug，其实是测试自己
    /// 把请求发给了一个已经死掉的节点。这个坑值得用一次崩溃来记住。
    async fn kill(&mut self, id: u64) {
        if let Some(node) = self.nodes[(id - 1) as usize].take() {
            node.shutdown().await;
        }
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
            .unwrap_or_else(|error| panic!("往节点 {id} 写入 {key} 失败：{error}"))
    }

    /// 对键做一次自增。
    ///
    /// 刻意挑一条**非幂等**的命令来验证转发：它最能暴露「把结果未知当成失败来重试」
    /// 这类缺陷——重试一次，计数器就会多加一。
    async fn incr(&self, id: u64, key: &str) -> Reply {
        self.node(id)
            .write(WriteOp::IncrBy {
                key: bytes::Bytes::copy_from_slice(key.as_bytes()),
                delta: 1,
            })
            .await
            .expect("自增应当成功")
    }

    /// 取一个**存活且不是主节点**的节点。
    ///
    /// 必须同时检查存活：在故障转移测试里，编号最小的非主节点很可能正是刚被关掉的
    /// 那一个，直接挑它会得到一个永远连不上的地址。
    fn follower_of(&self, leader: u64) -> u64 {
        (1..=CLUSTER_SIZE)
            .find(|id| *id != leader && self.nodes[(*id - 1) as usize].is_some())
            .expect("集群中必然存在存活的从节点")
    }

    /// 读一个键。
    async fn get(&self, id: u64, key: &str) -> Reply {
        let key = bytes::Bytes::copy_from_slice(key.as_bytes());
        self.node(id)
            .read(move |store| store.get(&key, xrl_db::kv::now_ms()))
            .await
            .expect("读取应当成功")
    }

    /// 尝试写入，失败时把错误文本带出来而不是 panic。
    ///
    /// 与 [`TestCluster::set`] 的区别：那个用于「必须成功」的写入，这个用于
    /// 「必须失败」的断言。在辅助函数里直接 panic 会把断言本身也一并吞掉。
    async fn try_set(&self, id: u64, key: &str, value: &str) -> Result<Reply, String> {
        self.node(id)
            .write(WriteOp::Set {
                key: bytes::Bytes::copy_from_slice(key.as_bytes()),
                value: bytes::Bytes::copy_from_slice(value.as_bytes()),
                expire_at: None,
                condition: SetCondition::Always,
            })
            .await
            .map_err(|error| error.to_string())
    }

    /// 尝试读取，失败时把错误文本带出来而不是 panic。
    async fn try_get(&self, id: u64, key: &str) -> Result<Reply, String> {
        let key = bytes::Bytes::copy_from_slice(key.as_bytes());
        self.node(id)
            .read(move |store| store.get(&key, xrl_db::kv::now_ms()))
            .await
            .map_err(|error| error.to_string())
    }
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
    let mut cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    const KEY_COUNT: usize = 100;
    for index in 0..KEY_COUNT {
        cluster
            .set(leader, &format!("key-{index}"), &format!("value-{index}"))
            .await;
    }

    // 杀掉主节点
    cluster.kill(leader).await;

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
    let mut cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    cluster.set(leader, "before", "1").await;
    cluster.kill(leader).await;

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

// ============================================================ 客户端转发
//
// 下面这组测试验证的是立项时列出的另一条验收标准：
//
// > **客户端连任意一个节点都能读写。**
//
// Redis Cluster 做不到这一点：连到从节点的写入会被 `MOVED` 重定向弹回去，客户端
// 必须自己维护槽位映射、自己找主节点。我们替客户端把这件事做完。

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_forwards_writes_to_the_leader() {
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;
    let follower = cluster.follower_of(leader);

    // 写入发到从节点——服务端应当转交给主节点，而不是回一个「我不是主节点」
    assert_eq!(
        cluster.set(follower, "via-follower", "hello").await,
        Reply::ok(),
        "发往从节点的写入应当被转发，而不是失败"
    );

    // 数据必须真的进了集群：主节点与从节点都读得到
    let expected = Reply::Bulk(bytes::Bytes::from_static(b"hello"));
    assert_eq!(
        cluster.get(leader, "via-follower").await,
        expected,
        "转发过去的写入必须真的被提交"
    );
    assert_eq!(cluster.get(follower, "via-follower").await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarding_does_not_duplicate_non_idempotent_writes() {
    // ★ 这条是转发路径上最要命的缺陷模式 ★
    //
    // 自增不是幂等的：一旦「主节点其实已经执行了，只是响应丢了」被当成失败重试，
    // 计数器就会多加。这里连着做 20 次自增并核对最终值——多执行一次都会被抓出来。
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;
    let follower = cluster.follower_of(leader);

    const ROUNDS: i64 = 20;
    for round in 1..=ROUNDS {
        assert_eq!(
            cluster.incr(follower, "counter").await,
            Reply::Integer(round),
            "第 {round} 次自增的返回值不符——多一次或少一次都说明转发路径重复执行了命令"
        );
    }

    assert_eq!(
        cluster.get(leader, "counter").await,
        Reply::Bulk(bytes::Bytes::from(ROUNDS.to_string())),
        "转发 20 次自增之后，计数器的值必须精确等于 20"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_reads_the_latest_committed_write() {
    // 从节点的读不能返回旧数据。这里「写完立刻从从节点读」，中间不给任何等待——
    // 如果读路径没有先确认读索引、没有等状态机追平，就会读到空值。
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;
    let follower = cluster.follower_of(leader);

    for round in 0..20 {
        let key = format!("latest-{round}");
        let value = format!("value-{round}");

        cluster.set(leader, &key, &value).await;
        assert_eq!(
            cluster.get(follower, &key).await,
            Reply::Bulk(bytes::Bytes::from(value.clone())),
            "从节点读不到刚刚提交的写入（第 {round} 轮）——读路径没有做到线性一致"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarding_survives_a_leader_change() {
    // 客户端一直连在同一个**从节点**上，期间主节点挂了、集群换了主节点。
    // 客户端不该有任何感知：它既不需要重连，也不需要知道主节点换成了谁。
    let mut cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;
    let follower = cluster.follower_of(leader);

    cluster.set(follower, "before", "1").await;

    // 杀掉主节点
    cluster.kill(leader).await;

    // 剩下的两个节点里必然有一个成为新主节点，从节点也会更新自己的视图
    let new_leader = wait_for_leader_among(&cluster, 1..=CLUSTER_SIZE, Some(leader))
        .await
        .expect("剩余节点应能选出新主节点");

    // 仍然往原来那个从节点写——它现在应当转发给新主节点
    let target = if follower == new_leader {
        // 原来那个从节点自己当上了主节点，那就换一个从节点来验证转发
        cluster.follower_of(new_leader)
    } else {
        follower
    };

    assert_eq!(
        cluster.set(target, "after", "2").await,
        Reply::ok(),
        "主节点变更后，从节点应能转发到新主节点"
    );
    assert_eq!(
        cluster.get(new_leader, "after").await,
        Reply::Bulk(bytes::Bytes::from_static(b"2"))
    );
    // 换届之前的数据一个都不能丢
    assert_eq!(
        cluster.get(new_leader, "before").await,
        Reply::Bulk(bytes::Bytes::from_static(b"1"))
    );
}

// ============================================================ 失去多数派
//
// 这一组验证 CP 语义：「分区时拒绝写入」不是一句口号，而是可观测的行为。

/// 连从节点的一次读写允许的中位耗时上限。
///
/// 取 100 毫秒：正常值是个位数到几十毫秒（实测读约 1.4ms、写约 30ms），而退避
/// 缺陷会在这之上再加满 200 毫秒——两边都离这条界很远，既不会误报也不会漏报。
const FOLLOWER_MEDIAN_BUDGET: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_does_not_pay_a_retry_delay() {
    // ★ 这条测试锁定的是一个真实修复过的性能缺陷 ★
    //
    // 从节点的第 0 次尝试必然在本地得到 `ForwardToLeader`——它本来就不是主节点。
    // 而退避逻辑曾经不分青红皂白地作用于**每一次**尝试，于是转发的第一步要先睡满
    // RETRY_DELAY（200ms）：**从节点上的每一次读写都固定多花 200 毫秒**。
    //
    // 实测一次读 205ms，240 次读要 49 秒；修好之后 240 次读 0.33 秒。
    //
    // 这个缺陷不会让任何断言变红，它只是让「客户端连任意节点都能读写」这个卖点
    // 在实际使用中难以忍受——而那种退化是没有任何测试会替我们发现的。
    // 因此这里必须专门盯住**量级**，不能只盯正确性。
    let cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;
    let follower = cluster.follower_of(leader);

    // 先写一个键，让后面读到的是真实存在的数据
    cluster.set(leader, "warmup", "1").await;

    const SAMPLES: usize = 21;

    let mut read_times = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = std::time::Instant::now();
        let value = cluster.get(follower, "warmup").await;
        read_times.push(started.elapsed());

        assert_eq!(
            value,
            Reply::Bulk(bytes::Bytes::from_static(b"1")),
            "连从节点的读拿到了错的数据"
        );
    }

    let mut write_times = Vec::with_capacity(SAMPLES);
    for index in 0..SAMPLES {
        let started = std::time::Instant::now();
        cluster.set(follower, &format!("probe-{index}"), "x").await;
        write_times.push(started.elapsed());
    }

    // 用中位数而不是最大值：单次抖动（线程调度、磁盘 fsync）不该让这条断言变红，
    // 而退避缺陷会让**每一次**都慢，中位数必然被顶上去。
    for (label, mut times) in [("读", read_times), ("写", write_times)] {
        times.sort();
        let median = times[times.len() / 2];
        let worst = times[times.len() - 1];

        assert!(
            median < FOLLOWER_MEDIAN_BUDGET,
            "连从节点的{label}操作中位耗时 {median:?}（最慢 {worst:?}），\
             超过了 {FOLLOWER_MEDIAN_BUDGET:?}。从节点转发本身是确定要做的动作，\
             为它先退避一次 RETRY_DELAY 会让每一次读写都固定多花 200 毫秒。"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_minority_refuses_writes_and_never_hangs() {
    // ★ 这条测试锁定的是一个真实修复过的缺陷 ★
    //
    // 孤立的主节点（失去多数派之后）调用 `client_write` 会一直等待一个不可能到来的
    // 多数派确认，**永远不会返回**——客户端于是永远收不到回复，连接一直挂着。
    // 比起一个明确的错误，一个永不返回的请求要糟糕得多：调用方连「要不要重试」
    // 都无从判断。
    //
    // 所以这里既断言「拒绝」，也断言「在有限时间内拒绝」。
    let mut cluster = TestCluster::start().await;
    let leader = cluster.wait_for_leader().await;

    // 先写一个键，确认多数派正常时一切照常
    cluster.set(leader, "before-partition", "1").await;

    // 干掉两个节点，只留一个——无论它是不是主节点，都不再构成多数派
    let doomed: Vec<u64> = (1..=CLUSTER_SIZE).filter(|id| *id != leader).collect();
    for id in &doomed {
        cluster.kill(*id).await;
    }
    assert_eq!(survivors(&cluster), 1, "只剩一个节点，已经没有多数派");

    // 此时无论连到幸存者的是写还是读，都必须得到一个错误，而且必须**及时**得到
    for attempt in 0..3 {
        let outcome = tokio::time::timeout(
            MAJORITY_LOST_TIMEOUT,
            cluster.try_set(leader, "should-not-land", "x"),
        )
        .await;

        match outcome {
            // 外面的超时先触发，说明请求挂住了——这正是要防的那个缺陷
            Err(_elapsed) => panic!(
                "失去多数派之后，第 {attempt} 次写入在 {MAJORITY_LOST_TIMEOUT:?} 内没有返回。\
                 请求挂起比返回错误糟糕得多：调用方无从判断该不该重试。"
            ),
            Ok(Ok(reply)) => panic!(
                "失去多数派之后写入竟然成功了（返回 {reply:?}）。\
                 这是脑裂的前兆：少数派必须拒绝写入。"
            ),
            Ok(Err(message)) => {
                assert!(
                    !message.is_empty(),
                    "拒绝写入时必须说明原因，实际是一条空错误"
                );
            }
        }
    }

    // 读同样必须被拒绝：无法确认时效性的读等于返回可能过期的数据
    let read = tokio::time::timeout(
        MAJORITY_LOST_TIMEOUT,
        cluster.try_get(leader, "before-partition"),
    )
    .await
    .expect("失去多数派之后的读取也必须及时返回")
    .expect_err("失去多数派之后读取必须被拒绝，而不是返回可能过期的值");

    assert!(!read.is_empty(), "拒绝读取时必须说明原因，实际是一条空错误");
}

/// 等待「失去多数派之后」的操作返回的上限。
///
/// 给得比 `LOCAL_TIMEOUT` 宽松得多：这里要证明的是「不会永远挂住」，
/// 而不是「多快返回」。
const MAJORITY_LOST_TIMEOUT: Duration = Duration::from_secs(30);

/// 统计还活着的节点数。
fn survivors(cluster: &TestCluster) -> usize {
    cluster.nodes.iter().filter(|node| node.is_some()).count()
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
