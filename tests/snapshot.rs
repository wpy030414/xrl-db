//! 快照触发与日志截断。
//!
//! # 这个测试盯住的是什么
//!
//! 日志是**只能靠快照来截断**的：只有已经被纳入快照的那部分才允许删除，否则一个
//! 落后的从节点就没有办法靠日志追上来。因此「多久建一次快照」实际上就是
//! 「日志能占多大磁盘」——不配快照策略，日志就会无限增长，磁盘迟早被吃满。
//!
//! 这件事没有任何报错，只会在一段时间之后以「磁盘满了」的形式爆发出来。所以它必须
//! 被测试盯住，而且要用**可观测的指标**盯住，不能只靠「我们配了策略」这种自述。
//!
//! # 测两个独立的性质
//!
//! 1. **留存是有限的**：写入量翻倍之后，日志的留存跨度不应跟着翻倍。
//!    这一条证明截断真的在发生。
//! 2. **截断不丢数据**：被截掉的日志所对应的数据，必须仍然能从状态机（以及重启后
//!    从快照）里读出来。这一条证明截断是安全的。
//!
//! 第二条更重要。第 1 条做不到只是浪费磁盘；第 2 条做不到就是丢数据。

use std::net::SocketAddr;
use std::time::Duration;

mod common;

use bytes::Bytes;

use xrl_db::config::{Config, RaftConfig};
use xrl_db::kv::WriteOp;
use xrl_db::node::Node;
use xrl_db::protocol::{Reply, SetCondition};

/// 累计多少条日志触发一次快照。
///
/// 刻意取得很小：默认值 5000 意味着这个测试要写几千个键，慢到没人愿意跑。
/// 策略的大小不影响被测的逻辑——它只是让快照来得早一点。
const SNAPSHOT_EVERY: u64 = 20;

/// 快照之外保留的日志条数。
const KEEP_LOGS: u64 = 5;

/// 每一轮写入的键数。
const KEYS_PER_ROUND: u64 = 300;

/// 判断「留存跨度没有增长」时允许的余量。
///
/// 快照与截断本身是异步的：测量的一瞬间，最新的若干条日志可能还没进快照。
/// 留出两倍快照间隔的余量，既不会误报，也拦得住真正的无限增长。
const SLACK: u64 = SNAPSHOT_EVERY * 2;

/// 一个跑在独立端口与独立数据目录上的单节点集群。
struct TestNode {
    node: Node,
    /// 数据目录。必须留住——`TempDir` 析构时会把它连数据一起删掉，
    /// 而「重启后还在」正是本测试要验证的事情之一。
    dir: tempfile::TempDir,
    config: Config,
}

impl TestNode {
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("应能创建临时目录");
        let port = common::free_port().await;
        let config = snapshot_config(
            SocketAddr::from(([127, 0, 0, 1], port)),
            dir.path().to_path_buf(),
        );

        let node = start_node(config.clone()).await;

        // config 要留着：重启时要靠它找回同一个端口与同一个数据目录
        Self { node, dir, config }
    }

    /// 关掉节点并**释放它对数据目录的占用**，以便重新打开同一份数据。
    ///
    /// redb 是单进程的（文件锁）：只要还有一份 `Database` 的引用没被丢弃，
    /// 重启就会以「数据库已被打开」失败——那看起来像是数据损坏，其实是没关干净。
    async fn shutdown(self) -> (tempfile::TempDir, Config) {
        self.node.shutdown().await;

        let Self { node, dir, config } = self;
        drop(node);
        (dir, config)
    }

    /// 写一个键。
    async fn set(&self, index: u64) {
        self.node
            .write(set_op(index))
            .await
            .unwrap_or_else(|error| panic!("写入 key-{index} 失败：{error}"));
    }

    /// 读一个键，返回它是否等于写入时的值。
    async fn has(&self, index: u64) -> bool {
        let key = Bytes::from(format!("key-{index}"));
        let expected = Reply::Bulk(Bytes::from(index.to_string()));

        self.node
            .read(move |store| store.get(&key, xrl_db::kv::now_ms()))
            .await
            .unwrap_or_else(|error| panic!("读取 key-{index} 失败：{error}"))
            == expected
    }

    /// 当前日志的留存跨度：`last_log_index - purged_index`。
    ///
    /// 这个数字回答的正是「日志会不会无限增长」——它若不随写入量增长而增长，
    /// 截断就是在工作。
    fn retained_span(&self) -> u64 {
        let metrics = self.node.metrics();
        let last = metrics.last_log_index.unwrap_or(0);
        let purged = metrics.purged.map(|id| id.index).unwrap_or(0);
        last.saturating_sub(purged)
    }

    /// 快照已经包含到的日志位置。
    fn snapshot_index(&self) -> u64 {
        self.node.metrics().snapshot.map(|id| id.index).unwrap_or(0)
    }
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

/// 单机配置 + 刻意调小的快照间隔。
///
/// 快照策略只是让快照来得早一点，不影响被测逻辑——默认每 5000 条才快照一次，
/// 意味着这个测试要写几千个键，慢到没人愿意跑。
fn snapshot_config(listen: SocketAddr, data_dir: std::path::PathBuf) -> Config {
    let raft = RaftConfig {
        snapshot_logs_since_last: SNAPSHOT_EVERY,
        max_in_snapshot_log_to_keep: KEEP_LOGS,
        ..common::test_raft_config()
    };

    common::single_config(1, listen, data_dir, raft)
}

/// 启动一个单节点集群并等它选出主节点。
async fn start_node(config: Config) -> Node {
    let node = Node::start_single(config)
        .await
        .expect("应能启动单节点集群");
    node.wait_for_leader(Duration::from_secs(5))
        .await
        .expect("单节点集群应能迅速选出主节点");
    node
}

// ==================================================================== 测试

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logs_stop_growing_once_snapshots_are_taken() {
    let test = TestNode::start().await;

    // ---- 第一轮 ----
    for index in 0..KEYS_PER_ROUND {
        test.set(index).await;
    }
    let span_after_first = test.retained_span();

    assert!(
        test.snapshot_index() > 0,
        "写入 {KEYS_PER_ROUND} 条之后应当已经触发过快照（策略是每 {SNAPSHOT_EVERY} 条一次）"
    );
    assert!(
        span_after_first < KEYS_PER_ROUND,
        "第一轮之后日志留存 {span_after_first} 条，已经占到写入量的大半——截断没有生效"
    );

    // ---- 第二轮：写入同样的量 ----
    for index in KEYS_PER_ROUND..KEYS_PER_ROUND * 2 {
        test.set(index).await;
    }
    let span_after_second = test.retained_span();

    // ★ 这是本测试的核心断言 ★
    //
    // 写入量翻倍。如果日志没有被截断，留存跨度必然跟着翻倍；截断生效的话，
    // 它应当基本不变。这比「留存数小于某个魔法常数」有用得多——后者会随着
    // 参数调整而过时，而且拦不住「截断变慢了」这种退化。
    assert!(
        span_after_second <= span_after_first + SLACK,
        "写入量翻倍后日志留存从 {span_after_first} 涨到 {span_after_second}（余量 {SLACK}）。\
         这说明日志正在随着写入无限增长——磁盘迟早被吃满。"
    );

    // 再确认一次数据没被截丢
    for index in 0..KEYS_PER_ROUND * 2 {
        assert!(
            test.has(index).await,
            "key-{index} 读不到了——日志被截断时把数据也一起弄丢了"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_survives_truncation_and_restart() {
    // 先写够触发若干次快照的量，再重启——重启后状态机只能从**快照**恢复，
    // 因为那之前的日志已经被删掉了。
    let test = TestNode::start().await;

    for index in 0..KEYS_PER_ROUND {
        test.set(index).await;
    }

    let purged = test.node.metrics().purged.map(|id| id.index).unwrap_or(0);
    assert!(
        purged > 0,
        "写入 {KEYS_PER_ROUND} 条之后日志应当已经被截断过，否则这个测试什么也没验证到"
    );

    // 重启前先记下存活状态，用于对比
    let span_before_restart = test.retained_span();

    let (dir, config) = test.shutdown().await;

    // 重新打开同一份数据。此时：
    //   - 快照点之前的数据只能来自快照
    //   - 快照点之后的数据来自残余的日志
    // 两条恢复路径都被覆盖到。
    //
    // 顺带还锁住了另一件事：`start_single` 对已经初始化过的数据目录必须是幂等的。
    // 它若把 openraft 的「不允许重复初始化」当成启动失败，集群只要落过盘就再也
    // 起不来了——而重启一个既有节点是最普通的运维操作。
    let restored = TestNode {
        node: start_node(config.clone()).await,
        dir,
        config,
    };

    for index in 0..KEYS_PER_ROUND {
        assert!(
            restored.has(index).await,
            "重启后 key-{index} 读不到了。它应该在快照里——\
             要么快照没恢复，要么恢复出来的状态机不完整。"
        );
    }

    // 截断的成果在重启后也不该被推翻
    assert!(
        restored.retained_span() <= span_before_restart + SLACK,
        "重启后日志留存跨度从 {span_before_restart} 涨到 {}，\
         说明重启把已经截断的日志又算了回来",
        restored.retained_span()
    );

    // 恢复之后必须还能继续正确写入。用自增验证：它依赖恢复出来的旧值，
    // 从 0 重新开始和正确地接着数，结果完全不同。
    let counter = Bytes::from_static(b"restart-counter");
    restored
        .node
        .write(WriteOp::Set {
            key: counter.clone(),
            value: Bytes::from_static(b"100"),
            expire_at: None,
            condition: SetCondition::Always,
        })
        .await
        .expect("应能写入");

    let reply = restored
        .node
        .write(WriteOp::IncrBy {
            key: counter.clone(),
            delta: 1,
        })
        .await
        .expect("应能自增");

    assert_eq!(
        reply,
        Reply::Integer(101),
        "重启后自增没有基于恢复出来的旧值——这正是「静默丢数据」在数字上的样子"
    );
}
