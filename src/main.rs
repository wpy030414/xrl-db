//! XRLDB 进程入口。
//!
//! 职责：解析命令行参数 → 加载并校验配置 → 启动节点 → 提供客户端服务。
//!
//! 配置优先级：**CLI 参数 > 环境变量 > 配置文件 > 内置默认值**。
//! 前两者由 clap 统一处理（见 [`Cli`] 各字段的 `env` 属性），后两者由
//! [`xrl_db::config::Config`] 负责。

// 与 lib.rs 同一原因：openraft 的错误类型体积很大（224 字节），而它出现在我们
// 无法改变签名的位置。二进制目标需要单独豁免——lib 上的 allow 不会传导过来。
#![allow(clippy::result_large_err)]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use openraft::impls::BasicNode;

use xrl_db::backend::Backend;
use xrl_db::config::Config;
use xrl_db::error::{Error, Result};
use xrl_db::node::{Node, state_name};
use xrl_db::protocol::Reply;
use xrl_db::server;

/// 兼容 Redis RESP3 的强一致键值数据库。
#[derive(Debug, Parser)]
#[command(name = "xrl-db", version, about)]
struct Cli {
    /// 配置文件路径。
    ///
    /// `.toml` 扩展名按 TOML 解析，其他扩展名按 redis.conf 风格解析。
    #[arg(short, long, value_name = "FILE", env = "XRLDB_CONFIG")]
    config: Option<PathBuf>,

    /// 覆盖配置中的监听地址，如 `127.0.0.1:7001`。
    #[arg(long, value_name = "ADDR", env = "XRLDB_LISTEN")]
    listen: Option<std::net::SocketAddr>,

    /// 覆盖配置中的节点 ID。
    #[arg(long, value_name = "N", env = "XRLDB_NODE_ID")]
    node_id: Option<u64>,

    /// 由本节点负责组建集群。
    ///
    /// **整个集群里只能有一个节点带这个开关。**加进来之后，本节点会把
    /// `cluster.peers` 里列出的其他节点逐个纳入集群。不带这个开关的节点只会启动，
    /// 静静地等着被纳入。
    ///
    /// 单节点部署（未启用集群，或 peers 里只有自己）本来就无需引导，会自动完成。
    #[arg(long, env = "XRLDB_BOOTSTRAP")]
    bootstrap: bool,

    /// 只校验配置并退出，不启动服务。
    ///
    /// 供部署脚本在正式启动前先行检查，避免配置错误要到服务启动时才暴露。
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        // 错误走 stderr 且退出码非零——脚本据此判断启动是否成功
        eprintln!("错误：{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = resolve_config(&cli)?;

    if cli.check {
        println!("配置校验通过。");
        return Ok(());
    }

    let bootstrap = should_bootstrap(&config, cli.bootstrap);
    // 组建集群的过程性消息先收集起来，等启动横幅一起打印——否则它们会出现在
    // 「XRLDB 0.1.0」之前，读起来像是上一次运行的残留输出。
    let mut notes = Vec::new();
    let node = Arc::new(start_node(&config, bootstrap, &mut notes).await?);
    print_startup(&config, &node, bootstrap, &notes);

    let backend = Arc::new(Backend::new(node, config.clone()));
    server::serve(config, backend).await
}

/// 启动节点，必要时组建集群。
async fn start_node(config: &Config, bootstrap: bool, notes: &mut Vec<String>) -> Result<Node> {
    if !bootstrap {
        // 多节点集群里的非引导节点：只启动，成员关系由引导节点负责建立。
        //
        // 这里**绝不自举**——若每个节点都把自己初始化成一个单节点集群，它们会成为
        // 三个互不相干的集群，而且**永远不会合并**：每个都认为自己已经是一个完整的
        // 集群了。这种故障没有任何报错，只是三份数据各自增长。
        let node = Node::start(config.clone())
            .await
            .map_err(|error| Error::Node(Box::new(error)))?;

        // 等一小会儿主节点。引导节点通常在脚本里紧接着本节点启动，稍等片刻就能
        // 等到它联系过来——这样启动横幅里显示的成员数与主节点都是真实的，
        // 而不是「还没有」这种需要运维自己再跑一遍命令去确认的状态。
        // 超时不算失败：集群可能确实还没起来，稍后自会恢复。
        if config.cluster.enabled {
            node.wait_for_leader(NON_BOOTSTRAP_LEADER_WAIT).await;
        }

        return Ok(node);
    }

    let node = Node::start_single(config.clone())
        .await
        .map_err(|error| Error::Node(Box::new(error)))?;

    if config.cluster.enabled && config.cluster.peers.len() > 1 {
        grow_cluster(&node, config, notes).await?;
    }

    Ok(node)
}

/// 非引导节点启动后等待主节点的时长。
///
/// 只影响启动横幅的可读性，不影响任何功能——等不到就照常启动。
const NON_BOOTSTRAP_LEADER_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

/// 判断本节点是否需要自举。
fn should_bootstrap(config: &Config, requested: bool) -> bool {
    // 单节点部署（未启用集群，或 peers 里只有自己）必须自举，
    // 否则它永远选不出主节点——那不是「保守」，那是坏掉。
    requested || !config.cluster.enabled || config.cluster.peers.len() <= 1
}

/// 把配置里列出的其他节点逐个纳入集群。
///
/// # 为什么要重试
///
/// 启动脚本通常同时拉起所有进程，引导节点几乎必然比同伴先就绪。一次连不上就放弃，
/// 会留下一个「看起来成功、实际只有自己」的集群——运维要等到写入不再复制时才会
/// 发现。重试窗口的成本远低于这种沉默的失败。
///
/// # 为什么失败是致命的
///
/// 运维写了三个节点就是要三个节点。少一个却照常启动，等于把配置降级成建议——
/// 这正是本项目在配置层坚持「未知的配置项直接报错」的同一个理由。
async fn grow_cluster(node: &Node, config: &Config, notes: &mut Vec<String>) -> Result<()> {
    let deadline = tokio::time::Instant::now() + BOOTSTRAP_DEADLINE;

    for peer in &config.cluster.peers {
        if peer.id == config.node.id {
            continue;
        }

        let rpc_addr = xrl_db::config::rpc_addr_of(peer.addr)?;
        let member = BasicNode::new(rpc_addr.to_string());

        loop {
            // 包一层超时：`add_learner` 会等待新节点追平日志，而「对端还没起来」
            // 这种情况未必会以一个错误的形式返回，也可能一直等下去。
            // 不能让整条启动路径被一个还没有启动的同伴卡死。
            let attempt =
                tokio::time::timeout(ADD_MEMBER_TIMEOUT, node.add_member(peer.id, member.clone()))
                    .await;

            match attempt {
                Ok(Ok(())) => {
                    notes.push(format!(
                        "已纳入节点 {} @ {}（节点间 {rpc_addr}）",
                        peer.id, peer.addr
                    ));
                    break;
                }
                Ok(Err(error)) => {
                    // 主节点可能还在选举中，或对端还没起来——两种情况都值得再试
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Error::Bootstrap {
                            id: peer.id,
                            addr: peer.addr.to_string(),
                            source: error.to_string(),
                        });
                    }
                }
                Err(_elapsed) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Error::Bootstrap {
                            id: peer.id,
                            addr: peer.addr.to_string(),
                            source: format!(
                                "等待 {ADD_MEMBER_TIMEOUT:?} 仍未能把它纳入集群（节点可能未启动）"
                            ),
                        });
                    }
                }
            }

            tokio::time::sleep(BOOTSTRAP_RETRY_INTERVAL).await;
        }
    }

    Ok(())
}

/// 把节点合并进集群的总时限。
const BOOTSTRAP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// 纳入单个节点的时限。
const ADD_MEMBER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// 两次纳入尝试之间的间隔。
const BOOTSTRAP_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// 按优先级合并各来源，得到最终配置。
fn resolve_config(cli: &Cli) -> Result<Config> {
    let mut config = match &cli.config {
        Some(path) => Config::from_file(path)?,
        None => Config::default(),
    };

    // CLI 与环境变量的覆盖优先于配置文件
    if let Some(listen) = cli.listen {
        config.node.listen = listen;
    }
    if let Some(node_id) = cli.node_id {
        config.node.id = node_id;
    }

    config.resolve()
}

/// 打印启动摘要。
///
/// 刻意把「当前能力」与「尚未具备的能力」都写出来——运维应当一眼看清手上的
/// 这个东西能做什么、不能做什么，而不是靠试。
fn print_startup(config: &Config, node: &Node, bootstrapped: bool, notes: &[String]) {
    let metrics = node.metrics();

    println!("XRLDB {}", xrl_db::VERSION);
    println!("  节点 ID    {}", config.node.id);
    println!("  客户端      {}", config.node.listen);
    println!("  节点间通信  {}", node.rpc_addr());
    println!("  数据目录    {}", config.storage_path().display());

    if config.cluster.enabled {
        println!(
            "  集群        配置中有 {} 个节点",
            config.cluster.peers.len()
        );
        for peer in &config.cluster.peers {
            let marker = if peer.id == config.node.id {
                "  ← 本节点"
            } else {
                ""
            };
            println!(
                "               - 节点 {} @ {}{}",
                peer.id, peer.addr, marker
            );
        }
    } else {
        println!("  集群        未启用（单节点）");
    }

    println!(
        "  Raft        选举超时 {}ms，心跳间隔 {}ms",
        config.raft.election_timeout_ms, config.raft.heartbeat_interval_ms
    );
    println!("  当前状态    {}", state_name(metrics.state));
    println!(
        "  投票成员    {} 个",
        metrics.membership_config.voter_ids().count()
    );

    // 只有单节点集群能在启动瞬间就确定 leader；多节点需要等选举完成
    match metrics.current_leader {
        Some(id) => println!("  当前主节点  {id}"),
        None => println!("  当前主节点  尚未选出（选举进行中）"),
    }

    for note in notes {
        println!("  {note}");
    }

    if config.cluster.enabled {
        println!();
        if bootstrapped {
            println!("说明：本节点负责组建集群，配置里列出的其他节点已全部纳入。");
            println!("      后续要再增加节点，对主节点执行 RAFT ADD-NODE <id> <地址>。");
        } else {
            println!("说明：本节点不是引导节点，启动后处于等待状态。");
            println!("      它需要由引导节点（带 --bootstrap 的那个）纳入集群；");
            println!("      在纳入之前，本节点无法提供读写服务。");
        }
    }
    println!();

    // 用一个只读探测确认状态机确实可读，避免「启动成功但读不了」这种假阳性
    match futures_block_on_read(node) {
        Ok(count) => println!("状态机就绪，当前 {count} 个键。"),
        Err(message) => println!("警告：状态机暂时不可读（{message}）。"),
    }
    println!();
}

/// 同步地做一次只读探测。
///
/// 启动横幅是同步打印的，而读路径是异步的，因此这里在已有的 tokio 运行时上用
/// `block_in_place` 就地等待——不会新建运行时，也不会阻塞整个线程池。
fn futures_block_on_read(node: &Node) -> std::result::Result<i64, String> {
    // 没有主节点时读本来就不成立：线性一致读必须先确认领导权，而确认的对象就是
    // 主节点。此时探测只会等出一串徒劳的重试，还会平白拖慢启动。
    if node.leader().is_none() {
        return Err("集群尚未选出主节点，稍后会自动恢复".to_string());
    }

    let read = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(async { node.read(|store| store.dbsize(xrl_db::kv::now_ms())).await })
    });

    match read {
        Ok(Reply::Integer(count)) => Ok(count),
        Ok(_) => Ok(0),
        Err(error) => Err(error.to_string()),
    }
}
