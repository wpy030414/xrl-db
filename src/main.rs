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

    // 单节点同样走完整的 Raft——「单机」与「集群」只是成员数量之差，
    // 不存在两套代码路径。
    let node = Arc::new(
        Node::start_single(config.clone())
            .await
            .map_err(|error| Error::Node(Box::new(error)))?,
    );
    print_startup(&config, &node);

    let backend = Arc::new(Backend::new(node, config.clone()));
    server::serve(config, backend).await
}

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
fn print_startup(config: &Config, node: &Node) {
    let metrics = node.metrics();

    println!("XRLDB {}", xrl_db::VERSION);
    println!("  节点 ID    {}", config.node.id);
    println!("  客户端      {}", config.node.listen);
    println!("  节点间通信  {}", node.rpc_addr());
    println!("  数据目录    {}", config.storage_path().display());

    if config.cluster.enabled {
        println!("  集群        已配置 {} 个节点", config.cluster.peers.len());
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

    // 只有单节点集群能在启动瞬间就确定 leader；多节点需要等选举完成
    match metrics.current_leader {
        Some(id) => println!("  当前主节点  {id}"),
        None => println!("  当前主节点  尚未选出（选举进行中）"),
    }

    if !config.cluster.enabled {
        println!();
        println!("说明：多节点集群需要显式引导（初始化成员、添加学习者）。");
        println!("      当前为单节点，已自动完成引导，数据会落盘到上述数据目录。");
    }
    println!();

    // 用一个只读探测确认状态机确实可读，避免「启动成功但读不了」这种假阳性
    match futures_block_on_read(node) {
        Ok(count) => println!("状态机就绪，当前 {count} 个键。"),
        Err(message) => println!("警告：状态机暂时不可读（{message}），稍后会自动恢复。"),
    }
    println!();
}

/// 同步地做一次只读探测。
///
/// 启动横幅是同步打印的，而读路径是异步的，因此这里在已有的 tokio 运行时上用
/// `block_in_place` 就地等待——不会新建运行时，也不会阻塞整个线程池。
fn futures_block_on_read(node: &Node) -> std::result::Result<i64, String> {
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
