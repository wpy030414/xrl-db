//! XRLDB 进程入口。
//!
//! 职责：解析命令行参数 → 加载配置 → （后续阶段）启动节点。
//!
//! 配置优先级：**CLI 参数 > 环境变量 > 配置文件 > 内置默认值**。
//! 前两者由 clap 统一处理（见 [`Cli`] 各字段的 `env` 属性），后两者由
//! [`Config`] 负责。

use std::path::PathBuf;

use clap::Parser;

use xrl_db::config::Config;
use xrl_db::error::Result;

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
}

fn main() {
    if let Err(err) = run() {
        // 错误走 stderr 且退出码非零——脚本据此判断启动是否成功
        eprintln!("错误：{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    let mut cfg = match &cli.config {
        Some(path) => Config::from_file(path)?,
        None => Config::default(),
    };

    // CLI 与环境变量的覆盖优先于配置文件
    if let Some(listen) = cli.listen {
        cfg.node.listen = listen;
    }
    if let Some(node_id) = cli.node_id {
        cfg.node.id = node_id;
    }

    let cfg = cfg.resolve()?;
    print_summary(&cfg);

    Ok(())
}

/// 打印解析后的配置摘要。
fn print_summary(cfg: &Config) {
    println!("XRLDB 节点 {}", cfg.node.id);
    println!("  监听地址  {}", cfg.node.listen);
    println!("  数据目录  {}", cfg.storage_path().display());

    if cfg.cluster.enabled {
        println!("  集群模式  已启用，共 {} 个节点", cfg.cluster.peers.len());
        for peer in &cfg.cluster.peers {
            let marker = if peer.id == cfg.node.id {
                "  ← 本节点"
            } else {
                ""
            };
            println!("            - 节点 {} @ {}{}", peer.id, peer.addr, marker);
        }
    } else {
        println!("  集群模式  未启用（单节点）");
    }

    println!(
        "  Raft      选举超时 {}ms，心跳间隔 {}ms",
        cfg.raft.election_timeout_ms, cfg.raft.heartbeat_interval_ms
    );
    println!();
    // 如实说明当前进度，避免让人误以为已经能提供服务
    println!("注意：网络服务尚未实现，当前仅验证配置解析链路。");
}
