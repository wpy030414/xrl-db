//! XRLDB 进程入口。
//!
//! 职责：解析命令行参数 → 加载并校验配置 → 启动节点。
//!
//! 配置优先级：**CLI 参数 > 环境变量 > 配置文件 > 内置默认值**。
//! 前两者由 clap 统一处理（见 [`Cli`] 各字段的 `env` 属性），后两者由
//! [`xrl_db::config::Config`] 负责。

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

    xrl_db::node::run(config).await
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
