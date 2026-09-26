//! 节点协调。
//!
//! 按依赖顺序拼装各层，并把启动信息呈现给运维。
//!
//! 当前拼装的是**单机形态**：配置 + 状态机 + 网络服务。
//! 接入 Raft 后，这里会多出一个共识层，而 [`crate::backend::Backend`]
//! 的内部实现会随之改变——但对外接口保持不变。

use std::sync::Arc;

use crate::backend::Backend;
use crate::config::Config;
use crate::error::Result;
use crate::server;

/// 启动一个节点并阻塞运行。
pub async fn run(config: Config) -> Result<()> {
    print_startup(&config);

    let backend = Arc::new(Backend::new(config.clone()));
    server::serve(config, backend).await
}

/// 打印启动摘要。
///
/// 刻意把「当前能力」与「尚未具备的能力」都写出来——运维应当一眼看清
/// 手上的这个东西能做什么、不能做什么，而不是靠试。
fn print_startup(config: &Config) {
    println!("XRLDB {}", crate::VERSION);
    println!("  节点 ID    {}", config.node.id);
    println!("  监听地址   {}", config.node.listen);
    println!("  数据目录   {}", config.storage_path().display());

    if config.cluster.enabled {
        println!("  集群       已配置 {} 个节点", config.cluster.peers.len());
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
        println!("  一致性     未启用（共识层尚未接入）");
    } else {
        println!("  集群       未启用（单节点）");
    }

    println!(
        "  Raft       选举超时 {}ms，心跳间隔 {}ms",
        config.raft.election_timeout_ms, config.raft.heartbeat_interval_ms
    );

    if !config.cluster.enabled {
        println!();
        println!("提示：当前为单机模式，数据不落盘、进程退出即丢失。");
    }
    println!();
}
