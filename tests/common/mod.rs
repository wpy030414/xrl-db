//! 集成测试共用的脚手架。
//!
//! 放在 `tests/common/` 而不是 `tests/` 下的根目录：Cargo 会把 `tests/*.rs` 每一个
//! 都当成独立的测试 crate，而子目录里的模块不会——它只能被 `mod common;` 显式引入。
//!
//! # 为什么地址分配必须共用一份
//!
//! 「找一对可用端口」这件事比它看起来容易错：客户端端口由操作系统分配，
//! 而节点间通信端口是**客户端端口 +10000**，必须另行确认可用；两个节点的端口
//! 之间也不能撞车。这段逻辑写错的症状是测试随机失败——而随机失败的测试比没有
//! 测试更糟，它会耗尽对整套测试的信任。
//!
//! 所以它只有一份实现。

// 每个集成测试都是独立 crate，各自编译一份本模块的副本，而每个 crate 只会用到其中
// 一部分辅助函数。于是「另一半没被用到」是必然的，不是缺陷——在模块级关掉这个 lint，
// 换来的是不必为每个测试文件各写一份重复的端口分配逻辑。
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;

use xrl_db::config::{
    ClusterConfig, Config, NodeConfig, Peer, RPC_PORT_OFFSET, RaftConfig, StorageConfig,
};

/// 测试环境的 Raft 参数：只把两个超时调快，其余沿用默认值。
///
/// 生产默认值更保守，是为了在真实网络抖动下不误触发选举；测试环境没有那个顾虑，
/// 等 300ms 而不是几秒能让整套测试快很多。
pub fn test_raft_config() -> RaftConfig {
    RaftConfig {
        election_timeout_ms: 300,
        heartbeat_interval_ms: 100,
        ..Default::default()
    }
}

/// 为 `count` 个节点各找一对可用端口，返回 `(节点 ID, 客户端地址)`。
///
/// 客户端端口由操作系统分配；节点间通信端口是客户端端口 +10000，必须另行确认可用。
/// 找不到就整体重来——这比固定端口号可靠得多，固定端口在并行测试时必然冲突。
pub fn find_port_pairs(count: u64) -> Vec<(u64, SocketAddr)> {
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

/// 找一个客户端端口可用、且 +10000 的节点间通信端口也可用的端口。
///
/// 单节点测试用它——那种场景不需要预先知道全部地址，一个端口就够。
pub async fn free_port() -> u16 {
    loop {
        let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
            continue;
        };
        let port = listener.local_addr().expect("应能取得地址").port();
        drop(listener);

        let Some(rpc_port) = port.checked_add(RPC_PORT_OFFSET) else {
            continue;
        };
        if std::net::TcpListener::bind(("127.0.0.1", rpc_port)).is_ok() {
            return port;
        }
    }
}

/// 多节点集群中一个节点的配置。
///
/// `peers` 里必须包含本节点自己——集群成员的地址表对每个节点都是同一份，
/// 而本节点的监听地址也从这里推导，避免两处写得不一致。
pub fn cluster_config(
    id: u64,
    peers: &[(u64, SocketAddr)],
    data_dir: PathBuf,
    raft: RaftConfig,
) -> Config {
    let listen = peers
        .iter()
        .find(|(peer_id, _)| *peer_id == id)
        .map(|(_, addr)| *addr)
        .expect("本节点地址应存在于 peers 中");

    let config = Config {
        node: NodeConfig { id, listen },
        cluster: ClusterConfig {
            enabled: true,
            peers: peers
                .iter()
                .map(|(peer_id, addr)| Peer {
                    id: *peer_id,
                    addr: *addr,
                })
                .collect(),
        },
        storage: StorageConfig {
            path: Some(data_dir),
        },
        raft,
    };

    // `resolve` 会推导默认值并做语义校验，返回可直接使用的配置
    config.resolve().expect("测试配置应合法")
}

/// 单机模式（未启用集群）的配置。
pub fn single_config(id: u64, listen: SocketAddr, data_dir: PathBuf, raft: RaftConfig) -> Config {
    let config = Config {
        node: NodeConfig { id, listen },
        storage: StorageConfig {
            path: Some(data_dir),
        },
        raft,
        ..Default::default()
    };

    config.resolve().expect("测试配置应合法")
}
