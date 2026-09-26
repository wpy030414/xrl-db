//! TCP 监听与连接接受。

use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};

use super::session;
use crate::backend::Backend;
use crate::config::Config;
use crate::error::{Error, Result};

/// 绑定配置中的监听地址并进入接受循环，直到收到中断信号。
pub async fn serve(config: Config, backend: Arc<Backend>) -> Result<()> {
    let listener = TcpListener::bind(config.node.listen)
        .await
        .map_err(|source| Error::Bind {
            addr: config.node.listen,
            source,
        })?;

    serve_with(listener, backend).await
}

/// 在一个**已绑定**的监听器上进入接受循环。
///
/// 与 [`serve`] 分开是为了让集成测试能自己绑定端口（通常用 `:0` 让操作系统分配
/// 空闲端口），从而避免测试之间抢占固定端口。生产路径则由 [`serve`] 负责绑定。
pub async fn serve_with(listener: TcpListener, backend: Arc<Backend>) -> Result<()> {
    let local_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "<未知>".to_string());

    println!("XRLDB {} 已启动，监听 {local_addr}", crate::VERSION);
    println!("按 Ctrl-C 停止。");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        configure_connection(&stream, peer);
                        let backend = Arc::clone(&backend);
                        tokio::spawn(async move {
                            if let Err(error) = session::serve(stream, backend).await {
                                // 帧层面错误意味着字节流失去对齐，只能断开这一条连接。
                                // 其他连接不受影响，因此记录即可，不退出进程。
                                eprintln!("连接 {peer} 因协议错误中断：{error}");
                            }
                        });
                    }
                    Err(error) => {
                        // 单次 accept 失败通常是一过性的（如文件描述符暂时耗尽）。
                        // 记录后继续接受，不应让整个服务倒下。
                        eprintln!("接受连接失败：{error}");
                    }
                }
            }

            signal = tokio::signal::ctrl_c() => {
                match signal {
                    Ok(()) => println!("\n收到中断信号，正在停止服务"),
                    // 信号处理器注册失败时同样应当停止——继续运行会失去优雅停止的能力
                    Err(error) => eprintln!("\n无法监听中断信号：{error}"),
                }
                return Ok(());
            }
        }
    }
}

/// 调整新连接的套接字选项。
fn configure_connection(stream: &TcpStream, peer: std::net::SocketAddr) {
    // 关闭 Nagle 算法。本协议的请求与响应都很小，等待攒满一个 MSS 再发送会
    // 显著增加往返延迟，而这类小包往返恰恰是本服务的主要用途。
    if let Err(error) = stream.set_nodelay(true) {
        eprintln!("设置 TCP_NODELAY 失败（{peer}）：{error}");
    }
}
