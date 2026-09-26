//! 单连接会话。
//!
//! 一个会话的生命周期：读命令 → 执行 → 写回复，如此往复，直到：
//!
//! - 客户端发送 `QUIT`
//! - 客户端断开连接
//! - 发生**帧层面**错误（字节流失去对齐，无法恢复）
//!
//! 注意「命令层面」的错误（未知命令、参数不合法）**不会**中断会话——
//! 只回复一条错误，然后继续读下一条命令。

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::backend::Backend;
use crate::protocol::codec::{FramingError, RespCodec};
use crate::protocol::{Command, Dialect, Reply};

/// 服务一条客户端连接，直到它结束。
///
/// 连接计数在此处维护，因此无论会话以何种方式结束，计数都会被正确回收。
pub async fn serve(stream: TcpStream, backend: Arc<Backend>) -> Result<(), FramingError> {
    backend.connection_opened();
    let result = run(stream, Arc::clone(&backend)).await;
    backend.connection_closed();
    result
}

/// 会话主循环。
async fn run(stream: TcpStream, backend: Arc<Backend>) -> Result<(), FramingError> {
    // 默认使用 RESP2——这是未发送 HELLO 的客户端所期望的方言
    let mut framed = Framed::new(stream, RespCodec::default());

    while let Some(item) = framed.next().await {
        // item 是嵌套的 Result：外层是帧层面错误，内层是命令层面错误。
        // 两者命运不同，必须分开处理——详见 protocol::codec 的模块说明。
        let command = match item {
            Ok(Ok(command)) => command,

            // 命令层面的错误：回复错误文本后**继续**服务这条连接。
            // 客户端打错一个字母就断连的服务是不可用的。
            Ok(Err(command_error)) => {
                framed.send(Reply::error(command_error.to_string())).await?;
                continue;
            }

            // 帧层面错误：字节流已失去对齐，这条连接无法继续
            Err(framing_error) => return Err(framing_error),
        };

        // HELLO 会改变本连接的协议方言，必须在生成回复**之前**处理，
        // 否则 HELLO 自己的回复就会用错方言。
        if let Command::Hello { version } = &command {
            match version {
                // 客户端显式指定了版本
                Some(3) => framed.codec_mut().set_dialect(Dialect::Resp3),
                Some(2) => framed.codec_mut().set_dialect(Dialect::Resp2),
                // 未指定版本：按 Redis 惯例停留在当前方言
                _ => {}
            }
        }

        // QUIT 需要先回复 +OK 再断开，因此在这里记下意图
        let should_quit = matches!(command, Command::Quit);

        let reply = backend.execute(command);
        framed.send(reply).await?;

        if should_quit {
            break;
        }
    }

    Ok(())
}
