//! 节点间 RPC：线路协议与服务端。
//!
//! # 线路格式
//!
//! 每条消息是一帧：`[4 字节大端长度][postcard 序列化的消息体]`。
//!
//! 之所以不直接复用 RESP 协议：RESP 是**面向客户端**的，而节点间通信的载荷是
//! Raft 的内部消息（含二进制快照分片），两者没有重叠。用独立的自有格式可以
//! 避免为内部消息硬套一套客户端语义。
//!
//! 独立端口（客户端端口 + 10000）也是同样的考虑：混在一个端口上就得做协议嗅探，
//! 而嗅探规则一旦与客户端行为冲突（比如内联命令也以字母开头）就会误判。
//!
//! # 远端错误为什么要结构化传回
//!
//! 远端可能明确拒绝一次请求（「你的任期过时了」），这与「网络不通」是**完全不同**
//! 的情况：前者应当让 leader 立即退位，后者只值得重试。如果把远端错误压成一个字符串，
//! 客户端就无法区分二者。因此 [`RpcResponse`] 里直接带上 `RaftError`——它在
//! openraft 的 `serde` feature 下可序列化。

use openraft::Raft;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::{NodeId, TypeConfig};

/// 长度前缀占用的字节数。
const LENGTH_PREFIX_BYTES: usize = 4;

/// 单条消息的长度上限。
///
/// 快照分片可能相当大，因此上限给得宽松；但**必须有**——否则一条伪造的长度前缀
/// 就能让服务端分配出海量内存。
const MAX_MESSAGE_BYTES: u32 = 256 * 1024 * 1024;

/// 一次 RPC 的请求。
#[derive(Debug, Serialize, serde::Deserialize)]
pub enum RpcRequest {
    /// 请求投票。
    Vote(VoteRequest<NodeId>),
    /// 追加日志条目（也用作心跳）。
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    /// 传输一个快照分片。
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

/// 一次 RPC 的响应。
///
/// 内层的 `Result` 承载远端的处理结果——`Err` 表示远端**收到了并明确拒绝**，
/// 与网络层面的失败截然不同。
#[derive(Debug, Serialize, serde::Deserialize)]
pub enum RpcResponse {
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ),
}

/// 写入一帧消息。
pub async fn write_message<T>(stream: &mut TcpStream, message: &T) -> std::io::Result<()>
where
    T: Serialize,
{
    let payload = postcard::to_allocvec(message).map_err(invalid_data)?;

    if payload.len() > MAX_MESSAGE_BYTES as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("待发送的 RPC 消息过大：{} 字节", payload.len()),
        ));
    }

    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&payload).await?;
    stream.flush().await
}

/// 读取一帧消息。
///
/// 返回 `UnexpectedEof` 表示对端正常关闭了连接——这是常见情况（节点重启），
/// 调用方应当据此安静地结束，而不是当成错误上报。
pub async fn read_message<T>(stream: &mut TcpStream) -> std::io::Result<T>
where
    T: DeserializeOwned,
{
    let mut prefix = [0u8; LENGTH_PREFIX_BYTES];
    stream.read_exact(&mut prefix).await?;

    let length = u32::from_be_bytes(prefix);
    if length > MAX_MESSAGE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("RPC 消息长度 {length} 超出上限 {MAX_MESSAGE_BYTES}，拒绝分配内存"),
        ));
    }

    let mut payload = vec![0u8; length as usize];
    stream.read_exact(&mut payload).await?;

    postcard::from_bytes(&payload).map_err(invalid_data)
}

/// 把序列化错误转成 IO 错误。
fn invalid_data(error: postcard::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}

/// 在一个已绑定的监听器上接受来自其他节点的 RPC 连接。
///
/// 每条连接独立成任务，且会持续复用（openraft 会反复向同一对端发起 RPC）。
pub async fn serve(listener: TcpListener, raft: Raft<TypeConfig>) -> std::io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;

        // 节点间消息同样是小包往返，禁用 Nagle 降低选举与心跳的延迟
        if let Err(error) = stream.set_nodelay(true) {
            eprintln!("设置 RPC 连接的 TCP_NODELAY 失败（{peer}）：{error}");
        }

        let raft = raft.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, raft).await {
                // 单条连接出错不影响其他连接；Raft 会自行重试或与其他节点另建连接
                eprintln!("来自 {peer} 的 RPC 连接结束：{error}");
            }
        });
    }
}

/// 处理一条 RPC 连接，直到对端关闭。
async fn handle_connection(mut stream: TcpStream, raft: Raft<TypeConfig>) -> std::io::Result<()> {
    loop {
        let request: RpcRequest = match read_message(&mut stream).await {
            Ok(request) => request,
            // 对端正常关闭：安静退出
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };

        let response = dispatch(&raft, request).await;
        write_message(&mut stream, &response).await?;
    }
}

/// 把请求交给本地 Raft 实例处理。
///
/// 这里**不设置超时**：处理入站 RPC 只是把消息投递给本地的 Raft 状态机，
/// 不涉及网络等待。真正的超时应当由发起方（客户端侧）控制。
async fn dispatch(raft: &Raft<TypeConfig>, request: RpcRequest) -> RpcResponse {
    match request {
        RpcRequest::Vote(request) => RpcResponse::Vote(raft.vote(request).await),
        RpcRequest::AppendEntries(request) => {
            RpcResponse::AppendEntries(raft.append_entries(request).await)
        }
        RpcRequest::InstallSnapshot(request) => {
            RpcResponse::InstallSnapshot(raft.install_snapshot(request).await)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::Vote;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn framing_round_trips_a_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("应能绑定");
        let addr = listener.local_addr().expect("应能取得地址");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("应能接受连接");
            let request: RpcRequest = read_message(&mut stream).await.expect("应能读取请求");
            write_message(
                &mut stream,
                &RpcResponse::Vote(Ok(VoteResponse::new(Vote::default(), None, true))),
            )
            .await
            .expect("应能写回响应");
            request
        });

        let mut client = TcpStream::connect(addr).await.expect("应能连接");
        let sent = RpcRequest::Vote(VoteRequest {
            vote: Vote::default(),
            last_log_id: None,
        });
        write_message(&mut client, &sent)
            .await
            .expect("应能写入请求");

        let response: RpcResponse = read_message(&mut client).await.expect("应能读取响应");
        assert!(matches!(response, RpcResponse::Vote(Ok(_))));

        let received = server.await.expect("服务端任务应正常结束");
        assert!(matches!(received, RpcRequest::Vote(_)));
    }

    #[tokio::test]
    async fn oversized_length_prefix_is_rejected() {
        // 伪造一个巨大的长度前缀，服务端必须拒绝而不是尝试分配内存
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("应能绑定");
        let addr = listener.local_addr().expect("应能取得地址");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("应能接受连接");
            read_message::<RpcRequest>(&mut stream).await
        });

        let mut client = TcpStream::connect(addr).await.expect("应能连接");
        client
            .write_all(&u32::MAX.to_be_bytes())
            .await
            .expect("应能写入");

        let result = server.await.expect("服务端任务应结束");
        assert!(result.is_err(), "超大长度前缀必须被拒绝");
    }
}
