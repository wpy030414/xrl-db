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
//!
//! # 为什么「客户端转发」也走这条通道
//!
//! 客户端连到非主节点时，该节点需要把请求转给主节点（见 [`crate::raft::forward`]）。
//! 这件事完全可以另开一个端口、另写一套协议，但那会带来两个新的问题：多一个需要
//! 配置与放行的端口，以及一套与现有通道并行的连接管理逻辑。
//!
//! 更重要的是：节点间的这条通道**已经是**「把请求交给对端 Raft 实例」的抽象，
//! 而写入与读索引恰好就是这种请求。因此 [`RpcRequest`] 里多了两个变体，
//! 载荷类型（[`WriteOp`]、[`Reply`]）也本就是本层类型配置里已有的类型——
//! 没有引入任何新的跨层依赖。

use openraft::error::{CheckIsLeaderError, ClientWriteError, InstallSnapshotError, RaftError};
use openraft::impls::BasicNode;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{LogId, Raft};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use super::{NodeId, TypeConfig};
use crate::kv::WriteOp;
use crate::protocol::Reply;

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

    // ---- 以下两个变体服务于「客户端连到非主节点」的场景，见模块文档 ----
    /// 把一条写操作交给对方提交。
    ///
    /// 只有主节点能把条目写进日志，因此接收到本请求的节点若不是主节点，会如实拒绝
    /// 并告知真正的主节点是谁——**绝不自作主张地代为提交**。
    ClientWrite(WriteOp),
    /// 请求对方确认领导权，并给出一个可用于线性一致读的日志索引。
    ///
    /// 收到响应的节点只需等待自己的状态机追平到该索引，便可在**本地**完成读取。
    /// 这样读操作不必把数据搬来搬去，也避免了把客户端命令塞进节点间的消息里。
    ReadIndex,
}

/// 一次转发写入的处理结果。
///
/// 单独起别名是因为 openraft 的错误类型参数很长，而这里刻意**保留**了完整的错误
/// 结构而不是压成字符串（原因见 [`RpcResponse`] 的说明）。
pub type ClientWriteResult = Result<Reply, RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>;

/// 一次读索引请求的处理结果。
pub type ReadIndexResult =
    Result<Option<LogId<NodeId>>, RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>>;

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

    /// 对方对写操作的处理结果。
    ///
    /// 这里保留了完整的 [`ClientWriteError`] 而不压成字符串，是因为发起方必须能区分
    /// 「对方不是主节点，这次写入肯定没生效」（可以安全重试）与「结果不确定」
    /// （重试可能重复执行一条 `INCR`）。压成字符串这个区别就丢了。
    ClientWrite(ClientWriteResult),
    /// 对方对读索引请求的处理结果。
    ReadIndex(ReadIndexResult),
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
///
/// # 为什么要一个停止信号
///
/// 这个循环持有监听器不放。如果它只在进程退出时才结束，那么**关闭一个节点并不会
/// 释放它的端口**——同一个进程里重启这个节点会直接失败在「地址已被占用」上，
/// 而那个端口其实属于一个已经被「关掉」的节点。它还会一直握着一份 `Raft` 引用，
/// 让整个共识实例连同 redb 的文件锁都无法释放。
pub async fn serve(
    listener: TcpListener,
    raft: Raft<TypeConfig>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    loop {
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => accepted?,

            // 收到停止信号：立刻交出监听器，让端口可以被重新绑定
            _ = shutdown.changed() => return Ok(()),
        };

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
///
/// 本函数同时被两处使用：RPC 服务端（处理来自其他节点的请求），以及
/// [`crate::raft::forward`] 的本地快路径（本节点恰好是主节点时直接在这里提交，
/// 不必绕网络一圈）。两处共用同一个实现，就不会出现「本地提交与服务端提交行为不一致」
/// 这类只在集群里才暴露的缺陷。
pub async fn dispatch(raft: &Raft<TypeConfig>, request: RpcRequest) -> RpcResponse {
    match request {
        RpcRequest::Vote(request) => RpcResponse::Vote(raft.vote(request).await),
        RpcRequest::AppendEntries(request) => {
            RpcResponse::AppendEntries(raft.append_entries(request).await)
        }
        RpcRequest::InstallSnapshot(request) => {
            RpcResponse::InstallSnapshot(raft.install_snapshot(request).await)
        }
        // 客户端写入。openraft 的响应里只有 `data` 是我们关心的（即状态机返回的
        // `Reply`）；日志位置等信息对转发方没有意义。
        RpcRequest::ClientWrite(op) => {
            RpcResponse::ClientWrite(raft.client_write(op).await.map(|response| response.data))
        }
        // 线性一致读的第一步。`ensure_linearizable` 返回的是**状态机应当追平到的
        // 日志位置**——本节点此时已经追平，而发起方拿它去等自己的状态机。
        RpcRequest::ReadIndex => RpcResponse::ReadIndex(raft.ensure_linearizable().await),
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
