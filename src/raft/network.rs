//! 节点间 RPC 的客户端侧。
//!
//! 实现 [`RaftNetwork`] 与 [`RaftNetworkFactory`]，即「作为发起方向其他节点发消息」。
//! 服务端侧（接收并转交给本地 Raft）在 [`super::rpc`]。
//!
//! # 连接策略：一次请求一条连接
//!
//! Raft 的 RPC 之间天然存在间隔（心跳、日志复制、投票），维持长连接需要为每条连接
//! 做保活与重连，复杂度不划算。而短连接在局域网里多出的那一次 TCP 握手，相对于
//! 一次 `fsync` 完全可以忽略——后者才是这条路径上的真正瓶颈。
//!
//! # 超时从哪来
//!
//! 不自己拍一个数字，而是用 openraft 通过 [`RPCOption`] 传入的 `hard_ttl`。
//! 那是它根据自己的选举超时与心跳间隔算出来的预期值，比我们的猜测更贴合实际。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use openraft::error::{NetworkError, RPCError, RemoteError, Unreachable};
use openraft::impls::BasicNode;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::rpc::{RpcRequest, RpcResponse, read_message, write_message};
use super::{NodeId, TypeConfig};
use crate::config::Config;

/// 传输层的失败。
///
/// 自己定义一个小枚举而不是直接构造 [`RPCError`]，是因为后者的 `RemoteError` 变体
/// 带一个错误类型参数，而传输层失败用不到它——分开表示可以让三种 RPC 方法
/// 各自映射到正确的 `RPCError<..>` 类型。
enum TransportFailure {
    /// 配置里没有这个节点的地址。
    UnknownTarget(String),
    /// 连接、读写或序列化失败。
    Io(std::io::Error),
    /// 超过 openraft 给出的期限仍未收到响应。
    Timeout,
}

/// 到某个对端节点的连接。
pub struct Network {
    /// 目标节点。
    target: NodeId,
    /// 目标节点的 RPC 地址；`None` 表示配置里没有它。
    addr: Option<SocketAddr>,
}

impl Network {
    /// 发起一次 RPC：建连接、发请求、读响应。
    ///
    /// 超时由调用方通过 `hard_ttl` 给出。
    async fn call(
        &self,
        request: RpcRequest,
        hard_ttl: Duration,
    ) -> Result<RpcResponse, TransportFailure> {
        let Some(addr) = self.addr else {
            return Err(TransportFailure::UnknownTarget(format!(
                "集群配置中没有节点 {} 的地址，无法与它通信",
                self.target
            )));
        };

        match timeout(hard_ttl, Self::exchange(addr, request)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(TransportFailure::Io(error)),
            Err(_) => Err(TransportFailure::Timeout),
        }
    }

    /// 完成一次请求-响应往返。
    async fn exchange(addr: SocketAddr, request: RpcRequest) -> std::io::Result<RpcResponse> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        write_message(&mut stream, &request).await?;
        read_message(&mut stream).await
    }
}

/// 把传输层失败转换成 openraft 的 RPC 错误。
///
/// 泛型参数 `E` 只用于满足 `RPCError` 的类型形状——传输层失败不会产生 `RemoteError`。
fn into_rpc_error<E>(failure: TransportFailure, target: NodeId) -> RPCError<NodeId, BasicNode, E>
where
    E: std::error::Error,
{
    match failure {
        // 配置缺失与超时都归为「够不着」：重试也不会有结果，需要人工介入或等选举
        TransportFailure::UnknownTarget(message) => RPCError::Unreachable(unreachable(
            std::io::Error::new(std::io::ErrorKind::NotFound, message),
        )),
        TransportFailure::Timeout => RPCError::Unreachable(unreachable(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("与节点 {target} 的 RPC 超时"),
        ))),
        // 真正的 IO 失败是「网络」问题，通常重试即可
        TransportFailure::Io(error) => RPCError::Network(NetworkError::new(&error)),
    }
}

/// 构造一个「够不着」错误。
fn unreachable(error: std::io::Error) -> Unreachable {
    Unreachable::new(&error)
}

/// 构造一个「响应与请求不匹配」错误。
///
/// 这属于协议实现层面的 bug，正常运行时不可能出现；但它必须是**可处理的错误**
/// 而不是 panic——一个节点的协议错误不该让整个进程倒下。
fn mismatched_response(target: NodeId, response: &RpcResponse) -> NetworkError {
    NetworkError::new(&std::io::Error::other(format!(
        "节点 {target} 返回了与请求不匹配的响应：{response:?}"
    )))
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        let target = self.target;
        match self
            .call(RpcRequest::AppendEntries(rpc), option.hard_ttl())
            .await
        {
            Ok(RpcResponse::AppendEntries(Ok(response))) => Ok(response),
            // 远端明确拒绝——通常是「你的任期过时了」。必须结构化传回，
            // 因为 leader 需要据此立即退位，而不是像网络错误那样重试。
            Ok(RpcResponse::AppendEntries(Err(source))) => {
                Err(RemoteError::new(target, source).into())
            }
            Ok(other) => Err(RPCError::Network(mismatched_response(target, &other))),
            Err(failure) => Err(into_rpc_error(failure, target)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>>
    {
        let target = self.target;
        match self.call(RpcRequest::Vote(rpc), option.hard_ttl()).await {
            Ok(RpcResponse::Vote(Ok(response))) => Ok(response),
            Ok(RpcResponse::Vote(Err(source))) => Err(RemoteError::new(target, source).into()),
            Ok(other) => Err(RPCError::Network(mismatched_response(target, &other))),
            Err(failure) => Err(into_rpc_error(failure, target)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let target = self.target;
        match self
            .call(RpcRequest::InstallSnapshot(rpc), option.hard_ttl())
            .await
        {
            Ok(RpcResponse::InstallSnapshot(Ok(response))) => Ok(response),
            Ok(RpcResponse::InstallSnapshot(Err(source))) => {
                Err(RemoteError::new(target, source).into())
            }
            Ok(other) => Err(RPCError::Network(mismatched_response(target, &other))),
            Err(failure) => Err(into_rpc_error(failure, target)),
        }
    }
}

/// 网络工厂：为每个对端节点创建连接。
///
/// openraft 对每个目标节点调用一次 [`RaftNetworkFactory::new_client`] 并长期持有
/// 返回的 [`Network`]，因此这里只做地址查找，不在构造时建立连接。
pub struct NetworkFactory {
    /// 本节点已知的全部 RPC 地址，按节点 ID 索引。
    members: HashMap<NodeId, SocketAddr>,
}

impl NetworkFactory {
    /// 依据集群配置构建工厂。
    ///
    /// 地址由各节点的客户端地址推导而来（端口 + 10000），见
    /// [`crate::config::RPC_PORT_OFFSET`]。推导失败的节点会被跳过——配置校验
    /// 早已拦下这种情况，这里只是不让一个坏地址拖垮整个工厂。
    pub fn new(config: &Config) -> Self {
        Self {
            members: config.peer_rpc_addrs(),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        Network {
            target,
            addr: self.members.get(&target).copied(),
        }
    }
}
