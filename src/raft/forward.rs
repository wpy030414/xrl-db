//! 把客户端的读写请求转交到主节点。
//!
//! # 为什么需要它
//!
//! Raft 里只有主节点能把条目写进日志，也只有主节点能确认「此刻我还是主节点」。
//! 因此一个连接到从节点的客户端，如果服务端不替它转交，就只能收到一个错误，
//! 由客户端自己去找到主节点——那正是 Redis Cluster 的做法，也是运维与客户端库
//! 长期抱怨的痛点（槽位映射、`MOVED` 重定向、拓扑刷新）。
//!
//! 本模块兑现的是项目立项时的承诺之一：**客户端连任意一个节点都能读写**。
//!
//! # 写与读用的是两条不同的路子
//!
//! - **写**必须由主节点执行，所以把 [`WriteOp`] 原样转发过去，等它回一个结果。
//! - **读不需要转发**。从节点只需向主节点要一个「线性一致读索引」，然后等自己的
//!   状态机追平到该索引，就可以在**本地**读取。数据不必搬来搬去，读的吞吐也不会
//!   因为所有请求都汇聚到主节点而受限。这就是 Raft 论文里的 ReadIndex。
//!
//! 两条路都能保证线性一致——读绝不会返回旧数据。区别只在于代价。
//!
//! # 重试策略：什么情况能重试，什么情况绝对不能
//!
//! 这是本模块最需要小心的部分。一次转发失败之后**盲目重试是有代价的**：
//! 如果主节点其实已经执行了 `INCR`，只是响应在回程丢了，重试就会让计数器多加一次。
//!
//! 因此重试的判据不是「失败了没有」，而是「**能否证明它没有生效**」：
//!
//! | 情况 | 能否证明未生效 | 处置 |
//! |---|---|---|
//! | 连接就建立不起来 | 能——请求根本没发出去 | 重试 |
//! | 主节点回复 `ForwardToLeader` | 能——写入从未进入日志 | 重试 |
//! | 主节点回复成员变更错误 | 能 | 重试 |
//! | 请求已发出但超时 / 连接中断 | **不能** | 立即返回「结果未知」 |
//! | 处理请求的节点在回复前停止 | **不能**——可能已提交但来不及回复 | 立即返回「结果未知」 |
//!
//! 最后一行的取舍值得说明：宁可告诉客户端「不知道」，也不能把它伪装成「成功」或
//! 「失败」。前者会让客户端以为写丢了而重复写，后者会让客户端以为写成了而丢数据。
//! 这种「结果未知」的情形与客户端直连 Redis 时连接被切断是同一类问题，
//! 语义上没有变得更差；但我们**多了一跳**，也就多了一份发生概率，因此必须说清楚。

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::{LogId, Raft};
use tokio::net::TcpStream;

use super::rpc::{RpcRequest, RpcResponse, read_message, write_message};
use super::{NodeId, TypeConfig};
use crate::config::Config;
use crate::kv::WriteOp;
use crate::protocol::Reply;

/// 单次转发的超时。
///
/// 这是**兜底**而非预期值：局域网里一次转发往返只该是毫秒量级。设成秒级是为了
/// 容忍主节点正忙于把一批日志落盘（`fsync` 是这条路径上的真正瓶颈）。
const FORWARD_TIMEOUT: Duration = Duration::from_secs(3);

/// 转发的尝试上限。
///
/// 第一个尝试在本地完成（见 [`Forwarder::with_leader`]），因此实际网络转发次数
/// 至多是 `MAX_ATTEMPTS - 1`。留出几次是为了容忍「主节点刚刚换届」这种瞬态。
const MAX_ATTEMPTS: usize = 4;

/// 两次尝试之间的间隔。
///
/// 存在的意义是给选举留出时间：客户端恰好在换届瞬间发来请求时，短暂等待远好过
/// 立刻甩一个错误回去。几轮加起来约 600 毫秒，覆盖一次正常的选举。
const RETRY_DELAY: Duration = Duration::from_millis(200);

/// 转发过程中可能出现的失败。
///
/// 每个变体都对应一种**明确的语义**，而不是笼统的「转发失败」——调用方据此决定
/// 是重试、是上报、还是告诉客户端「结果未知」。
#[derive(Debug)]
pub enum ForwardError {
    /// 集群尚未选出主节点。
    ///
    /// 这是瞬态：选举完成后自会恢复。客户端稍后重试即可。
    NoLeader,

    /// 配置中没有这个节点的地址，联系不上。
    UnknownPeer(NodeId),

    /// 主节点联系不上多数派，无法确认自己的领导权。
    ///
    /// 这是 CP 语义下的**正确行为**：此时宁可拒绝读，也绝不返回可能已经过期的数据。
    NoQuorum,

    /// 没能拿到响应，因而**无法判断请求是否已经生效**。
    ///
    /// 两种情况会走到这里：请求已经发出但连接在回程中断，或者处理它的节点在回复前
    /// 停止了服务。两者的共同点是**都排除了「确定没生效」**。
    ///
    /// 调用方必须原样把它报告给客户端，不得重试。
    OutcomeUnknown {
        /// 处理请求的节点。
        leader: NodeId,
        /// 具体原因，用于诊断。
        reason: String,
    },

    /// 反复遇到主节点变更或联系不上，重试用尽。
    Unstable {
        /// 尝试次数。
        attempts: usize,
        /// 最后一次的具体原因。
        reason: String,
    },

    /// 收到了无法解释的响应。
    ///
    /// 正常运行时不可能出现——出现了就是本层的实现缺陷。但它必须是**可处理的错误**
    /// 而不是 panic：一个节点的协议错误不该让整个进程倒下。
    Protocol {
        /// 具体原因。
        reason: String,
    },
}

impl fmt::Display for ForwardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForwardError::NoLeader => {
                write!(f, "集群当前没有主节点（选举可能正在进行），请稍后重试")
            }
            ForwardError::UnknownPeer(id) => {
                write!(f, "配置中没有节点 {id} 的地址，无法与它通信")
            }
            ForwardError::NoQuorum => write!(
                f,
                "主节点联系不上多数派节点，无法确认读取结果的时效性；\
                 为避免返回过期数据，本次读取已被拒绝"
            ),
            ForwardError::OutcomeUnknown { leader, reason } => write!(
                f,
                "请求已发往主节点 {leader}，但在收到响应前中断（{reason}）。\
                 这条命令是否已经生效无法确定，请勿在非幂等命令上直接重试"
            ),
            ForwardError::Unstable { attempts, reason } => write!(
                f,
                "连续 {attempts} 次未能联系上主节点（最后一次：{reason}），集群可能正在选举"
            ),
            ForwardError::Protocol { reason } => {
                write!(f, "节点间通信出现协议错误：{reason}")
            }
        }
    }
}

impl std::error::Error for ForwardError {}

/// 一次尝试的结果。
enum Outcome<T> {
    /// 拿到了确定的结果。
    Done(T),
    /// 对方明确表示无法处理，且**可以证明没有产生副作用**——重试是安全的。
    Retryable {
        /// 具体原因，用于最终的诊断信息。
        reason: String,
        /// 对方告知的新主节点（若有）。用它比用本地缓存更及时。
        hint: Option<NodeId>,
    },
}

/// 传输失败。
///
/// 与 [`Outcome`] 的区别：`Outcome` 说的是「对方说了什么」，这里说的是
/// 「话有没有送到」。**两者混为一谈正是重复执行的来源**，因此分成不同的类型
/// 强迫调用方分开处理。
enum TransportFailure {
    /// 连接就建立不起来——请求**肯定没有送达**。
    NotSent(std::io::Error),
    /// 请求已经发出，但结果不明。
    Unknown(std::io::Error),
}

/// 把客户端请求转交给主节点。
pub struct Forwarder {
    /// 本节点 ID。用来识别「本节点就是主节点」这一最省的情况。
    self_id: NodeId,
    /// 节点间 RPC 地址表。
    peers: HashMap<NodeId, SocketAddr>,
    /// 本地的 Raft 实例。
    ///
    /// 每个尝试都**先**在本地做一次：本节点就是主节点时这是最省的路径；
    /// 不是主节点时，本地会直接给出真正的主节点是谁——比任何缓存都可靠。
    raft: Raft<TypeConfig>,
}

impl Forwarder {
    /// 依据配置与本地 Raft 实例构建转发器。
    pub fn new(self_id: NodeId, raft: Raft<TypeConfig>, config: &Config) -> Self {
        Self {
            self_id,
            peers: config.peer_rpc_addrs(),
            raft,
        }
    }

    /// 提交一条写操作。
    ///
    /// 返回时，写入**已经存在于多数派节点上**——承诺的兑现点与直接写在主节点上完全一致。
    pub async fn write(&self, op: WriteOp) -> Result<Reply, ForwardError> {
        self.with_leader(
            || RpcRequest::ClientWrite(op.clone()),
            |leader, response| match response {
                RpcResponse::ClientWrite(Ok(reply)) => Ok(Outcome::Done(reply)),

                // 对方不是主节点。openraft 明确区分了这个错误，它意味着这次写入
                // **从未进入日志**——重试不会重复执行任何东西。
                RpcResponse::ClientWrite(Err(RaftError::APIError(
                    ClientWriteError::ForwardToLeader(hint),
                ))) => Ok(Outcome::Retryable {
                    reason: "对方不是主节点".to_string(),
                    hint: hint.leader_id,
                }),

                // 写操作不会触发成员变更；真出现了说明请求用错了地方，但同样可以
                // 确定它没有生效，因此重试仍然是安全的。
                RpcResponse::ClientWrite(Err(RaftError::APIError(
                    ClientWriteError::ChangeMembershipError(source),
                ))) => Ok(Outcome::Retryable {
                    reason: format!("主节点拒绝了这次写入：{source}"),
                    hint: None,
                }),

                // 主节点正在停止。它可能已经提交了这条写入，只是来不及回复——
                // 因此绝不能再往别处重试，否则就是重复执行。
                RpcResponse::ClientWrite(Err(RaftError::Fatal(source))) => {
                    Err(self.stopped(leader, source))
                }

                other => Err(self.mismatch("写入", &other)),
            },
        )
        .await
    }

    /// 取得一个可用于线性一致读的日志索引。
    ///
    /// 返回 `None` 表示日志为空——此时任何状态机都是空的，无需等待即可读。
    /// 调用方拿到索引后应当等待自己的状态机追平到该索引，然后读取本地状态。
    pub async fn read_index(&self) -> Result<Option<LogId<NodeId>>, ForwardError> {
        self.with_leader(
            || RpcRequest::ReadIndex,
            |leader, response| match response {
                RpcResponse::ReadIndex(Ok(log_id)) => Ok(Outcome::Done(log_id)),

                RpcResponse::ReadIndex(Err(RaftError::APIError(
                    CheckIsLeaderError::ForwardToLeader(hint),
                ))) => Ok(Outcome::Retryable {
                    reason: "对方不是主节点".to_string(),
                    hint: hint.leader_id,
                }),

                // 主节点联系不上多数派。**不能退化成读本地**——那样就会返回可能
                // 已经过期的数据，正是本项目要消灭的东西。如实拒绝。
                RpcResponse::ReadIndex(Err(RaftError::APIError(
                    CheckIsLeaderError::QuorumNotEnough(_),
                ))) => Err(ForwardError::NoQuorum),

                RpcResponse::ReadIndex(Err(RaftError::Fatal(source))) => {
                    Err(self.stopped(leader, source))
                }

                other => Err(self.mismatch("读索引", &other)),
            },
        )
        .await
    }

    // ------------------------------------------------------------ 内部

    /// 反复尝试，直到拿到确定的结果或确认再也拿不到。
    ///
    /// **第一个尝试永远在本地做**。这不只是为了省一次网络往返：
    /// 本地的 Raft 实例对「谁是主节点」的判断比任何缓存都权威，由它直接给出
    /// `ForwardToLeader` 里附带的主节点提示，比我们自己猜要可靠得多。
    async fn with_leader<T>(
        &self,
        build_request: impl Fn() -> RpcRequest,
        interpret: impl Fn(NodeId, RpcResponse) -> Result<Outcome<T>, ForwardError>,
    ) -> Result<T, ForwardError> {
        // 不变量：每一次循环要么返回结果，要么留下 reason 继续。
        let mut reason = "尚未开始".to_string();
        let mut hint: Option<NodeId> = None;

        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(RETRY_DELAY).await;
            }

            // 第 0 次在本地；之后优先采信上一轮对方告知的主节点，
            // 它比本地缓存的视图更新。
            let target = if attempt == 0 {
                Target::Local
            } else {
                match hint.take().or_else(|| self.known_leader()) {
                    Some(leader) if leader != self.self_id => match self.peers.get(&leader) {
                        Some(addr) => Target::Remote(leader, *addr),
                        None => {
                            reason = ForwardError::UnknownPeer(leader).to_string();
                            continue;
                        }
                    },
                    // 还不知道主节点是谁，或者缓存的答案就是自己（可能已过期）。
                    // 再等一轮，把新一次的本地尝试留给下一轮循环。
                    _ => {
                        reason = ForwardError::NoLeader.to_string();
                        continue;
                    }
                }
            };

            let (leader, response) = match target {
                // 本地：与 RPC 服务端共用同一个 dispatch，行为不可能分叉
                Target::Local => (
                    self.self_id,
                    super::rpc::dispatch(&self.raft, build_request()).await,
                ),
                Target::Remote(leader, addr) => {
                    match exchange(addr, build_request(), FORWARD_TIMEOUT).await {
                        Ok(response) => (leader, response),
                        // 连接都没建立起来 → 请求肯定没送达 → 重试是安全的
                        Err(TransportFailure::NotSent(source)) => {
                            reason = format!("无法连接节点 {leader}：{source}");
                            continue;
                        }
                        // 已经发出去了 → 结果不明 → **立即返回，绝不重试**
                        Err(TransportFailure::Unknown(source)) => {
                            return Err(ForwardError::OutcomeUnknown {
                                leader,
                                reason: source.to_string(),
                            });
                        }
                    }
                }
            };

            match interpret(leader, response)? {
                Outcome::Done(value) => return Ok(value),
                Outcome::Retryable {
                    reason: why,
                    hint: new_hint,
                } => {
                    reason = why;
                    hint = new_hint;
                }
            }
        }

        Err(ForwardError::Unstable {
            attempts: MAX_ATTEMPTS,
            reason,
        })
    }

    /// 从本地 Raft 指标里读出当前主节点。
    ///
    /// 这只是一个**提示**：它可能已经过期。真正的权威判断由每次尝试里的本地
    /// `dispatch` 给出。
    fn known_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// 构造「响应类型与请求不匹配」的错误。
    fn mismatch(&self, what: &str, response: &RpcResponse) -> ForwardError {
        ForwardError::Protocol {
            reason: format!("{what}请求收到了不相干的响应：{response:?}"),
        }
    }

    /// 构造「处理请求的节点在回复前停止」的错误。
    ///
    /// 措辞刻意区分「本节点」与「远端主节点」：两者对运维的含义完全不同——
    /// 前者说明这个进程正在关闭（比如收到了停止信号），后者说明集群可能要换届了。
    /// 把它们说成同一句话，排查时就没有方向。
    fn stopped(&self, leader: NodeId, source: impl fmt::Display) -> ForwardError {
        let who = if leader == self.self_id {
            "本节点".to_string()
        } else {
            format!("主节点 {leader}")
        };

        ForwardError::OutcomeUnknown {
            leader,
            reason: format!("{who}在处理请求时停止服务：{source}"),
        }
    }
}

/// 一次尝试的目标。
enum Target {
    /// 本节点自己。
    Local,
    /// 远端主节点及其 RPC 地址。
    Remote(NodeId, SocketAddr),
}

/// 发出一次转发请求。
///
/// **连接与收发被刻意分成两步**：它们失败的语义完全不同。连不上意味着请求没有送出去，
/// 可以放心重试；已经送出去之后的任何失败都无法排除「对方已经执行了」的可能。
/// 如果像普通 RPC 那样把两步合并、统一返回一个 io 错误，这个区别就永远丢了。
async fn exchange(
    addr: SocketAddr,
    request: RpcRequest,
    limit: Duration,
) -> Result<RpcResponse, TransportFailure> {
    let stream = match tokio::time::timeout(limit, TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(source)) => return Err(TransportFailure::NotSent(source)),
        Err(_elapsed) => {
            return Err(TransportFailure::NotSent(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "建立连接超时",
            )));
        }
    };

    let mut stream = stream;
    let _ = stream.set_nodelay(true);

    // 从这里开始，请求可能被对方收到并执行——失败一律归为「结果未知」
    let round_trip = async {
        write_message(&mut stream, &request).await?;
        read_message::<RpcResponse>(&mut stream).await
    };

    match tokio::time::timeout(limit, round_trip).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(source)) => Err(TransportFailure::Unknown(source)),
        Err(_elapsed) => Err(TransportFailure::Unknown(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "等待响应超时",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_unknown_message_warns_against_retrying() {
        // 这条错误信息的措辞是有实际后果的：客户端读到它之后是否重试，
        // 决定了数据会不会被写两遍。因此措辞本身值得被测试锁定。
        let error = ForwardError::OutcomeUnknown {
            leader: 2,
            reason: "连接被重置".to_string(),
        };
        let message = error.to_string();

        assert!(message.contains("无法确定"), "应说明结果不确定：{message}");
        assert!(
            message.contains("请勿") && message.contains("重试"),
            "应明确劝阻盲目重试：{message}"
        );
    }

    #[test]
    fn no_quorum_explains_why_the_read_was_refused() {
        let message = ForwardError::NoQuorum.to_string();

        assert!(
            message.contains("过期"),
            "应说明拒绝的原因是为了避免返回过期数据：{message}"
        );
    }

    #[test]
    fn unstable_error_reports_the_attempt_count() {
        let error = ForwardError::Unstable {
            attempts: 4,
            reason: "对方不是主节点".to_string(),
        };
        let message = error.to_string();

        assert!(message.contains('4'), "应报告尝试次数：{message}");
        assert!(
            message.contains("对方不是主节点"),
            "应保留最后一次的具体原因：{message}"
        );
    }
}
