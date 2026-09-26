//! RESP3 协议层。
//!
//! 本层分四步，职责严格分离：
//!
//! 1. **底层编解码**：由 `redis-protocol` 的 codec 提供（选型依据见 ADR-011），
//!    负责「网络字节 ⇄ 帧」。这一步不是我们写的，也不是我们该写的。
//! 2. **方言适配**（[`codec`]）：把 RESP2 与 RESP3 的差异封在本层内部。
//!    上层只看到 `Result<Command, CommandError>` 的流与 `Reply` 的汇，无需感知方言。
//! 3. **请求解析**（[`command`]）：把帧翻译成强类型的 [`Command`]。
//! 4. **响应编码**（[`reply`]）：把 [`Reply`] 翻译回帧。
//!
//! 这一层**完全不知道 Raft 的存在**。它只做数据表示的转换，不碰任何业务语义——
//! 因此可以脱离集群单独测试，这也是实施顺序上「先做出单机可用版本」的架构基础。

pub mod codec;
pub mod command;
pub mod reply;

pub use codec::{FramingError, RespCodec};
pub use command::{Command, CommandError, SetCondition, SetExpire};
pub use reply::{Dialect, Reply};
