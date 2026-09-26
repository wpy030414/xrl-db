//! 键值数据模型与状态机。
//!
//! # 分层
//!
//! - [`op`] — [`WriteOp`]：可复制的写操作，**不含相对时间**
//! - [`store`] — [`Store`]：纯内存状态机，所有方法显式接收当前时刻
//! - [`glob`] — Redis 风格的键模式匹配
//!
//! # 时间从哪来
//!
//! 本模块**唯一**读取系统时钟的地方是 [`now_ms`]。除此之外，所有涉及时间的函数
//! 都要求调用方把时刻作为参数传进来。
//!
//! 这个约束看起来啰嗦，但它是本项目能平滑地从「单机」走到「集群」的关键：
//! 单机模式下调用方传入 [`now_ms`]，集群模式下由 leader 把时刻写进日志再传给各副本。
//! 状态机本身完全不知道自己在哪种模式下运行，也就不需要任何改动。

pub mod glob;
pub mod op;
pub mod store;

pub use op::{TimestampMs, WriteOp};
pub use store::Store;

/// 当前时刻，单位毫秒（Unix 纪元）。
///
/// 这是本模块唯一读取系统时钟的地方。之所以把它单独拎出来，是为了让「读时钟」
/// 这个动作在代码里显眼且集中——任何在状态机内部偷偷调用它的写法都会立刻暴露。
pub fn now_ms() -> TimestampMs {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_millis().min(u64::MAX as u128) as u64,
        // 系统时钟早于 Unix 纪元。这属于极端异常（时钟被错误设置），
        // 但进程不该因此崩溃——返回 0 让服务继续可用。
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ms_is_a_plausible_wall_clock_value() {
        let value = now_ms();

        // 应当晚于 2020-01-01，早于 2100-01-01。范围放宽是为了不因
        // 运行环境的时钟差异而误报，但仍能抓住「单位搞错」这类错误
        // （比如误用秒而不是毫秒）。
        const YEAR_2020_MS: u64 = 1_577_836_800_000;
        const YEAR_2100_MS: u64 = 4_102_444_800_000;

        assert!(
            value > YEAR_2020_MS && value < YEAR_2100_MS,
            "当前时刻 {value} 不在合理范围内，单位可能不是毫秒"
        );
    }

    #[test]
    fn now_ms_is_monotonic_within_a_short_window() {
        // 系统时钟不应在两次相邻调用之间倒退
        let first = now_ms();
        let second = now_ms();

        assert!(second >= first, "时钟出现倒退：{first} → {second}");
    }
}
