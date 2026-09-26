# AGENTS.md

本文件约束 AI Agent（Claude / Cursor / Copilot 等）在本项目中的行为边界。人类开发者同样建议遵守。

## 概述

- **本项目是什么**：XRLDB，一个用 Rust 实现的、兼容 Redis RESP3 协议的分布式键值数据库，基于 openraft 提供 Raft 强一致性。
- **当前阶段**：原型开发中。架构为「单 Raft 组」，分片能力留待下一阶段。
- **项目的立足点**：兼容 Redis 生态 + 提供 Redis 拿不到的强一致性。任何违背这个定位的改动都应当被质疑。

## 边界与范围

### 范围内

- Redis RESP3 协议兼容（基于 `redis-protocol` 的 codec 模块 + tokio-util `Framed`）
- 单 Raft 组强一致性（openraft + redb）
- String 类型命令（见 PRD 的命令清单）
- 写请求在 follower 上透明转发到 leader
- TOML 与 `redis.conf` 双格式配置
- 快照与日志截断
- 故障注入测试（kill leader、网络分区、节点重启）

### 非目标（明确排除）

- **不做** 分片 / 多 Raft 组（架构 3）——这是下一阶段的事
- **不做** Hash / List / Set / ZSet 数据结构
- **不做** AUTH 与访问控制（默认只监听 `127.0.0.1`）
- **不做** `CONFIG REWRITE` 的「温和重写」语义
- **不做** 从 follower 读的降级模式
- **不做** 前端 UI 与管理面板
- **不做** 自研存储引擎——崩溃一致性不重造轮子
- **不做** 自研 Raft——共识正确性不重造轮子

## Agent 操作指南

### 铁律

1. **分阶段可运行**：任何时刻，项目都必须**可编译、可测试、可运行**。不允许留下「编译不过的中间状态」提交进仓库。
2. **禁止在可预期的错误路径上使用 `unwrap()` / `expect()`**。网络、磁盘、解析、序列化这些地方出错是常态，必须显式处理。仅在逻辑上不可能失败的地方（如已判定的 `Some`）才可放宽，并加注释说明为什么不可能失败。
3. **错误类型统一**：使用 `src/error.rs` 中定义的自定义错误类型，通过 `thiserror` 风格的手写实现或统一枚举传递，禁止到处 `Box<dyn Error>`。
4. **先做出单机可用版本，再接入 Raft**。这是刻意的顺序——分布式调试最怕整条链路同时不可用，先让协议层单独跑通。
5. **openraft 一律以 0.9 的 API 为准**。0.9 已把旧的单一 `RaftStorage` 拆成 `RaftLogStorage` + `RaftStateMachine` 两个 trait，网上大量教程（含官方 getting-started 页）仍是 0.8 旧 API，**照抄会编译不过**。

### 代码风格

- **全中文注释**。文档注释（`///`）与行内注释（`//`）一律使用中文。
- 标识符（变量、函数、类型名）使用英文。
- Rust edition 2024。
- 提交前必须 `cargo clippy` 无警告。
- 新增模块必须有模块级文档注释（`//!`）。

### 测试约定

- 单元测试写在源文件内的 `#[cfg(test)] mod tests`。
- 集成测试放 `tests/` 目录（每个顶层 `.rs` 是独立 crate，只能访问 `pub` API）。
- **redb 是单进程的**（文件锁，重复打开会返回 `DatabaseAlreadyOpen`）。任何多节点测试必须给每个节点**独立目录**，绝不能共享同一个 redb 文件。
- 存储实现必须先通过 `openraft::testing::Suite::test_all`，再接入网络层。
- 测试失败必须以非零退出码结束。

## 目录速查

| 路径 | 职责 |
|---|---|
| `src/main.rs` | 入口：CLI 解析、配置加载、启动 |
| `src/config.rs` | 配置加载与校验（TOML 与 `redis.conf` 归一） |
| `src/error.rs` | 统一错误类型 |
| `src/node.rs` | 节点协调：拼装 Raft + 网络 + 存储 |
| `src/protocol/` | RESP3 编解码与命令解析 |
| `src/server/` | TCP 监听、连接管理、命令分发 |
| `src/raft/` | openraft 存储 trait 实现与节点间 RPC |
| `src/kv/` | 键值数据模型与 TTL 逻辑 |
| `docs/` | 项目文档（见 README 的文档索引） |
| `docs/specs/` | 各模块详细规格，随模块实现逐步填充 |
| `scripts/` | 集群启停与验证脚本 |
| `tests/` | 集成测试 |
