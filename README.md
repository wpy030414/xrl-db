# XRLDB

一个兼容 Redis RESP3 协议、基于 Raft 提供强一致性的分布式键值数据库。

## 这是什么？

- **定位**：用 Rust 实现的、便于配置的集群键值数据库。客户端可以直接使用任何现成的 Redis 客户端，无需改造。
- **解决的核心问题**：现有的 Redis 兼容方案在网络分区与主节点故障时会丢数据。XRLDB 用 Raft 共识解决这个问题——**保留 Redis 的生态兼容性，同时把一致性做到数据库级别**。

## 为什么存在？

Redis 是世界上最流行的内存数据结构服务，Redis Cluster 也被广泛用于集群场景。但它的官方文档明确说明：**Redis Cluster 不提供强一致性保证**。它使用异步复制，主节点故障切换时，尚未同步到副本的写入会永久丢失。

这对「把 Redis 当作缓存」的场景完全够用，但对「把 Redis 当作可靠存储」的场景是一个真实的风险——你可能在下单成功后丢掉那笔订单。

XRLDB 的出发点是一个具体的疑问：

> **能不能保留 Redis 的全部生态兼容性，同时提供 Redis 拿不到的强一致性？**

如果能做到，那么从 Redis 迁移到 XRLDB 的成本就只是改一个端口号，而收益是数据不再丢失。

## 如何安装和运行？

### 前置要求

- Rust 1.97.0 或更高版本
- 可选：`redis-cli`（用于验证兼容性）

### 安装

```bash
git clone https://github.com/wpy030414/xrl-db.git
cd xrl-db
cargo build --release
```

### 运行单节点

```bash
cargo run --release -- --config xrldb.toml
```

### 运行三节点集群

```bash
./scripts/cluster-up.sh      # 拉起三个节点
./scripts/verify-cluster.sh  # 验证故障转移后数据不丢
./scripts/cluster-down.sh    # 停止集群
```

### 用 Redis 客户端连接

```bash
redis-cli -p 7001
> SET foo bar
OK
> GET foo
"bar"
```

## 当前状态

- **阶段**：原型开发中
- **已实现**：*（随开发进度更新）*
- **已知限制**：
  - 尚未支持分片；集群为单 Raft 组，数据量受单机内存限制
  - 尚未支持 AUTH，默认只监听 `127.0.0.1`
  - 仅支持 String 类型命令；Hash / List / Set / ZSet 尚未实现
  - `CONFIG GET/SET/REWRITE` 尚未实现

## 核心技术

| 领域 | 选型 |
|---|---|
| 语言 | Rust（edition 2024） |
| 网络协议 | Redis RESP3 兼容（自研编解码） |
| 共识 | openraft（单 Raft 组，强一致 CP） |
| 存储 | redb（纯 Rust、ACID、快照隔离） |
| 异步运行时 | tokio |
| 配置 | TOML 原生 + `redis.conf` 兼容层 |

## 文档

| 文档 | 内容 |
|---|---|
| [docs/PRD.md](docs/PRD.md) | 产品目标、用户场景、功能范围 |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 系统结构、模块职责、数据流 |
| [docs/DECISIONS.md](docs/DECISIONS.md) | 关键技术决策及其理由 |
| [docs/specs/](docs/specs/) | 各模块的详细规格 |
| [AGENTS.md](AGENTS.md) | 本项目的开发约定与 AI Agent 行为边界 |

## 许可证

*（待定）*
