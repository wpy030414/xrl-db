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

### 用 Redis 客户端连接

```bash
redis-cli -p 7001
> SET foo bar
OK
> GET foo
"bar"
```

### 运行三节点集群

```bash
./scripts/cluster-up.sh
```

一条命令拉起三个真实进程（端口 7001~7003），等集群选出主节点后返回。节点在后台运行，日志与 PID 在 `target/cluster/` 下。

```bash
$ redis-cli -p 7001 SET foo bar      # 连主节点
OK
$ redis-cli -p 7002 GET foo          # 连从节点 —— 一样能读，也能写
"bar"
$ redis-cli -p 7002 SET baz qux      # 发往从节点的写会被转发给主节点
OK
```

客户端**连任意一个节点都可以**——不需要知道谁是主节点，也没有 `MOVED` 重定向。这是相对 Redis Cluster 的一点额外便利：那边要求客户端自己维护槽位映射。

停止：`./scripts/cluster-down.sh`；连数据一起清掉：`./scripts/cluster-down.sh --wipe`。

#### 手工启动（写自己的 systemd / 容器编排时）

```bash
# 每个节点一份配置，cluster.peers 里列出全部三个节点
xrl-db --config node-1.toml --bootstrap   # 只有这一个节点带 --bootstrap
xrl-db --config node-2.toml
xrl-db --config node-3.toml
```

**整个集群里只能有一个节点带 `--bootstrap`。** 它会按 `cluster.peers` 把其余节点逐个纳入；
不带这个开关的节点只启动、等待被纳入。若让每个节点各自引导，会得到三个互不相干、
且永远不会合并的集群——而且**没有任何报错**。

单节点部署（未启用集群，或 `peers` 里只有自己）不需要这个开关，会自动完成引导。

运行时增加节点：

```bash
redis-cli -p 7001 RAFT ADD-NODE 4 127.0.0.1:7004   # 对**主节点**执行
```

### 验证集群真的不会丢数据

```bash
./scripts/verify-cluster.sh
```

这个脚本用**真实的操作系统进程**和真实的 `kill -9` 跑一遍完整验收：写入 100 个键 → `kill -9` 主节点 → 等新主节点产生 → 确认 100 个键一个不丢 → 重启被杀节点确认它能追平 → 整组重启确认数据仍在 → 只留一个节点确认它拒绝服务。

退出码 0 表示全部通过。

## 当前状态

- **阶段**：开发中。单机与三节点集群均可用，最核心的「不丢数据」承诺已经兑现并有测试盯住。

### 已可用

- **强一致性**：Raft 单组共识。客户端收到 `OK` 时，写入已经存在于多数派节点上——主节点被 `kill -9` 也不会丢
- **线性一致读**：读经过 ReadIndex 确认，**绝不会返回过期数据**；失去多数派时宁可拒绝读
- **客户端零改造**：连任意节点都能读写。发往从节点的请求由服务端转发给主节点
- **协议兼容**：RESP2 与 RESP3 双方言（含 `HELLO` 协商与内联命令），官方 `redis-cli` 零改造直连
- **持久化**：日志与状态机落盘到 redb，进程重启后数据仍在
- **快照与日志截断**：日志不会无限增长
- **数据模型**：String 类型 + TTL（惰性过期）
- **配置**：TOML 与 `redis.conf` 双格式，解析后归一

已支持的命令：

```
连接:   PING  ECHO  QUIT  HELLO
字符串: GET  SET（含 EX/PX/EXAT/PXAT/NX/XX/KEEPTTL）
        DEL  EXISTS  MSET  MGET  APPEND  STRLEN
        INCR  DECR  INCRBY  DECRBY
键管理: EXPIRE  TTL  PERSIST  KEYS  SCAN  TYPE
服务器: INFO  DBSIZE  FLUSHDB
集群:   CLUSTER INFO
        RAFT LEADER / RAFT INFO / RAFT ADD-NODE
```

尚未支持的命令会返回规范的 Redis 错误，而非静默失败或断开连接。

### 已知限制

这几条是真实的、会影响使用的限制，列在这里而不是藏起来：

- **写入可能返回「结果未知」**。请求已经发出、但连接在回程中断时，服务端**无法判断**这次写入究竟有没有生效。它会把这句话原样告诉客户端，并明确劝阻盲目重试。Redis 在客户端连接被切断时是同样的情况，但我们比它多了一跳转发，也就多了一份发生概率。彻底的解法是「请求去重」（客户端带请求号 + 服务端去重表），尚未实现——见 `docs/DECISIONS.md`。
- **不做分片**。单 Raft 组意味着**写入吞吐上限 = 单个主节点的吞吐上限**，数据量受单机磁盘限制。分片留待下一阶段。
- **节点间 RPC 每次往返都新建一条 TCP 连接**，所以持续的高读压会耗尽本机的临时端口：`connect()` 开始返回 `EADDRNOTAVAIL`（`os error 49`），表现是转发失败（`ERR 连续 4 次未能完成转发：无法连接节点 N：Can't assign requested address`），或者主节点以「联系不上多数派节点」为由拒绝读，随后可能换届。本机实测从节点的读约 12000 次/秒时开始出现，而临时端口只有 16384 个（49152–65535）。修法是给节点间 RPC 加**按对端复用**的连接池——见 `docs/DECISIONS.md` 的 ADR-018。
  复现（集群跑着的时候）：`redis-benchmark -p 7002 -t set,get,incr -n 3000 -c 50`
- **单个客户端可以靠一个夸大的长度前缀让服务端缓冲区无限增长**。收到 `$9999999999999\r\n` 这样的头部时，服务端会一直等下去，并把收到的字节都攒在内存里（Redis 用 `proto-max-bulk-len` 挡住这件事，本项未实现）。默认只监听回环地址，所以这不是一个远程可利用的问题；但把它暴露出去之前必须补上。
- **仅支持 String 类型**；Hash / List / Set / ZSet 尚未实现。即便只用已知命令，异构数据结构的运维脚本也会受影响。
- **未实现 AUTH**，默认只监听 `127.0.0.1`。**不要把它暴露到不可信网络**。
- **成员变更必须手工执行**（`RAFT ADD-NODE`），且只能对主节点执行。新增节点必须出现在所有现有节点的 `cluster.peers` 配置里。
- `CONFIG GET/SET/REWRITE` 未实现
- 无连接数上限与空闲超时
- 未做性能基准，README 里也就没有任何吞吐数字

## 性能

数字来自 `scripts/bench.sh`（用官方的 `redis-benchmark` 打真实进程，走完整的协议与共识路径），在一台开发机上测得。**这不是基准测试报告**——同一份脚本可以自己跑一遍核对，但绝对数字会随硬件变化。

| 场景 | 写入 p50（单连接） | 读取 p50（单连接） |
|---|---|---|
| 单节点 | 7.0 ms | 0.06 ms |
| 三节点 · 主节点 | 20.0 ms | 0.18 ms |
| 三节点 · 从节点 | 20.0 ms（转发给主节点） | 1.0 ms（一次 ReadIndex 往返） |

吞吐（50 个并发客户端）：读约 11 万次/秒（单节点）、1.2～1.8 万次/秒（三节点）；写约 137 次/秒。

**写入为什么是这个量级**：每次提交都要一次 `fsync`（`Durability::Immediate`，见 ADR-005），而且提交路径是**串行**的。所以 50 个并发客户端与 1 个客户端测出来的写入吞吐是一样的（都约 137 次/秒）——加并发只增加排队，并发那一轮的 p50 从 7 ms 涨到 390 ms 就是这个原因。这不是缺陷，是「不丢数据」的价钱：Redis 不做这次 `fsync`，所以它能快两个数量级，代价是故障切换时会丢写。

**读取为什么能快这么多**：读不写日志、不落盘。单节点的一次读只有协议解析与一次内存查表；从节点多一次 ReadIndex 往返（ADR-012），但数据本身不搬运。

```bash
./scripts/bench.sh              # 单节点
./scripts/bench.sh --cluster    # 三节点，分别量主节点与从节点
```

## 核心技术

| 领域 | 选型 |
|---|---|
| 语言 | Rust（edition 2024） |
| 网络协议 | Redis RESP3 兼容（基于 `redis-protocol` codec） |
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
