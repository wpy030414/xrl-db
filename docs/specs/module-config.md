# Spec — 配置加载与校验

## 要构建什么

- **目标**：把配置文件（TOML 或 `redis.conf` 风格）解析为一个强类型的 `Config` 结构，并在使用前完成语义校验。
- **为什么需要**：「便于配置」是本项目的卖点之一。沿用 Redis 的运维应当可以把现有配置文件直接拿来用，不需要学新格式；同时类型安全的原生格式也要可用。
- **位置**：`src/config.rs`

## 行为

- 按**文件扩展名**分派解析器：`.toml` 走 TOML，其他一律按 `redis.conf` 风格的行式格式解析。
- 两种格式解析后**归一到完全相同的 `Config` 值**——这是核心承诺，有测试锁定。
- 解析（`from_file`）与校验（`resolve`）分离：前者只关心语法，后者关心语义。
- **遇到不认识的配置项一律报错**，绝不静默忽略。静默忽略一次配置是运维事故的经典成因——使用者以为什么都配好了，实际没有。
- 未指定数据目录时，按节点 ID 推导为 `./data/node-{id}`，避免同机多节点共用一个 redb 文件（redb 是单进程的，共用会导致后启动的节点打不开）。

## 输入 / 输出

### 输入

两个构造函数入口：

| 入口 | 说明 |
|---|---|
| `Config::from_file(&Path)` | 按扩展名分派解析器 |
| `Config::default()` | 全默认值，用于纯命令行/环境变量启动 |

配置文件结构（TOML）：

| 分节 | 字段 | 说明 |
|---|---|---|
| `node` | `id` | 集群内唯一的节点标识 |
| | `listen` | 监听地址，默认 `127.0.0.1:7001` |
| `cluster` | `enabled` | 是否启用集群模式 |
| | `peers` | 全部节点（**必须含本节点**） |
| `storage` | `path` | 数据目录，未指定时按节点 ID 推导 |
| `raft` | `election_timeout_ms` | 选举超时，默认 300 |
| | `heartbeat_interval_ms` | 心跳间隔，默认 100 |

`redis.conf` 风格的配置项映射：

| 行式配置项 | 对应字段 |
|---|---|
| `bind <ip>` | `node.listen` 的地址部分 |
| `port <n>` | `node.listen` 的端口部分 |
| `dir <path>` | `storage.path` |
| `cluster-enabled yes\|no` | `cluster.enabled` |
| `cluster-node-id <n>` | `node.id` |
| `cluster-peer <id> <addr>` | 追加一项到 `cluster.peers` |
| `cluster-election-timeout-ms <n>` | `raft.election_timeout_ms` |
| `cluster-heartbeat-interval-ms <n>` | `raft.heartbeat_interval_ms` |

### 输出

- 成功：`Config`（经 `resolve()` 后保证已填充默认值且通过校验）
- 失败：`Error::ConfigRead` / `Error::ConfigToml` / `Error::ConfigLine` / `Error::ConfigInvalid`

## 约束

- **必须**拒绝未知的配置项（TOML 用 `deny_unknown_fields`，行式格式用显式白名单匹配）。
- **必须**在错误信息中给出具体位置：行式格式给行号，TOML 给字段名。
- **不得**在 `Config` 中引入 `Option` 字段让调用方自行处理默认值——默认值推导由 `finalize()` 集中完成。
- **不得**让 `storage_path()` panic，即使配置未经 `resolve()`。
- 配置优先级**必须**为：CLI 参数 > 环境变量 > 配置文件 > 内置默认值。
- 注释语言为中文。

## 边界条件

| 情况 | 行为 |
|---|---|
| 配置文件只有注释和空行 | 全部忽略，得到默认配置 |
| 行内 `#` 之后的内容 | 视为注释丢弃（本项目配置项的值不需要包含 `#`） |
| `bind` 值带 `-` 前缀（Redis 的「绑定失败不致命」语义） | 只取地址本身，忽略前缀 |
| `redis.conf` 只指定了 `bind` 或只指定了 `port` | 另一项回落到默认值后合并 |
| **均未**指定 `bind` 和 `port` | 保留默认监听地址，不被覆盖 |
| `cluster.enabled` 为 `true` 但 `peers` 为空 | 报错 |
| 本节点 ID 不在 `peers` 中 | 报错 |
| `peers` 中存在重复的 ID 或地址 | 报错 |
| `election_timeout_ms < heartbeat_interval_ms × 2` | 报错（否则 follower 会在收到心跳前超时，陷入持续选举） |
| `heartbeat_interval_ms` 为 0 | 报错 |
| `cluster.enabled` 为 `false` | 完全不要求 `peers`，单机模式开箱即用 |

## 验收标准

- [x] TOML 配置能解析全部分节
- [x] TOML 中拼错的字段被拒绝，且错误信息指出字段名
- [x] `redis.conf` 风格配置能解析
- [x] `redis.conf` 中不支持的指令被拒绝，且错误信息给出行号
- [x] 注释与空行被正确忽略
- [x] 两种格式解析后归一到**完全相同**的 `Config`（用 `assert_eq!` 锁定）
- [x] 默认数据目录按节点 ID 分开
- [x] 心跳间隔必须显著小于选举超时
- [x] 集群模式下本节点必须在 `peers` 中
- [x] 重复的节点 ID 被拒绝
- [x] 单机模式不要求 `peers`
- [x] CLI 与环境变量能覆盖配置文件
- [x] 实际二进制用两种配置格式启动，输出逐字一致（端到端验证，非仅单测）

## 完成定义

- `cargo test` 中 `config` 模块的 10 个测试全部通过
- `cargo clippy --all-targets` 无警告
- `cargo fmt --check` 无差异
- 用 `conf/xrldb.example.toml` 与 `conf/redis.example.conf` 分别启动二进制，输出一致
