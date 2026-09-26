# Spec — RESP3 协议层

## 要构建什么

- **目标**：把网络字节流翻译成强类型的 [`Command`]，把 [`Reply`] 翻译回字节流。**对上层屏蔽 RESP2 与 RESP3 的方言差异。**
- **为什么需要**：「兼容 Redis 生态」是本项目的立足点。这一层是全部兼容性的落点，也是唯一与客户端直接对话的层。
- **位置**：`src/protocol/`，分三个文件：
  - `codec.rs` — 统一编解码器，对外唯一入口
  - `command.rs` — 请求解析
  - `reply.rs` — 响应构造与编码

## 行为

### 分层

底层帧编解码由 `redis-protocol` 提供（选型依据见 ADR-011）。本模块在其上做三件事：

1. **方言适配**：RESP2 与 RESP3 的帧类型不同，由 `codec.rs` 归一，上层无感
2. **命令解析**：帧 → `Command` 枚举，含参数校验
3. **响应编码**：`Reply` → 帧，按当前方言选择编码方式

### 两种错误，两种命运

这是本层最重要的设计：

| 错误 | 类型 | 含义 | 处理 |
|---|---|---|---|
| 命令层面 | `Item = Err(CommandError)` | 帧结构完整，但命令不认识或参数不合法 | 回复错误，**连接继续可用** |
| 帧层面 | `Error = FramingError` | 字节流已失去对齐 | 不可恢复，**必须关闭连接** |

把前者做成 `Item` 而不是 `Error`，是因为**客户端发错命令是正常现象**——打错一个字母就断连的服务是不可用的。

### 方言协商

默认 RESP2（未发送 `HELLO` 的客户端所期望的）。客户端发送 `HELLO 3` 后，会话层调用 `RespCodec::set_dialect` 切换。

必须区分方言的根本原因在于**空值的编码不同**：

| 语义 | RESP2 | RESP3 |
|---|---|---|
| 空值 | `$-1\r\n` | `_\r\n` |

对只懂 RESP2 的客户端发送 `_\r\n` 会导致其解析失败。

### 错误文本的语言

`CommandError` 的 `Display` 输出**会直接发给客户端**，因此采用 Redis 的标准英文措辞；面向人类阅读的错误（如配置错误）才用中文——那些走 stderr，不上网络。有一条测试专门锁定「错误文本必须是纯 ASCII」。

## 输入 / 输出

### 输入

- 网络字节流（标准 RESP 数组，或内联命令文本）
- 上层调用 `RespCodec::set_dialect` 切换方言

### 输出

- `Decoder::Item = Result<Command, CommandError>` — 命令流
- `Encoder<Reply>` — 响应汇

### 支持的命令

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

## 约束

- **不得**在协议层引入任何业务语义——它只做数据表示的转换，不知道 Raft、存储的存在。
- **不得**让客户端发送垃圾数据导致 panic 或进程崩溃。
- `Decoder::Item` 承载命令层面的错误，**不得**把这类错误升级为 `DecodeError`。
- 走网络的错误文本**必须**是纯 ASCII 且符合 Redis 措辞。
- 命令名匹配**必须**大小写不敏感。
- 块字符串**必须**二进制安全（长度前缀是唯一边界，不得对内容做任何转义或截断）。

## 边界条件

| 情况 | 行为 |
|---|---|
| 数据未收全（TCP 分包） | 返回 `None` 等待，**不得**报错 |
| 一次收到多条命令（流水线） | 逐条取出，不丢不乱 |
| **内联命令**（首字节非 RESP 类型前缀） | 自行解析；行未收全时等待而非报错 |
| **内联命令被 TCP 切开** | 返回 `None` 等待，**不得**交给底层解码器（它会报致命错误） |
| 内联空行（telnet 常见） | 跳过，不当作错误 |
| 内联命令有连续多个空格 | 视作单个分隔符 |
| 未知命令 | `CommandError::UnknownCommand`，连接继续 |
| 参数个数不对 | `CommandError::WrongArity`，连接继续 |
| 参数不是标量帧（如嵌套数组） | `CommandError::NotACommand`，连接继续 |
| 块字符串含 `\r\n` 或 NUL | 原样传输，长度前缀决定边界 |

## 已探明的两个坑（实测得出，非推测）

1. **`redis-protocol` 不支持内联命令。** 遇到首字节不是 RESP 类型前缀的输入，它返回 `Invalid frame type` 的解码错误。由于帧层面错误在本设计中意味着断连，必须由我们在 `try_parse_inline` 中**先行拦截**。上游调研曾声称该库「已处理内联命令」，实测证明是错的。
2. **`RespVersion::to_byte()` 返回的是 ASCII 字符**（`b'2'` / `b'3'`，即 50/51），而非数字版本号 2/3。转换需减去 `b'0'`。

## 验收标准

- [x] 标准 RESP 数组命令能解析为 `Command`
- [x] 命令名大小写不敏感
- [x] `SET` 的全部选项（EX/PX/EXAT/PXAT/NX/XX/KEEPTTL）能解析，选项顺序可颠倒
- [x] 变参命令（DEL/MSET/MGET）能解析
- [x] 整数参数能解析，非整数被拒绝
- [x] `CLUSTER` / `RAFT` 子命令能解析
- [x] 未知命令返回符合 Redis 措辞的错误
- [x] 参数个数错误的文本以 Redis 标准措辞开头
- [x] `HELLO 2` / `HELLO 3` 能正确解析版本号
- [x] 非标量帧参数被拒绝，不 panic
- [x] 错误文本全部为 ASCII
- [x] 两种方言的响应编码符合各自规范
- [x] **空值在两种方言下编码不同**（`$-1\r\n` vs `_\r\n`）
- [x] 块字符串二进制安全
- [x] 嵌套数组编码正确
- [x] 空数组与空值不混淆
- [x] 半包返回 `None` 而非报错
- [x] 命令层面的错误不终止连接，后续命令仍可解析
- [x] 方言可切换，切换后编码随之改变
- [x] **内联命令能解析**
- [x] **内联命令被 TCP 切开时等待而非报错**
- [x] 内联空行被跳过
- [x] 内联命令的连续空格被正确切分
- [x] 内联与标准帧可在同一连接上混用
- [x] 流水线命令逐条取出

## 完成定义

- `cargo test` 中 `protocol` 模块的 33 个测试全部通过
- `cargo clippy --all-targets` 无警告
- `cargo fmt --check` 无差异
- 测试**全部通过真实的 `redis-protocol` 编解码器**，而非手工构造帧——否则验证不了与底层库的衔接
