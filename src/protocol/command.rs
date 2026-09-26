//! 客户端命令的解析。
//!
//! 把 `redis-protocol` 解码出的 [`BytesFrame`] 翻译成强类型的 [`Command`]。
//!
//! # 为什么错误信息是英文
//!
//! [`CommandError`] 的 `Display` 输出会**直接作为 RESP 错误发给客户端**，
//! 因此采用 Redis 的标准英文措辞（`ERR wrong number of arguments for 'get' command`）。
//! 客户端库与测试可能匹配这些字符串，改成中文会破坏兼容性。
//! 面向人类阅读的错误（如配置错误）才使用中文——那些走的是 stderr，不上网络。
//!
//! [`BytesFrame`]: redis_protocol::resp3::types::BytesFrame

use std::fmt;

use bytes::Bytes;
use redis_protocol::resp3::types::BytesFrame;

/// 一条解析完成的客户端命令。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    // ---------- 连接 ----------
    /// `PING [message]`
    Ping { message: Option<Bytes> },
    /// `ECHO message`
    Echo { message: Bytes },
    /// `QUIT`
    Quit,
    /// `HELLO [version]`
    Hello { version: Option<u8> },

    // ---------- 字符串 ----------
    /// `GET key`
    Get { key: Bytes },
    /// `SET key value [EX s | PX ms | EXAT ts | PXAT ts | KEEPTTL] [NX | XX]`
    Set {
        key: Bytes,
        value: Bytes,
        expire: Option<SetExpire>,
        condition: SetCondition,
    },
    /// `DEL key [key ...]`
    Del { keys: Vec<Bytes> },
    /// `EXISTS key [key ...]`
    Exists { keys: Vec<Bytes> },
    /// `MSET key value [key value ...]`
    MSet { pairs: Vec<(Bytes, Bytes)> },
    /// `MGET key [key ...]`
    MGet { keys: Vec<Bytes> },
    /// `APPEND key value`
    Append { key: Bytes, value: Bytes },
    /// `STRLEN key`
    Strlen { key: Bytes },
    /// `INCR key`
    Incr { key: Bytes },
    /// `DECR key`
    Decr { key: Bytes },
    /// `INCRBY key delta`
    IncrBy { key: Bytes, delta: i64 },
    /// `DECRBY key delta`
    DecrBy { key: Bytes, delta: i64 },

    // ---------- 键管理 ----------
    /// `EXPIRE key seconds`
    Expire { key: Bytes, seconds: i64 },
    /// `TTL key`
    Ttl { key: Bytes },
    /// `PERSIST key`
    Persist { key: Bytes },
    /// `KEYS pattern`
    Keys { pattern: Bytes },
    /// `SCAN cursor [MATCH pattern] [COUNT count]`
    Scan {
        cursor: u64,
        pattern: Option<Bytes>,
        count: Option<u64>,
    },
    /// `TYPE key`
    Type { key: Bytes },

    // ---------- 服务器 ----------
    /// `INFO [section]`
    Info { section: Option<Bytes> },
    /// `DBSIZE`
    Dbsize,
    /// `FLUSHDB`
    FlushDb,

    // ---------- 集群 ----------
    /// `CLUSTER INFO`
    ClusterInfo,
    /// `RAFT LEADER`
    RaftLeader,
    /// `RAFT INFO`
    RaftInfo,
    /// `RAFT ADD-NODE <id> <addr>`
    RaftAddNode { id: u64, addr: String },
}

/// `SET` 的过期时间设定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetExpire {
    /// `EX`：相对秒数
    Seconds(i64),
    /// `PX`：相对毫秒数
    Milliseconds(i64),
    /// `EXAT`：绝对 Unix 秒时间戳
    SecondsAt(i64),
    /// `PXAT`：绝对 Unix 毫秒时间戳
    MillisecondsAt(i64),
}

/// `SET` 的存在性条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetCondition {
    /// 无条件写入
    #[default]
    Always,
    /// `NX`：仅当键不存在时写入
    IfAbsent,
    /// `XX`：仅当键已存在时写入
    IfPresent,
}

/// 命令解析失败。
///
/// 这类失败是**预期内的**——客户端可以发送任何东西，服务器必须优雅回应而非崩溃。
/// 因此它不向上传播为全局错误，而是直接转成 RESP 错误回复。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    /// 帧结构不是一条命令（既不是数组，也不是内联命令）。
    NotACommand,
    /// 命令名不是合法的 UTF-8。
    MalformedName,
    /// 无法识别的命令名。
    UnknownCommand(String),
    /// 参数个数不对。
    WrongArity { name: String, expected: String },
    /// 参数格式不合法。
    InvalidArgument { name: String, detail: String },
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 这些文本会直接发给客户端，采用 Redis 的标准英文措辞以保证兼容
        match self {
            CommandError::NotACommand => {
                write!(f, "ERR Protocol error: expected an array of bulk strings")
            }
            CommandError::MalformedName => {
                write!(f, "ERR Protocol error: command name is not valid UTF-8")
            }
            CommandError::UnknownCommand(name) => {
                write!(f, "ERR unknown command '{name}'")
            }
            CommandError::WrongArity { name, expected } => {
                write!(
                    f,
                    "ERR wrong number of arguments for '{name}' command (expected {expected})"
                )
            }
            CommandError::InvalidArgument { name, detail } => {
                write!(f, "ERR invalid argument for '{name}': {detail}")
            }
        }
    }
}

impl std::error::Error for CommandError {}

impl Command {
    /// 从解码后的 RESP3 帧解析出一条命令。
    ///
    /// # 错误
    ///
    /// 任何解析问题都返回 [`CommandError`]，其 `Display` 可直接作为 RESP 错误回复。
    /// 本方法**不会 panic**，也不会因客户端发送垃圾数据而影响连接以外的东西。
    pub fn from_frame(frame: BytesFrame) -> Result<Self, CommandError> {
        let parts = match frame {
            // 常规命令：bulk string 组成的数组
            BytesFrame::Array { data, .. } => data,
            // 解码器会把 HELLO 识别为专用帧类型，这里直接接住。
            // 注意 `to_byte()` 返回的是 ASCII 字符（b'2' / b'3'）而非数字版本号，
            // 需要减去 b'0' 才是 2 / 3。
            BytesFrame::Hello { version, .. } => {
                return Ok(Command::Hello {
                    version: Some(version.to_byte() - b'0'),
                });
            }
            _ => return Err(CommandError::NotACommand),
        };

        let mut parts = parts.into_iter();
        let name_frame = parts.next().ok_or(CommandError::NotACommand)?;
        let name_bytes = frame_into_bytes(name_frame).ok_or(CommandError::NotACommand)?;
        let name = std::str::from_utf8(&name_bytes)
            .map_err(|_| CommandError::MalformedName)?
            .to_ascii_uppercase();

        let args: Vec<Bytes> = parts
            .map(|frame| frame_into_bytes(frame).ok_or(CommandError::NotACommand))
            .collect::<Result<_, _>>()?;

        parse(&name, &args)
    }
}

/// 取出标量帧的字节内容。
///
/// 命令参数只可能是这几种标量帧；数组、映射等复合帧作为参数一律视为协议错误。
fn frame_into_bytes(frame: BytesFrame) -> Option<Bytes> {
    match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::VerbatimString { data, .. } => Some(data),
        _ => None,
    }
}

/// 把参数按 ASCII 转成大写字符串，用于大小写不敏感地匹配选项名。
fn ascii_upper(bytes: &Bytes) -> Option<String> {
    std::str::from_utf8(bytes)
        .ok()
        .map(|s| s.to_ascii_uppercase())
}

/// 校验参数个数恰好为 `n`。
fn require_exact(name: &str, args: &[Bytes], n: usize) -> Result<(), CommandError> {
    if args.len() == n {
        Ok(())
    } else {
        Err(CommandError::WrongArity {
            name: name.to_ascii_lowercase(),
            expected: format!("exactly {n}, got {}", args.len()),
        })
    }
}

/// 校验参数个数至少为 `n`。
fn require_at_least(name: &str, args: &[Bytes], n: usize) -> Result<(), CommandError> {
    if args.len() >= n {
        Ok(())
    } else {
        Err(CommandError::WrongArity {
            name: name.to_ascii_lowercase(),
            expected: format!("at least {n}, got {}", args.len()),
        })
    }
}

/// 校验参数个数为 0 或 1。
fn require_at_most_one(name: &str, args: &[Bytes]) -> Result<(), CommandError> {
    if args.len() <= 1 {
        Ok(())
    } else {
        Err(CommandError::WrongArity {
            name: name.to_ascii_lowercase(),
            expected: format!("at most 1, got {}", args.len()),
        })
    }
}

/// 把参数解析为 `i64`。
fn parse_i64(name: &str, bytes: &Bytes) -> Result<i64, CommandError> {
    let text = std::str::from_utf8(bytes).map_err(|_| CommandError::InvalidArgument {
        name: name.to_ascii_lowercase(),
        detail: "value is not valid UTF-8".to_string(),
    })?;
    text.parse::<i64>()
        .map_err(|_| CommandError::InvalidArgument {
            name: name.to_ascii_lowercase(),
            detail: format!("'{text}' is not an integer or out of range"),
        })
}

/// 把参数解析为 `u64`。
fn parse_u64(name: &str, bytes: &Bytes) -> Result<u64, CommandError> {
    let text = std::str::from_utf8(bytes).map_err(|_| CommandError::InvalidArgument {
        name: name.to_ascii_lowercase(),
        detail: "value is not valid UTF-8".to_string(),
    })?;
    text.parse::<u64>()
        .map_err(|_| CommandError::InvalidArgument {
            name: name.to_ascii_lowercase(),
            detail: format!("'{text}' is not a non-negative integer or out of range"),
        })
}

/// 命令分发表。
fn parse(name: &str, args: &[Bytes]) -> Result<Command, CommandError> {
    match name {
        // ---------------- 连接 ----------------
        "PING" => {
            require_at_most_one(name, args)?;
            Ok(Command::Ping {
                message: args.first().cloned(),
            })
        }
        "ECHO" => {
            require_exact(name, args, 1)?;
            Ok(Command::Echo {
                message: args[0].clone(),
            })
        }
        "QUIT" => {
            require_exact(name, args, 0)?;
            Ok(Command::Quit)
        }
        "HELLO" => {
            let version = match args.first() {
                Some(raw) => Some(parse_i64(name, raw).map(u8::try_from).and_then(|r| {
                    r.map_err(|_| CommandError::InvalidArgument {
                        name: name.to_ascii_lowercase(),
                        detail: "unsupported protocol version".to_string(),
                    })
                })?),
                None => None,
            };
            Ok(Command::Hello { version })
        }

        // ---------------- 字符串 ----------------
        "GET" => {
            require_exact(name, args, 1)?;
            Ok(Command::Get {
                key: args[0].clone(),
            })
        }
        "SET" => parse_set(name, args),
        "DEL" => {
            require_at_least(name, args, 1)?;
            Ok(Command::Del {
                keys: args.to_vec(),
            })
        }
        "EXISTS" => {
            require_at_least(name, args, 1)?;
            Ok(Command::Exists {
                keys: args.to_vec(),
            })
        }
        "MSET" => {
            require_at_least(name, args, 2)?;
            if !args.len().is_multiple_of(2) {
                return Err(CommandError::WrongArity {
                    name: name.to_ascii_lowercase(),
                    expected: "an even number of key-value arguments".to_string(),
                });
            }
            Ok(Command::MSet {
                pairs: args
                    .chunks_exact(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect(),
            })
        }
        "MGET" => {
            require_at_least(name, args, 1)?;
            Ok(Command::MGet {
                keys: args.to_vec(),
            })
        }
        "APPEND" => {
            require_exact(name, args, 2)?;
            Ok(Command::Append {
                key: args[0].clone(),
                value: args[1].clone(),
            })
        }
        "STRLEN" => {
            require_exact(name, args, 1)?;
            Ok(Command::Strlen {
                key: args[0].clone(),
            })
        }
        "INCR" => {
            require_exact(name, args, 1)?;
            Ok(Command::Incr {
                key: args[0].clone(),
            })
        }
        "DECR" => {
            require_exact(name, args, 1)?;
            Ok(Command::Decr {
                key: args[0].clone(),
            })
        }
        "INCRBY" => {
            require_exact(name, args, 2)?;
            Ok(Command::IncrBy {
                key: args[0].clone(),
                delta: parse_i64(name, &args[1])?,
            })
        }
        "DECRBY" => {
            require_exact(name, args, 2)?;
            Ok(Command::DecrBy {
                key: args[0].clone(),
                delta: parse_i64(name, &args[1])?,
            })
        }

        // ---------------- 键管理 ----------------
        "EXPIRE" => {
            require_exact(name, args, 2)?;
            Ok(Command::Expire {
                key: args[0].clone(),
                seconds: parse_i64(name, &args[1])?,
            })
        }
        "TTL" => {
            require_exact(name, args, 1)?;
            Ok(Command::Ttl {
                key: args[0].clone(),
            })
        }
        "PERSIST" => {
            require_exact(name, args, 1)?;
            Ok(Command::Persist {
                key: args[0].clone(),
            })
        }
        "KEYS" => {
            require_exact(name, args, 1)?;
            Ok(Command::Keys {
                pattern: args[0].clone(),
            })
        }
        "SCAN" => parse_scan(name, args),
        "TYPE" => {
            require_exact(name, args, 1)?;
            Ok(Command::Type {
                key: args[0].clone(),
            })
        }

        // ---------------- 服务器 ----------------
        "INFO" => {
            require_at_most_one(name, args)?;
            Ok(Command::Info {
                section: args.first().cloned(),
            })
        }
        "DBSIZE" => {
            require_exact(name, args, 0)?;
            Ok(Command::Dbsize)
        }
        "FLUSHDB" => {
            require_exact(name, args, 0)?;
            Ok(Command::FlushDb)
        }

        // ---------------- 集群 ----------------
        "CLUSTER" => {
            require_at_least(name, args, 1)?;
            match ascii_upper(&args[0]).as_deref() {
                Some("INFO") => Ok(Command::ClusterInfo),
                _ => Err(CommandError::UnknownCommand(format!(
                    "CLUSTER {}",
                    String::from_utf8_lossy(&args[0])
                ))),
            }
        }
        "RAFT" => parse_raft(name, &args[1..], args.first()),
        "COMMAND" => {
            // 部分客户端（含官方 redis-cli）连接后会发送 COMMAND / COMMAND DOCS 探测能力。
            // 我们尚未实现它，但必须给出**规范的错误**而不是崩溃或静默断连。
            Err(CommandError::UnknownCommand("COMMAND".to_string()))
        }

        other => Err(CommandError::UnknownCommand(other.to_ascii_lowercase())),
    }
}

/// 解析 `SET key value [EX s | PX ms | EXAT ts | PXAT ts | KEEPTTL] [NX | XX]`。
fn parse_set(name: &str, args: &[Bytes]) -> Result<Command, CommandError> {
    require_at_least(name, args, 2)?;

    let key = args[0].clone();
    let value = args[1].clone();
    let mut expire = None;
    let mut condition = SetCondition::Always;

    let mut index = 2;
    while index < args.len() {
        let option = ascii_upper(&args[index]).ok_or_else(|| CommandError::InvalidArgument {
            name: name.to_ascii_lowercase(),
            detail: "option is not valid UTF-8".to_string(),
        })?;

        match option.as_str() {
            // 带值的选项：先确认后面还有参数，再消费它
            "EX" | "PX" | "EXAT" | "PXAT" => {
                index += 1;
                let raw = args
                    .get(index)
                    .ok_or_else(|| CommandError::InvalidArgument {
                        name: name.to_ascii_lowercase(),
                        detail: format!("'{option}' requires a value"),
                    })?;
                let amount = parse_i64(name, raw)?;
                expire = Some(match option.as_str() {
                    "EX" => SetExpire::Seconds(amount),
                    "PX" => SetExpire::Milliseconds(amount),
                    "EXAT" => SetExpire::SecondsAt(amount),
                    _ => SetExpire::MillisecondsAt(amount),
                });
            }
            "NX" => condition = SetCondition::IfAbsent,
            "XX" => condition = SetCondition::IfPresent,
            // KEEPTTL 与我们的默认行为一致（写入不改变既有 TTL 语义由状态机决定），
            // 这里接受但不需要额外记录
            "KEEPTTL" => {}
            other => {
                return Err(CommandError::InvalidArgument {
                    name: name.to_ascii_lowercase(),
                    detail: format!("unsupported option '{other}'"),
                });
            }
        }
        index += 1;
    }

    Ok(Command::Set {
        key,
        value,
        expire,
        condition,
    })
}

/// 解析 `SCAN cursor [MATCH pattern] [COUNT count]`。
fn parse_scan(name: &str, args: &[Bytes]) -> Result<Command, CommandError> {
    require_at_least(name, args, 1)?;

    let cursor = parse_u64(name, &args[0])?;
    let mut pattern = None;
    let mut count = None;

    let mut index = 1;
    while index < args.len() {
        let option = ascii_upper(&args[index]).ok_or_else(|| CommandError::InvalidArgument {
            name: name.to_ascii_lowercase(),
            detail: "option is not valid UTF-8".to_string(),
        })?;
        index += 1;
        let raw = args
            .get(index)
            .ok_or_else(|| CommandError::InvalidArgument {
                name: name.to_ascii_lowercase(),
                detail: format!("'{option}' requires a value"),
            })?;

        match option.as_str() {
            "MATCH" => pattern = Some(raw.clone()),
            "COUNT" => count = Some(parse_u64(name, raw)?),
            other => {
                return Err(CommandError::InvalidArgument {
                    name: name.to_ascii_lowercase(),
                    detail: format!("unsupported option '{other}'"),
                });
            }
        }
        index += 1;
    }

    Ok(Command::Scan {
        cursor,
        pattern,
        count,
    })
}

/// 解析 `RAFT <子命令> [...]`。
fn parse_raft(
    name: &str,
    rest: &[Bytes],
    subcommand: Option<&Bytes>,
) -> Result<Command, CommandError> {
    let subcommand = subcommand.ok_or_else(|| CommandError::WrongArity {
        name: name.to_ascii_lowercase(),
        expected: "a subcommand".to_string(),
    })?;
    let upper = ascii_upper(subcommand).unwrap_or_default();

    match upper.as_str() {
        "LEADER" => Ok(Command::RaftLeader),
        "INFO" => Ok(Command::RaftInfo),
        "ADD-NODE" => {
            if rest.len() != 2 {
                return Err(CommandError::WrongArity {
                    name: "raft add-node".to_string(),
                    expected: "exactly <id> <addr>".to_string(),
                });
            }
            let id = parse_u64(name, &rest[0])?;
            let addr = std::str::from_utf8(&rest[1])
                .map_err(|_| CommandError::InvalidArgument {
                    name: "raft add-node".to_string(),
                    detail: "address is not valid UTF-8".to_string(),
                })?
                .to_string();
            Ok(Command::RaftAddNode { id, addr })
        }
        other => Err(CommandError::UnknownCommand(format!("RAFT {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_protocol::codec::Resp3;
    use tokio_util::codec::Decoder;

    /// 把一段原始的 RESP 字节喂给真实的解码器，再解析成 `Command`。
    ///
    /// 这个辅助函数**刻意走完整的解码链路**而不是手工构造 `BytesFrame`——
    /// 否则测的就只是我们自己的匹配逻辑，验证不了与 `redis-protocol` 的衔接。
    fn parse_bytes(raw: &[u8]) -> Result<Command, CommandError> {
        let mut codec = Resp3::default();
        let mut buffer = bytes::BytesMut::from(raw);
        let frame = codec
            .decode(&mut buffer)
            .expect("解码不应失败")
            .expect("应得到一个完整帧");
        Command::from_frame(frame)
    }

    /// 构造一条标准的 RESP 数组命令
    fn array_command(parts: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            out.extend_from_slice(part.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    #[test]
    fn parses_simple_get_and_set() {
        assert_eq!(
            parse_bytes(&array_command(&["GET", "foo"])).expect("应能解析"),
            Command::Get {
                key: Bytes::from_static(b"foo")
            }
        );

        assert_eq!(
            parse_bytes(&array_command(&["SET", "foo", "bar"])).expect("应能解析"),
            Command::Set {
                key: Bytes::from_static(b"foo"),
                value: Bytes::from_static(b"bar"),
                expire: None,
                condition: SetCondition::Always,
            }
        );
    }

    #[test]
    fn command_name_is_case_insensitive() {
        // 客户端不保证命令名的大小写，必须一律接受
        let lower = parse_bytes(&array_command(&["get", "foo"])).expect("小写应能解析");
        let upper = parse_bytes(&array_command(&["GET", "foo"])).expect("大写应能解析");
        let mixed = parse_bytes(&array_command(&["GeT", "foo"])).expect("混合大小写应能解析");

        assert_eq!(lower, upper);
        assert_eq!(upper, mixed);
    }

    #[test]
    fn parses_set_with_expire_and_condition() {
        let cmd =
            parse_bytes(&array_command(&["SET", "k", "v", "EX", "60", "NX"])).expect("应能解析");
        assert_eq!(
            cmd,
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                expire: Some(SetExpire::Seconds(60)),
                condition: SetCondition::IfAbsent,
            }
        );

        // 选项顺序颠倒也应同样接受
        let reversed =
            parse_bytes(&array_command(&["SET", "k", "v", "XX", "PX", "500"])).expect("应能解析");
        assert_eq!(
            reversed,
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                expire: Some(SetExpire::Milliseconds(500)),
                condition: SetCondition::IfPresent,
            }
        );
    }

    #[test]
    fn rejects_set_with_unknown_option() {
        let err =
            parse_bytes(&array_command(&["SET", "k", "v", "BOGUS"])).expect_err("未知选项应被拒绝");
        assert!(
            err.to_string().contains("BOGUS"),
            "错误信息应指出是哪个选项，实际为：{err}"
        );
    }

    #[test]
    fn rejects_set_option_missing_its_value() {
        // EX 后面没有数字，必须报错而不是 panic
        let err =
            parse_bytes(&array_command(&["SET", "k", "v", "EX"])).expect_err("缺失值应被拒绝");
        assert!(
            err.to_string().contains("requires a value"),
            "错误信息应说明缺少值，实际为：{err}"
        );
    }

    #[test]
    fn parses_variadic_commands() {
        assert_eq!(
            parse_bytes(&array_command(&["DEL", "a", "b", "c"])).expect("应能解析"),
            Command::Del {
                keys: vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ]
            }
        );

        assert_eq!(
            parse_bytes(&array_command(&["MSET", "k1", "v1", "k2", "v2"])).expect("应能解析"),
            Command::MSet {
                pairs: vec![
                    (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
                ]
            }
        );
    }

    #[test]
    fn rejects_mset_with_odd_argument_count() {
        let err = parse_bytes(&array_command(&["MSET", "k1", "v1", "k2"]))
            .expect_err("参数不成对应被拒绝");
        assert!(
            err.to_string().contains("even number"),
            "错误信息应说明需要偶数个参数，实际为：{err}"
        );
    }

    #[test]
    fn parses_integer_arguments() {
        assert_eq!(
            parse_bytes(&array_command(&["INCRBY", "counter", "-5"])).expect("应能解析"),
            Command::IncrBy {
                key: Bytes::from_static(b"counter"),
                delta: -5,
            }
        );
    }

    #[test]
    fn rejects_non_integer_argument() {
        let err =
            parse_bytes(&array_command(&["INCRBY", "counter", "abc"])).expect_err("非整数应被拒绝");
        assert!(
            err.to_string().contains("not an integer"),
            "错误信息应说明不是整数，实际为：{err}"
        );
    }

    #[test]
    fn parses_cluster_and_raft_subcommands() {
        assert_eq!(
            parse_bytes(&array_command(&["CLUSTER", "INFO"])).expect("应能解析"),
            Command::ClusterInfo
        );
        assert_eq!(
            parse_bytes(&array_command(&["RAFT", "LEADER"])).expect("应能解析"),
            Command::RaftLeader
        );
        assert_eq!(
            parse_bytes(&array_command(&["RAFT", "ADD-NODE", "3", "127.0.0.1:7003"]))
                .expect("应能解析"),
            Command::RaftAddNode {
                id: 3,
                addr: "127.0.0.1:7003".to_string(),
            }
        );
    }

    #[test]
    fn rejects_unknown_command_with_redis_style_error() {
        let err = parse_bytes(&array_command(&["NOSUCHCMD"])).expect_err("未知命令应被拒绝");
        // 错误文本必须符合 Redis 惯例，客户端才能正确识别
        assert_eq!(err.to_string(), "ERR unknown command 'nosuchcmd'");
    }

    #[test]
    fn arity_error_matches_redis_wording() {
        let err = parse_bytes(&array_command(&["GET"])).expect_err("参数不足应被拒绝");
        assert!(
            err.to_string()
                .starts_with("ERR wrong number of arguments for 'get' command"),
            "错误文本应以 Redis 标准措辞开头，实际为：{err}"
        );
    }

    #[test]
    fn answers_hello_with_parsed_version() {
        // HELLO 会被解码器识别为专用帧，必须能正确接住版本号
        let cmd = parse_bytes(b"HELLO 3\r\n").expect("HELLO 应能解析");
        assert_eq!(cmd, Command::Hello { version: Some(3) });

        let cmd2 = parse_bytes(b"HELLO 2\r\n").expect("HELLO 2 应能解析");
        assert_eq!(cmd2, Command::Hello { version: Some(2) });
    }

    #[test]
    fn rejects_command_with_non_string_argument() {
        // 参数是嵌套数组——这不符合任何命令的形态，必须报协议错误而非 panic
        let raw = b"*2\r\n$3\r\nGET\r\n*1\r\n$3\r\nfoo\r\n";
        let err = parse_bytes(raw).expect_err("嵌套数组参数应被拒绝");
        assert_eq!(err, CommandError::NotACommand);
    }

    #[test]
    fn error_messages_never_contain_chinese() {
        // 走网络的错误文本必须是英文，否则客户端与测试的字符串匹配会失效
        let errors = [
            CommandError::NotACommand,
            CommandError::MalformedName,
            CommandError::UnknownCommand("foo".to_string()),
            CommandError::WrongArity {
                name: "get".to_string(),
                expected: "1".to_string(),
            },
            CommandError::InvalidArgument {
                name: "set".to_string(),
                detail: "bad".to_string(),
            },
        ];
        for err in errors {
            let text = err.to_string();
            assert!(text.is_ascii(), "错误文本出现了非 ASCII 字符：{text}");
        }
    }
}
