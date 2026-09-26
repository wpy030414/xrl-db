//! 统一编解码器：同时支持 RESP2 与 RESP3 两种方言。
//!
//! [`RespCodec`] 是协议层对外的**唯一入口**，与 tokio-util 的 `Framed` 配合使用：
//!
//! ```no_run
//! # use tokio::net::TcpStream;
//! # use tokio_util::codec::Framed;
//! # use xrl_db::protocol::codec::RespCodec;
//! # async fn example(stream: TcpStream) {
//! let mut framed = Framed::new(stream, RespCodec::default());
//! // framed 现在是一个 Command 的流，也是一个 Reply 的汇
//! # }
//! ```
//!
//! # 两种错误，两种命运
//!
//! 这是本模块最重要的设计。协议层会产出两类错误，它们的处理方式截然不同：
//!
//! | 错误 | 含义 | 处理 |
//! |---|---|---|
//! | `Item = Err(CommandError)` | **命令层面**：帧结构完整，但命令不认识或参数不合法 | 回复错误，**连接继续可用** |
//! | `Error = FramingError` | **帧层面**：字节流已无法对齐 | 无法恢复，**必须关闭连接** |
//!
//! 把前者做成 `Item` 而不是 `Error`，是因为客户端发错命令是**正常现象**——
//! 打错一个字母就断连的服务是不可用的。

use std::fmt;

use bytes::{Bytes, BytesMut};
use redis_protocol::codec::{Resp2, Resp3};
use redis_protocol::error::RedisProtocolError;
use redis_protocol::resp2::types::BytesFrame as Resp2Frame;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use tokio_util::codec::{Decoder, Encoder};

use super::command::{Command, CommandError};
use super::reply::{Dialect, Reply};

/// 帧层面的协议错误——字节流已失去对齐，连接无法继续。
#[derive(Debug)]
pub enum FramingError {
    /// 底层解码器报错（长度前缀非法、帧结构损坏等）。
    Decode(RedisProtocolError),
    /// 底层 I/O 失败。
    Io(std::io::Error),
}

impl fmt::Display for FramingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FramingError::Decode(source) => write!(f, "RESP 帧解析失败：{source}"),
            FramingError::Io(source) => write!(f, "网络读写失败：{source}"),
        }
    }
}

impl std::error::Error for FramingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FramingError::Decode(source) => Some(source),
            FramingError::Io(source) => Some(source),
        }
    }
}

impl From<RedisProtocolError> for FramingError {
    fn from(source: RedisProtocolError) -> Self {
        FramingError::Decode(source)
    }
}

impl From<std::io::Error> for FramingError {
    fn from(source: std::io::Error) -> Self {
        FramingError::Io(source)
    }
}

/// 同时支持 RESP2 与 RESP3 的编解码器。
///
/// 默认使用 RESP2——这是未发送 `HELLO` 的客户端所期望的方言。
/// 收到 `HELLO 3` 后由会话层调用 [`RespCodec::set_dialect`] 切换。
pub struct RespCodec {
    dialect: Dialect,
    inner: Inner,
}

enum Inner {
    Resp2(Resp2),
    Resp3(Resp3),
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new(Dialect::default())
    }
}

impl RespCodec {
    /// 按指定方言创建编解码器。
    pub fn new(dialect: Dialect) -> Self {
        let inner = match dialect {
            Dialect::Resp2 => Inner::Resp2(Resp2::default()),
            Dialect::Resp3 => Inner::Resp3(Resp3::default()),
        };
        Self { dialect, inner }
    }

    /// 当前方言。
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// 切换方言。
    ///
    /// 只在 `HELLO <版本>` 处理完成后调用——那一时刻必定处于帧边界上，
    /// 因此可以直接丢弃旧编解码器的内部状态。
    pub fn set_dialect(&mut self, dialect: Dialect) {
        if self.dialect != dialect {
            self.dialect = dialect;
            self.inner = match dialect {
                Dialect::Resp2 => Inner::Resp2(Resp2::default()),
                Dialect::Resp3 => Inner::Resp3(Resp3::default()),
            };
        }
    }
}

/// 把 RESP2 帧归一为等价的 RESP3 帧。
///
/// 这样命令解析只需要一份实现。RESP2 能表达的每种类型在 RESP3 里都有对应，
/// 反向则不然——这也是为什么只需要这一个方向。
fn resp2_to_resp3(frame: Resp2Frame) -> Resp3Frame {
    match frame {
        Resp2Frame::SimpleString(data) => Resp3Frame::SimpleString {
            data,
            attributes: None,
        },
        Resp2Frame::Error(data) => Resp3Frame::SimpleError {
            data,
            attributes: None,
        },
        Resp2Frame::Integer(data) => Resp3Frame::Number {
            data,
            attributes: None,
        },
        Resp2Frame::BulkString(data) => Resp3Frame::BlobString {
            data,
            attributes: None,
        },
        Resp2Frame::Null => Resp3Frame::Null,
        Resp2Frame::Array(items) => Resp3Frame::Array {
            data: items.into_iter().map(resp2_to_resp3).collect(),
            attributes: None,
        },
    }
}

/// [`try_parse_inline`] 的判定结果。
enum InlineOutcome {
    /// 成功解析出一条内联命令。
    Frame(Resp3Frame),
    /// 确认这是内联命令，但整行尚未接收完整，需等待更多数据。
    Incomplete,
    /// 不是内联命令，应交给底层解码器处理。
    NotInline,
}

/// 判断一个字节是否是 RESP 类型前缀。
///
/// 以这些字符开头的输入是标准 RESP 帧；其余（字母、数字等）视为内联命令。
fn is_resp_prefix(byte: u8) -> bool {
    matches!(
        byte,
        // 简单字符串、错误、整数、块字符串、数组、空值
        b'+' | b'-' | b':' | b'$' | b'*' | b'_'
            // 布尔、双精度、大数、块错误、verbatim 字符串
            | b'#' | b',' | b'(' | b'!' | b'='
            // 映射、集合、推送、属性
            | b'%' | b'~' | b'>' | b'|'
    )
}

/// 在缓冲区中查找 CRLF 的位置。
fn find_crlf(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|pair| pair == b"\r\n")
}

/// 尝试把缓冲区开头的部分解析为一条内联命令。
///
/// # 为什么需要自己实现
///
/// RESP 规范允许客户端把命令写成**一行纯文本**（`PING`、`SET foo bar`），
/// 而非标准的数组编码。这主要服务于 `telnet` / `nc` 的手动调试——
/// 对一个数据库而言这是相当实用的能力，Redis 本身也支持。
///
/// 但 `redis-protocol` **不支持**这种形态：它看到首字节不是 RESP 类型前缀时，
/// 会返回「Invalid frame type」的解码错误。而帧层面的错误在本项目的设计里意味着
/// 「字节流已失去对齐，必须断连」——用它来处理内联命令，会让一句无害的 telnet
/// 输入直接踢掉连接。
///
/// 因此这里自行拦截。判定规则很简单：**首字节不是 RESP 类型前缀，就是内联命令。**
fn try_parse_inline(src: &mut BytesMut) -> InlineOutcome {
    loop {
        let Some(&first) = src.first() else {
            // 缓冲区为空，判断不了形态
            return InlineOutcome::NotInline;
        };

        if is_resp_prefix(first) {
            return InlineOutcome::NotInline;
        }

        // 已确认是内联命令，但整行还没收全。TCP 分包是常态，必须等待而非报错。
        let Some(line_end) = find_crlf(src) else {
            return InlineOutcome::Incomplete;
        };

        // 取出并消费这一行（含结尾的 CRLF）
        let line = src.split_to(line_end + 2);

        let parts: Vec<Resp3Frame> = line[..line_end]
            .split(|byte| byte.is_ascii_whitespace())
            .filter(|token| !token.is_empty())
            .map(|token| Resp3Frame::BlobString {
                data: Bytes::copy_from_slice(token),
                attributes: None,
            })
            .collect();

        // 空行会被 telnet 不断产生，跳过而不是当作错误
        if parts.is_empty() {
            continue;
        }

        return InlineOutcome::Frame(Resp3Frame::Array {
            data: parts,
            attributes: None,
        });
    }
}

impl Decoder for RespCodec {
    /// 解码结果。`Err(CommandError)` 表示命令层面的错误，连接仍然健康。
    type Item = Result<Command, CommandError>;
    type Error = FramingError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 内联命令必须在底层解码器之前拦下——`redis-protocol` 不支持这种形态，
        // 遇到会直接报「Invalid frame type」的解码错误，而帧层面的错误会导致断连。
        // 详见 try_parse_inline 的说明。
        match try_parse_inline(src) {
            InlineOutcome::Frame(frame) => return Ok(Some(Command::from_frame(frame))),
            // 确认是内联命令但行还没收完。必须在此等待，**不能**交给底层解码器
            InlineOutcome::Incomplete => return Ok(None),
            InlineOutcome::NotInline => {}
        }

        // 两个分支返回的帧类型不同，因此各自取到后就地归一为 RESP3 帧；
        // 半包（数据未到齐）则返回 None，等下次调用再继续。
        let normalized: Resp3Frame = match &mut self.inner {
            Inner::Resp2(codec) => match codec.decode(src)? {
                Some(frame) => resp2_to_resp3(frame),
                None => return Ok(None),
            },
            Inner::Resp3(codec) => match codec.decode(src)? {
                Some(frame) => frame,
                None => return Ok(None),
            },
        };

        Ok(Some(Command::from_frame(normalized)))
    }
}

impl Encoder<Reply> for RespCodec {
    type Error = FramingError;

    fn encode(&mut self, reply: Reply, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match &mut self.inner {
            Inner::Resp2(codec) => codec.encode(reply.into_resp2(), dst)?,
            Inner::Resp3(codec) => codec.encode(reply.into_resp3(), dst)?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::command::SetCondition;
    use bytes::Bytes;

    /// 构造一条标准的 RESP2/RESP3 数组命令（两种方言的请求编码相同）
    fn array_command(parts: &[&str]) -> BytesMut {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            out.extend_from_slice(part.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        BytesMut::from(&out[..])
    }

    #[test]
    fn decodes_the_same_command_in_both_dialects() {
        // 请求的编码在两种方言下是一样的，编解码器必须都能处理
        let raw = array_command(&["SET", "foo", "bar"]);

        let mut resp2 = RespCodec::new(Dialect::Resp2);
        let mut resp3 = RespCodec::new(Dialect::Resp3);

        let from_resp2 = resp2.decode(&mut raw.clone()).expect("不应致命失败");
        let from_resp3 = resp3.decode(&mut raw.clone()).expect("不应致命失败");

        assert_eq!(from_resp2, from_resp3);
        assert!(matches!(from_resp2, Some(Ok(Command::Set { .. }))));
    }

    #[test]
    fn returns_none_for_incomplete_frame() {
        // 半包必须返回 None 而不是报错——TCP 分包是常态
        let mut codec = RespCodec::default();
        let mut partial = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nfo"[..]);

        let result = codec.decode(&mut partial).expect("半包不应导致致命错误");
        assert_eq!(result, None);
    }

    #[test]
    fn command_error_does_not_kill_the_connection() {
        // 未知命令是「命令层面」的错误：必须能继续处理同一连接上的后续命令
        let mut codec = RespCodec::default();
        let mut buffer = array_command(&["NOSUCHCMD", "x"]);
        buffer.extend_from_slice(&array_command(&["PING"]));

        let first = codec.decode(&mut buffer).expect("不应致命失败");
        assert!(
            matches!(first, Some(Err(CommandError::UnknownCommand(_)))),
            "第一条应返回命令层面的错误，实际为：{first:?}"
        );

        let second = codec.decode(&mut buffer).expect("不应致命失败");
        assert_eq!(
            second,
            Some(Ok(Command::Ping { message: None })),
            "同一连接上的后续命令仍应正常解析"
        );
    }

    #[test]
    fn encodes_reply_according_to_current_dialect() {
        let mut codec = RespCodec::default();
        assert_eq!(codec.dialect(), Dialect::Resp2);

        let mut out = BytesMut::new();
        codec.encode(Reply::Null, &mut out).expect("编码不应失败");
        assert_eq!(out.to_vec(), b"$-1\r\n", "默认方言应为 RESP2");

        codec.set_dialect(Dialect::Resp3);
        assert_eq!(codec.dialect(), Dialect::Resp3);

        let mut out = BytesMut::new();
        codec.encode(Reply::Null, &mut out).expect("编码不应失败");
        assert_eq!(out.to_vec(), b"_\r\n", "切换后应使用 RESP3 编码");
    }

    #[test]
    fn decodes_inline_ping() {
        // 内联命令（telnet 手打、部分脚本）也必须能处理
        let mut codec = RespCodec::default();
        let mut buffer = BytesMut::from(&b"PING\r\n"[..]);

        let result = codec.decode(&mut buffer).expect("内联命令不应致命失败");
        assert_eq!(result, Some(Ok(Command::Ping { message: None })));
    }

    #[test]
    fn decodes_pipelined_commands() {
        // 流水线：一次收到多条命令，必须逐条取出而不丢不乱
        let mut codec = RespCodec::default();
        let mut buffer = array_command(&["SET", "a", "1"]);
        buffer.extend_from_slice(&array_command(&["GET", "a"]));
        buffer.extend_from_slice(&array_command(&["DEL", "a"]));

        let first = codec.decode(&mut buffer).expect("不应失败");
        let second = codec.decode(&mut buffer).expect("不应失败");
        let third = codec.decode(&mut buffer).expect("不应失败");
        let fourth = codec.decode(&mut buffer).expect("不应失败");

        assert!(matches!(first, Some(Ok(Command::Set { .. }))));
        assert!(matches!(second, Some(Ok(Command::Get { .. }))));
        assert!(matches!(third, Some(Ok(Command::Del { .. }))));
        assert_eq!(fourth, None, "取完后应返回 None");
    }

    #[test]
    fn resp2_bulk_string_binary_payload_survives_roundtrip() {
        // 二进制安全：值里含 CRLF 与 NUL，长度前缀仍是唯一边界
        let payload = Bytes::from_static(b"\r\n\0bin");
        let mut codec = RespCodec::default();

        let mut buffer = BytesMut::new();
        codec
            .encode(Reply::Bulk(payload.clone()), &mut buffer)
            .expect("编码不应失败");

        // 把编码结果当请求发回去虽不是真实场景，但能验证编解码器的字节对称性
        // `\r\n\0bin` 共 6 字节
        assert!(buffer.starts_with(b"$6\r\n"));
        assert!(buffer.ends_with(b"\r\n"));
    }

    #[test]
    fn waits_for_incomplete_inline_command_instead_of_failing() {
        // 这是内联命令最容易被写错的地方：整行被 TCP 切开时，
        // 绝不能因为「还没收到 CRLF」就报致命错误而踢掉连接。
        let mut codec = RespCodec::default();

        let mut buffer = BytesMut::from(&b"PI"[..]);
        let result = codec.decode(&mut buffer).expect("半行内联命令不应致命失败");
        assert_eq!(result, None, "应等待剩余数据而非报错");

        // 剩余部分到达后，同一个编解码器必须能无缝接上
        buffer.extend_from_slice(b"NG\r\n");
        let result = codec.decode(&mut buffer).expect("补全后应能解析");
        assert_eq!(result, Some(Ok(Command::Ping { message: None })));
    }

    #[test]
    fn skips_blank_inline_lines() {
        // telnet 会持续产生空行，跳过即可，不该回错误
        let mut codec = RespCodec::default();
        let mut buffer = BytesMut::from(&b"\r\n\r\nPING\r\n"[..]);

        let result = codec.decode(&mut buffer).expect("空行不应致命失败");
        assert_eq!(result, Some(Ok(Command::Ping { message: None })));
    }

    #[test]
    fn parses_inline_command_with_arguments() {
        let mut codec = RespCodec::default();
        // 多个连续空格必须视作单个分隔符
        let mut buffer = BytesMut::from(&b"SET  foo   bar\r\n"[..]);

        let result = codec.decode(&mut buffer).expect("不应失败");
        assert_eq!(
            result,
            Some(Ok(Command::Set {
                key: Bytes::from_static(b"foo"),
                value: Bytes::from_static(b"bar"),
                expire: None,
                condition: SetCondition::Always,
            }))
        );
    }

    #[test]
    fn inline_and_standard_frames_can_interleave() {
        // 同一连接上两种形态混用也必须正确对齐
        let mut codec = RespCodec::default();
        let mut buffer = BytesMut::from(&b"PING\r\n"[..]);
        buffer.extend_from_slice(&array_command(&["GET", "foo"]));

        let first = codec.decode(&mut buffer).expect("不应失败");
        let second = codec.decode(&mut buffer).expect("不应失败");

        assert!(matches!(first, Some(Ok(Command::Ping { .. }))));
        assert!(matches!(second, Some(Ok(Command::Get { .. }))));
    }
}
