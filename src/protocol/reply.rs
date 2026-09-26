//! 服务端响应的构造与编码。
//!
//! # 为什么要处理两种方言
//!
//! RESP3 与 RESP2 的表达能力不同，同一个语义在两者里的编码并不一样。最典型的是**空值**：
//!
//! | 语义 | RESP2 | RESP3 |
//! |---|---|---|
//! | 空值 | `$-1\r\n` | `_\r\n` |
//! | 错误 | `-ERR ...\r\n` | `-ERR ...\r\n`（简单错误）或 `!N\r\n...`（块错误） |
//!
//! 如果对只懂 RESP2 的客户端发送 `_\r\n`，对方会解析失败。因此服务器必须**跟随客户端
//! 的方言**：默认 RESP2，客户端发送 `HELLO 3` 之后切换为 RESP3。
//!
//! [`Reply`] 是与方言无关的语义表示，由 [`Reply::into_resp2`] / [`Reply::into_resp3`]
//! 分别编码成对应方言的帧。

use bytes::Bytes;
use redis_protocol::bytes_utils::Str;
use redis_protocol::resp2::types::BytesFrame as Resp2Frame;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;

/// 客户端当前使用的协议方言。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    /// RESP2：Redis 6 之前的事实标准，也是客户端未发送 `HELLO` 时的默认方言。
    #[default]
    Resp2,
    /// RESP3：客户端发送 `HELLO 3` 后切换。
    Resp3,
}

/// 一条待发送给客户端的响应，与方言无关。
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    /// 简单字符串，如 `+OK`
    Simple(String),
    /// 错误，如 `-ERR unknown command`
    Error(String),
    /// 整数，如 `:42`
    Integer(i64),
    /// 二进制安全的块字符串
    Bulk(Bytes),
    /// 空值。RESP2 与 RESP3 的编码不同，这正是需要区分方言的原因
    Null,
    /// 数组，元素递归编码
    Array(Vec<Reply>),
    /// 映射。
    ///
    /// RESP3 有原生映射类型，RESP2 没有——后者用扁平数组 `[k1, v1, k2, v2, ...]` 表达，
    /// 这也是 Redis 自身的惯例（`CONFIG GET`、`HELLO` 的回复都是这个形状）。
    /// 又一个必须区分方言的地方。
    Map(Vec<(Reply, Reply)>),
}

impl Reply {
    /// 构造 `+OK`。
    pub fn ok() -> Self {
        Reply::Simple("OK".to_string())
    }

    /// 构造一条错误响应。
    ///
    /// 传入的文本应遵循 Redis 惯例（以 `ERR` / `WRONGTYPE` 等错误码开头）。
    pub fn error(message: impl Into<String>) -> Self {
        Reply::Error(message.into())
    }

    /// 构造块字符串响应。
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Reply::Bulk(data.into())
    }

    /// 按 RESP2 方言编码。
    pub fn into_resp2(self) -> Resp2Frame {
        match self {
            Reply::Simple(text) => Resp2Frame::SimpleString(Bytes::from(text)),
            // Str 是 Redis 客户端期望的错误文本类型
            Reply::Error(text) => Resp2Frame::Error(Str::from(text)),
            Reply::Integer(number) => Resp2Frame::Integer(number),
            Reply::Bulk(data) => Resp2Frame::BulkString(data),
            // RESP2 的空值是 `$-1\r\n`
            Reply::Null => Resp2Frame::Null,
            Reply::Array(items) => {
                Resp2Frame::Array(items.into_iter().map(Reply::into_resp2).collect())
            }
            Reply::Map(pairs) => {
                // RESP2 没有映射类型，展开成交替的键值序列
                let mut flat = Vec::with_capacity(pairs.len() * 2);
                for (key, value) in pairs {
                    flat.push(key.into_resp2());
                    flat.push(value.into_resp2());
                }
                Resp2Frame::Array(flat)
            }
        }
    }

    /// 按 RESP3 方言编码。
    pub fn into_resp3(self) -> Resp3Frame {
        match self {
            Reply::Simple(text) => Resp3Frame::SimpleString {
                data: Bytes::from(text),
                attributes: None,
            },
            Reply::Error(text) => Resp3Frame::SimpleError {
                data: Str::from(text),
                attributes: None,
            },
            Reply::Integer(number) => Resp3Frame::Number {
                data: number,
                attributes: None,
            },
            Reply::Bulk(data) => Resp3Frame::BlobString {
                data,
                attributes: None,
            },
            // RESP3 的空值是 `_\r\n`
            Reply::Null => Resp3Frame::Null,
            Reply::Array(items) => Resp3Frame::Array {
                data: items.into_iter().map(Reply::into_resp3).collect(),
                attributes: None,
            },
            Reply::Map(pairs) => {
                let mut data = std::collections::HashMap::with_capacity(pairs.len());
                for (key, value) in pairs {
                    data.insert(key.into_resp3(), value.into_resp3());
                }
                Resp3Frame::Map {
                    data,
                    attributes: None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_protocol::codec::{Resp2, Resp3};
    use tokio_util::codec::Encoder;

    /// 把响应按指定方言编码成真实字节。
    ///
    /// 同样刻意走完整的编码器，而不是手工比对帧结构——要验证的是**最终上线字节**
    /// 是否符合协议，这才是客户端实际看到的东西。
    fn encode(reply: Reply, dialect: Dialect) -> Vec<u8> {
        let mut buffer = bytes::BytesMut::new();
        match dialect {
            Dialect::Resp2 => Resp2::default()
                .encode(reply.into_resp2(), &mut buffer)
                .expect("RESP2 编码不应失败"),
            Dialect::Resp3 => Resp3::default()
                .encode(reply.into_resp3(), &mut buffer)
                .expect("RESP3 编码不应失败"),
        }
        buffer.to_vec()
    }

    #[test]
    fn encodes_ok_in_both_dialects() {
        // 简单字符串在两种方言下字节完全相同
        assert_eq!(encode(Reply::ok(), Dialect::Resp2), b"+OK\r\n");
        assert_eq!(encode(Reply::ok(), Dialect::Resp3), b"+OK\r\n");
    }

    #[test]
    fn null_differs_between_dialects() {
        // 这是区分方言的**核心原因**：对只懂 RESP2 的客户端发 `_\r\n` 会解析失败
        assert_eq!(encode(Reply::Null, Dialect::Resp2), b"$-1\r\n");
        assert_eq!(encode(Reply::Null, Dialect::Resp3), b"_\r\n");
    }

    #[test]
    fn encodes_error_in_redis_wording() {
        let reply = Reply::error("ERR unknown command 'foo'");
        // 错误必须以 `-` 开头，客户端据此识别
        assert_eq!(
            encode(reply.clone(), Dialect::Resp2),
            b"-ERR unknown command 'foo'\r\n"
        );
        assert_eq!(
            encode(reply, Dialect::Resp3),
            b"-ERR unknown command 'foo'\r\n"
        );
    }

    #[test]
    fn encodes_bulk_and_integer() {
        assert_eq!(encode(Reply::bulk("bar"), Dialect::Resp2), b"$3\r\nbar\r\n");
        assert_eq!(encode(Reply::Integer(42), Dialect::Resp2), b":42\r\n");
        assert_eq!(encode(Reply::bulk("bar"), Dialect::Resp3), b"$3\r\nbar\r\n");
        assert_eq!(encode(Reply::Integer(42), Dialect::Resp3), b":42\r\n");
    }

    #[test]
    fn bulk_is_binary_safe() {
        // 值里包含 CRLF 和 NUL 也必须原样传输，长度前缀是唯一的边界依据
        let payload = Bytes::from_static(b"a\r\nb\0c");
        let encoded = encode(Reply::Bulk(payload.clone()), Dialect::Resp3);

        let mut expected = b"$6\r\n".to_vec();
        expected.extend_from_slice(&payload);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(encoded, expected);
    }

    #[test]
    fn encodes_nested_arrays() {
        let reply = Reply::Array(vec![
            Reply::Integer(1),
            Reply::bulk("two"),
            Reply::Array(vec![Reply::Null]),
        ]);
        assert_eq!(
            encode(reply.clone(), Dialect::Resp2),
            b"*3\r\n:1\r\n$3\r\ntwo\r\n*1\r\n$-1\r\n"
        );
        assert_eq!(
            encode(reply, Dialect::Resp3),
            b"*3\r\n:1\r\n$3\r\ntwo\r\n*1\r\n_\r\n"
        );
    }

    #[test]
    fn empty_array_is_distinct_from_null() {
        // `*0\r\n`（空集合）与空值语义不同，不能混淆
        assert_eq!(encode(Reply::Array(vec![]), Dialect::Resp2), b"*0\r\n");
        assert_ne!(
            encode(Reply::Array(vec![]), Dialect::Resp2),
            encode(Reply::Null, Dialect::Resp2)
        );
    }

    #[test]
    fn map_uses_native_type_in_resp3_and_flat_array_in_resp2() {
        let reply = Reply::Map(vec![
            (Reply::bulk("proto"), Reply::Integer(3)),
            (Reply::bulk("mode"), Reply::bulk("standalone")),
        ]);

        // RESP3：原生映射类型，以 `%2` 开头（2 个键值对）
        let resp3 = encode(reply.clone(), Dialect::Resp3);
        assert!(
            resp3.starts_with(b"%2\r\n"),
            "RESP3 应使用原生映射，实际为：{resp3:?}"
        );

        // RESP2：没有映射类型，展开为扁平数组，`*4`（2 对 = 4 个元素）
        let resp2 = encode(reply, Dialect::Resp2);
        assert!(
            resp2.starts_with(b"*4\r\n"),
            "RESP2 应展开为扁平数组，实际为：{resp2:?}"
        );
    }
}
