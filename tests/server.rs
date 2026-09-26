//! 集成测试：启动真实的 TCP 服务，用**原始 RESP 字节**与之对话。
//!
//! # 为什么不用客户端库
//!
//! 这些测试刻意不经过任何 Redis 客户端库。它们直接构造协议字节、直接检查返回的字节，
//! 因此验证的是「服务端在网络上实际说了什么」，而不是「某个客户端库认为它说了什么」。
//!
//! 后者只能证明我们和那个库能互相理解；前者才能证明**任何**符合协议的客户端都能理解我们。
//! 别忘了「官方 redis-cli 零改造可用」是项目的核心验收标准，这个标准必须由协议字节来背书。

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use xrl_db::backend::Backend;
use xrl_db::config::{Config, RPC_PORT_OFFSET};
use xrl_db::node::Node;
use xrl_db::server;

/// 一个测试用的服务实例。
struct TestServer {
    port: u16,
    /// 本实例的数据目录。
    ///
    /// **必须留住**：`TempDir` 在析构时会删除目录，只保留路径是不够的。
    /// 同时它也是每个测试实例相互隔离的关键——redb 是单进程的，若多个实例
    /// 共用一个文件，后启动的会直接报 `DatabaseAlreadyOpen`。
    _data_dir: tempfile::TempDir,
}

impl TestServer {
    /// 启动一个监听在空闲端口上的服务。
    ///
    /// 每个测试实例都会启动一个真正的单节点 Raft——测试因此覆盖了与生产完全相同的
    /// 代码路径，而不是一条「专供测试的简化路径」。
    async fn start() -> Self {
        loop {
            // 让操作系统分配一个空闲端口作为客户端端口
            let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
                continue;
            };
            let port = listener.local_addr().expect("应能取得本地地址").port();

            // 节点间通信端口由客户端端口推导而来（+10000），两者都必须可用。
            // 这里只做探测，随即释放——正式绑定由 `Node::start` 完成。
            let Some(rpc_port) = port.checked_add(RPC_PORT_OFFSET) else {
                continue;
            };
            if std::net::TcpListener::bind(("127.0.0.1", rpc_port)).is_err() {
                continue;
            }

            // 每个测试实例一个独立的数据目录
            let data_dir = tempfile::tempdir().expect("应能创建临时目录");

            let mut config = Config::default();
            config.node.listen = SocketAddr::from(([127, 0, 0, 1], port));
            config.storage.path = Some(data_dir.path().to_path_buf());
            let config = config.resolve().expect("默认配置应合法");

            let node = Arc::new(
                Node::start_single(config.clone())
                    .await
                    .expect("应能启动单节点集群"),
            );
            // 选举是异步的，等到选出 leader 再开始服务，否则首批写入会撞上
            // 「还没有 leader」——那会让测试变得随机失败
            node.wait_for_leader(Duration::from_secs(5))
                .await
                .expect("单节点集群应能迅速选出 leader");

            let backend = Arc::new(Backend::new(node, config));

            tokio::spawn(async move {
                // 测试结束时任务随运行时一起被丢弃，因此不关心返回值
                let _ = server::serve_with(listener, backend).await;
            });

            return Self {
                port,
                _data_dir: data_dir,
            };
        }
    }

    /// 建立一条客户端连接。
    async fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(("127.0.0.1", self.port))
            .await
            .expect("应能连接到测试服务");
        stream.set_nodelay(true).expect("应能设置 TCP_NODELAY");
        stream
    }
}

/// 把命令编码为 RESP 数组格式。
fn encode_command(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// 读取一条完整的 RESP 回复，返回便于断言的简化表示。
///
/// - 简单字符串 / 整数 → 原文
/// - 错误 → `ERR:原文`
/// - 空值 → `(nil)`
/// - 数组 → `[元素,元素]`
fn read_reply<'a>(stream: &'a mut TcpStream) -> Pin<Box<dyn Future<Output = String> + 'a>> {
    Box::pin(async move {
        let mut tag = [0u8; 1];
        stream.read_exact(&mut tag).await.expect("应能读取回复类型");

        match tag[0] {
            b'+' | b':' => read_line(stream).await,
            b'-' => format!("ERR:{}", read_line(stream).await),

            b'$' => {
                let length: i64 = read_line(stream).await.parse().expect("长度应是整数");
                if length < 0 {
                    return "(nil)".to_string();
                }
                let mut data = vec![0u8; length as usize];
                stream.read_exact(&mut data).await.expect("应能读取数据");
                skip_crlf(stream).await;
                String::from_utf8_lossy(&data).into_owned()
            }

            // RESP3 的空值
            b'_' => {
                let _ = read_line(stream).await;
                "(nil)".to_string()
            }

            b'*' => {
                let count: i64 = read_line(stream).await.parse().expect("长度应是整数");
                if count < 0 {
                    return "(nil)".to_string();
                }
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    items.push(read_reply(stream).await);
                }
                format!("[{}]", items.join(","))
            }

            // RESP3 的原生映射
            b'%' => {
                let pairs: i64 = read_line(stream).await.parse().expect("长度应是整数");
                let mut items = Vec::with_capacity(pairs as usize * 2);
                for _ in 0..pairs * 2 {
                    items.push(read_reply(stream).await);
                }
                format!("{{{}}}", items.join(","))
            }

            other => panic!("未预期的回复类型：{}", other as char),
        }
    })
}

/// 读取一行（到 CRLF 为止），不含 CRLF。
///
/// 注意 `\r` 被消费后**只剩 `\n`**，这里必须只补读一个字节。
/// 若误以为还要再读完整的 CRLF，就会把下一条回复的首字节也吃掉，
/// 整个字节流从此错位——表现为测试随机挂起，且极难定位。
async fn read_line(stream: &mut TcpStream) -> String {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        stream.read_exact(&mut byte).await.expect("应能读取");
        if byte[0] == b'\r' {
            stream.read_exact(&mut byte).await.expect("应能读取");
            assert_eq!(byte[0], b'\n', "协议要求行尾必须是 CRLF");
            break;
        }
        line.push(byte[0]);
    }

    String::from_utf8_lossy(&line).into_owned()
}

/// 消费掉一个完整的 CRLF（用于块字符串的尾部）。
async fn skip_crlf(stream: &mut TcpStream) {
    let mut crlf = [0u8; 2];
    stream.read_exact(&mut crlf).await.expect("应能读取 CRLF");
    assert_eq!(&crlf, b"\r\n", "协议要求行尾必须是 CRLF");
}

/// 发送一条命令并读取回复。
async fn command(stream: &mut TcpStream, parts: &[&str]) -> String {
    stream
        .write_all(&encode_command(parts))
        .await
        .expect("应能写入命令");
    read_reply(stream).await
}

// ==================================================================== 测试

#[tokio::test]
async fn set_and_get_round_trip() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    assert_eq!(command(&mut stream, &["SET", "foo", "bar"]).await, "OK");
    assert_eq!(command(&mut stream, &["GET", "foo"]).await, "bar");
    // 缺失的键必须返回空值而不是空字符串——两者在协议层面不同
    assert_eq!(command(&mut stream, &["GET", "missing"]).await, "(nil)");
}

#[tokio::test]
async fn unknown_command_does_not_kill_the_connection() {
    // 这是可用性的底线：打错一个字母就断连的服务没人能用
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    let error = command(&mut stream, &["NOSUCHCOMMAND"]).await;
    assert!(
        error.starts_with("ERR:ERR unknown command"),
        "应返回 Redis 风格的错误，实际为：{error}"
    );

    // 连接必须仍然可用
    assert_eq!(command(&mut stream, &["PING"]).await, "PONG");
    assert_eq!(command(&mut stream, &["SET", "k", "v"]).await, "OK");
    assert_eq!(command(&mut stream, &["GET", "k"]).await, "v");
}

#[tokio::test]
async fn arity_error_keeps_connection_alive() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    let error = command(&mut stream, &["GET"]).await;
    assert!(
        error.contains("wrong number of arguments"),
        "应报告参数个数错误，实际为：{error}"
    );

    assert_eq!(command(&mut stream, &["PING"]).await, "PONG");
}

#[tokio::test]
async fn values_are_binary_safe() {
    // 值里含 CRLF——长度前缀是唯一边界，任何转义或截断都是 bug
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    let payload = "line1\r\nline2";
    assert_eq!(command(&mut stream, &["SET", "bin", payload]).await, "OK");
    assert_eq!(command(&mut stream, &["GET", "bin"]).await, payload);
    assert_eq!(
        command(&mut stream, &["STRLEN", "bin"]).await,
        payload.len().to_string()
    );
}

#[tokio::test]
async fn pipelined_commands_are_answered_in_order() {
    // 流水线：一次性写入多条命令，回复必须按顺序逐条返回
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    let mut batch = Vec::new();
    batch.extend_from_slice(&encode_command(&["SET", "p", "1"]));
    batch.extend_from_slice(&encode_command(&["GET", "p"]));
    batch.extend_from_slice(&encode_command(&["INCR", "p"]));
    batch.extend_from_slice(&encode_command(&["GET", "p"]));
    stream.write_all(&batch).await.expect("应能写入批量命令");

    // 回复必须严格对应命令的顺序
    assert_eq!(read_reply(&mut stream).await, "OK");
    assert_eq!(read_reply(&mut stream).await, "1");
    assert_eq!(read_reply(&mut stream).await, "2");
    assert_eq!(read_reply(&mut stream).await, "2");
}

#[tokio::test]
async fn inline_commands_are_accepted() {
    // 内联命令是 RESP 规范的一部分，telnet / nc 调试依赖它
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    stream
        .write_all(b"SET inline hello\r\n")
        .await
        .expect("应能写入内联命令");
    assert_eq!(read_reply(&mut stream).await, "OK");

    stream
        .write_all(b"GET inline\r\n")
        .await
        .expect("应能写入内联命令");
    assert_eq!(read_reply(&mut stream).await, "hello");
}

#[tokio::test]
async fn inline_command_split_across_packets_does_not_break_connection() {
    // 内联命令被 TCP 切开时，服务端必须等待而不是报错断连
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    stream.write_all(b"PI").await.expect("应能写入");
    stream.flush().await.expect("应能刷新");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    stream.write_all(b"NG\r\n").await.expect("应能写入");

    assert_eq!(read_reply(&mut stream).await, "PONG");
}

#[tokio::test]
async fn resp3_null_uses_a_different_encoding_than_resp2() {
    // 这是区分两种方言最实在的证据，因此直接比对原始字节
    let server = TestServer::start().await;

    // 默认（RESP2）：空值是 `$-1\r\n`
    let mut stream = server.connect().await;
    stream
        .write_all(&encode_command(&["GET", "missing"]))
        .await
        .expect("应能写入");
    let mut bytes = [0u8; 5];
    stream.read_exact(&mut bytes).await.expect("应能读取");
    assert_eq!(&bytes, b"$-1\r\n", "RESP2 的空值编码错误");

    // 切换到 RESP3 后：空值是 `_\r\n`
    let mut stream = server.connect().await;
    let hello = command(&mut stream, &["HELLO", "3"]).await;
    // 读取器把原生映射格式化为 `{...}`，出现花括号即证明服务端用的是 `%` 而非扁平数组
    assert!(
        hello.starts_with('{'),
        "HELLO 3 应返回 RESP3 原生映射，实际为：{hello}"
    );
    assert!(
        hello.contains("proto"),
        "HELLO 应包含协议版本信息，实际为：{hello}"
    );

    stream
        .write_all(&encode_command(&["GET", "missing"]))
        .await
        .expect("应能写入");
    let mut bytes = [0u8; 3];
    stream.read_exact(&mut bytes).await.expect("应能读取");
    assert_eq!(&bytes, b"_\r\n", "RESP3 的空值编码错误");
}

#[tokio::test]
async fn ttl_commands_report_remaining_time() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    // 不存在的键
    assert_eq!(command(&mut stream, &["TTL", "nope"]).await, "-2");

    // 存在但无过期时间
    assert_eq!(command(&mut stream, &["SET", "p", "v"]).await, "OK");
    assert_eq!(command(&mut stream, &["TTL", "p"]).await, "-1");

    // 设置了过期时间
    assert_eq!(
        command(&mut stream, &["SET", "t", "v", "EX", "100"]).await,
        "OK"
    );
    assert_eq!(command(&mut stream, &["TTL", "t"]).await, "100");

    // 移除过期时间
    assert_eq!(command(&mut stream, &["PERSIST", "t"]).await, "1");
    assert_eq!(command(&mut stream, &["TTL", "t"]).await, "-1");
}

#[tokio::test]
async fn keys_expire_for_real() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    assert_eq!(
        command(&mut stream, &["SET", "brief", "v", "PX", "50"]).await,
        "OK"
    );
    assert_eq!(command(&mut stream, &["GET", "brief"]).await, "v");

    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    assert_eq!(command(&mut stream, &["GET", "brief"]).await, "(nil)");
    assert_eq!(command(&mut stream, &["TTL", "brief"]).await, "-2");
    assert_eq!(command(&mut stream, &["EXISTS", "brief"]).await, "0");
}

#[tokio::test]
async fn set_nx_does_not_overwrite_existing_key() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    assert_eq!(command(&mut stream, &["SET", "k", "first"]).await, "OK");
    // NX 条件不满足时返回空值，且不得改动原值
    assert_eq!(
        command(&mut stream, &["SET", "k", "second", "NX"]).await,
        "(nil)"
    );
    assert_eq!(command(&mut stream, &["GET", "k"]).await, "first");
}

#[tokio::test]
async fn array_replies_are_properly_framed() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    command(&mut stream, &["MSET", "a", "1", "b", "2"]).await;
    // 缺失的键必须在数组中占位为 null，不能跳过——否则客户端无法对齐位置
    assert_eq!(
        command(&mut stream, &["MGET", "a", "missing", "b"]).await,
        "[1,(nil),2]"
    );

    assert_eq!(
        command(&mut stream, &["MGET", "none1", "none2"]).await,
        "[(nil),(nil)]"
    );
}

#[tokio::test]
async fn concurrent_connections_share_one_state_machine() {
    let server = TestServer::start().await;

    let mut writer = server.connect().await;
    assert_eq!(
        command(&mut writer, &["SET", "shared", "from-writer"]).await,
        "OK"
    );

    // 另一条连接应当看到同一条数据
    let mut reader = server.connect().await;
    assert_eq!(
        command(&mut reader, &["GET", "shared"]).await,
        "from-writer"
    );
}

#[tokio::test]
async fn dbsize_reflects_live_keys_only() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    command(&mut stream, &["SET", "a", "1"]).await;
    command(&mut stream, &["SET", "b", "2"]).await;
    command(&mut stream, &["SET", "gone", "3", "PX", "50"]).await;

    assert_eq!(command(&mut stream, &["DBSIZE"]).await, "3");

    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    // 已过期的键不计入
    assert_eq!(command(&mut stream, &["DBSIZE"]).await, "2");
}

#[tokio::test]
async fn quit_replies_before_closing() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    assert_eq!(command(&mut stream, &["QUIT"]).await, "OK");

    // 服务端应当主动关闭连接：后续读取会得到 EOF（读到 0 字节）
    let mut buffer = [0u8; 1];
    let read = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut buffer))
        .await
        .expect("服务端应在 QUIT 后关闭连接，而不是一直挂着")
        .expect("读取应正常结束");

    assert_eq!(read, 0, "应读到 EOF");
}
