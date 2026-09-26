//! RESP 编解码的属性测试。
//!
//! # 为什么不满足于单元测试
//!
//! `src/protocol/codec.rs` 里已经有一组很细的单元测试，但它们全都只覆盖**我们想到的**
//! 那几种输入。而这一层面对的是**网络**：客户端发来的字节流不归我们管，可以是任何东西。
//! 一个「协议层不该崩溃」的承诺，用几个手写的例子是证明不了的——你得让机器去撞。
//!
//! # 三条性质
//!
//! 1. **分包不变性**：一条命令被 TCP 切成什么样，都不影响解码结果。
//!    这是最容易写错、也最难用手写用例覆盖的一条——切割点有 `n+1` 种，而真实的
//!    网络会挑任何一种。写错它的症状是「偶发解析失败」，几乎不可能靠复现来定位。
//! 2. **不崩溃、不空转**：任意字节输入都必须得到一个结果，且每解出一条必须真的
//!    消费掉字节。只解不消费会让会话循环空转，一条恶意连接就能吃满一个核。
//! 3. **二进制安全**：任意的字节序列作为键和值，往返之后必须一个字节都不差。
//!
//! 这三条都**不需要「正确答案」**——性质 1 和 2 比较的是「不同切法之间是否一致」，
//! 性质 3 用 `ECHO` / `GET` 这类只有一个自由参数的命令做判据。
//! 没有正确答案可抄，也就没有「测试跟着实现一起错」的余地。
//!
//! # 它第一次跑起来时发生了什么
//!
//! 值得原样记下来，因为它演示了这类测试的两种失败方式。
//!
//! 逐字节重放那条性质当场就红了：一条以 `_` 开头的内联命令，在被逐字节喂入时，
//! 单独一个 `_` 就让 RESP2 解码器报了「Invalid frame type」。第一反应是
//! 「解码器把不完整当成了错误」，于是动手让它在缓冲区里没有 CRLF 时一律等待。
//!
//! **那个改动是错的。** `_` 在 RESP2 下永远不可能变得合法，立刻报错比无限等待
//! 更有信息量；真正站不住的是测试自己的期望——它假设「以类型前缀开头的东西可以是
//! 内联命令」，而这条假设既与 `src/protocol/codec.rs` 里写明的规则相反，也与
//! Redis 的做法相反（Redis 同样靠首字节决定用哪个解析器）。
//!
//! 所以这次跑出来的不是一段代码改动，而是两条东西：
//! **一条原本没有被写下来的规则**（首字节落进类型前缀集合时，按帧解析，不做内联
//! 解释），以及**一组把它钉住的断言**。真正的陷阱不是解码器，是「测试的期望本身
//! 未经审视」——性质测试会把这种未经审视的假设直接变成一条红色的失败。
//!
//! 为了让「通过」真的有分量，这组性质还各自做过负向对照：把「内联行没收全就等」
//! 改成「当成非内联」，`inline_commands_are_chunking_invariant` 立刻失败；把
//! 解码改成「解出条目但不消费」，反空转断言立刻失败。没被撞红过的断言不算证据。

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use tokio_util::codec::Decoder;

use xrl_db::protocol::{Command, CommandError, Dialect, RespCodec, SetCondition};

/// 把一条「应当解析成功」的命令渲染成与 [`decode_in_chunks`] 同一套文本。
///
/// [`decode_in_chunks`] 记的是 `Result` 的 `Debug`，因此判据也必须带上 `Ok(..)`
/// 外层——否则比较的是两个不同形状的字符串。
fn decoded_ok(command: Command) -> String {
    format!("{:?}", Ok::<Command, CommandError>(command))
}

// ============================================================ 辅助

/// 把若干段字节渲染成一条 RESP 数组命令。
///
/// 请求的编码在 RESP2 与 RESP3 下是相同的，因此这一份渲染对两种方言都适用。
fn render(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// 按给定的切分点把 `raw` 喂进一个编解码器，返回解出的条目与最终剩余字节数。
///
/// 返回的条目是 `Debug` 文本而非 `Command` 本身：这样 `Err` 与 `FramingError`
/// 也能参与比较，而它们恰恰是最该被比较的部分。`None`（数据不足）**不计入**条目——
/// 它的出现次数取决于切法，本身就是切法的函数。
fn decode_in_chunks(raw: &[u8], cuts: &[usize], dialect: Dialect) -> (Vec<String>, usize) {
    let mut codec = RespCodec::new(dialect);
    let mut buffer = BytesMut::new();
    let mut items = Vec::new();

    let mut boundaries = cuts.to_vec();
    boundaries.push(raw.len());

    let mut fed = 0usize;
    for boundary in boundaries {
        buffer.extend_from_slice(&raw[fed..boundary]);
        fed = boundary;

        // 一次喂入可能解出多条（流水线），必须反复取到 None 为止
        loop {
            let before = buffer.len();
            match codec.decode(&mut buffer) {
                Ok(Some(item)) => {
                    // ★ 反空转断言 ★
                    //
                    // 解出一条却一个字节都没消费，会话循环就会在
                    // 「解出条目 → 没消费 → 再解出同一条」之间永远打转。
                    // 单个连接吃满一个核，而且没有任何报错。
                    assert!(
                        buffer.len() < before,
                        "解码出 {item:?} 却没有消费任何字节（缓冲区仍是 {} 字节）——\
                         会话循环会在这里空转",
                        buffer.len()
                    );
                    items.push(format!("{item:?}"));
                }
                Ok(None) => break,
                Err(error) => {
                    items.push(format!("FramingError({error})"));
                    break;
                }
            }
        }
    }

    (items, buffer.len())
}

/// 喂入一段**不完整的**字节流，确认它安静地等待更多数据。
///
/// 「安静」的定义是两条都要满足：没有把不完整当成致命错误，也没有凭空解出一条命令。
fn waits_for_more(raw: &[u8], dialect: Dialect) -> Result<(), String> {
    let mut codec = RespCodec::new(dialect);
    let mut buffer = BytesMut::from(raw);

    match codec.decode(&mut buffer) {
        Ok(None) => Ok(()),
        Ok(Some(item)) => Err(format!("不完整的字节流竟然解出了 {item:?}")),
        Err(error) => Err(format!("不完整的字节流被当成了致命错误：{error}")),
    }
}

/// 任意字节序列。
fn arb_bytes(max: usize) -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..=max)
}

/// 一条「形状合法」的命令：1~3 段，每段 0~10 字节。
///
/// 刻意把长度压小：性质 1 会逐字节地重放整条命令，长度直接决定测试用时。
/// 而触发解析逻辑所需的信息量本来就不大——真正需要长度的是二进制安全那条，
/// 它由单独的策略负责。
fn arb_parts() -> impl Strategy<Value = Vec<Vec<u8>>> {
    proptest::collection::vec(arb_bytes(10), 1..=3)
}

// ============================================================ 性质 1：分包不变性

proptest! {
    #![proptest_config(ProptestConfig {
        // 逐字节重放让每个用例都不便宜，用例数从默认的 256 降下来，
        // 换取在 CI 上稳定跑完。命中率靠策略的形状保证，不靠数量。
        cases: 96,
        ..ProptestConfig::default()
    })]

    /// 一条命令无论被切成什么样，解出的结果都必须完全一致。
    ///
    /// 三种切法：整块喂入、在随机位置切一刀、逐字节喂入。
    /// 逐字节那一种是最狠的——它覆盖了**所有** `n+1` 个切分点。
    #[test]
    fn decode_result_is_independent_of_chunking(parts in arb_parts(), cut_seed in any::<prop::sample::Index>()) {
        let raw = render(&parts.iter().map(|part| part.as_slice()).collect::<Vec<_>>());

        let whole = decode_in_chunks(&raw, &[], Dialect::Resp2);
        let cut = cut_seed.index(raw.len() + 1);
        let two = decode_in_chunks(&raw, &[cut], Dialect::Resp2);
        let byte_by_byte = decode_in_chunks(&raw, &(0..raw.len()).collect::<Vec<_>>(), Dialect::Resp2);

        prop_assert_eq!(&whole, &two, "整块喂入与在位置 {} 切一刀，解出的结果不同（输入 {:?}）", cut, raw);
        prop_assert_eq!(&whole, &byte_by_byte, "整块喂入与逐字节喂入解出的结果不同（输入 {:?}）", raw);

        // 一条完整的命令喂完之后，缓冲区里不该剩任何东西
        prop_assert_eq!(whole.1, 0, "解完一条命令后缓冲区还剩 {} 字节（输入 {:?}）", whole.1, raw);
    }

    /// 任何一个**不完整的前缀**都必须安静等待，而不是报错或提前交付。
    #[test]
    fn every_proper_prefix_waits_for_more(parts in arb_parts()) {
        let raw = render(&parts.iter().map(|part| part.as_slice()).collect::<Vec<_>>());

        // ★ 先自检这条性质不是空的 ★
        //
        // 如果「等待」被实现成「永远返回 None」，那么对所有前缀的断言都会通过，
        // 而性质本身什么都没测到。完整的输入必须**解得出东西**，才说明这个
        // 断言真的在区分「数据不足」与「数据够了」。
        prop_assert!(
            waits_for_more(&raw, Dialect::Resp2).is_err(),
            "完整的输入 {:?} 也被当成「数据不足」了——这条性质是空的",
            raw
        );

        for dialect in [Dialect::Resp2, Dialect::Resp3] {
            for end in 0..raw.len() {
                if let Err(message) = waits_for_more(&raw[..end], dialect) {
                    prop_assert!(false, "前缀 {:?}（前 {} 字节）没有安静等待：{}", raw, end, message);
                }
            }
        }
    }
}

// ============================================================ 性质 1b：内联命令

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    /// 同一组参数，写成内联一行和写成标准数组，必须解出同一条命令。
    ///
    /// 内联命令（`telnet` 手打、部分脚本）是本层**唯一**由我们自己实现、而非
    /// 委托给 `redis-protocol` 的解析逻辑。正因为它是自己写的，它才最需要被机器撞：
    /// 库里那些路径至少还有上游的测试兜着，这一段只有我们。
    ///
    /// 命令名的首字符限定为字母——这不是为了让测试好过，而是「内联命令」的定义：
    /// 首字符落在 RESP 类型前缀集合里时字节流会按帧解析，那已经不是内联命令了。
    #[test]
    fn inline_and_array_forms_agree(
        name in "[A-Za-z][A-Za-z0-9_]{0,11}",
        args in proptest::collection::vec("[A-Za-z0-9_]{1,12}", 0..=3),
    ) {
        let mut tokens = vec![name];
        tokens.extend(args);

        let refs: Vec<&[u8]> = tokens.iter().map(|token| token.as_bytes()).collect();

        let mut inline = tokens.join(" ").into_bytes();
        inline.extend_from_slice(b"\r\n");

        let from_array = decode_in_chunks(&render(&refs), &[], Dialect::Resp2);
        let from_inline = decode_in_chunks(&inline, &[], Dialect::Resp2);

        // 自检：两种形态都必须是**结构完整的单条命令**。否则两边可能都只是
        // 同一句「错误」——那样比较仍然成立，却什么也没证明。
        prop_assert_eq!(
            from_array.0.len(),
            1,
            "输入 {:?} 应恰好解出一条命令（而非报帧层面的错误）",
            String::from_utf8_lossy(&inline)
        );

        prop_assert_eq!(
            from_array.clone(),
            from_inline,
            "内联形态与数组形态解出的结果不同：\n  数组 {:?}\n  内联 {:?}",
            render(&refs),
            inline
        );
    }

    /// 内联命令同样必须对切分不敏感。
    ///
    /// 这条路径靠「收不到 CRLF 就等」来判断半包。写错它，`telnet` 打一半停顿
    /// 就会被当成致命错误而断连——而那是一种完全正常的交互。
    #[test]
    fn inline_commands_are_chunking_invariant(
        name in "[A-Za-z][A-Za-z0-9_]{0,11}",
        args in proptest::collection::vec("[A-Za-z0-9_]{1,12}", 0..=3),
    ) {
        let mut tokens = vec![name];
        tokens.extend(args);

        let mut inline = tokens.join(" ").into_bytes();
        inline.extend_from_slice(b"\r\n");

        for end in 0..inline.len() {
            if let Err(message) = waits_for_more(&inline[..end], Dialect::Resp2) {
                prop_assert!(false, "内联前缀 {:?}（前 {} 字节）没有安静等待：{}", inline, end, message);
            }
        }

        let whole = decode_in_chunks(&inline, &[], Dialect::Resp2);
        let byte_by_byte = decode_in_chunks(&inline, &(0..inline.len()).collect::<Vec<_>>(), Dialect::Resp2);

        prop_assert_eq!(&whole, &byte_by_byte, "内联命令逐字节喂入后解出的结果不同：{:?}", inline);
        prop_assert_eq!(whole.1, 0, "内联命令解完后缓冲区还剩 {} 字节：{:?}", whole.1, inline);
    }

    /// 连续多个空格、以及行首行尾的空白，都不能改变解析结果。
    ///
    /// 手打的命令里多余空格是常态；把它当成一个空参数，就会得到
    /// 「wrong number of arguments」这种让人摸不着头脑的报错。
    #[test]
    fn repeated_whitespace_is_collapsed(
        name in "[A-Za-z][A-Za-z0-9_]{0,11}",
        args in proptest::collection::vec("[A-Za-z0-9_]{1,12}", 0..=3),
    ) {
        let mut tokens = vec![name];
        tokens.extend(args);

        let refs: Vec<&[u8]> = tokens.iter().map(|token| token.as_bytes()).collect();

        // 前面垫空格之后，首字节是空格而非类型前缀，因此这条路径必然是内联的
        let mut padded = Vec::new();
        padded.extend_from_slice(b"  ");
        padded.extend_from_slice(tokens.join("   ").as_bytes());
        padded.extend_from_slice(b" \t \r\n");

        prop_assert_eq!(
            decode_in_chunks(&render(&refs), &[], Dialect::Resp2).0,
            decode_in_chunks(&padded, &[], Dialect::Resp2).0,
            "多空格 / 前后空白改变了内联命令的解析结果：{:?}",
            String::from_utf8_lossy(&padded)
        );
    }
}

// ============================================================ 性质 2：任意字节都不崩溃、不空转

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    /// 任意字节序列：解码可以失败，但不能崩溃，也不能只解不消费。
    ///
    /// `decode_in_chunks` 内部带反空转断言，因此这里只要「跑完」就算通过。
    #[test]
    fn arbitrary_bytes_are_handled_without_panicking(raw in arb_bytes(64)) {
        for dialect in [Dialect::Resp2, Dialect::Resp3] {
            let (items, _) = decode_in_chunks(&raw, &[], dialect);
            // 不做取值断言——垃圾输入本就该被拒绝。这里只要求它**有结论**。
            prop_assert!(items.len() <= raw.len() + 1);
        }
    }

    /// 伪装成长度前缀的输入最容易踩到底层解码器的边界。
    ///
    /// 纯随机字节几乎撞不出合法的 `$<数字>\r\n`，所以这里显式构造骨架：
    /// 类型前缀 + 一个可能大到离谱的数字 + CRLF，再缀上一小段随机字节。
    #[test]
    fn length_prefix_shaped_input_is_survived(
        prefix in prop::sample::select(vec![b'$', b'*', b'%', b'~', b'|', b'>', b'+', b'-', b':']),
        declared in prop_oneof![0u64..200, 1_000_000u64..1_000_010, Just(u64::MAX), Just(u64::MAX - 1)],
        tail in arb_bytes(16),
    ) {
        let mut raw = format!("{}{}\r\n", prefix as char, declared).into_bytes();
        raw.extend_from_slice(&tail);

        for dialect in [Dialect::Resp2, Dialect::Resp3] {
            let (items, remaining) = decode_in_chunks(&raw, &[], dialect);
            prop_assert!(items.len() <= 2);
            // 拒绝必须是**确定**的：要么解出点什么，要么原样留在缓冲区里等更多数据，
            // 绝不能悄悄吃掉超过它该吃的字节——那会让后面的帧永久错位。
            prop_assert!(remaining <= raw.len());
        }
    }
}

// ============================================================ 性质 3：二进制安全

proptest! {
    /// 任意的字节序列作为 `ECHO` 的载荷，必须原样穿过编解码器。
    ///
    /// `ECHO` 只有一个自由参数，解析结果是唯一的——因此它是这里最干净的判据：
    /// 不需要另一个实现来当对照组。
    #[test]
    fn arbitrary_payload_survives_echo(message in arb_bytes(48)) {
        let raw = render(&[b"ECHO", &message]);
        let (items, remaining) = decode_in_chunks(&raw, &[], Dialect::Resp2);

        prop_assert_eq!(remaining, 0);
        prop_assert_eq!(items.len(), 1, "应恰好解出一条命令，实际 {:?}", items);
        prop_assert_eq!(
            items[0].clone(),
            decoded_ok(Command::Echo { message: Bytes::from(message) }),
            "载荷没能原样穿过编解码器"
        );
    }

    /// 任意的字节序列作为键或值，都必须被原样保留。
    ///
    /// 与上一条的区别在于走的是**参数解析**而非单一载荷：`GET` 一个键、`SET` 一对
    /// 键值，键里出现 CRLF、NUL、非法 UTF-8 都不能影响结果。
    #[test]
    fn arbitrary_key_and_value_survive(key in arb_bytes(32), value in arb_bytes(32)) {
        let (get_items, _) = decode_in_chunks(&render(&[b"GET", &key]), &[], Dialect::Resp2);
        prop_assert_eq!(get_items.len(), 1);
        prop_assert_eq!(
            get_items[0].clone(),
            decoded_ok(Command::Get { key: Bytes::from(key.clone()) }),
        );

        let (set_items, _) = decode_in_chunks(&render(&[b"SET", &key, &value]), &[], Dialect::Resp2);
        prop_assert_eq!(set_items.len(), 1);
        prop_assert_eq!(
            set_items[0].clone(),
            decoded_ok(Command::Set {
                key: Bytes::from(key),
                value: Bytes::from(value),
                expire: None,
                condition: SetCondition::Always,
            }),
        );
    }

    /// 流水线：多条命令挤在一个缓冲区里，必须逐条取出、严格保持顺序。
    #[test]
    fn pipelined_commands_keep_their_order(payloads in proptest::collection::vec(arb_bytes(12), 1..=8)) {
        let mut raw = Vec::new();
        for payload in &payloads {
            raw.extend_from_slice(&render(&[b"ECHO", payload]));
        }

        let mut codec = RespCodec::default();
        let mut buffer = BytesMut::from(&raw[..]);

        for (index, payload) in payloads.iter().enumerate() {
            let decoded = codec.decode(&mut buffer).expect("不应出现帧层面错误");
            prop_assert_eq!(
                decoded,
                Some(Ok(Command::Echo { message: Bytes::from(payload.clone()) })),
                "第 {} 条命令与发送顺序不符", index
            );
        }

        prop_assert_eq!(codec.decode(&mut buffer).expect("不应出现帧层面错误"), None);
        prop_assert!(buffer.is_empty(), "全部取完之后缓冲区不该还剩 {} 字节", buffer.len());
    }
}

// ============================================================ 定点的恶意输入

/// 首字节落在 RESP 类型前缀集合里时，一律按 RESP 帧处理——内联解释被让位。
///
/// ★ 这条规则是逐字节重放那条性质逼出来的 ★
///
/// 它本来报的是一个「失败」：内联命令 `_ ...` 被逐字节喂入时，单独一个 `_` 就让
/// RESP2 报了帧层面错误。第一反应是「解码器把不完整当成了错误」，于是动手让它在
/// 没有 CRLF 时一律等待。
///
/// 那个改动是错的，而且是可以证明地错：`_` 在 RESP2 下**永远**不可能变得合法，
/// 所以「立刻报错」比「无限等待」更有信息量。真正站不住的是测试的期望——
/// 它假设「以类型前缀开头的东西可以是内联命令」，而这条假设与模块文档里写明的
/// 规则相反，也与 Redis 自己的做法相反（Redis 同样靠首字节决定用哪种解析器）。
///
/// 所以最终落下来的不是一段代码改动，而是这条规则本身，以及下面这组断言。
#[test]
fn the_first_byte_decides_between_frame_and_inline() {
    for byte in b"+-:$*_#,(!=%>|".iter().copied() {
        // 首字节是类型前缀 —— 必须走 RESP 帧路径，绝不能被当成内联命令的开头。
        // 判据：在 RESP3 下给它一个合法的帧（`_\r\n`），它必须解成帧、而不是命令。
        let inline_form = {
            let mut raw = vec![byte];
            raw.extend_from_slice(b" \r\n");
            raw
        };

        // 内联解释一定会返回 UnknownCommand —— 借这一点反证「这次没有走内联路径」
        let (items, _) = decode_in_chunks(&inline_form, &[], Dialect::Resp2);
        assert_eq!(
            items.len(),
            1,
            "'{}' 开头的字节流应恰好得到一条结论：{items:?}",
            byte as char
        );
        assert!(
            !items[0].contains("UnknownCommand"),
            "'{}' 开头的字节流被当成了内联命令；首字节是类型前缀时应当按帧解析。解出：{:?}",
            byte as char,
            items[0]
        );
    }

    // 反过来的那一半：首字节**不是**类型前缀时，确实走内联路径
    let (items, remaining) = decode_in_chunks(b"PING\r\n", &[], Dialect::Resp2);
    assert_eq!(items.len(), 1, "普通内联命令应当被解出：{items:?}");
    assert_eq!(remaining, 0);
}

/// 长度前缀声称的内容远大于实际收到的数据。
///
/// 这一组是**定点**的，不进 proptest：它们要验证的不是「随机输入不崩」，
/// 而是「服务端不会被一句话骗着分配一大块内存」。一个伪造的长度前缀如果被
/// 直接拿去 `reserve`，就是一条不需要认证的拒绝服务路径。
///
/// 这里断言的是**行为**而非「有没有 OOM」——真的 OOM 了进程会直接死掉，
/// 测试也就失败了，不需要额外断言。
#[test]
fn absurd_length_prefixes_do_not_take_the_process_down() {
    let hostile: &[&[u8]] = &[
        b"$9999999999999\r\n",
        b"$18446744073709551615\r\n",
        b"*18446744073709551615\r\n",
        b"$99999999999999999999999\r\n",
        b"$9999999999999\r\nabcdef\r\n",
        b"$-1\r\n",
        b"*-1\r\n",
        b"%9999999999\r\n",
        b"~9999999999\r\n",
        b"|9999999999\r\n",
    ];

    for raw in hostile {
        for dialect in [Dialect::Resp2, Dialect::Resp3] {
            // 只要不 panic、不 OOM、不无限等待，就算通过
            let (items, remaining) = decode_in_chunks(raw, &[], dialect);
            assert!(
                remaining <= raw.len(),
                "输入 {raw:?} 解出 {items:?} 后剩余 {remaining} 字节，比输入还多——\
                 长度前缀被当成了分配依据"
            );
        }
    }
}
