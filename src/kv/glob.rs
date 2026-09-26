//! Redis 风格的 glob 模式匹配。
//!
//! `KEYS` 与 `SCAN ... MATCH` 都依赖它。支持的语法与 Redis 一致：
//!
//! | 模式 | 含义 |
//! |---|---|
//! | `*` | 匹配任意长度（含空）的字符序列 |
//! | `?` | 匹配恰好一个字符 |
//! | `[abc]` | 匹配集合中的任一字符 |
//! | `[a-z]` | 匹配范围内的字符 |
//! | `[^abc]` | 匹配**不在**集合中的字符 |
//! | `\x` | 转义，按字面匹配 `x` |
//!
//! 之所以不用现成的 glob 库：Redis 的语义与 POSIX 的 glob（通过 `fnmatch`）并不完全一致，
//! 而这段逻辑本身很短，自己实现反而更容易与 Redis 的行为对齐。

/// 判断 `text` 是否匹配 `pattern`。
///
/// 模式与文本都按字节处理——Redis 的键是二进制安全的，不做 UTF-8 假设。
pub fn matches(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0usize;
    let mut t = 0usize;

    // 最近一次遇到的 `*` 的位置，用于回溯。
    // 记下的是「星号在模式中的下标」与「当时文本推进到的位置」。
    let mut star: Option<(usize, usize)> = None;

    while t < text.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    // 先假设这个 `*` 匹配空串。若后续匹配失败，
                    // 再回到这里让它多吃一个字符。
                    star = Some((p, t));
                    p += 1;
                    continue;
                }
                b'?' => {
                    // 任意单字符
                    p += 1;
                    t += 1;
                    continue;
                }
                b'[' => {
                    if let Some((hit, next_p)) = match_class(pattern, p, text[t]) {
                        if hit {
                            p = next_p;
                            t += 1;
                            continue;
                        }
                        // 未命中：落到下面的回溯逻辑
                    } else if pattern[p] == text[t] {
                        // `[` 未闭合，按字面量处理
                        p += 1;
                        t += 1;
                        continue;
                    }
                }
                b'\\' if p + 1 < pattern.len() => {
                    if pattern[p + 1] == text[t] {
                        p += 2;
                        t += 1;
                        continue;
                    }
                }
                literal if literal == text[t] => {
                    p += 1;
                    t += 1;
                    continue;
                }
                _ => {}
            }
        }

        // 当前位置匹配不上：回退到最近的 `*`，让它多消费一个字符
        match star {
            Some((star_p, star_t)) => {
                p = star_p + 1;
                t = star_t + 1;
                star = Some((star_p, star_t + 1));
            }
            None => return false,
        }
    }

    // 文本已耗尽，模式剩下的部分必须全是 `*` 才算匹配
    // （`foo*` 匹配 `foo`，但 `foo?` 不匹配 `foo`）
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

/// 匹配一个 `[...]` 字符集合。
///
/// 返回 `(是否命中, 集合结束后的下标)`；若 `[` 没有闭合则返回 `None`，
/// 由调用方按字面量处理。
fn match_class(pattern: &[u8], start: usize, ch: u8) -> Option<(bool, usize)> {
    let mut i = start + 1;

    let negated = if i < pattern.len() && pattern[i] == b'^' {
        i += 1;
        true
    } else {
        false
    };

    let mut hit = false;
    // `]` 出现在集合首位时表示字面量 `]`，而不是集合结束
    let mut at_first = true;

    while i < pattern.len() {
        if pattern[i] == b']' && !at_first {
            return Some((hit != negated, i + 1));
        }
        at_first = false;

        // 读取范围的下界（可能是转义字符）
        let (low, low_len) = read_char(pattern, i);
        i += low_len;

        // 若紧跟 `-` 且其后不是 `]`，则这是一个范围
        if i + 1 < pattern.len() && pattern[i] == b'-' && pattern[i + 1] != b']' {
            let (high, high_len) = read_char(pattern, i + 1);
            i += 1 + high_len;
            // 范围的端点若顺序颠倒，Redis 视为不匹配任何字符
            if low <= ch && ch <= high {
                hit = true;
            }
        } else if low == ch {
            hit = true;
        }
    }

    // 没有闭合的 `]`
    None
}

/// 从 `index` 处读取一个字符，处理反斜杠转义。
///
/// 返回 `(字符, 消费的字节数)`。
fn read_char(pattern: &[u8], index: usize) -> (u8, usize) {
    if pattern[index] == b'\\' && index + 1 < pattern.len() {
        (pattern[index + 1], 2)
    } else {
        (pattern[index], 1)
    }
}

#[cfg(test)]
mod tests {
    use super::matches;

    /// 断言辅助：让测试读起来更接近自然语言
    fn assert_matches(pattern: &str, text: &str) {
        assert!(
            matches(pattern.as_bytes(), text.as_bytes()),
            "模式 `{pattern}` 应当匹配 `{text}`"
        );
    }

    fn assert_not_matches(pattern: &str, text: &str) {
        assert!(
            !matches(pattern.as_bytes(), text.as_bytes()),
            "模式 `{pattern}` 不应匹配 `{text}`"
        );
    }

    #[test]
    fn star_matches_everything() {
        assert_matches("*", "");
        assert_matches("*", "anything");
        assert_matches("*", "多字节中文");
    }

    #[test]
    fn star_as_prefix_suffix_and_infix() {
        assert_matches("foo*", "foo");
        assert_matches("foo*", "foobar");
        assert_not_matches("foo*", "fo");

        assert_matches("*bar", "bar");
        assert_matches("*bar", "foobar");
        assert_not_matches("*bar", "barfoo");

        assert_matches("f*o", "fo");
        assert_matches("f*o", "foobar-o");
        assert_not_matches("f*o", "fax");
    }

    #[test]
    fn multiple_stars_backtrack_correctly() {
        // 这是最容易被写错的场景：回溯点必须吃掉尽可能少的字符
        assert_matches("*a*b", "aXb");
        assert_matches("*a*b", "aaab");
        assert_matches("*a*b*", "xaybz");
        assert_not_matches("*a*b", "ba");
        assert_not_matches("*a*b", "abx");

        assert_matches("*aa*", "aaa");
        assert_matches("a*a*a", "aaaa");
    }

    #[test]
    fn literal_pattern_requires_exact_match() {
        assert_matches("foo", "foo");
        assert_not_matches("foo", "foobar");
        assert_not_matches("foo", "fo");
        assert_not_matches("foo", "bar");
    }

    #[test]
    fn single_char_wildcard() {
        assert_matches("f?o", "foo");
        assert_matches("f?o", "fao");
        // `?` 只匹配一个字符，不匹配空
        assert_not_matches("f?o", "fo");
        assert_not_matches("f?o", "fooo");
    }

    #[test]
    fn trailing_wildcard_star_can_match_nothing() {
        // `foo*` 应当匹配 `foo`——星号可以匹配空串
        assert_matches("foo*", "foo");
        // 但 `foo?` 不能
        assert_not_matches("foo?", "foo");
    }

    #[test]
    fn character_class() {
        assert_matches("[abc]x", "ax");
        assert_matches("[abc]x", "bx");
        assert_matches("[abc]x", "cx");
        assert_not_matches("[abc]x", "dx");
        assert_not_matches("[abc]x", "abx");
    }

    #[test]
    fn character_range() {
        assert_matches("user[0-9]", "user5");
        assert_not_matches("user[0-9]", "usera");

        assert_matches("[a-z][0-9]", "x7");
        assert_not_matches("[a-z][0-9]", "7x");
    }

    #[test]
    fn negated_class() {
        assert_matches("[^abc]x", "dx");
        assert_not_matches("[^abc]x", "ax");
        assert_matches("key[^0-9]", "keyA");
        assert_not_matches("key[^0-9]", "key5");
    }

    #[test]
    fn closing_bracket_at_start_is_literal() {
        // Redis 允许 `]` 作为集合的首字符来表示字面量
        assert_matches("[]x]", "]");
        assert_matches("[]x]", "x");
        assert_not_matches("[]x]", "y");
    }

    #[test]
    fn unterminated_class_is_treated_as_literal() {
        // `[` 没有闭合时按字面量处理，而不是报错或吞掉输入
        assert_matches("a[b", "a[b");
        assert_not_matches("a[b", "ab");
    }

    #[test]
    fn escape_sequences() {
        assert_matches(r"a\*b", "a*b");
        // 转义后的 `*` 是字面量，不应匹配任意串
        assert_not_matches(r"a\*b", "aXb");

        assert_matches(r"\?", "?");
        assert_not_matches(r"\?", "a");

        assert_matches(r"\[abc\]", "[abc]");
    }

    #[test]
    fn redis_typical_patterns() {
        // 实际使用中最常见的几种模式
        assert_matches("user:*", "user:1001");
        assert_not_matches("user:*", "session:1001");

        assert_matches("cache:*:v2", "cache:home:v2");
        assert_not_matches("cache:*:v2", "cache:home:v1");

        assert_matches("?", "a");
        assert_not_matches("?", "ab");
    }

    #[test]
    fn binary_safe() {
        // 键是二进制安全的，模式匹配也不应假设 UTF-8
        let pattern = b"bin\0\0*";
        let text = b"bin\0\0ary";
        assert!(matches(pattern, text));

        let other = b"bin\0a";
        assert!(!matches(pattern, other));
    }
}
