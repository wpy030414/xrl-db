# Spec — 统一错误类型

## 要构建什么

- **目标**：为整个项目提供一个可枚举的错误类型，各层通过 `From` 向上转换。
- **为什么需要**：若各层各自使用 `Box<dyn Error>`，调用方将无法完整枚举「可能失败的方式」，也无法对特定错误做出差异化处理。集中定义换来的是错误类型可被穷举匹配。
- **位置**：`src/error.rs`

## 行为

- 全项目共用一个 `Error` 枚举，按**出错原因**而非**出错位置**划分变体。
- 实现 `std::error::Error::source()`，使底层错误可被追溯（配合 `anyhow` / `eyre` 一类的库可打印完整因果链）。
- 所有变体的 `Display` 输出为**中文**，且必须包含定位信息（文件路径、行号、字段名等），让使用者一眼知道去哪里改。

## 输入 / 输出

- **输入**：不放宽任何输入——本模块只定义类型。
- **输出**：`Result<T> = std::result::Result<T, Error>` 别名，供全项目使用。

当前变体：

| 变体 | 触发场景 | 携带信息 |
|---|---|---|
| `ConfigRead` | 配置文件无法读取 | 路径 + 底层 `io::Error` |
| `ConfigToml` | TOML 语法或类型错误 | 路径 + 底层 `toml::de::Error`（其 `Display` 含行列） |
| `ConfigLine` | `redis.conf` 风格的行式错误 | 路径 + 行号 + 说明 |
| `ConfigInvalid` | 语义上不合法（语法正确但组合说不通） | 说明 |

## 约束

- **不得**使用 `Box<dyn Error>` 作为项目的错误传递方式。
- **不得**在可预期的错误路径上 `unwrap()` / `expect()`。
- `Display` 实现**必须**是中文，且**必须**包含足够定位问题的信息。
- 新增变体时**必须**同步更新 `source()` 的实现（若该变体有底层错误）。
- 无底层错误的变体在 `source()` 中返回 `None`，不得伪造成有底层错误。

## 边界条件

| 情况 | 行为 |
|---|---|
| 变体无底层错误（`ConfigLine` / `ConfigInvalid`） | `source()` 返回 `None` |
| `toml::de::Error` 的行列信息 | 不重复包装，直接用其 `Display` 输出 |
| 错误信息中的路径 | 用 `Path::display()` 输出，避免非 UTF-8 路径导致 panic |

## 验收标准

- [x] `Error` 实现 `Debug`、`Display`、`std::error::Error`
- [x] `source()` 对含底层错误的变体返回 `Some`，对不含的返回 `None`
- [x] 所有 `Display` 输出为中文且包含定位信息
- [x] 各变体在 `main` 中通过 `eprintln!` 输出并以非零退出码结束

## 完成定义

- 项目内不存在 `Box<dyn Error>`
- 项目内不存在可预期错误路径上的 `unwrap()` / `expect()`（由 code review 与 clippy 共同保障）
