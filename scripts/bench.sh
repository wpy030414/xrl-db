#!/usr/bin/env bash
#
# 量一下真实的吞吐与延迟。
#
# 用法：
#   ./scripts/bench.sh                      # 单节点
#   ./scripts/bench.sh --cluster            # 三节点，分别量主节点与从节点
#   XRLDB_BENCH_REQUESTS=200000 ./scripts/bench.sh
#   XRLDB_BENCH_CLIENTS=100 XRLDB_BENCH_PIPELINE=16 ./scripts/bench.sh
#
# # 为什么需要这个脚本
#
# 「高性能」是立项时的四大支柱之一，但在此之前 README 里连一个吞吐数字都拿不出来。
# 一个没有数字的性能主张不是主张，是形容词。
#
# # 为什么要用 redis-benchmark
#
# 它是 Redis 官方的压测工具，用的是**真实的 RESP 协议**、真实的客户端库、真实的
# 网络往返。拿它来量，量到的就是用户实际会遇到的路径——包括协议解析、共识提交、
# 落盘 fsync。自己写一个「直接调用内部函数」的微基准，恰好会跳过这条路径上真正
# 昂贵的部分。
#
# 它由 redis-cli 所在的包提供；没装的话这个脚本会明确告诉你，而不是给一个假数字。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

REQUESTS="${XRLDB_BENCH_REQUESTS:-5000}"
SERIAL_REQUESTS="${XRLDB_BENCH_SERIAL_REQUESTS:-200}"
CLIENTS="${XRLDB_BENCH_CLIENTS:-50}"
PIPELINE="${XRLDB_BENCH_PIPELINE:-1}"
TESTS="${XRLDB_BENCH_TESTS:-set,get,incr}"
BIN="${XRLDB_BIN:-$ROOT/target/release/xrl-db}"

BENCH_DIR="${XRLDB_BENCH_DIR:-$ROOT/target/bench}"
BENCH_PORT="${XRLDB_BENCH_PORT:-7901}"

MODE="single"

# 是否有请求被服务端拒绝
REJECTED=0

log() { printf '%s\n' "$*"; }
die() { printf '错误：%s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) MODE="cluster" ;;
    -h|--help)
      sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) die "无法识别的参数：$1" ;;
  esac
  shift
done

command -v redis-benchmark >/dev/null 2>&1 \
  || die "找不到 redis-benchmark。它随 redis-cli 一起安装（macOS 上可用 brew install redis）。

没有它就拿不到真实数字，而我宁可什么都不给，也不给一个来路不明的数字。"

# ---------------------------------------------------------------- 单节点

# 单节点的数据目录与日志放在 target/bench 下。
SINGLE_PID=""

cleanup_single() {
  if [ -n "$SINGLE_PID" ] && kill -0 "$SINGLE_PID" 2>/dev/null; then
    kill "$SINGLE_PID" 2>/dev/null || true
    wait "$SINGLE_PID" 2>/dev/null || true
  fi
}
trap cleanup_single EXIT

wait_ready() {
  local port="$1" limit="$2" waited=0
  while [ "$waited" -lt "$limit" ]; do
    if redis-cli -p "$port" PING 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.2
    waited=$((waited + 1))
  done
  return 1
}

# 跑一轮 redis-benchmark。
#
# 必须过滤：这个版本的 redis-benchmark 即使加了 `-q`，也会逐秒打印进度行
#（用 `\r` 分隔，所以整轮输出其实是「一行」），会把结论淹没掉。结论行的特征是
# 含 "requests per second"。
#
# 顺带说一句输出里的 `WARNING: Could not fetch server CONFIG`：redis-benchmark 会
# 试着读 `CONFIG GET` 来判断服务端的持久化设置。本项目不实现 `CONFIG`（见 README
# 的「已知限制」），所以这条警告是预期的，与压测结果无关。
run_round() {
  local port="$1" label="$2" requests="$3" clients="$4" pipeline="$5"

  # 暂时关掉 errexit：redis-benchmark 只要收到**一个**服务端错误就会以非零码退出，
  # 而「有请求被拒绝」恰恰是要报告的信息之一，不该把整个脚本打断在半路。
  set +e
  local out code
  out="$(redis-benchmark -p "$port" -t "$TESTS" \
    -n "$requests" -c "$clients" -P "$pipeline" -q 2>&1)"
  code=$?
  set -e

  printf '%s\n' "$out" | tr '\r' '\n' | grep -E 'requests per second' | sed 's/^ */    /'

  if [ "$code" -ne 0 ]; then
    REJECTED=1
    printf '    ⚠ 本次运行有请求被服务端拒绝（redis-benchmark 退出码 %s）：\n' "$code"
    printf '%s\n' "$out" | tr '\r' '\n' \
      | grep -iE "Error from server" | sort | uniq -c | sed 's/^ */      /'
  fi
}

bench() {
  local port="$1" label="$2"

  log ""
  log "──────── ${label}（端口 ${port}）────────"

  log "吞吐（${CLIENTS} 个并发客户端，${REQUESTS} 次请求，流水线 ${PIPELINE}）："
  run_round "$port" "$label" "$REQUESTS" "$CLIENTS" "$PIPELINE"

  # 单连接那一轮是**真正**要看延迟的一轮。
  #
  # 并发那一轮的 p50 里混着排队时间：几十个客户端撞在一条串行的提交路径上，
  # 排在后面的请求测到的是队列长度，不是服务本身的延迟。一个客户端跑出来的
  # 才是「一次操作要等多久」。
  log "单连接串行延迟（1 个客户端，${SERIAL_REQUESTS} 次请求）："
  run_round "$port" "$label" "$SERIAL_REQUESTS" 1 1
}

# 把「有请求被拒绝」这件事解释清楚，而不是留给读者去猜。
report_rejections() {
  [ "$REJECTED" -eq 0 ] && return 0

  cat <<'NOTE'

────────────────────────────────────────────────────────────────
⚠ 上面的运行里有请求被服务端拒绝。这不是压测工具的问题，而是一个**已知的**
  扩展性缺陷，在这里如实报告出来：

  节点间的每一次 RPC 都会新建一条 TCP 连接（见 src/raft/network.rs 的模块文档），
  而从节点上的每一次读都要一次这样的 RPC。速率一高，connect() 就会开始返回
  EADDRNOTAVAIL（os error 49，「Can't assign requested address」），表现为
  转发失败，或者主节点「联系不上多数派节点」而拒绝读。

  本机实测：从节点的读在约 12000 次/秒时开始出现该错误，而本机的临时端口范围
  只有 16384 个（49152-65535）——量级正好对得上。

  详见 README 的「已知限制」与 docs/DECISIONS.md 的 ADR-018。
────────────────────────────────────────────────────────────────
NOTE
}

run_single() {
  [ -x "$BIN" ] || die "找不到 ${BIN}。先执行：cargo build --release"

  # 每次都从空目录开始：否则上一次跑剩下来的键与日志会让两次结果不可比
  rm -rf "$BENCH_DIR/single"
  mkdir -p "$BENCH_DIR/single"

  local config="$BENCH_DIR/single/xrldb.toml"
  cat >"$config" <<TOML
[node]
id = 1
listen = "127.0.0.1:${BENCH_PORT}"

[storage]
path = "${BENCH_DIR}/single/data"
TOML

  log "启动单节点（端口 ${BENCH_PORT}，数据目录 ${BENCH_DIR}/single/data）"
  "$BIN" --config "$config" >"$BENCH_DIR/single/node.log" 2>&1 &
  SINGLE_PID=$!

  wait_ready "$BENCH_PORT" 100 \
    || die "节点在 20 秒内没有就绪，看看 ${BENCH_DIR}/single/node.log"

  log "参数：${REQUESTS} 次请求，${CLIENTS} 个并发客户端，流水线 ${PIPELINE}"
  bench "$BENCH_PORT" "单节点"

  log ""
  log "DBSIZE: $(redis-cli -p "$BENCH_PORT" DBSIZE)"
  log "节点日志：${BENCH_DIR}/single/node.log"
}

# ---------------------------------------------------------------- 三节点集群

# 取多数派共同认可的主节点编号。
#
# 与 cluster-up.sh 里那份同样的理由：刚换届时旧主节点的视图可能还没更新，
# 采信它会得到一个已经作废的答案。
cluster_leader() {
  local id seen answer="" agree
  for id in 1 2 3; do
    seen="$(redis-cli -p "$((7001 + id - 1))" --raw CLUSTER INFO 2>/dev/null \
      | sed -n 's/^xrldb_leader:\(.*\)$/\1/p' | tr -d '\r' || true)"
    [ -z "$seen" ] && continue
    [ "$seen" = "unknown" ] && continue
    if [ -z "$answer" ]; then
      answer="$seen"
      agree=1
    elif [ "$answer" = "$seen" ]; then
      agree=$((agree + 1))
    fi
  done

  [ "$agree" -ge 2 ] || die "没有获得多数派认可的主节点"
  echo "$answer"
}

run_cluster() {
  log "拉起三节点集群..."
  "$ROOT/scripts/cluster-up.sh" >/dev/null

  local leader follower
  leader="$(cluster_leader)"
  # 挑一个不是主节点的端口来量转发路径
  follower=1
  [ "$follower" = "$leader" ] && follower=2

  log "参数：${REQUESTS} 次请求，${CLIENTS} 个并发客户端，流水线 ${PIPELINE}"
  bench "$((7001 + leader - 1))" "主节点 ${leader}"
  bench "$((7001 + follower - 1))" "从节点 ${follower}（写会被转发，读走 ReadIndex）"

  log ""
  log "停止集群（保留数据）。清掉数据用：./scripts/cluster-down.sh --wipe"
  "$ROOT/scripts/cluster-down.sh" >/dev/null
}

# ----------------------------------------------------------------

log "构建 release 版本（bench 必须量发布版，debug 版没有意义）"
(cd "$ROOT" && cargo build --release --quiet)

if [ "$MODE" = "cluster" ]; then
  run_cluster
else
  run_single
fi

report_rejections
