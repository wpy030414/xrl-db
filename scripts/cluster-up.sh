#!/usr/bin/env bash
#
# 在本地拉起一个集群。
#
# 用法：
#   ./scripts/cluster-up.sh                     # 3 节点，端口 7001~7003
#   XRLDB_NODES=5 ./scripts/cluster-up.sh       # 5 节点
#   XRLDB_BASE_PORT=8000 ./scripts/cluster-up.sh
#
# 脚本在集群真正用起来（多数派认可同一个主节点）之后才返回，因此可以直接写进
# CI 或者串在别的命令后面，不必自己 sleep。
#
# 节点在后台运行，日志与 PID 都存在 target/cluster/ 下。停止用 cluster-down.sh。
#
# 这里刻意用固定端口而非随机端口：集群配置要求每个节点知道其他所有节点的地址，
# 端口写死了，生成的配置、日志里的信息、以及你手动敲的 redis-cli 命令才能对得上。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

WORK="${XRLDB_CLUSTER_DIR:-$ROOT/target/cluster}"
BASE_PORT="${XRLDB_BASE_PORT:-7001}"
NODES="${XRLDB_NODES:-3}"
BOOTSTRAP_NODE="${XRLDB_BOOTSTRAP_NODE:-1}"
ELECTION_TIMEOUT_MS="${XRLDB_ELECTION_TIMEOUT_MS:-300}"
HEARTBEAT_INTERVAL_MS="${XRLDB_HEARTBEAT_INTERVAL_MS:-100}"
BIN="${XRLDB_BIN:-$ROOT/target/release/xrl-db}"

# 等待主节点的上限（秒）。给得宽松：首次启动要编译、要建目录，慢一点很正常。
LEADER_TIMEOUT="${XRLDB_LEADER_TIMEOUT:-30}"

log() { printf '%s\n' "$*"; }
die() { printf '错误：%s\n' "$*" >&2; exit 1; }

port_of() { echo $((BASE_PORT + $1 - 1)); }

# 读取某个节点眼中主节点的编号；节点连不上或尚无主节点时输出空串。
leader_from() {
  redis-cli -p "$(port_of "$1")" --raw CLUSTER INFO 2>/dev/null \
    | sed -n 's/^xrldb_leader:\(.*\)$/\1/p' \
    | tr -d '\r'
}

# 取得多数派共同认可的主节点编号。
#
# 必须要求多数派，而不是「随便问一个节点」：刚换届时旧主节点自己的视图可能还没
# 更新，采信它会得到一个已经作废的答案。
consensus_leader() {
  local id answer="" agree=0 quorum=$((NODES / 2 + 1))
  for id in $(seq 1 "$NODES"); do
    local seen
    seen="$(leader_from "$id")"
    [ -z "$seen" ] && continue          # 连不上就跳过
    [ "$seen" = "unknown" ] && continue # 已经答复，但还没有主节点

    if [ -z "$answer" ]; then
      answer="$seen"
      agree=1
    elif [ "$answer" = "$seen" ]; then
      agree=$((agree + 1))
    fi
  done

  if [ -n "$answer" ] && [ "$agree" -ge "$quorum" ]; then
    # 候选者自己也必须认为自己是主节点。
    #
    # 少了这一步，**刚刚被 SIGKILL 掉的主节点会被一致地选中**：幸存者要过一小会儿
    # 才会改口，而那段窗口里它们的旧视图完全一致——多数派规则照样成立。
    # 「大家都说你是」和「你自己也说你是」是两回事，两个条件都要。
    if [ "$(leader_from "$answer")" = "$answer" ]; then
      echo "$answer"
    fi
  fi
}

wait_for_leader() {
  local limit="$1"
  local deadline=$((SECONDS + limit))
  local found=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    found="$(consensus_leader)"
    if [ -n "$found" ]; then
      echo "$found"
      return 0
    fi
    sleep 0.2
  done
  return 1
}

write_config() {
  local id="$1" cfg="$WORK/node-$id.toml" peer

  {
    cat <<EOF
# 由 scripts/cluster-up.sh 生成，请勿手工修改——下次启动会被覆盖。
#
# 想调整集群形态，改脚本顶部的环境变量；想固化一份自己的配置，
# 把它复制出去再用 --config 指过去。

[node]
id = $id
listen = "127.0.0.1:$(port_of "$id")"

[cluster]
enabled = true
peers = [
EOF
    for peer in $(seq 1 "$NODES"); do
      echo "  { id = $peer, addr = \"127.0.0.1:$(port_of "$peer")\" },"
    done
    cat <<EOF
]

[storage]
path = "$WORK/data-$id"

[raft]
election_timeout_ms = $ELECTION_TIMEOUT_MS
heartbeat_interval_ms = $HEARTBEAT_INTERVAL_MS
EOF
  } >"$cfg"
}

start_node() {
  local id="$1" cfg="$WORK/node-$id.toml"
  local log="$WORK/node-$id.log" pidfile="$WORK/node-$id.pid"

  # 节点 1 负责组建集群：它会把自己初始化成单节点集群，再把其余节点逐个纳入。
  # 其余节点只启动、不引导——若它们各自自举，就会变成三个互不相干的集群，
  # 而且永远不会合并。
  if [ "$id" = "$BOOTSTRAP_NODE" ]; then
    ( cd "$ROOT" && exec "$BIN" --config "$cfg" --bootstrap ) >"$log" 2>&1 &
  else
    ( cd "$ROOT" && exec "$BIN" --config "$cfg" ) >"$log" 2>&1 &
  fi

  echo $! >"$pidfile"

  # 把后台进程移出作业表。否则 bash 会在它被杀掉时打印一行「Killed: 9」——
  # 那正是我们期望发生的事，出现在输出里只会像是脚本出了错。
  disown "$(cat "$pidfile")" 2>/dev/null || true
}

already_running() {
  local id pidfile
  for id in $(seq 1 "$NODES"); do
    pidfile="$WORK/node-$id.pid"
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
      return 0
    fi
  done
  return 1
}

# ------------------------------------------------------------------ 主流程

command -v redis-cli >/dev/null 2>&1 \
  || die "需要 redis-cli 来确认集群状态。它同时也证明了「官方客户端零改造可用」这条承诺。"

if [ ! -x "$BIN" ]; then
  log "未找到 ${BIN}，先构建……"
  ( cd "$ROOT" && cargo build --release )
fi

mkdir -p "$WORK"

if already_running; then
  die "$WORK 下已有正在运行的节点。先执行 ./scripts/cluster-down.sh，或换个目录（XRLDB_CLUSTER_DIR=...）。"
fi

# 清掉上一次运行残留的 PID 文件，避免 cluster-down.sh 误伤无关进程
rm -f "$WORK"/*.pid

log "在 $WORK 下生成 $NODES 个节点的配置（端口 $(port_of 1)~$(port_of "$NODES")）"
for id in $(seq 1 "$NODES"); do
  write_config "$id"
done

log "启动节点……"
for id in $(seq 1 "$NODES"); do
  start_node "$id"
  log "  节点 $id  PID $(cat "$WORK/node-$id.pid")  日志 $WORK/node-$id.log"
done

log "等待集群选出主节点……"
if ! LEADER="$(wait_for_leader "$LEADER_TIMEOUT")"; then
  log ""
  log "超时：${LEADER_TIMEOUT}s 内没有形成多数派认可的主节点。各节点日志尾部："
  for id in $(seq 1 "$NODES"); do
    log "--- 节点 $id ---"
    tail -n 15 "$WORK/node-$id.log" 2>/dev/null || true
  done
  exit 1
fi

log ""
log "集群就绪：主节点是 ${LEADER}，成员 $(redis-cli -p "$(port_of "$LEADER")" --raw RAFT INFO | sed -n '/^voters$/{n;p;}')"
log ""
log "试试看（连任意一个节点都可以）："
log "  redis-cli -p $(port_of 1) SET foo bar"
log "  redis-cli -p $(port_of 2) GET foo"
log ""
log "停止：./scripts/cluster-down.sh        连数据一起清掉：./scripts/cluster-down.sh --wipe"
