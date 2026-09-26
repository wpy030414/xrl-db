#!/usr/bin/env bash
#
# 端到端验收：把「它真的是个集群」这件事用真实的进程、真实的 kill -9 证明一遍。
#
# 与 tests/cluster.rs 的分工：
#
#   tests/cluster.rs     三个节点跑在同一个进程里，用 Node::shutdown「杀掉」主节点
#   verify-cluster.sh    三个真实的操作系统进程，用 kill -9 杀主节点
#
# 后者才是对「崩溃」的最终验证：SIGKILL 不给进程任何清理的机会，未落盘的东西
# 是真的没了。shutdown 走的是优雅路径，两者在持久化语义上不完全等价。
#
# 用法：./scripts/verify-cluster.sh
#
# 退出码 0 表示全部通过。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${XRLDB_CLUSTER_DIR:-$ROOT/target/cluster}"
BASE_PORT="${XRLDB_BASE_PORT:-7001}"
NODES=3
BIN="${XRLDB_BIN:-$ROOT/target/release/xrl-db}"

export XRLDB_CLUSTER_DIR="$WORK" XRLDB_BASE_PORT="$BASE_PORT" XRLDB_NODES="$NODES"

KEY_COUNT=100
LEADER_TIMEOUT=30

passed=0
failed=0

log() { printf '%s\n' "$*"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok() { passed=$((passed + 1)); printf '  \033[32m✓\033[0m %s\n' "$*"; }
bad() { failed=$((failed + 1)); printf '  \033[31m✗\033[0m %s\n' "$*" >&2; }
die() { printf '\n\033[31m中止：%s\033[0m\n' "$*" >&2; cleanup; exit 1; }

cleanup() {
  "$ROOT/scripts/cluster-down.sh" >/dev/null 2>&1 || true
}
trap cleanup EXIT

port_of() { echo $((BASE_PORT + $1 - 1)); }

pid_of() { cat "$WORK/node-$1.pid" 2>/dev/null || true; }

running() { kill -0 "$1" 2>/dev/null; }

# 限时执行 redis-cli。
#
# 挂住的请求正是本脚本要防的问题之一，所以不能直接调用 redis-cli——那会让脚本
# 本身也挂住，而且挂住时没有任何输出，看起来像是卡在了别的地方。
# 超时输出 __TIMEOUT__，调用方据此判断。
cli() {
  local limit="$1"; shift
  local out="$WORK/.cli.out"
  : >"$out"

  redis-cli "$@" >"$out" 2>&1 &
  local pid=$!
  local ticks=0 max_ticks=$((limit * 20)) # 每 tick 50 毫秒
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$ticks" -ge "$max_ticks" ]; then
      kill -9 "$pid" 2>/dev/null || true
      echo "__TIMEOUT__"
      return 124
    fi
    sleep 0.05
    ticks=$((ticks + 1))
  done
  wait "$pid" 2>/dev/null || true
  cat "$out"
}

leader_from() {
  redis-cli -p "$(port_of "$1")" --raw CLUSTER INFO 2>/dev/null \
    | sed -n 's/^xrldb_leader:\(.*\)$/\1/p' | tr -d '\r'
}

consensus_leader() {
  local id answer="" agree=0 quorum=$((NODES / 2 + 1))
  for id in $(seq 1 "$NODES"); do
    local seen
    seen="$(leader_from "$id")"
    [ -z "$seen" ] && continue
    [ "$seen" = "unknown" ] && continue
    if [ -z "$answer" ]; then
      answer="$seen"; agree=1
    elif [ "$answer" = "$seen" ]; then
      agree=$((agree + 1))
    fi
  done
  if [ -n "$answer" ] && [ "$agree" -ge "$quorum" ]; then
    # 候选者自己也必须认为自己是主节点。少了这一步，刚刚被 SIGKILL 掉的节点
    # 会被「一致地」选中：幸存者要过一小会儿才改口，而那段窗口里它们的旧视图
    # 完全一致，多数派规则照样成立。
    if [ "$(leader_from "$answer")" = "$answer" ]; then echo "$answer"; fi
  fi
}

wait_for_leader() {
  local limit="$1"
  local deadline=$((SECONDS + limit))
  local found=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    found="$(consensus_leader)"
    [ -n "$found" ] && { echo "$found"; return 0; }
    sleep 0.2
  done
  return 1
}

# ---------------------------------------------------------------- 主流程

step "0. 起一个干净的三节点集群"

"$ROOT/scripts/cluster-down.sh" --wipe >/dev/null 2>&1 || true
"$ROOT/scripts/cluster-up.sh" || die "集群没有起来"

LEADER="$(wait_for_leader "$LEADER_TIMEOUT")" || die "等不到主节点"
ok "集群就绪，主节点是 $LEADER"

FOLLOWER="$(for id in $(seq 1 "$NODES"); do [ "$id" != "$LEADER" ] && { echo "$id"; break; }; done)"
log "  主节点 ${LEADER}（端口 $(port_of "$LEADER")），从节点 ${FOLLOWER}（端口 $(port_of "$FOLLOWER")）"

step "1. 通过不同节点写入 $KEY_COUNT 个键"
log "  一半走主节点，一半走从节点——后者验证的是写转发"

half=$((KEY_COUNT / 2))
for i in $(seq 1 "$half"); do
  result="$(cli 10 -p "$(port_of "$LEADER")" SET "leader-$i" "value-$i")"
  [ "$result" = "OK" ] || die "向主节点写入 leader-$i 失败：$result"
done
ok "通过主节点写入 $half 个键"

for i in $(seq $((half + 1)) "$KEY_COUNT"); do
  result="$(cli 10 -p "$(port_of "$FOLLOWER")" SET "follower-$i" "value-$i")"
  [ "$result" = "OK" ] || die "向从节点写入 follower-$i 失败：$result"
done
ok "通过从节点写入 $((KEY_COUNT - half)) 个键（服务端已转发给主节点）"

step "2. kill -9 主节点 $LEADER"
LEADER_PID="$(pid_of "$LEADER")"
[ -n "$LEADER_PID" ] || die "读不到主节点的 PID"
kill -9 "$LEADER_PID"
sleep 0.5
if running "$LEADER_PID"; then die "kill -9 之后进程 $LEADER_PID 居然还活着"; fi
ok "主节点 ${LEADER}（PID ${LEADER_PID}）已被 SIGKILL 杀死，没有任何清理机会"

step "3. 等剩余节点选出新主节点"
NEW_LEADER="$(wait_for_leader "$LEADER_TIMEOUT")" || die "剩余节点没能在 ${LEADER_TIMEOUT}s 内选出新主节点"
[ "$NEW_LEADER" != "$LEADER" ] || die "新主节点竟然还是已经死掉的 $LEADER"
ok "新主节点是 ${NEW_LEADER}（原主节点 $LEADER 已不在）"

step "4. 确认 $KEY_COUNT 个键一个不丢"
# 从一个**幸存者**上读。挑一个不是新主节点的，顺带再走一遍转发路径。
READ_FROM="$(for id in $(seq 1 "$NODES"); do [ "$id" != "$LEADER" ] && [ "$id" != "$NEW_LEADER" ] && { echo "$id"; break; }; done)"
[ -n "$READ_FROM" ] || READ_FROM="$NEW_LEADER"
log "  从节点 $READ_FROM 读取"

missing=0
for i in $(seq 1 "$KEY_COUNT"); do
  if [ "$i" -le "$half" ]; then key="leader-$i"; else key="follower-$i"; fi
  got="$(cli 10 -p "$(port_of "$READ_FROM")" --raw GET "$key")"
  if [ "$got" != "value-$i" ]; then
    missing=$((missing + 1))
    [ "$missing" -le 5 ] && log "    缺失：${key}（读到：${got:-<空>}）"
  fi
done

if [ "$missing" -eq 0 ]; then
  ok "故障转移后 $KEY_COUNT 个键全部可读，一个不丢 —— 这正是相对 Redis Cluster 的核心优势"
else
  bad "故障转移后有 $missing 个键丢失。已经向客户端确认过的写入丢失了。"
fi

step "5. 重启被杀死的主节点 ${LEADER}，确认它能重新加入并追平"
( cd "$ROOT" && exec "$BIN" --config "$WORK/node-$LEADER.toml" ) >>"$WORK/node-$LEADER.log" 2>&1 &
echo $! >"$WORK/node-$LEADER.pid"

# 移出作业表：后面第 6 步会用 kill -9 再杀它一次，届时 bash 会打印一行
# 「Killed: 9」——那正是本脚本要验证的行为，出现在输出里只会像是脚本出错了。
disown "$(cat "$WORK/node-$LEADER.pid")" 2>/dev/null || true
sleep 3

rejoined=0
for _ in $(seq 1 60); do
  state="$(redis-cli -p "$(port_of "$LEADER")" --raw RAFT INFO 2>/dev/null | sed -n '/^state$/{n;p;}' | tr -d '\r')"
  if [ "$state" = "follower" ] || [ "$state" = "leader" ]; then rejoined=1; break; fi
  sleep 0.5
done

if [ "$rejoined" = "1" ]; then
  ok "节点 $LEADER 已重新加入集群，当前角色 $state"
else
  bad "节点 $LEADER 重启后未能重新加入集群"
fi

# 让它证明自己真的追平了：写一个新键，再从它身上读出来。
# 读路径会向主节点要一个读索引并等本地状态机追平，读得到就说明数据确实到了。
FRESH="fresh-after-rejoin"
result="$(cli 10 -p "$(port_of "$NEW_LEADER")" SET "$FRESH" "v")"
if [ "$result" = "OK" ]; then
  got="$(cli 10 -p "$(port_of "$LEADER")" --raw GET "$FRESH")"
  if [ "$got" = "v" ]; then
    ok "节点 $LEADER 能读到重启后新写入的数据 —— 它确实追平了"
  else
    bad "节点 $LEADER 读不到重启后写入的数据（读到：${got:-<空>}）"
  fi
else
  bad "向新主节点 $NEW_LEADER 写入失败：$result"
fi

step "6. 只留主节点一个，确认它拒绝写入（CP 语义）"
# 刻意让**主节点**成为唯一的幸存者：它失去了多数派，却仍以为自己是主节点。
# 这正是一开始那个「写入永远挂住、客户端拿不到任何回复」的场景——把从节点留下
# 来测是测不到的，从节点会立刻回一句「没有主节点」。
CURRENT_LEADER="$(wait_for_leader "$LEADER_TIMEOUT")" || die "步骤 6 前找不到主节点"
SURVIVOR="$CURRENT_LEADER"
log "  幸存者选定为当前主节点 $SURVIVOR"

for id in $(seq 1 "$NODES"); do
  [ "$id" = "$SURVIVOR" ] && continue
  kill -9 "$(pid_of "$id")" 2>/dev/null || true
  log "  已杀死节点 $id"
done
sleep 3

log "  现在只剩节点 ${SURVIVOR}，它的角色是 $(redis-cli -p "$(port_of "$SURVIVOR")" --raw RAFT INFO 2>/dev/null | sed -n '/^state$/{n;p;}' | tr -d '\r')"

accepted=0
hung=0
for attempt in 1 2 3; do
  result="$(cli 10 -p "$(port_of "$SURVIVOR")" SET "must-fail-$attempt" x)"
  case "$result" in
    OK) accepted=$((accepted + 1)) ;;
    __TIMEOUT__) hung=$((hung + 1)) ;;
    *) : ;;
  esac
done

if [ "$accepted" -eq 0 ] && [ "$hung" -eq 0 ]; then
  ok "失去多数派后三次写入全部被拒绝，且都在限时内返回"
elif [ "$hung" -gt 0 ]; then
  bad "有 $hung 次写入挂住了没有返回。请求挂起比返回错误糟糕得多：调用方无从判断该不该重试。"
else
  bad "失去多数派后竟然有 $accepted 次写入成功——这是脑裂"
fi

read_result="$(cli 10 -p "$(port_of "$SURVIVOR")" --raw GET leader-1)"
case "$read_result" in
  value-1) bad "失去多数派后读取返回了数据。无法确认时效性的读等于返回可能过期的值。" ;;
  __TIMEOUT__) bad "失去多数派后读取挂住了没有任何回复" ;;
  *) ok "失去多数派后读取也被拒绝：${read_result:0:60}" ;;
esac

# ---------------------------------------------------------------- 汇总

step "结果"
log "  通过 $passed 项，失败 $failed 项"

if [ "$failed" -eq 0 ]; then
  log ""
  log "全部通过。数据在 kill -9 之后一个不丢，失去多数派时拒绝服务。"
  exit 0
fi

log ""
log "有 $failed 项未通过。各节点日志在 $WORK/ 下。"
exit 1
