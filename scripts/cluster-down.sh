#!/usr/bin/env bash
#
# 停止由 cluster-up.sh 拉起的集群。
#
# 用法：
#   ./scripts/cluster-down.sh          # 停止，保留数据
#   ./scripts/cluster-down.sh --wipe   # 停止并删除数据目录
#
# 默认用 SIGTERM，给节点一个干净退出的机会。要模拟崩溃请用 verify-cluster.sh，
# 它用的是 kill -9 —— 两者的语义完全不同，不能互相替代。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${XRLDB_CLUSTER_DIR:-$ROOT/target/cluster}"
NODES="${XRLDB_NODES:-3}"

WIPE=0
for arg in "$@"; do
  case "$arg" in
    --wipe) WIPE=1 ;;
    *) echo "错误：未知参数 $arg" >&2; exit 1 ;;
  esac
done

log() { printf '%s\n' "$*"; }

if [ ! -d "$WORK" ]; then
  log "$WORK 不存在，没有需要停止的东西。"
  exit 0
fi

stopped=0
pids=""

for id in $(seq 1 "$NODES"); do
  pidfile="$WORK/node-$id.pid"
  [ -f "$pidfile" ] || continue

  pid="$(cat "$pidfile")"

  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    log "已停止节点 ${id}（PID ${pid}）"
    pids="$pids $pid"
    stopped=1
  else
    log "节点 ${id}（PID ${pid}）已不在运行"
  fi

  rm -f "$pidfile"
done

# 等进程真正退出再删数据。直接删目录的话，redb 还持有文件句柄，删掉的是一个
# 正在被写入的目录——数据目录的内容会变得难以预测。
if [ -n "$pids" ]; then
  for _ in $(seq 1 50); do
    alive=0
    for pid in $pids; do
      if kill -0 "$pid" 2>/dev/null; then alive=1; fi
    done
    [ "$alive" = "0" ] && break
    sleep 0.1
  done

  for pid in $pids; do
    if kill -0 "$pid" 2>/dev/null; then
      log "PID $pid 未在 5 秒内退出，强制结束"
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
fi

if [ "$stopped" = "0" ]; then
  log "没有正在运行的节点。"
fi

if [ "$WIPE" = "1" ]; then
  rm -rf "$WORK"
  log "已删除 ${WORK}（含全部数据与日志）"
fi
