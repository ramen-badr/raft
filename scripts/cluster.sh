#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."
BASE_PORT=${BASE_PORT:-8001}
DATA_DIR=".cluster"
BIN="bin/node"

mkdir -p "$DATA_DIR"

peers() {
  local n=$1 out=""
  for ((i = 0; i < n; i++)); do
    out+="${i}=127.0.0.1:$((BASE_PORT + i)),"
  done
  echo "${out%,}"
}

launch() {
  local id=$1 n=$2 inner cmd
  inner=$(printf 'echo $$ > %q; exec %q -id %d -http 127.0.0.1:%d -peers %q' \
    "$DATA_DIR/node$id.pid" "$BIN" "$id" "$((BASE_PORT + id))" "$(peers "$n")")
  cmd=$(printf 'cd %q && sh -c %q' "$PWD" "$inner")
  cmd=${cmd//\\/\\\\}
  cmd=${cmd//\"/\\\"}
  osascript >/dev/null <<EOF
tell application "Terminal"
  set t to do script "$cmd"
  set custom title of t to "raft: узел $id"
  activate
end tell
EOF
}

stop_node() {
  local id=$1 pid
  pid=$(cat "$DATA_DIR/node$id.pid" 2>/dev/null || true)
  rm -f "$DATA_DIR/node$id.pid"
  [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || return 1
}

cmd_start() {
  local n=${1:-3} cluster=${2:-$n}
  command -v go >/dev/null || { echo "нужен Go в PATH"; exit 1; }
  go build -o "$BIN" ./cmd/node

  echo "$cluster" >"$DATA_DIR/size"
  for ((i = 0; i < n; i++)); do
    launch "$i" "$cluster"
  done

  echo "Запущено узлов: $n (id 0..$((n - 1))), кластер из $cluster, порты $BASE_PORT..$((BASE_PORT + cluster - 1)), у каждого свое окно Terminal"
  sleep 2
  "$0" status || true
}

cmd_stop() {
  local f id
  for f in "$DATA_DIR"/node*.pid; do
    [[ -f "$f" ]] || continue
    id=${f##*/node}
    stop_node "${id%.pid}" || true
  done
  echo "Кластер остановлен"
}

cmd_status() {
  for ((i = 0; i < 64; i++)); do
    local port=$((BASE_PORT + i))
    local out
    out=$(curl -s --max-time 0.5 "http://127.0.0.1:$port/status") || continue
    echo "$out" | jq -r '"узел \(.id): \(.state), term=\(.term), лидер=\(.leaderId), живые пиры=\(.alivePeers)"'
  done
}

cmd_rm() { cmd_stop; rm -rf "$DATA_DIR"; echo "Каталог $DATA_DIR очищен"; }

check_node() {
  [[ -f "$DATA_DIR/size" ]] || { echo "нет запущенного кластера"; exit 1; }
  local cluster
  read -r cluster <"$DATA_DIR/size"
  [[ "$1" =~ ^[0-9]+$ ]] && (($1 < cluster)) ||
    { echo "нет узла $1: в кластере из $cluster узлы 0..$((cluster - 1))"; exit 1; }
}

node_pid() { cat "$DATA_DIR/node$1.pid" 2>/dev/null || true; }

cmd_start_node() {
  local id=$1 pid cluster
  check_node "$id"
  read -r cluster <"$DATA_DIR/size"
  pid=$(node_pid "$id")
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then echo "узел $id уже запущен (PID $pid)"; exit 1; fi

  launch "$id" "$cluster"
  echo "Узел $id запущен в новом окне (порт $((BASE_PORT + id)))"
}

cmd_stop_node() {
  local id=$1
  check_node "$id"
  if stop_node "$id"; then echo "Узел $id остановлен"; else echo "узел $id не запущен"; fi
}

case "${1:-}" in
  start) shift; cmd_start "${1:-3}" "${2:-}" ;;
  start-node) cmd_start_node "${2:-}" ;;
  stop-node) cmd_stop_node "${2:-}" ;;
  stop) cmd_stop ;;
  status) cmd_status ;;
  rm) cmd_rm ;;
  *) echo "Использование: $0 {start [N] [CLUSTER]|start-node ID|stop-node ID|status|stop|rm}"; exit 1 ;;
esac
