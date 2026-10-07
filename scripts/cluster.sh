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

cmd_start() {
  local n=${1:-3} pids=()
  command -v go >/dev/null || { echo "нужен Go в PATH"; exit 1; }
  go build -o "$BIN" ./cmd/node

  for ((i = 0; i < n; i++)); do
    "$BIN" -id "$i" -http "127.0.0.1:$((BASE_PORT + i))" -peers "$(peers "$n")" \
      >"$DATA_DIR/node$i.log" 2>&1 &
    pids+=($!)
  done
  printf '%s\n' "${pids[@]}" >"$DATA_DIR/pids"

  echo "Запущено узлов: $n (порты $BASE_PORT..$((BASE_PORT + n - 1)))"
  echo "PID: ${pids[*]}"
  echo "Логи: $DATA_DIR/node*.log   Статус: ./scripts/cluster.sh status"
  sleep 2
  "$0" status || true
}

cmd_stop() {
  [[ -f "$DATA_DIR/pids" ]] || { echo "нет запущенного кластера"; return; }
  while read -r pid; do kill "$pid" 2>/dev/null || true; done <"$DATA_DIR/pids"
  rm -f "$DATA_DIR/pids"
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
  [[ -f "$DATA_DIR/pids" ]] || { echo "нет запущенного кластера"; exit 1; }
  n=$(wc -l <"$DATA_DIR/pids")
  [[ "$1" =~ ^[0-9]+$ ]] && (($1 < n)) || { echo "нет узла $1: в кластере узлы 0..$((n - 1))"; exit 1; }
}

cmd_start_node() {
  local id=$1 n pid
  check_node "$id"
  pid=$(sed -n "$((id + 1))p" "$DATA_DIR/pids")
  if kill -0 "$pid" 2>/dev/null; then echo "узел $id уже запущен (PID $pid)"; exit 1; fi

  "$BIN" -id "$id" -http "127.0.0.1:$((BASE_PORT + id))" -peers "$(peers "$n")" \
    >>"$DATA_DIR/node$id.log" 2>&1 &
  pid=$!
  awk -v l="$((id + 1))" -v p="$pid" 'NR == l { $0 = p } 1' "$DATA_DIR/pids" >"$DATA_DIR/pids.tmp"
  mv "$DATA_DIR/pids.tmp" "$DATA_DIR/pids"
  echo "Узел $id запущен (порт $((BASE_PORT + id)), PID $pid)"
}

cmd_stop_node() {
  local id=$1 n pid
  check_node "$id"
  pid=$(sed -n "$((id + 1))p" "$DATA_DIR/pids")
  if kill "$pid" 2>/dev/null; then echo "Узел $id остановлен"; else echo "узел $id не запущен"; fi
}

case "${1:-}" in
  start) shift; cmd_start "${1:-3}" ;;
  start-node) cmd_start_node "${2:-}" ;;
  stop-node) cmd_stop_node "${2:-}" ;;
  stop) cmd_stop ;;
  status) cmd_status ;;
  rm) cmd_rm ;;
  *) echo "Использование: $0 {start [N]|start-node ID|stop-node ID|status|stop|rm}"; exit 1 ;;
esac
