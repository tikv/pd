#!/usr/bin/env bash
set -euo pipefail

# Deterministic sysbench runner for benchmark-router-cluster.
# Resolves TiDB endpoint from topology yaml, then executes single or multi-round runs in strict order.

usage() {
  cat <<'EOF'
Usage:
  run_sysbench_controller.sh \
    --topology-yaml /path/to/topology.yaml \
    --workload oltp_read_write|oltp_point_select \
    [--mysql-user root] [--mysql-password xxx] [--mysql-db sbtest] \
    --time 300 --tables 100 --table-size 10000 \
    (--threads 32 | --thread-list 16,32,64,128) \
    [--prepare] [--wait-seconds 60] [--report-interval 30]

Notes:
- mysql host/port are auto-resolved from first tidb_servers entry in topology yaml.
EOF
}

TOPOLOGY_YAML=""
WORKLOAD="oltp_read_write"
MYSQL_HOST=""
MYSQL_PORT=""
MYSQL_USER="root"
MYSQL_PASSWORD=""
MYSQL_DB="sbtest"
TIME=""
TABLES=""
TABLE_SIZE=""
THREADS=""
THREAD_LIST=""
DO_PREPARE=0
WAIT_SECONDS=60
REPORT_INTERVAL=30

while [[ $# -gt 0 ]]; do
  case "$1" in
    --topology-yaml) TOPOLOGY_YAML="$2"; shift 2 ;;
    --workload) WORKLOAD="$2"; shift 2 ;;
    --mysql-user) MYSQL_USER="$2"; shift 2 ;;
    --mysql-password) MYSQL_PASSWORD="$2"; shift 2 ;;
    --mysql-db) MYSQL_DB="$2"; shift 2 ;;
    --time) TIME="$2"; shift 2 ;;
    --tables) TABLES="$2"; shift 2 ;;
    --table-size) TABLE_SIZE="$2"; shift 2 ;;
    --threads) THREADS="$2"; shift 2 ;;
    --thread-list) THREAD_LIST="$2"; shift 2 ;;
    --prepare) DO_PREPARE=1; shift 1 ;;
    --wait-seconds) WAIT_SECONDS="$2"; shift 2 ;;
    --report-interval) REPORT_INTERVAL="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

[[ -n "$TOPOLOGY_YAML" && -n "$TIME" && -n "$TABLES" && -n "$TABLE_SIZE" ]] || { usage; exit 1; }
[[ -n "$THREADS" || -n "$THREAD_LIST" ]] || { echo "Need --threads or --thread-list"; exit 1; }
[[ -f "$TOPOLOGY_YAML" ]] || { echo "Topology yaml not found: $TOPOLOGY_YAML"; exit 1; }

# Resolve first tidb_servers host/port from topology yaml (simple parser for expected TiUP format)
MYSQL_HOST="$(awk '
  /tidb_servers:/ {in=1; next}
  in && /^\S/ {in=0}
  in && /host:/ {print $2; exit}
' "$TOPOLOGY_YAML")"

MYSQL_PORT="$(awk '
  /tidb_servers:/ {in=1; next}
  in && /^\S/ {in=0}
  in && /port:/ {print $2; exit}
' "$TOPOLOGY_YAML")"

[[ -n "$MYSQL_HOST" && -n "$MYSQL_PORT" ]] || {
  echo "Failed to parse tidb host/port from topology: $TOPOLOGY_YAML"; exit 1;
}

echo "== TOPOLOGY RESOLVED tidb=${MYSQL_HOST}:${MYSQL_PORT} =="

BASE=(
  --db-driver=mysql
  --mysql-host="$MYSQL_HOST"
  --mysql-port="$MYSQL_PORT"
  --mysql-user="$MYSQL_USER"
  --mysql-db="$MYSQL_DB"
  --time="$TIME"
  --tables="$TABLES"
  --table-size="$TABLE_SIZE"
)

if [[ -n "$MYSQL_PASSWORD" ]]; then
  BASE+=(--mysql-password="$MYSQL_PASSWORD")
fi

run_once() {
  local t="$1"
  echo "== ROUND threads=${t} START $(date '+%F %T') =="
  sysbench "$WORKLOAD" "${BASE[@]}" --threads="$t" --report-interval="$REPORT_INTERVAL" run
  echo "== ROUND threads=${t} DONE $(date '+%F %T') =="
}

if [[ "$DO_PREPARE" -eq 1 ]]; then
  echo "== PREPARE START $(date '+%F %T') =="
  sysbench "$WORKLOAD" "${BASE[@]}" prepare
  echo "== PREPARE DONE $(date '+%F %T') =="
fi

if [[ -n "$THREADS" ]]; then
  run_once "$THREADS"
else
  IFS=',' read -r -a arr <<< "$THREAD_LIST"
  n=${#arr[@]}
  for ((i=0; i<n; i++)); do
    run_once "${arr[$i]}"
    if (( i < n-1 )); then
      echo "== WAIT ${WAIT_SECONDS}s =="
      sleep "$WAIT_SECONDS"
    fi
  done
fi

echo "== ALL ROUNDS DONE $(date '+%F %T') =="
