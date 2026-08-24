#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NODE_1="http://localhost:8080"
NODE_2="http://localhost:8081"
NODE_3="http://localhost:8082"
NODE_4="http://localhost:8083"
NODE_5="http://localhost:8084"

if [[ -n "${KV_NODE_URLS:-}" ]]; then
  IFS=',' read -r -a node_urls <<<"$KV_NODE_URLS"
  NODE_1="${node_urls[0]}"
  NODE_2="${node_urls[1]}"
  NODE_3="${node_urls[2]}"
  NODE_4="${node_urls[3]}"
  NODE_5="${node_urls[4]}"
fi

PARTITIONED_NODE="node-5"
PARTITIONED_URL="$NODE_5"
DURATION_SECONDS=15
WRITERS=8
KEYSPACE=50
read -r -a COMPOSE_SERVICES <<<"${KV_COMPOSE_SERVICES:-node-1 node-2 node-3 node-4 node-5}"

usage() {
  cat <<EOF
Usage: ./scripts/chaos-demo.sh [options]

Options:
  --duration SECONDS     How long to hammer while partitioned. Default: ${DURATION_SECONDS}
  --writers COUNT        Number of concurrent writers. Default: ${WRITERS}
  --keyspace COUNT       Number of hammer keys. Default: ${KEYSPACE}
  --help                 Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration)
      DURATION_SECONDS="$2"
      shift 2
      ;;
    --writers)
      WRITERS="$2"
      shift 2
      ;;
    --keyspace)
      KEYSPACE="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      printf "Unknown option: %s\n\n" "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

fail() {
  printf "\nERROR: %s\n" "$1" >&2
  exit 1
}

log() {
  printf "\n==> %s\n" "$1"
}

request() {
  curl --silent --show-error "$@"
}

wait_for_node() {
  local node_url="$1"
  local name="$2"

  for _ in $(seq 1 30); do
    if request --fail "${node_url}/healthz" >/dev/null 2>&1; then
      printf "%s is healthy\n" "$name"
      return
    fi
    sleep 1
  done

  fail "${name} did not become healthy"
}

last_log_index() {
  local node_url="$1"
  local body
  body="$(request --fail "${node_url}/log")"
  printf "%s" "$body" | sed -n 's/.*"index":\([0-9][0-9]*\).*/\1/p' | tail -n 1
}

compact_log() {
  local node_url="$1"
  request --fail "${node_url}/log" | tr -d '[:space:]'
}

wait_for_convergence() {
  for _ in $(seq 1 30); do
    if [[ "$(compact_log "$NODE_1")" == "$(compact_log "$NODE_2")" ]] &&
      [[ "$(compact_log "$NODE_1")" == "$(compact_log "$NODE_3")" ]] &&
      [[ "$(compact_log "$NODE_1")" == "$(compact_log "$NODE_4")" ]] &&
      [[ "$(compact_log "$NODE_1")" == "$(compact_log "$NODE_5")" ]]; then
      printf "all node logs converged\n"
      return
    fi
    sleep 1
  done

  fail "logs did not converge"
}

cd "$ROOT_DIR"

log "Starting a clean five-node cluster"
docker compose stop "${COMPOSE_SERVICES[@]}" >/dev/null 2>&1 || true
docker compose rm -f -v "${COMPOSE_SERVICES[@]}" >/dev/null 2>&1 || true
docker compose up --build -d "${COMPOSE_SERVICES[@]}"

log "Waiting for nodes"
wait_for_node "$NODE_1" "node-1"
wait_for_node "$NODE_2" "node-2"
wait_for_node "$NODE_3" "node-3"
wait_for_node "$NODE_4" "node-4"
wait_for_node "$NODE_5" "node-5"

log "Partitioning ${PARTITIONED_NODE} from leader replication and catch-up"
request --fail -X POST "${NODE_1}/faults/replication/${PARTITIONED_NODE}" >/dev/null
request --fail "${NODE_1}/faults"

log "Hammering traffic while ${PARTITIONED_NODE} is partitioned"
"${ROOT_DIR}/scripts/hammer.sh" \
  --duration "$DURATION_SECONDS" \
  --writers "$WRITERS" \
  --keyspace "$KEYSPACE" \
  --nodes "${NODE_1},${NODE_2},${NODE_3},${NODE_4}"

log "Checking that majority moved ahead while ${PARTITIONED_NODE} lagged"
node_1_index="$(last_log_index "$NODE_1")"
node_2_index="$(last_log_index "$NODE_2")"
node_3_index="$(last_log_index "$NODE_3")"
node_4_index="$(last_log_index "$NODE_4")"
partitioned_index="$(last_log_index "$PARTITIONED_URL")"
node_1_index="${node_1_index:-0}"
node_2_index="${node_2_index:-0}"
node_3_index="${node_3_index:-0}"
node_4_index="${node_4_index:-0}"
partitioned_index="${partitioned_index:-0}"

printf "node-1 index: %s\n" "$node_1_index"
printf "node-2 index: %s\n" "$node_2_index"
printf "node-3 index: %s\n" "$node_3_index"
printf "node-4 index: %s\n" "$node_4_index"
printf "%s index: %s\n" "$PARTITIONED_NODE" "$partitioned_index"

if [[ "$node_1_index" -eq 0 || "$node_2_index" -eq 0 || "$node_3_index" -eq 0 || "$node_4_index" -eq 0 ]]; then
  fail "majority nodes did not accept traffic"
fi

if [[ "$partitioned_index" -ge "$node_1_index" ]]; then
  fail "${PARTITIONED_NODE} did not lag while partitioned"
fi

log "Healing ${PARTITIONED_NODE}"
request --fail -X DELETE "${NODE_1}/faults/replication/${PARTITIONED_NODE}" >/dev/null
request --fail "${NODE_1}/faults"

log "Waiting for catch-up"
wait_for_convergence

log "Verifying cluster correctness"
"${ROOT_DIR}/scripts/verify-cluster.sh" --timeout 60

log "Chaos demo passed"
