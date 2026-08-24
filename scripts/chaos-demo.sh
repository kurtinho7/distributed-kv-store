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

NODES=(
  "node-1=${NODE_1}"
  "node-2=${NODE_2}"
  "node-3=${NODE_3}"
  "node-4=${NODE_4}"
  "node-5=${NODE_5}"
)

PARTITIONED_NODE=""
PARTITIONED_URL=""
LEADER_ID=""
LEADER_URL=""
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

node_id() {
  printf "%s" "${1%%=*}"
}

node_url() {
  printf "%s" "${1#*=}"
}

cleanup() {
  if [[ -n "$LEADER_URL" && -n "$PARTITIONED_NODE" ]]; then
    request -X DELETE "${LEADER_URL}/faults/replication/${PARTITIONED_NODE}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

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

find_leader() {
  for node in "${NODES[@]}"; do
    local id
    local url
    local body
    id="$(node_id "$node")"
    url="$(node_url "$node")"

    if ! body="$(request --fail "${url}/raft" 2>/dev/null)"; then
      continue
    fi

    if printf "%s" "$body" | grep -q '"role":"leader"'; then
      LEADER_ID="$id"
      LEADER_URL="$url"
      return
    fi
  done

  fail "could not find a leader"
}

choose_partitioned_follower() {
  for node in "${NODES[@]}"; do
    local id
    id="$(node_id "$node")"
    if [[ "$id" != "$LEADER_ID" ]]; then
      PARTITIONED_NODE="$id"
      PARTITIONED_URL="$(node_url "$node")"
      return
    fi
  done

  fail "could not choose a follower to partition"
}

last_log_index() {
  local node_url="$1"
  local body
  body="$(request --fail "${node_url}/log")"
  printf "%s" "$body" | sed -n 's/.*"index":\([0-9][0-9]*\).*/\1/p' | tail -n 1
}

hammer_targets() {
  local targets=()
  for node in "${NODES[@]}"; do
    if [[ "$(node_id "$node")" == "$PARTITIONED_NODE" ]]; then
      continue
    fi
    targets+=("$(node_url "$node")")
  done

  local joined
  joined="$(IFS=,; printf "%s" "${targets[*]}")"
  printf "%s" "$joined"
}

compact_log() {
  local node_url="$1"
  request --fail "${node_url}/log" | tr -d '[:space:]'
}

wait_for_convergence() {
  for _ in $(seq 1 30); do
    local expected=""
    local converged=true

    for node in "${NODES[@]}"; do
      local compact
      compact="$(compact_log "$(node_url "$node")")"
      if [[ -z "$expected" ]]; then
        expected="$compact"
        continue
      fi

      if [[ "$compact" != "$expected" ]]; then
        converged=false
        break
      fi
    done

    if [[ "$converged" == true ]]; then
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
for node in "${NODES[@]}"; do
  wait_for_node "$(node_url "$node")" "$(node_id "$node")"
done

log "Finding current leader"
find_leader
printf "current leader: %s\n" "$LEADER_ID"

choose_partitioned_follower
printf "partitioned follower: %s\n" "$PARTITIONED_NODE"

log "Partitioning ${PARTITIONED_NODE} from leader replication and catch-up"
request --fail -X POST "${LEADER_URL}/faults/replication/${PARTITIONED_NODE}" >/dev/null
request --fail "${LEADER_URL}/faults"

log "Hammering traffic while ${PARTITIONED_NODE} is partitioned"
"${ROOT_DIR}/scripts/hammer.sh" \
  --duration "$DURATION_SECONDS" \
  --writers "$WRITERS" \
  --keyspace "$KEYSPACE" \
  --nodes "$(hammer_targets)"

log "Checking that majority moved ahead while ${PARTITIONED_NODE} lagged"
leader_index="$(last_log_index "$LEADER_URL")"
partitioned_index="$(last_log_index "$PARTITIONED_URL")"
leader_index="${leader_index:-0}"
partitioned_index="${partitioned_index:-0}"

for node in "${NODES[@]}"; do
  if [[ "$(node_id "$node")" == "$PARTITIONED_NODE" ]]; then
    continue
  fi

  index="$(last_log_index "$(node_url "$node")")"
  index="${index:-0}"
  printf "%s index: %s\n" "$(node_id "$node")" "$index"

  if [[ "$index" -eq 0 ]]; then
    fail "$(node_id "$node") did not accept traffic"
  fi
done
printf "%s index: %s\n" "$PARTITIONED_NODE" "$partitioned_index"

if [[ "$partitioned_index" -ge "$leader_index" ]]; then
  fail "${PARTITIONED_NODE} did not lag while partitioned"
fi

log "Healing ${PARTITIONED_NODE}"
request --fail -X DELETE "${LEADER_URL}/faults/replication/${PARTITIONED_NODE}" >/dev/null
request --fail "${LEADER_URL}/faults"
PARTITIONED_NODE=""

log "Waiting for catch-up"
wait_for_convergence

log "Verifying cluster correctness"
"${ROOT_DIR}/scripts/verify-cluster.sh" --timeout 60

log "Chaos demo passed"
