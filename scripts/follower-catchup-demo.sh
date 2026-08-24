#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NODES=(
  "node-1=http://localhost:8080"
  "node-2=http://localhost:8081"
  "node-3=http://localhost:8082"
  "node-4=http://localhost:8083"
  "node-5=http://localhost:8084"
)

DURATION_SECONDS=20
WRITERS=3
KEYSPACE=100
PARTITIONED_NODE=""
PARTITIONED_URL=""
LEADER_ID=""
LEADER_URL=""

usage() {
  cat <<EOF
Usage: ./scripts/follower-catchup-demo.sh [options]

Options:
  --duration SECONDS     How long to hammer while partitioned. Default: ${DURATION_SECONDS}
  --writers COUNT        Number of concurrent writers. Default: ${WRITERS}
  --keyspace COUNT       Number of hammer keys. Default: ${KEYSPACE}
  --node NODE_ID         Follower to partition. Default: first non-leader node.
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
    --node)
      PARTITIONED_NODE="$2"
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

url_for_node() {
  local target="$1"
  for node in "${NODES[@]}"; do
    if [[ "$(node_id "$node")" == "$target" ]]; then
      node_url "$node"
      return
    fi
  done
  return 1
}

cleanup() {
  if [[ -n "$LEADER_URL" && -n "$PARTITIONED_NODE" ]]; then
    request -X DELETE "${LEADER_URL}/faults/replication/${PARTITIONED_NODE}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

wait_for_node() {
  local node="$1"
  local id
  local url
  id="$(node_id "$node")"
  url="$(node_url "$node")"

  for _ in $(seq 1 30); do
    if request --fail "${url}/healthz" >/dev/null 2>&1; then
      printf "%s is healthy\n" "$id"
      return
    fi
    sleep 1
  done

  fail "${id} did not become healthy"
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
  if [[ -n "$PARTITIONED_NODE" ]]; then
    PARTITIONED_URL="$(url_for_node "$PARTITIONED_NODE")" || fail "unknown node ${PARTITIONED_NODE}"
    if [[ "$PARTITIONED_NODE" == "$LEADER_ID" ]]; then
      fail "cannot partition current leader ${PARTITIONED_NODE}; choose a follower"
    fi
    return
  fi

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

cd "$ROOT_DIR"

log "Ensuring five-node cluster is running"
docker compose up -d

log "Waiting for nodes"
for node in "${NODES[@]}"; do
  wait_for_node "$node"
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

log "Measuring lag while partitioned"
leader_index="$(last_log_index "$LEADER_URL")"
partitioned_index="$(last_log_index "$PARTITIONED_URL")"
leader_index="${leader_index:-0}"
partitioned_index="${partitioned_index:-0}"
lag=$((leader_index - partitioned_index))

printf "leader index: %s\n" "$leader_index"
printf "%s index while partitioned: %s\n" "$PARTITIONED_NODE" "$partitioned_index"
printf "lag while partitioned: %s entries\n" "$lag"

if [[ "$lag" -le 0 ]]; then
  fail "${PARTITIONED_NODE} did not lag while partitioned"
fi

log "Healing ${PARTITIONED_NODE}"
request --fail -X DELETE "${LEADER_URL}/faults/replication/${PARTITIONED_NODE}" >/dev/null
request --fail "${LEADER_URL}/faults"
PARTITIONED_NODE=""

log "Giving healed follower time to catch up"
sleep 10

log "Verifying convergence after follower catch-up"
"${ROOT_DIR}/scripts/verify-cluster.sh" --timeout 60

log "Follower partition and catch-up passed"
