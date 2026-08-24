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

if [[ -n "${KV_NODE_URLS:-}" ]]; then
  IFS=',' read -r -a node_urls <<<"$KV_NODE_URLS"
  NODES=()
  for index in "${!node_urls[@]}"; do
    NODES+=("node-$((index + 1))=${node_urls[$index]}")
  done
fi

DURATION_SECONDS=30
WRITERS=10
KEYSPACE=100
FAIL_AFTER_SECONDS=5
KILLED_LEADER=""

read -r -a COMPOSE_SERVICES <<<"${KV_COMPOSE_SERVICES:-node-1 node-2 node-3 node-4 node-5}"

usage() {
  cat <<EOF
Usage: ./scripts/leader-failover-demo.sh [options]

Options:
  --duration SECONDS       How long to hammer the cluster. Default: ${DURATION_SECONDS}
  --writers COUNT          Number of concurrent writers. Default: ${WRITERS}
  --keyspace COUNT         Number of hammer keys. Default: ${KEYSPACE}
  --fail-after SECONDS     When to stop the current leader. Default: ${FAIL_AFTER_SECONDS}
  --help                   Show this help.
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
    --fail-after)
      FAIL_AFTER_SECONDS="$2"
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
  if [[ -n "$KILLED_LEADER" ]]; then
    docker compose start "$KILLED_LEADER" >/dev/null 2>&1 || true
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
      printf "%s" "$id"
      return
    fi
  done

  return 1
}

wait_for_leader() {
  local previous_leader="${1:-}"

  for _ in $(seq 1 30); do
    local leader
    if leader="$(find_leader)" && [[ -n "$leader" && "$leader" != "$previous_leader" ]]; then
      printf "%s" "$leader"
      return
    fi
    sleep 1
  done

  return 1
}

cd "$ROOT_DIR"

log "Ensuring five-node cluster is running"
docker compose up -d "${COMPOSE_SERVICES[@]}"

log "Waiting for nodes"
for node in "${NODES[@]}"; do
  wait_for_node "$node"
done

log "Finding current leader"
leader="$(wait_for_leader "")" || fail "could not find a leader"
printf "current leader: %s\n" "$leader"

log "Hammering traffic for ${DURATION_SECONDS}s"
hammer_output="$(mktemp)"
"${ROOT_DIR}/scripts/hammer.sh" \
  --duration "$DURATION_SECONDS" \
  --writers "$WRITERS" \
  --keyspace "$KEYSPACE" >"$hammer_output" 2>&1 &
hammer_pid="$!"

sleep "$FAIL_AFTER_SECONDS"

log "Stopping leader ${leader} while traffic is running"
KILLED_LEADER="$leader"
docker compose stop "$leader"

log "Waiting for a new leader"
new_leader="$(wait_for_leader "$leader")" || fail "new leader was not elected"
printf "new leader: %s\n" "$new_leader"

log "Waiting for hammer to finish"
if ! wait "$hammer_pid"; then
  cat "$hammer_output"
  fail "hammer did not complete successfully"
fi
cat "$hammer_output"
rm -f "$hammer_output"

log "Restarting old leader ${leader}"
docker compose start "$leader"
KILLED_LEADER=""

for node in "${NODES[@]}"; do
  wait_for_node "$node"
done

log "Giving restarted leader time to catch up"
sleep 10

log "Verifying convergence after failover"
"${ROOT_DIR}/scripts/verify-cluster.sh" --timeout 120

log "Leader failover under load passed"
