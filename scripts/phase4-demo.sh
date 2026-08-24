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

KEY="phase4-demo"
VALUE="heals-after-partition"
read -r -a COMPOSE_SERVICES <<<"${KV_COMPOSE_SERVICES:-node-1 node-2 node-3 node-4 node-5}"

log() {
  printf "\n==> %s\n" "$1"
}

fail() {
  printf "\nERROR: %s\n" "$1" >&2
  exit 1
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

assert_contains() {
  local actual="$1"
  local expected="$2"
  local message="$3"

  if [[ "$actual" != *"$expected"* ]]; then
    printf "Actual response:\n%s\n" "$actual" >&2
    fail "$message"
  fi
}

assert_key_value() {
  local node_url="$1"
  local name="$2"
  local key="$3"
  local value="$4"

  local body
  body="$(request --fail "${node_url}/kv/${key}")"
  assert_contains "$body" "\"value\":\"${value}\"" "${name} should contain ${key}=${value}"
  printf "%s has %s=%s\n" "$name" "$key" "$value"
}

assert_key_missing() {
  local node_url="$1"
  local name="$2"
  local key="$3"

  local body
  body="$(request "${node_url}/kv/${key}")"
  assert_contains "$body" "key not found" "${name} should not contain ${key}"
  printf "%s is missing %s as expected\n" "$name" "$key"
}

wait_for_key() {
  local node_url="$1"
  local name="$2"
  local key="$3"
  local value="$4"

  for _ in $(seq 1 15); do
    local body
    body="$(request "${node_url}/kv/${key}")"
    if [[ "$body" == *"\"value\":\"${value}\""* ]]; then
      printf "%s caught up with %s=%s\n" "$name" "$key" "$value"
      return
    fi
    sleep 1
  done

  fail "${name} did not catch up with ${key}"
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

log "Partitioning node-3 from leader replication and catch-up"
request --fail -X POST "${NODE_1}/faults/replication/node-3"
request --fail "${NODE_1}/faults"

log "Writing while node-3 is partitioned"
request --fail -X PUT "${NODE_1}/kv/${KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"value\":\"${VALUE}\"}"

log "Verifying majority committed and partitioned node missed the write"
assert_key_value "$NODE_1" "node-1" "$KEY" "$VALUE"
assert_key_value "$NODE_2" "node-2" "$KEY" "$VALUE"
assert_key_value "$NODE_4" "node-4" "$KEY" "$VALUE"
assert_key_value "$NODE_5" "node-5" "$KEY" "$VALUE"
assert_key_missing "$NODE_3" "node-3" "$KEY"

log "Healing node-3"
request --fail -X DELETE "${NODE_1}/faults/replication/node-3"
request --fail "${NODE_1}/faults"

log "Waiting for periodic catch-up"
wait_for_key "$NODE_3" "node-3" "$KEY" "$VALUE"

log "Verifying logs converged"
request --fail "${NODE_1}/log"
request --fail "${NODE_2}/log"
request --fail "${NODE_3}/log"

log "Phase 4 demo passed"
