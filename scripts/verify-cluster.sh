#!/usr/bin/env bash
set -euo pipefail

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

TIMEOUT_SECONDS=20

usage() {
  cat <<EOF
Usage: ./scripts/verify-cluster.sh [options]

Options:
  --timeout SECONDS      How long to wait for convergence. Default: ${TIMEOUT_SECONDS}
  --help                 Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout)
      TIMEOUT_SECONDS="$2"
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

if ! [[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [[ "$TIMEOUT_SECONDS" -lt 1 ]]; then
  printf "timeout must be a positive integer\n" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

fail() {
  printf "\nERROR: %s\n" "$1" >&2
  exit 1
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

fetch_snapshot() {
  local node="$1"
  local id
  local url
  id="$(node_id "$node")"
  url="$(node_url "$node")"

  if ! request --fail "${url}/healthz" >/dev/null 2>&1; then
    return 1
  fi

  request --fail "${url}/raft" >"${TMP_DIR}/${id}.raft.json"
  request --fail "${url}/log" >"${TMP_DIR}/${id}.log.json"
  request --fail "${url}/cluster" >"${TMP_DIR}/${id}.cluster.json"
}

log_indexes() {
  for node in "${NODES[@]}"; do
    local id
    id="$(node_id "$node")"
    if [[ -f "${TMP_DIR}/${id}.log.json" ]]; then
      local last_index
      last_index="$(sed -n 's/.*"index":\([0-9][0-9]*\).*/\1/p' "${TMP_DIR}/${id}.log.json" | tail -n 1)"
      printf "%s=%s\n" "$id" "${last_index:-0}"
    fi
  done
}

logs_converged() {
  local expected=""

  for node in "${NODES[@]}"; do
    local id
    id="$(node_id "$node")"
    local file="${TMP_DIR}/${id}.log.json"
    if [[ ! -f "$file" ]]; then
      continue
    fi

    local compact
    compact="$(tr -d '[:space:]' <"$file")"
    if [[ -z "$expected" ]]; then
      expected="$compact"
      continue
    fi

    if [[ "$compact" != "$expected" ]]; then
      return 1
    fi
  done

  return 0
}

leader_count() {
  local count=0

  for node in "${NODES[@]}"; do
    local id
    id="$(node_id "$node")"
    local file="${TMP_DIR}/${id}.raft.json"
    if [[ ! -f "$file" ]]; then
      continue
    fi

    if grep -q '"role":"leader"' "$file"; then
      count=$((count + 1))
    fi
  done

  printf "%s" "$count"
}

assert_no_log_conflicts() {
  local seen_file="${TMP_DIR}/seen-indexes.txt"
  : >"$seen_file"

  for node in "${NODES[@]}"; do
    local id
    id="$(node_id "$node")"
    local file="${TMP_DIR}/${id}.log.json"
    if [[ ! -f "$file" ]]; then
      continue
    fi

    tr '{' '\n' <"$file" |
      sed -n 's/.*"index":\([0-9][0-9]*\).*"operation":"\([^"]*\)".*"key":"\([^"]*\)".*/\1|\2|\3/p' |
      while IFS= read -r signature; do
        local index="${signature%%|*}"
        local existing
        existing="$(grep "^${index}|" "$seen_file" || true)"

        if [[ -n "$existing" && "$existing" != "$signature" ]]; then
          printf "Existing: %s\nNew:      %s\n" "$existing" "$signature" >&2
          fail "conflicting log entries at index ${index}"
        fi

        if [[ -z "$existing" ]]; then
          printf "%s\n" "$signature" >>"$seen_file"
        fi
      done
  done
}

printf "Verifying cluster convergence, timeout=%ss\n" "$TIMEOUT_SECONDS"

deadline=$((SECONDS + TIMEOUT_SECONDS))
while [[ "$SECONDS" -le "$deadline" ]]; do
  rm -f "${TMP_DIR}"/*.json

  reachable=0
  for node in "${NODES[@]}"; do
    if fetch_snapshot "$node"; then
      reachable=$((reachable + 1))
    fi
  done

  if [[ "$reachable" -lt 2 ]]; then
    sleep 1
    continue
  fi

  assert_no_log_conflicts

  leaders="$(leader_count)"
  if [[ "$leaders" -eq 1 ]] && logs_converged; then
    printf "\nCluster verified\n"
    printf "Reachable nodes: %s\n" "$reachable"
    printf "Leaders:         %s\n" "$leaders"
    printf "Log indexes:\n"
    log_indexes
    exit 0
  fi

  sleep 1
done

printf "\nFinal log indexes:\n"
log_indexes
printf "\nLeader count: %s\n" "$(leader_count)"
fail "cluster did not converge before timeout"
