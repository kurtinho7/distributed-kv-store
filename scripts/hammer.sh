#!/usr/bin/env bash
set -euo pipefail

DURATION_SECONDS=30
WRITERS=10
KEYSPACE=100
READ_AFTER_WRITE=false
NODES=(
  "http://localhost:8080"
  "http://localhost:8081"
  "http://localhost:8082"
)

usage() {
  cat <<EOF
Usage: ./scripts/hammer.sh [options]

Options:
  --duration SECONDS     How long to run. Default: ${DURATION_SECONDS}
  --writers COUNT        Number of concurrent writers. Default: ${WRITERS}
  --keyspace COUNT       Number of keys to spread writes across. Default: ${KEYSPACE}
  --nodes URLS           Comma-separated node URLs. Default: all local nodes.
  --read-after-write     Read each key after writing it.
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
    --nodes)
      IFS=',' read -r -a NODES <<<"$2"
      shift 2
      ;;
    --read-after-write)
      READ_AFTER_WRITE=true
      shift
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

if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || [[ "$DURATION_SECONDS" -lt 1 ]]; then
  printf "duration must be a positive integer\n" >&2
  exit 1
fi

if ! [[ "$WRITERS" =~ ^[0-9]+$ ]] || [[ "$WRITERS" -lt 1 ]]; then
  printf "writers must be a positive integer\n" >&2
  exit 1
fi

if ! [[ "$KEYSPACE" =~ ^[0-9]+$ ]] || [[ "$KEYSPACE" -lt 1 ]]; then
  printf "keyspace must be a positive integer\n" >&2
  exit 1
fi

if [[ "${#NODES[@]}" -eq 0 ]]; then
  printf "nodes must include at least one URL\n" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

END_TIME=$((SECONDS + DURATION_SECONDS))

writer() {
  local writer_id="$1"
  local stats_file="${TMP_DIR}/writer-${writer_id}.stats"
  local writes=0
  local write_successes=0
  local write_failures=0
  local read_successes=0
  local read_failures=0

  while [[ "$SECONDS" -lt "$END_TIME" ]]; do
    local node="${NODES[$((RANDOM % ${#NODES[@]}))]}"
    local key="hammer-$((RANDOM % KEYSPACE))"
    local value="writer-${writer_id}-${SECONDS}-${RANDOM}"
    local status

    status="$(curl --silent --output /dev/null --write-out "%{http_code}" \
      -X PUT "${node}/kv/${key}" \
      -H "Content-Type: application/json" \
      -d "{\"value\":\"${value}\"}" || true)"

    writes=$((writes + 1))
    if [[ "$status" =~ ^2 ]]; then
      write_successes=$((write_successes + 1))
    else
      write_failures=$((write_failures + 1))
      continue
    fi

    if [[ "$READ_AFTER_WRITE" == true ]]; then
      status="$(curl --silent --output /dev/null --write-out "%{http_code}" "${node}/kv/${key}" || true)"
      if [[ "$status" =~ ^2 ]]; then
        read_successes=$((read_successes + 1))
      else
        read_failures=$((read_failures + 1))
      fi
    fi
  done

  printf "%d %d %d %d %d\n" \
    "$writes" \
    "$write_successes" \
    "$write_failures" \
    "$read_successes" \
    "$read_failures" >"$stats_file"
}

printf "Hammering cluster for %ss with %s writer(s), keyspace=%s\n" \
  "$DURATION_SECONDS" "$WRITERS" "$KEYSPACE"
printf "Targets: %s\n" "${NODES[*]}"

for writer_id in $(seq 1 "$WRITERS"); do
  writer "$writer_id" &
done

wait

total_writes=0
total_write_successes=0
total_write_failures=0
total_read_successes=0
total_read_failures=0

for stats_file in "${TMP_DIR}"/*.stats; do
  read -r writes write_successes write_failures read_successes read_failures <"$stats_file"
  total_writes=$((total_writes + writes))
  total_write_successes=$((total_write_successes + write_successes))
  total_write_failures=$((total_write_failures + write_failures))
  total_read_successes=$((total_read_successes + read_successes))
  total_read_failures=$((total_read_failures + read_failures))
done

ops_per_second=$((total_writes / DURATION_SECONDS))

cat <<EOF

Hammer summary
--------------
Duration:        ${DURATION_SECONDS}s
Writers:         ${WRITERS}
Keyspace:        ${KEYSPACE}
Writes:          ${total_writes}
Write successes: ${total_write_successes}
Write failures:  ${total_write_failures}
Read successes:  ${total_read_successes}
Read failures:   ${total_read_failures}
Approx ops/sec:  ${ops_per_second}
EOF

if [[ "$total_write_successes" -eq 0 ]]; then
  exit 1
fi
