#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${DEMOCTL_TOKEN:-}" ]]; then
  printf "DEMOCTL_TOKEN must be set before deploying the hosted demo.\n" >&2
  printf "Example: export DEMOCTL_TOKEN=\"$(openssl rand -hex 24)\"\n" >&2
  exit 1
fi

cd "$ROOT_DIR"

printf "Building web dashboard\n"
npm --prefix web ci
npm --prefix web run build

printf "\nStarting hosted demo stack\n"
docker compose -p kvstore-demo -f docker-compose.demo.yml up --build -d

printf "\nDemo stack status\n"
docker compose -p kvstore-demo -f docker-compose.demo.yml ps

printf "\nHosted demo is ready on port 80. Paste DEMOCTL_TOKEN into the dashboard to use demo controls.\n"
