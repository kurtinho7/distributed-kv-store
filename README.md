# Distributed KV Store

A resume-oriented distributed systems project: a key-value store with a Go backend and a React dashboard. The current milestone is a single-node store with durable append-only log replay and a cluster-shaped API, leaving clear extension points for replication, leader election, and failure simulation.

## Project Layout

- `server/` - Go HTTP API and KV store internals
- `web/` - React dashboard for interacting with and visualizing the cluster
- `docker-compose.yml` - local multi-service development skeleton

## Current Features

- In-memory `GET`, `PUT`, and `DELETE`
- Append-only operation log for mutations
- Optional log persistence and replay with `KV_LOG_PATH`
- JSON HTTP API
- Health endpoint
- Cluster state endpoint with leader/member placeholders
- React dashboard for KV operations, node status, and log history

## Next Milestones

1. Run multiple Go nodes from `docker-compose.yml`.
2. Add leader-based replication between nodes.
3. Add write forwarding from followers to the leader.
4. Add Raft-style leader election and quorum commits.
5. Add dashboard controls for node failures and network partitions.

## Run Locally

Backend:

```sh
cd server
KV_LOG_PATH=data/kv.log go run ./cmd/kvstore
```

Frontend:

```sh
cd web
npm install
npm run dev
```

The web app expects the API at `http://localhost:8080` by default.

Docker Compose:

```sh
docker compose up --build
```

The Compose setup persists the operation log in the `kvstore-data` volume.

## API Examples

Store a key:

```sh
curl -X PUT http://localhost:8080/kv/project \
  -H 'Content-Type: application/json' \
  -d '{"value":"distributed-kv"}'
```

Read a key:

```sh
curl http://localhost:8080/kv/project
```

Delete a key:

```sh
curl -X DELETE http://localhost:8080/kv/project
```

List stored keys:

```sh
curl http://localhost:8080/kv
```

View the operation log:

```sh
curl http://localhost:8080/log
```

View cluster state:

```sh
curl http://localhost:8080/cluster
```

Health check:

```sh
curl http://localhost:8080/healthz
```

## Persistence

Set `KV_LOG_PATH` to enable durable append-only logging. On startup, the server replays the log file into memory before accepting requests.

## Replication Model

The cluster uses leader-based replication with a static leader configured by `KV_LEADER_ID`.

Write flow:

1. Clients may send writes to any node.
2. Followers forward writes to the configured leader.
3. The leader appends the operation to its local log.
4. The leader sends the log entry to peer nodes through `POST /internal/replicate`.
5. The leader returns success after receiving a majority of acknowledgements.
6. Followers apply replicated log entries in index order.

Followers catch up on startup by requesting missing entries from the leader with `GET /internal/log?after=<index>`.

Current limitations:

- leader is static, not elected
- follower catch-up runs once at startup
- stopped followers miss writes until restart
- no network partition simulation yet
- no Raft terms or commit indexes yet
