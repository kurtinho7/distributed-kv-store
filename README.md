# Distributed KV Store

A resume-oriented distributed systems project: a key-value store with a Go backend and a React dashboard. The backend now runs as a small replicated cluster with append-only logs, follower write forwarding, majority acknowledgements, startup catch-up, and a Raft-inspired leader election layer.

## Project Layout

- `server/` - Go HTTP API and KV store internals
- `web/` - React dashboard for interacting with and visualizing the cluster
- `docker-compose.yml` - local three-node cluster plus dashboard

## Current Features

- In-memory `GET`, `PUT`, and `DELETE`
- Append-only operation log for mutations
- Optional log persistence and replay with `KV_LOG_PATH`
- Three-node Docker Compose cluster
- Leader-based replication with majority acknowledgements
- Follower write forwarding to the current elected leader
- Follower catch-up on startup through missing log replay
- Raft-inspired terms, roles, heartbeats, RequestVote, and leader failover
- JSON HTTP API
- Health endpoint
- Cluster state endpoint with elected leader/member view
- Raft state endpoint for debugging elections
- React dashboard for KV operations, node status, and log history

## Next Milestones

1. Persist Raft `currentTerm` and `votedFor`.
2. Add peer health probing so `/cluster` marks stopped nodes unhealthy.
3. Track commit index separately from applied log index.
4. Add network partition and delayed-message simulation.
5. Expand the dashboard with failure controls and elected-leader visualization.

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

The Compose setup starts `node-1`, `node-2`, `node-3`, and the web dashboard. Each node persists its operation log in its own named volume.

To stop the cluster:

```sh
docker compose down
```

To stop the cluster and clear persisted node logs:

```sh
docker compose down -v
```

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

View Raft state:

```sh
curl http://localhost:8080/raft
```

Health check:

```sh
curl http://localhost:8080/healthz
```

## Persistence

Set `KV_LOG_PATH` to enable durable append-only logging. On startup, the server replays the log file into memory before accepting requests.

## Replication Model

The cluster uses leader-based replication. The first node starts as the initial leader, then the Raft-inspired election layer can elect a new leader if the current leader stops sending heartbeats.

Write flow:

1. Clients may send writes to any node.
2. Followers forward writes to the current elected leader.
3. The leader appends the operation to its local log.
4. The leader sends the log entry to peer nodes through `POST /internal/replicate`.
5. The leader returns success after receiving a majority of acknowledgements.
6. Followers apply replicated log entries in index order.

Followers catch up on startup by requesting missing entries from the leader with `GET /internal/log?after=<index>`.

## Consensus Model

This project implements a small educational Raft-inspired consensus layer. It is intended to demonstrate the mechanics of leader election rather than provide a production-grade Raft implementation.

Implemented:

- follower, candidate, and leader roles
- monotonically increasing terms
- one vote per node per term
- `POST /internal/raft/request-vote`
- `POST /internal/raft/append-entries` heartbeats
- randomized election timeouts
- majority leader election
- old leaders step down when they observe a newer term
- writes route to the current elected leader

Example failover:

```sh
docker compose stop node-1
curl http://localhost:8081/raft
curl http://localhost:8082/raft
```

One remaining node should become leader, and the other should report that leader in its Raft state.

Current limitations:

- Raft term/vote state is in memory and not persisted yet
- follower catch-up runs once at startup
- stopped followers miss writes until they restart and catch up
- peer health is not actively probed yet
- replicated entries are applied immediately after majority acknowledgement; there is not yet a separate commit index
- no network partition simulation yet
