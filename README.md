# Distributed KV Store

A resume-oriented distributed systems project: a key-value store with a Go backend and a React dashboard. The backend now runs as a small replicated cluster with append-only logs, follower write forwarding, majority acknowledgements, Raft-inspired leader election, peer health checks, and controlled fault simulation.

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
- Follower catch-up through missing log replay
- Raft-inspired terms, roles, heartbeats, RequestVote, and leader failover
- Peer health probing for stopped or unreachable nodes
- Controlled replication/catch-up partition simulation
- Rollback of uncommitted writes when majority replication fails
- JSON HTTP API
- Health endpoint
- Cluster state endpoint with elected leader/member view
- Raft state endpoint for debugging elections
- React dashboard for KV operations, node status, and log history

## Next Milestones

1. Persist Raft `currentTerm` and `votedFor`.
2. Track commit index separately from applied log index.
3. Add delayed-message simulation.
4. Expand the dashboard with failure controls and elected-leader visualization.
5. Add benchmark and chaos-demo scripts.

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
The chaos demo panel expects the demo controller at `http://localhost:9090` by default. Override with `VITE_DEMOCTL_URL` if needed.

Demo controller:

```sh
cd server
go run ./cmd/democtl
```

The demo controller listens on `http://localhost:9090` and can run local scripts such as the chaos demo. Run it outside Docker because the chaos demo restarts the Compose cluster.

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
Followers also run periodic catch-up, so a lagging node can repair itself after a simulated partition heals.

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

## Fault Tolerance Demo

Run the automated Phase 4 harness:

```sh
./scripts/phase4-demo.sh
```

The script starts a clean cluster, partitions `node-3`, writes through the leader, verifies majority commit, heals the partition, and waits for `node-3` to catch up.

Hammer the cluster with concurrent writes:

```sh
./scripts/hammer.sh --duration 30 --writers 10 --keyspace 100
```

Optionally read after each write:

```sh
./scripts/hammer.sh --duration 30 --writers 10 --keyspace 100 --read-after-write
```

Verify cluster convergence and log consistency:

```sh
./scripts/verify-cluster.sh
```

Run traffic and then verify correctness:

```sh
./scripts/hammer.sh --duration 30 --writers 10 --keyspace 100
./scripts/verify-cluster.sh
```

Run the combined chaos demo:

```sh
./scripts/chaos-demo.sh --duration 15 --writers 8 --keyspace 50
```

The chaos demo starts a clean cluster, partitions `node-3`, hammers the remaining majority with traffic, verifies `node-3` lags, heals the partition, waits for catch-up, and verifies convergence.

Stop a follower and observe health:

```sh
docker compose stop node-3
curl http://localhost:8081/cluster
```

`node-3` should appear with `"healthy":false`. Start it again and health should recover:

```sh
docker compose start node-3
curl http://localhost:8081/cluster
```

Simulate a leader-to-follower partition:

```sh
curl -X POST http://localhost:8080/faults/replication/node-3
curl http://localhost:8080/faults
```

Write while `node-3` is partitioned:

```sh
curl -X PUT http://localhost:8080/kv/partitioned \
  -H 'Content-Type: application/json' \
  -d '{"value":"heals-later"}'
```

The leader can still commit with a majority:

```sh
curl http://localhost:8080/kv/partitioned
curl http://localhost:8081/kv/partitioned
curl http://localhost:8082/kv/partitioned
```

`node-1` and `node-2` should have the value. `node-3` should return `key not found`.

Heal the partition:

```sh
curl -X DELETE http://localhost:8080/faults/replication/node-3
```

After periodic catch-up runs, `node-3` should repair itself without a restart:

```sh
curl http://localhost:8082/kv/partitioned
curl http://localhost:8082/log
```

Current limitations:

- Raft term/vote state is in memory and not persisted yet
- fault simulation is in-memory and resets when a node restarts
- replicated entries are applied immediately after majority acknowledgement; there is not yet a separate commit index
- simulated partitions cover replication and catch-up traffic, not arbitrary client traffic
