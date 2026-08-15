# Distributed KV Store

A resume-oriented distributed systems project: a key-value store with a Go backend and a React dashboard. The first milestone is a single-node in-memory store with a cluster-shaped API, leaving clear extension points for replication, leader election, and failure simulation.

## Project Layout

- `server/` - Go HTTP API and KV store internals
- `web/` - React dashboard for interacting with and visualizing the cluster
- `docker-compose.yml` - local multi-service development skeleton

## Current Features

- In-memory `GET`, `PUT`, and `DELETE`
- Append-only operation log for mutations
- JSON HTTP API
- Health endpoint
- Cluster state endpoint with leader/member placeholders
- React dashboard for KV operations, node status, and log history

## Next Milestones

1. Add append-only log persistence.
2. Run multiple Go nodes from `docker-compose.yml`.
3. Add leader-based replication between nodes.
4. Add Raft-style leader election and quorum commits.
5. Add dashboard controls for node failures and network partitions.

## Run Locally

Backend:

```sh
cd server
go run ./cmd/kvstore
```

Frontend:

```sh
cd web
npm install
npm run dev
```

The web app expects the API at `http://localhost:8080` by default.
