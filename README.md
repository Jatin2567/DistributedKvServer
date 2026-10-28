# Distributed KV Console

A Go key-value server using Raft, with a browser console and a deployable three-node demo.

## Run

Requires Go 1.22 or newer:

```sh
go run ./cmd/demo
```

Open http://localhost:8080. Set PORT to override the public port. Three Raft nodes communicate over private loopback HTTP listeners; the public server exposes only the dashboard and client API.

## Render deployment

Create a free Go Web Service from this repository (or use render.yaml):

- Build: `go build -o app ./cmd/demo`
- Start: `./app`
- Health check: `/healthz`
- Go version: `1.25.4`

Vercel Functions are not suitable for the continuously running election and replication loops. Render runs the Go service continuously while active. Free services can sleep and restart.

## API

- `GET /api/cluster`: live roles, terms, log and commit positions.
- `GET /api/kv?key=example`: read from the currently known leader.
- `PUT /api/kv`: JSON `{ "key": "example", "value": "hello" }`.
- `DELETE /api/kv`: JSON `{ "key": "example" }`.
- `GET /healthz`: 200 when a leader exists; 503 during election.

Writes return success only after quorum replication and local application. A timeout is an unknown outcome: a write could still commit later. Reads use local leader state and are not linearizable during partitions. Values are strings; keys are limited to 256 bytes, values to 4096 bytes, request bodies to 8192 bytes, and the demo log to 10,000 entries.

## Scope and limitations

This is an experimental, public, shared learning playground, not a production database. All visitors share the same keyspace. Use sample data only. The three nodes run in one process on one host, so this deployment demonstrates Raft replication but does not provide host-level high availability. State, votes and logs are in memory and reset on restart or free-service sleep. The RocksDB file is an unused placeholder. Production use requires durable Raft state, snapshots, authentication, admission controls, linearizable reads and independent failure domains.

The original individual-node entry point remains available with `go run ./cmd/server configs/node1.json` (repeat for node2 and node3). Its legacy `/put`, `/get` and `/delete` API remains separate from the demo API.

## Validation

```sh
go test ./... -count=3
go vet ./...
```

Tests cover quorum sizing, heartbeat log preservation, malformed append rejection, single-node election, committed writes, three-node replication, leader failover, deletion, and rejection of successful acknowledgements without quorum.
