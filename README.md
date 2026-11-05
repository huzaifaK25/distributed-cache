# Distributed Cache

## Problem Statement

Modern applications, AI systems, APIs, and microservices rely on caching to
reduce database load and improve response times. A single cache node eventually
becomes a bottleneck: memory and throughput are finite, and that node is a
single point of failure. Growing systems need cache infrastructure that scales
horizontally, survives node failures, and keeps serving requests without
interruption.

## Why a Distributed Cache?

A distributed cache spreads data across multiple nodes instead of storing
everything in one instance:

- **Horizontal scalability** — add nodes to grow memory and throughput.
- **High availability** — replication and automatic failover keep data served.
- **Lower latency** — requests are routed to healthy, nearby nodes.
- **No single bottleneck** — one server no longer caps the whole system.
- **Fault resilience** — hardware, network, and service failures are tolerated.

## Why Build This Instead of Redis?

Redis is an excellent production system, but this project does not aim to
replace it. The goal is to understand and implement the distributed-systems
concepts behind large-scale infrastructure:

- Cluster membership, node discovery, and failure detection
- Distributed replication and leader election
- Consistent hashing and partition rebalancing
- Distributed observability and chaos engineering
- Production hardening

Building these from first principles shows how systems such as Redis Cluster,
DynamoDB, Cassandra, CockroachDB, and Hazelcast solve distributed-systems
problems.

## Project Status

Phases 1 through 6 are complete: a TCP-accessible in-memory cache with TTL
expiration, LRU eviction, bounded concurrency, a cluster membership layer with
gossip and health detection, WAL-backed primary/replica replication, and automatic
Raft-style failover with graceful leadership transfer.

Completed features:

- ✅ Thread-safe cache
- ✅ Statistics
- ✅ TTL support
- ✅ Cleanup workers
- ✅ LRU eviction
- ✅ Memory limits
- ✅ TCP network server
- ✅ Bounded worker pool
- ✅ Connection limits and idle timeouts
- ✅ Graceful shutdown
- ✅ Cluster membership and discovery
- ✅ Heartbeats and health tracking
- ✅ Gossip convergence
- ✅ Primary/replica replication
- ✅ Durable write-ahead log
- ✅ Full and incremental replica sync
- ✅ Replication acknowledgements and metrics
- ✅ Failure detection and peer health
- ✅ Raft-style leader election with durable terms and votes
- ✅ Quorum writes and split-brain protection
- ✅ Graceful leadership transfer
- ✅ Automatic node recovery and resynchronization

Phase 2 additions:

- `CacheItem` metadata (`CreatedAt`, `ExpiresAt`, `LastAccessed`)
- Lazy expiration on read and optional background cleanup
- `Expired` and `Evictions` metrics
- Graceful cleanup worker start/stop/restart
- TTL and eviction test suites
- TTL and eviction benchmarks

Phase 3 additions:

- Line-based TCP protocol (`PING`, `SET`, `GET`, `DEL`, `STATS`)
- Concurrent clients with a connection limit and idle timeout
- Fixed worker pool with a bounded request queue
- Graceful shutdown that finishes in-flight requests
- Protocol, worker-pool, multi-client, and 1000-connection tests
- Server throughput and connection benchmarks

Phase 4 additions:

- Node model, thread-safe registry, and cluster metadata
- TCP join, leave, heartbeat, and gossip messages
- `ONLINE`, `DEGRADED`, and `OFFLINE` health states
- Versioned membership updates and leave tombstones
- Push-pull anti-entropy gossip with join/leave convergence tests
- Simulated network delay and packet-loss tests

Phase 5 additions:

- `PRIMARY` and `REPLICA` roles with ordered SET, DEL, and TTL operations
- Checksummed, fsynced write-ahead log with torn-tail recovery
- Full snapshot bootstrap with bounded chunks
- Incremental WAL-based catch-up after reconnect or restart
- Cumulative acknowledgements, retries, queue bounds, and replication metrics
- WAL replay recovery tests and real-TCP multi-replica tests

Phase 6 additions:

- Static-membership `FOLLOWER`, `CANDIDATE`, and `LEADER` states
- Persistent terms and votes with atomic state-file replacement
- Randomized elections, log up-to-date checks, and majority quorum
- Leader lease and quorum-acknowledged writes for split-brain safety
- Dynamic promotion of a node's replication coordinator from replica to primary
- Graceful `timeout_now` leadership transfer
- Dead-node detection and full resynchronization when a node rejoins
- Kill-primary, transfer, quorum-loss, and data-integrity tests

## Requirements

- Go 1.27 or newer

## Project Layout

```text
distributed-cache/
|-- cmd/cache/            # TCP server entry point
|-- internal/cache/
|   |-- cache.go          # Cache interface and TTL error
|   |-- memory.go         # InMemoryCache implementation
|   |-- stats.go          # CacheStats snapshot type
|   |-- item.go           # CacheItem metadata
|   |-- config.go         # Config and capacity/cleanup options
|   |-- lru.go            # LRU list and eviction helpers
|   |-- cleanup.go        # Background expiration worker
|   `-- snapshot.go       # Consistent snapshot and restore for replication
|-- internal/server/
|   |-- server.go         # Lifecycle, listener, connections, shutdown
|   |-- config.go         # Server limits and timeouts
|   |-- connection.go     # Per-client protocol loop
|   |-- protocol.go       # Command parser
|   |-- handler.go        # Command-to-cache mapping
|   `-- workerpool.go     # Fixed workers and bounded queue
|-- internal/cluster/
|   |-- node.go           # Node, NodeID, and health states
|   |-- registry.go       # Thread-safe membership registry
|   |-- membership.go     # Lifecycle, join, leave, and metadata
|   |-- heartbeat.go      # I_AM_ALIVE loop and health updates
|   `-- gossip.go         # Wire protocol, transport, and anti-entropy
|-- internal/replication/
|   |-- replicator.go     # Roles, mutations, local state machine
|   |-- wal.go            # Durable append-only mutation log
|   |-- syncer.go         # Wire protocol, snapshot chunks, replica client
|   |-- coordinator.go    # Lifecycle, primary API, metrics
|   `-- session.go        # Primary-side replica sessions and acknowledgements
|-- internal/failover/
|   |-- state_machine.go  # Roles, durable term/vote state, timeouts
|   |-- detector.go       # Peer health tracking
|   |-- election.go       # Vote/heartbeat RPCs, campaigns, transfer
|   `-- leader.go         # Lifecycle, lease, replication promotion, quorum writes
|-- tests/
|   |-- cache_test.go     # Cache unit and concurrency tests
|   |-- ttl_test.go       # TTL and cleanup tests
|   |-- eviction_test.go  # LRU and limit tests
|   |-- server_test.go    # Protocol and multi-client tests
|   |-- server_connection_test.go # Limits, idle timeout, 1000 clients
|   |-- server_shutdown_test.go   # Graceful shutdown tests
|   |-- cluster_test.go   # Multi-node join, leave, health, convergence
|   |-- replication_test.go       # Real-TCP primary/replica replication
|   |-- benchmark_test.go # Cache benchmarks
|   `-- server_benchmark_test.go  # Server benchmarks
|-- docs/
|   |-- phase-1-core-cache.md
|   |-- phase-2-production-features.md
|   |-- phase-3-network-server.md
|   |-- phase-4-cluster-management.md
|   |-- phase-5-replication.md
|   `-- phase-6-failover.md
|-- go.mod
`-- README.md
```

## Usage

Start the server:

```sh
go run ./cmd/cache -addr 127.0.0.1:6379
```

Then connect with any TCP client and send line commands:

```text
PING
PONG
SET user:1 hello
OK
GET user:1
hello
GET missing
NOT_FOUND
DEL user:1
OK
STATS
hits=1 misses=1 sets=1 deletes=1 expired=0 evictions=0
```

The same service can be embedded:

```go
c := cache.New(cache.Config{
	MaxItems:        10_000,
	CleanupInterval: 30 * time.Second,
})
defer c.StopCleanupWorker()

s := server.New(c, server.Config{
	Address:        "127.0.0.1:6379",
	WorkerCount:    16,
	MaxConnections: 4096,
})
if err := s.Start(); err != nil {
	log.Fatal(err)
}
defer s.Stop()
```

Cluster membership is a separate layer that will be wired into the cache server in
later phases:

```go
a, err := cluster.New(cluster.Config{
	ClusterID: "cache-prod",
	NodeID:    "node-a",
	Address:   "127.0.0.1:7001",
})
if err != nil {
	log.Fatal(err)
}
if err := a.Start(); err != nil {
	log.Fatal(err)
}
defer a.Stop()

b, err := cluster.New(cluster.Config{
	ClusterID: "cache-prod",
	NodeID:    "node-b",
	Address:   "127.0.0.1:7002",
})
if err != nil {
	log.Fatal(err)
}
if err := b.Start(); err != nil {
	log.Fatal(err)
}
defer b.Stop()

if err := b.Join(a.Addr().String()); err != nil {
	log.Fatal(err)
}
```

`Join` accepts one or more seed addresses and succeeds when at least one seed is
reachable. Membership snapshots propagate transitively through gossip, so later
nodes can join any existing member. Use `Nodes`, `Node`, and `Metadata` to inspect
the current view. `Leave` announces a graceful exit; `Stop` shuts down without
announcing.

Replication is also a separate layer until later phases wire it into the server
and membership view:

```go
primary, err := replication.New(replication.Config{
	Role:       replication.RolePrimary,
	ID:         "node-a",
	Store:      cache.New(cache.Config{}),
	WALPath:    "data/node-a.wal",
	ListenAddr: "127.0.0.1:7101",
})
if err != nil {
	log.Fatal(err)
}
if err := primary.Start(); err != nil {
	log.Fatal(err)
}
defer primary.Stop()

replicaStore := cache.New(cache.Config{})
replica, err := replication.New(replication.Config{
	Role:        replication.RoleReplica,
	ID:          "node-b",
	Store:       replicaStore,
	WALPath:     "data/node-b.wal",
	PrimaryAddr: "127.0.0.1:7101",
})
if err != nil {
	log.Fatal(err)
}
if err := replica.Start(); err != nil {
	log.Fatal(err)
}
defer replica.Stop()

if err := primary.Set("user:1", "hello"); err != nil {
	log.Fatal(err)
}
```

New replicas bootstrap from a full snapshot, then receive only ordered deltas.
Every primary write is fsynced to the WAL before replication. `Stats` and
`Replicas` expose acknowledged, pending, failed, queue-depth, lag, and sync-time
metrics.

Failover sits above replication and elects one replication primary. All nodes
share a static peer list:

```go
node, err := failover.New(failover.Config{
	ClusterID:  "cache-prod",
	ID:         "node-a",
	ListenAddr: "127.0.0.1:7101",
	Peers: []failover.Peer{
		{ID: "node-b", Address: "127.0.0.1:7102"},
		{ID: "node-c", Address: "127.0.0.1:7103"},
	},
	Store:                 store,
	WALPath:               "data/node-a.wal",
	ReplicationListenAddr: "127.0.0.1:7201",
})
if err != nil {
	log.Fatal(err)
}
if err := node.Start(); err != nil {
	log.Fatal(err)
}
defer node.Stop()

// Writes are accepted only by the elected leader and wait for a replication
// quorum. Reads use the local store and may be stale on a follower.
if err := node.Set("user:1", "hello"); err != nil {
	log.Fatal(err)
}
```

`TransferLeadership` performs a graceful handoff. `Stats` reports the current
role, term, leader, quorum lease, replication address, and nested replication
metrics; `Health` reports each peer's `ALIVE`, `SUSPECT`, or `DEAD` status.

Protocol summary:

| Command | Response |
|---|---|
| `PING` | `PONG` |
| `SET key value` | `OK` (value may contain spaces) |
| `GET key` | value or `NOT_FOUND` |
| `DEL key` | `OK` (idempotent) |
| `STATS` | Cache hit/miss/set/delete/expiry/eviction counters |

Errors are returned as `ERR <message>`. Commands are case-insensitive and lines
may end with `\n` or `\r\n`.

## Configuration

Cache options:

| Option                 | Behavior                                                       |
| ---------------------- | -------------------------------------------------------------- |
| `MaxItems <= 0`        | Unlimited entries; no LRU eviction.                            |
| `MaxItems > 0`         | LRU eviction keeps the cache at or below this size.            |
| `CleanupInterval <= 0` | No background worker; expired keys are removed lazily on read. |
| `CleanupInterval > 0`  | Cleanup worker starts in `New` and sweeps on that interval.    |

Server options (zero values use `server.DefaultConfig`):

| Option | Default | Behavior |
|---|---:|---|
| `Address` | `127.0.0.1:6379` | TCP listen address. |
| `MaxConnections` | `4096` | Simultaneous registered clients. |
| `WorkerCount` | `4 x GOMAXPROCS` | Request execution goroutines. |
| `QueueSize` | `4096` | Bounded waiting requests; overflow returns server busy. |
| `IdleTimeout` | `5m` | Close clients that send no command. |
| `WriteTimeout` | `10s` | Bound each response write. |
| `ShutdownTimeout` | `10s` | Bound graceful shutdown. |
| `MaxRequestSize` | `64 KiB` | Maximum command-line bytes. |

Cluster options (zero values use `cluster.DefaultConfig`):

| Option | Default | Behavior |
|---|---:|---|
| `ClusterID` | required | Must match across members of one cluster. |
| `NodeID` | required | Unique member identity for the cluster lifetime. |
| `Address` | `127.0.0.1:0` | Cluster RPC listen address. |
| `HeartbeatInterval` | `1s` | Liveness probe and health-evaluation interval. |
| `GossipInterval` | `2s` | Membership anti-entropy interval. |
| `DegradedAfter` | `3s` | Silence before a node becomes `DEGRADED`. |
| `OfflineAfter` | `10s` | Silence before a node becomes `OFFLINE`; must exceed `DegradedAfter`. |
| `RequestTimeout` | `2s` | Bound on one join, heartbeat, gossip, or leave RPC. |
| `ShutdownTimeout` | `5s` | Bound on graceful cluster shutdown. |
| `MaxMessageSize` | `1 MiB` | Maximum encoded cluster message size. |

Replication options (zero values use `replication.DefaultConfig`):

| Option | Default | Behavior |
|---|---:|---|
| `Role` | required | `PRIMARY` or `REPLICA`. |
| `ID` | required | Stable unique node identity. |
| `Store` | required | Cache implementing `Snapshot`/`Restore`. |
| `WALPath` | required | Durable write-ahead log file. |
| `ListenAddr` | `127.0.0.1:0` | Primary replication listen address. |
| `PrimaryAddr` | replica required | Upstream primary address. |
| `ReconnectMin` / `ReconnectMax` | `100ms` / `2s` | Replica retry backoff bounds. |
| `HandshakeTimeout` | `5s` | Dial and hello deadline. |
| `AckTimeout` | `5s` | Frame write and batch acknowledgement deadline. |
| `MaxRetries` | `3` | Batch and snapshot attempts. |
| `MaxBatch` | `64` | Mutations per append message. |
| `SessionQueue` | `4096` | Buffered live mutations per replica. |
| `SnapshotBatch` | `256` | Entries per snapshot chunk before size splitting. |
| `MaxMessageSize` | `8 MiB` | Maximum encoded replication frame. |
| `ShutdownTimeout` | `5s` | Bound on replication shutdown. |

Failover options (zero values use `failover.DefaultConfig`):

| Option | Default | Behavior |
|---|---:|---|
| `ClusterID` | required | Must match across the failover cluster. |
| `ID` | required | Stable unique node identity. |
| `ListenAddr` | `127.0.0.1:0` | Control-plane TCP listen address. |
| `Peers` | empty | Every other node; membership is static. |
| `Store` | required | Cache implementing `replication.Store`. |
| `WALPath` | required | Node-private replication WAL. |
| `StatePath` | `WALPath + ".failover"` | Durable term/vote state. |
| `ReplicationListenAddr` | `127.0.0.1:0` | Primary replication listen address. |
| `ReplicationConfig` | replication defaults | Tuning for each coordinator. |
| `ElectionTimeoutMin` / `Max` | `300ms` / `600ms` | Randomized election window. |
| `HeartbeatInterval` | `100ms` | Leader heartbeat cadence. |
| `FailureTimeout` | `max(3 x election max, 2s)` | Detector death threshold. |
| `RPCTimeout` | `ElectionTimeoutMin / 3` | Control RPC deadline; below half the election timeout. |
| `WriteTimeout` | `2s` | Quorum acknowledgement deadline. |
| `TransferTimeout` | `max(3 x election max, 3s)` | Graceful handoff deadline. |
| `ReplicationRetry` | `200ms` | Endpoint transition retry delay. |
| `ShutdownTimeout` | `5s` | Grace period before shutdown forces control connections closed. |
| `MaxMessageSize` | `1 MiB` | Maximum encoded control message. |

Behavior notes:

- `Set` clears any previous TTL.
- `SetWithTTL` requires a positive duration and returns `cache.ErrInvalidTTL` otherwise.
- Expired entries are never returned.
- `StartCleanupWorker` and `StopCleanupWorker` are idempotent and support restart.
- `Server.Stop` is idempotent and finishes in-flight requests.
- A `Server` cannot be restarted after `Stop`.
- `Cluster.Leave` removes the node from peers and records a tombstone; `Cluster.Stop` does not announce a leave.
- Crashed nodes remain in the view as `OFFLINE`; `Leave` removes them from the view.
- A `Cluster` cannot be restarted after `Stop`.
- A primary fsyncs every mutation to its WAL before applying or replicating it.
- Replicas bootstrap from a snapshot, then apply only ordered deltas; a sequence gap forces a fresh full sync.
- Replica writes return `replication.ErrNotPrimary`.
- Replicated values must be JSON-serializable and are canonicalized on every node.
- A `Coordinator` cannot be restarted after `Stop`.
- Failover requires a static peer list that excludes the local node.
- A leader renews its lease only when a heartbeat majority responds; without a
  quorum it steps down and rejects writes.
- Leader writes return success only after a replication quorum acknowledges the
  sequence.
- A new replica coordinator performs one full sync, then reconnects incrementally.
- `Node.Stop` is idempotent and does not transfer leadership; call
  `TransferLeadership` first for a graceful exit.

## Build and Run

```sh
go build ./...
go run ./cmd/cache -addr 127.0.0.1:6379
```

Useful flags include `-workers`, `-queue-size`, `-max-conns`,
`-idle-timeout`, `-shutdown-timeout`, `-max-items`, and
`-cleanup-interval`. The process shuts down gracefully on `Ctrl+C` or
`SIGTERM`.

## Test

```sh
go test ./...
```

Race-detector test:

```sh
go test -race ./...
```

The race detector requires cgo and a C compiler (for example, MinGW-w64 GCC on
Windows). On Windows PowerShell, enable cgo before running it:

```powershell
$env:CGO_ENABLED = "1"
go test -race ./...
```

## Benchmarks

```sh
go test -bench . ./...
```

To skip unit tests and show allocation data:

```sh
go test ./tests -run "^$" -bench . -benchmem
```

Server-only benchmarks:

```sh
go test ./tests -run "^$" -bench "^BenchmarkServer" -benchmem
```

See the phase documentation for architecture, lifecycle diagrams, and recorded
results:

- [Phase 1 - Core Cache Engine](docs/phase-1-core-cache.md)
- [Phase 2 - Production Cache Features](docs/phase-2-production-features.md)
- [Phase 3 - Network Server](docs/phase-3-network-server.md)
- [Phase 4 - Cluster Management](docs/phase-4-cluster-management.md)
- [Phase 5 - Replication](docs/phase-5-replication.md)
- [Phase 6 - Failover](docs/phase-6-failover.md)

## Roadmap

1. ✅ Core Cache Engine
2. ✅ Production Cache Features (TTL, cleanup, LRU, limits)
3. ✅ Networked Cache Server
4. ✅ Cluster Membership (gossip, heartbeats, health)
5. ✅ Replication (WAL, full/incremental sync, acknowledgements)
6. ✅ Failover (election, quorum, transfer, recovery)
7. Consistent Hashing
8. Observability
9. Chaos Testing
10. Benchmarking and Documentation
