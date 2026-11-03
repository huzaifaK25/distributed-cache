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

Phases 1 through 3 are complete: a TCP-accessible, single-node in-memory cache
with TTL expiration, LRU eviction, bounded concurrency, and graceful shutdown.

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
|   `-- cleanup.go        # Background expiration worker
|-- internal/server/
|   |-- server.go         # Lifecycle, listener, connections, shutdown
|   |-- config.go         # Server limits and timeouts
|   |-- connection.go     # Per-client protocol loop
|   |-- protocol.go       # Command parser
|   |-- handler.go        # Command-to-cache mapping
|   `-- workerpool.go     # Fixed workers and bounded queue
|-- tests/
|   |-- cache_test.go     # Cache unit and concurrency tests
|   |-- ttl_test.go       # TTL and cleanup tests
|   |-- eviction_test.go  # LRU and limit tests
|   |-- server_test.go    # Protocol and multi-client tests
|   |-- server_connection_test.go # Limits, idle timeout, 1000 clients
|   |-- server_shutdown_test.go   # Graceful shutdown tests
|   |-- benchmark_test.go # Cache benchmarks
|   `-- server_benchmark_test.go  # Server benchmarks
|-- docs/
|   |-- phase-1-core-cache.md
|   |-- phase-2-production-features.md
|   `-- phase-3-network-server.md
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

Behavior notes:

- `Set` clears any previous TTL.
- `SetWithTTL` requires a positive duration and returns `cache.ErrInvalidTTL` otherwise.
- Expired entries are never returned.
- `StartCleanupWorker` and `StopCleanupWorker` are idempotent and support restart.
- `Server.Stop` is idempotent and finishes in-flight requests.
- A `Server` cannot be restarted after `Stop`.

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

## Roadmap

1. ✅ Core Cache Engine
2. ✅ Production Cache Features (TTL, cleanup, LRU, limits)
3. ✅ Networked Cache Server
4. Multi-Node Cluster
5. Replication
6. Failover
7. Consistent Hashing
8. Observability
9. Chaos Testing
10. Benchmarking and Documentation
