# Distributed Cache

A learning-focused distributed cache written in Go, built one phase at a time.
Each phase is implemented, tested, and documented before the next one begins.

## Project Status

Phase 1 and Phase 2 are complete: a production-oriented, single-node in-memory
cache with TTL expiration and LRU eviction.

Completed features:

- ✅ Thread-safe cache
- ✅ Statistics
- ✅ TTL support
- ✅ Cleanup workers
- ✅ LRU eviction
- ✅ Memory limits

Phase 2 additions:

- `CacheItem` metadata (`CreatedAt`, `ExpiresAt`, `LastAccessed`)
- Lazy expiration on read and optional background cleanup
- `Expired` and `Evictions` metrics
- Graceful cleanup worker start/stop/restart
- TTL and eviction test suites
- TTL and eviction benchmarks

## Requirements

- Go 1.27 or newer

## Project Layout

```text
distributed-cache/
|-- cmd/cache/            # Demo entry point
|-- internal/cache/
|   |-- cache.go          # Cache interface and TTL error
|   |-- memory.go         # InMemoryCache implementation
|   |-- stats.go          # CacheStats snapshot type
|   |-- item.go           # CacheItem metadata
|   |-- config.go         # Config and capacity/cleanup options
|   |-- lru.go            # LRU list and eviction helpers
|   `-- cleanup.go        # Background expiration worker
|-- tests/
|   |-- cache_test.go     # Unit and concurrency tests
|   |-- ttl_test.go       # TTL and cleanup tests
|   |-- eviction_test.go  # LRU and limit tests
|   `-- benchmark_test.go # Benchmarks
|-- docs/
|   |-- phase-1-core-cache.md
|   `-- phase-2-production-features.md
|-- go.mod
`-- README.md
```

## Usage

```go
package main

import (
	"fmt"
	"log"
	"time"

	"distributed-cache/internal/cache"
)

func main() {
	c := cache.New(cache.Config{
		MaxItems:        10_000,
		CleanupInterval: 30 * time.Second,
	})
	defer c.StopCleanupWorker() // Stop the background sweeper.

	if err := c.Set("language", "Go"); err != nil {
		log.Fatal(err)
	}

	if err := c.SetWithTTL("session:1", "token", 5*time.Minute); err != nil {
		log.Fatal(err)
	}

	if value, found := c.Get("session:1"); found {
		fmt.Println(value) // token
	}

	fmt.Printf("%+v\n", c.Stats())
}
```

## Configuration

| Option | Behavior |
|---|---|
| `MaxItems <= 0` | Unlimited entries; no LRU eviction. |
| `MaxItems > 0` | LRU eviction keeps the cache at or below this size. |
| `CleanupInterval <= 0` | No background worker; expired keys are removed lazily on read. |
| `CleanupInterval > 0` | Cleanup worker starts in `New` and sweeps on that interval. |

Behavior notes:

- `Set` clears any previous TTL.
- `SetWithTTL` requires a positive duration and returns `cache.ErrInvalidTTL` otherwise.
- Expired entries are never returned.
- `StartCleanupWorker` and `StopCleanupWorker` are idempotent and support restart.

## Build and Run

```sh
go build ./...
go run ./cmd/cache
```

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

See the phase documentation for architecture, lifecycle diagrams, and recorded
results:

- [Phase 1 - Core Cache Engine](docs/phase-1-core-cache.md)
- [Phase 2 - Production Cache Features](docs/phase-2-production-features.md)

## Roadmap

1. ✅ Core Cache Engine
2. ✅ Production Cache Features (TTL, cleanup, LRU, limits)
3. Networked Cache Server
4. Multi-Node Cluster
5. Replication
6. Failover
7. Consistent Hashing
8. Observability
9. Chaos Testing
10. Benchmarking and Documentation
