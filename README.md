# Distributed Cache

A learning-focused distributed cache written in Go, built one phase at a time.
Each phase is implemented, tested, and documented before the next one begins.

## Phase 1 Status

Phase 1 is complete: a production-quality, thread-safe in-memory cache engine.

Completed features:

- `Set`, `Get`, `Delete`, and `Stats` operations
- Thread-safe storage using `sync.RWMutex`
- Lock-free hit, miss, set, and delete counters
- Unit and concurrency tests (100 writers and 100 readers)
- Race-detector coverage
- Sequential benchmarks for Set, Get, and mixed workloads
- Architecture and concurrency documentation

## Requirements

- Go 1.27 or newer

## Project Layout

```text
distributed-cache/
|-- cmd/cache/            # Demo entry point
|-- internal/cache/
|   |-- cache.go          # Cache interface
|   |-- memory.go         # InMemoryCache implementation
|   `-- stats.go          # CacheStats snapshot type
|-- tests/
|   |-- cache_test.go     # Unit and concurrency tests
|   `-- benchmark_test.go # Benchmarks
|-- docs/
|   `-- phase-1-core-cache.md
|-- go.mod
`-- README.md
```

## Usage

```go
package main

import (
	"fmt"
	"log"

	"distributed-cache/internal/cache"
)

func main() {
	c := cache.New()

	if err := c.Set("language", "Go"); err != nil {
		log.Fatal(err)
	}

	if value, found := c.Get("language"); found {
		fmt.Println(value) // Go
	}

	if err := c.Delete("language"); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", c.Stats())
}
```

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

See [Phase 1 documentation](docs/phase-1-core-cache.md) for architecture,
thread-safety details, statistics semantics, and recorded results.

## Roadmap

1. Core Cache Engine
2. Production Cache Features (TTL, eviction)
3. Networked Cache Server
4. Multi-Node Cluster
5. Replication
6. Failover
7. Consistent Hashing
8. Observability
9. Chaos Testing
10. Benchmarking and Documentation
