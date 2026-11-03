# Phase 1 - Core Cache Engine

> Historical note: Phase 2 replaced the RWMutex with a plain Mutex because LRU
> tracking makes every `Get` a mutation. It also added TTL, cleanup, and
> eviction. See [Phase 2](phase-2-production-features.md) for the current design.

## Overview

Phase 1 implements a standalone, thread-safe in-memory cache library. It is the
foundation that later phases extend with TTL, eviction, networking, replication,
and clustering.

The cache contract lives in `internal/cache/cache.go`; the implementation lives
in `internal/cache/memory.go`.

## Architecture

```text
Application
    |
    v
cache.Cache (interface)
    |
    v
cache.InMemoryCache
    |-- sync.RWMutex ------> map[string]any
    `-- atomic counters ---> CacheStats
```

- `cache.Cache` defines the backend contract and uses errors so future remote
  backends can report failures.
- `cache.InMemoryCache` owns the map and all statistics.
- `sync.RWMutex` protects the map.
- Counters live outside the map lock and are updated atomically.

## Thread Safety

`InMemoryCache` uses one `sync.RWMutex`.

- `RLock` is a shared lock. Any number of goroutines can hold it and read the
  map concurrently.
- `Lock` is exclusive. A writer blocks every reader and every other writer.
- Go's `RWMutex` blocks new readers while a writer is waiting, so a continuous
  stream of reads cannot starve writes indefinitely.

This fits a cache because reads normally outnumber writes. The cost is that a
`RWMutex` is slightly heavier than a plain `Mutex` for write-heavy workloads;
that trade-off is revisited if later phases need sharding.

Locks are held only for the map operation. Hit and miss counters use
`atomic.Uint64`, so `Get` does not need a write lock to update statistics and
readers stay concurrent.

The cache stores values as `any` without deep-copying them. Storing a pointer,
map, or slice means callers share that value with the cache; callers must not
mutate shared values concurrently.

## Operations

| Method | Lock | Behavior |
|---|---|---|
| `Set` | write | Inserts or replaces a value, then increments `Sets`. Returns `nil`. |
| `Get` | read | Returns `(value, true)` or `(nil, false)`. Increments `Hits` or `Misses`. |
| `Delete` | write | Removes the key and increments `Deletes`. Returns `nil`. |
| `Stats` | none | Returns a lock-free snapshot of the counters. |

`Delete` is idempotent: deleting a missing key succeeds and still increments
`Deletes`. The counter therefore tracks delete requests, not only successful
removals.

## Statistics

| Counter | Meaning |
|---|---|
| `Hits` | `Get` calls that found a value. |
| `Misses` | `Get` calls that did not find a value. |
| `Sets` | `Set` calls that stored a value. |
| `Deletes` | `Delete` calls, including deletes of missing keys. |

Counters are cumulative for the lifetime of the cache and are never reset.

`Stats` returns a value copy, so callers cannot mutate cache state. Each counter
is loaded atomically, but the four loads are not a single transaction; under
concurrent load a snapshot can be slightly skewed across counters.

## Tests

`tests/cache_test.go` covers:

- Set, overwrite, Get, missing keys, nil values, and Delete.
- Exact statistics updates and independent snapshots.
- 100 concurrent writers and 100 concurrent readers with final state and counter
  verification.

`tests/benchmark_test.go` covers:

- `BenchmarkSet`
- `BenchmarkGet`
- `BenchmarkMixedReadWrite` (90% reads, 10% writes)

## Benchmark Results

Environment: Windows `amd64`, Go 1.27.3, Intel Core i5-8365U (4 logical CPUs).
Values are the median of three runs of:

```sh
go test ./tests -run "^$" -bench . -benchmem -count=3
```

| Benchmark | Time | Memory | Allocs |
|---|---:|---:|---:|
| `BenchmarkSet` | 175.3 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkGet` | 98.90 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkMixedReadWrite` | 112.5 ns/op | 0 B/op | 0 allocs/op |

These are single-goroutine measurements. Actual throughput depends on hardware,
key distribution, and concurrent access patterns.

## Out of Scope

Phase 1 intentionally excludes TTL expiration, eviction, persistence,
networking, replication, clustering, and consistent hashing.
