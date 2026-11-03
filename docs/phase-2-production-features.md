# Phase 2 - Production Cache Features

## Overview

Phase 2 extends the Phase 1 in-memory engine with TTL expiration, background
cleanup, a size limit, LRU eviction, and lifecycle management. The system
remains a single-node, in-memory cache.

## Architecture

```text
Application
    |
    v
cache.Cache (interface)
    |
    v
cache.InMemoryCache
    |-- sync.Mutex ---------> map[string]*CacheItem
    |                         LRU recency list
    |-- atomic counters ----> CacheStats
    `-- cleanup goroutine --> expired-key sweeper (optional)
```

- `map[string]*CacheItem` stores values and metadata.
- The LRU list tracks recent use; its index provides O(1) moves and removals.
- A single mutex guards the map and LRU list.
- Counters are atomic and can be snapshotted without the cache lock.
- The cleanup worker starts automatically when `CleanupInterval > 0`.

## Cache Item Lifecycle

```text
SET
 |
 v
Stored
 |
 v
Accessed
 |
 +--> Expires  (TTL elapsed)
 |
 `--> Evicted  (LRU size limit)
```

Each `CacheItem` stores:

| Field | Purpose |
|---|---|
| `Value` | Caller-provided value. |
| `CreatedAt` | Insertion or last-write time. |
| `ExpiresAt` | Expiration deadline; zero means no TTL. |
| `LastAccessed` | Last `Get` or `Set` time. |

## Expiration Flow

```text
Request
 |
 v
Check Expiration
 |
 v
Expired?
 |
 +-- No --> Update LRU / return value
 |
 `-- Yes -> Remove key -> increment Expired -> return miss
```

TTL rules:

- `Set` stores a value without expiration and clears any previous TTL.
- `SetWithTTL` replaces the value and TTL.
- `ttl <= 0` returns `ErrInvalidTTL` and does not store anything.
- Expired entries are never returned.
- A `Get` that finds an expired entry removes it immediately.

The `Expired` counter increments when an expired item is removed by either a
lazy read or the cleanup worker. Because removal is atomic under the cache
mutex, each item is counted once.

## Cleanup Worker

`New` starts the worker when `CleanupInterval` is positive. It wakes on a
ticker, scans the map, and removes every entry whose `ExpiresAt` has passed.

- `StartCleanupWorker` is idempotent.
- `StopCleanupWorker` closes a stop channel, waits for the goroutine to exit,
  and is safe to call repeatedly.
- A stopped worker can be started again.
- `CleanupInterval <= 0` disables background cleanup; lazy expiration on `Get`
  still works.

The sweep runs off the request path, but it holds the cache mutex while scanning
so it always sees a consistent map. The scan is O(n); very large caches may need
incremental or sharded cleanup in a later phase to keep lock pauses short.

## LRU and Size Limits

```text
Insert
 |
 v
Limit Reached?
 |
 +-- No --> Insert
 |
 `-- Yes -> Find LRU Entry -> Evict -> Insert New Entry
```

- The front of the LRU list is most recently used; the back is least recently
  used.
- `Get` and `Set` move the accessed key to the front.
- `Set` on an existing key updates the value without increasing the item count,
  so it never evicts.
- When inserting a new key at the limit, the least recently used entry is
  removed first.
- `MaxItems <= 0` means unlimited.
- `Evictions` increments for each size-driven removal.

All LRU operations are O(1): the map finds the item, and the list index finds
its node. The cache never exceeds `MaxItems` because insertion and eviction
happen under the same lock.

## Concurrency

Phase 1 used `sync.RWMutex` because reads did not mutate state. Phase 2 updates
`LastAccessed` and the LRU order on every `Get`, and may delete an expired key,
so a read is now a write internally. A plain `sync.Mutex` is simpler and avoids
the overhead of an exclusive lock disguised as an RWMutex.

- The cache mutex protects the map, `CacheItem` fields, and LRU list.
- The worker lifecycle mutex protects start/stop state only.
- Counters use `atomic.Uint64`, so `Stats` does not take the cache lock.
- A stats snapshot is individually atomic per counter, not a transaction across
  all counters.

## Configuration

```go
c := cache.New(cache.Config{
    MaxItems:        10_000,
    CleanupInterval: 30 * time.Second,
})
defer c.StopCleanupWorker()
```

| Option | Behavior |
|---|---|
| `MaxItems <= 0` | Unlimited entries; no eviction. |
| `MaxItems > 0` | LRU eviction keeps the cache at or below this size. |
| `CleanupInterval <= 0` | No background worker; expired keys are removed lazily. |
| `CleanupInterval > 0` | Worker starts in `New` and sweeps on that interval. |

## Tests

`tests/ttl_test.go` covers:

- Expiration and lazy misses
- Non-expired access
- TTL replacement and clearing
- Invalid TTL rejection
- Cleanup worker removal, shutdown, idempotent stop, restart, and disabled mode

`tests/eviction_test.go` covers:

- LRU eviction order
- `MaxItems` enforcement
- Recently accessed items surviving
- Existing-key updates not evicting
- Unlimited capacity

`tests/cache_test.go` retains the Phase 1 unit and 100-writer/100-reader
concurrency tests. `tests/benchmark_test.go` adds TTL and eviction benchmarks.

## Benchmark Results

Environment: Windows `amd64`, Go 1.27.3, Intel Core i5-8365U (4 logical CPUs).
Values are the median of three runs of:

```sh
go test ./tests -run "^$" -bench . -benchmem -count=3
```

| Benchmark | Time | Memory | Allocs |
|---|---:|---:|---:|
| `BenchmarkSet` | 215.3 ns/op | 1 B/op | 0 allocs/op |
| `BenchmarkGet` | 222.8 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkMixedReadWrite` | 262.8 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkTTLSet` | 298.8 ns/op | 1 B/op | 0 allocs/op |
| `BenchmarkTTLGet` | 265.4 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkEviction` | 1233 ns/op | 160 B/op | 3 allocs/op |
| `BenchmarkMixedWorkload` | 353.3 ns/op | 0 B/op | 0 allocs/op |

`BenchmarkEviction` is slower because every insertion removes an LRU node and
allocates a new one while the cache is at capacity. These are single-goroutine
measurements; concurrent throughput depends on lock contention and workload.

## Out of Scope

Phase 2 still excludes networking, clustering, replication, persistence, and
distributed coordination. Those arrive in later phases.
