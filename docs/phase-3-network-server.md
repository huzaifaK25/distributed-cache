# Phase 3 - Network Server

## Overview

Phase 3 turns the in-memory cache into a standalone TCP service. Clients send
newline-delimited commands and receive one-line responses. The cache engine
from Phases 1 and 2 is unchanged; the new `internal/server` package owns
networking, parsing, concurrency, limits, and lifecycle.

## Architecture

```text
TCP clients
    |
    v
net.Listener (accept loop)
    |
    v
connection goroutine  ---> bounded request queue
 (one per client)                 |
    |                             v
    |                       fixed worker pool
    |                             |
    |                             v
    `------------------------> cache.Cache
                                  |
                                  v
                            InMemoryCache
```

- The accept loop is single-threaded and only registers connections.
- One goroutine per connection allows thousands of clients, bounded by
  `MaxConnections`.
- Request execution is handed to a fixed worker pool with a bounded queue, so
  overload returns `ERR server busy` instead of growing memory without limit.
- The cache remains internally thread-safe, so workers can execute concurrently.

## Package Layout

| File | Responsibility |
|---|---|
| `internal/server/server.go` | Lifecycle, listener, connection registry, stats, shutdown |
| `internal/server/config.go` | Defaults and configuration |
| `internal/server/connection.go` | Per-client reads, writes, deadlines, cleanup |
| `internal/server/protocol.go` | Command types and parser |
| `internal/server/handler.go` | Command-to-cache mapping and responses |
| `internal/server/workerpool.go` | Fixed workers and bounded job queue |

## Protocol

The protocol is line-oriented. Commands are case-insensitive, lines end with
`\n` (an optional preceding `\r` is accepted), and each response is one line.

| Request | Response | Notes |
|---|---|---|
| `PING` | `PONG` | Liveness check. |
| `SET key value` | `OK` | Value is the rest of the line, so spaces are preserved. |
| `GET key` | value or `NOT_FOUND` | Values are rendered as text. |
| `DEL key` | `OK` | Deleting a missing key is not an error. |
| `STATS` | `hits=... misses=... sets=... deletes=... expired=... evictions=...` | Cache counters. |

Errors use the form `ERR <message>`:

```text
ERR unknown command "NOPE"
ERR usage: GET <key>
ERR usage: SET <key> <value>
ERR empty command
ERR request too large
ERR server busy
```

Validation rules:

- `PING` and `STATS` take no arguments.
- `GET` and `DEL` take exactly one non-empty key.
- `SET` requires a non-empty key and value; the value may contain spaces.
- Unknown commands and malformed argument counts keep the connection open.
- A request line larger than `MaxRequestSize` receives `ERR request too large`
  and the connection is closed because the line cannot be resynchronized.

Values are single-line strings over the network. If the cache is shared with
embedded code that stores another type, `GET` formats it with `fmt.Sprint`.

## Request Path

```text
read line
   |
   v
parse command ---- invalid ----> ERR ...
   |
   v
submit to worker pool ---- queue full ----> ERR server busy
   |
   v
cache operation
   |
   v
write response
```

The job result channel is buffered, so a worker never blocks if the client
disconnects while the operation is completing.

## Connection Management

Each connection records:

- Remote address
- Connection time
- Last activity time
- Read and write deadlines

`Server.Connections()` exposes a snapshot for diagnostics, while `Server.Stats()`
reports active, total, rejected, and request counts.

Limits and timeouts:

- `MaxConnections` bounds registered clients. Extra clients receive
  `ERR too many connections` and are closed.
- `IdleTimeout` closes a client that sends no command in time.
- `WriteTimeout` prevents a slow reader from blocking a connection goroutine.
- `MaxRequestSize` bounds a single command line.
- Connection cleanup is deferred, so every disconnect path unregisters the
  connection and releases its slot.

## Worker Pool

`Start` creates a pool with `WorkerCount` goroutines and a `QueueSize` job
channel. `New` allocates no workers, so a failed bind does not leak goroutines.

- `Submit` never blocks; a full queue returns `ERR server busy`.
- Workers recover panics so one bad request cannot kill the server.
- `Stop` closes the queue, drains queued jobs, and waits for workers.
- Because the queue is bounded and the connection count is bounded, goroutine
  and memory growth stay bounded under overload.

## Graceful Shutdown

`Stop` performs these steps:

1. Mark the server as stopping.
2. Close the listener so no new connections are accepted.
3. Wake blocked connection reads with an expired read deadline.
4. Wait for in-flight requests to finish and write their responses.
5. Close remaining connections and drain the worker pool.
6. Record the result and wake any concurrent `Stop` callers.

If work does not finish within `ShutdownTimeout`, connections are force-closed
and `ErrShutdownTimeout` is returned. `Stop` is idempotent. A server instance
cannot be restarted after shutdown.

## Configuration

```go
s := server.New(c, server.Config{
    Address:         "127.0.0.1:6379",
    MaxConnections:  4096,
    WorkerCount:     16,
    QueueSize:       4096,
    IdleTimeout:     5 * time.Minute,
    WriteTimeout:    10 * time.Second,
    ShutdownTimeout: 10 * time.Second,
    MaxRequestSize:  64 * 1024,
})
if err := s.Start(); err != nil {
    log.Fatal(err)
}
defer s.Stop()
```

Zero values are replaced by `DefaultConfig` values. The default listen address
is `127.0.0.1:6379`; use `0.0.0.0:6379` to listen on all interfaces.

## Running

```sh
go run ./cmd/cache -addr 127.0.0.1:6379 -workers 16 -max-conns 4096
```

Useful flags:

| Flag | Purpose |
|---|---|
| `-addr` | Listen address. |
| `-workers`, `-queue-size` | Worker pool size and queue limit. |
| `-max-conns` | Simultaneous client limit. |
| `-idle-timeout`, `-write-timeout`, `-shutdown-timeout` | Timeouts. |
| `-max-request-size` | Maximum command-line bytes. |
| `-max-items` | Cache entry limit; `<=0` means unlimited. |
| `-cleanup-interval` | Expired-key sweep interval; `<=0` disables it. |

The process exits gracefully on `Ctrl+C` or `SIGTERM`.

## Tests

`internal/server/protocol_test.go` covers valid commands, case handling,
whitespace, values containing spaces, and malformed input.

`internal/server/workerpool_test.go` covers concurrency limits, queue
saturation, panic recovery, queue draining, and submission after shutdown.

`tests/server_test.go` and `tests/server_connection_test.go` cover:

- `PING`, `SET`, `GET`, `DEL`, and `STATS`
- CRLF framing and values with spaces
- Protocol errors without desynchronizing the connection
- 100 concurrent clients
- 1000 simultaneous connections
- Connection limit rejection
- Idle timeout cleanup
- Oversized request rejection
- Start/Stop lifecycle and idempotency

`tests/server_shutdown_test.go` verifies that an in-flight request completes
before `Stop` returns, idle connections close promptly, and concurrent `Stop`
calls are safe.

## Benchmarks

Run:

```sh
go test ./tests -run "^$" -bench "^BenchmarkServer" -benchmem
```

Environment: Windows `amd64`, Go 1.27.3, Intel Core i5-8365U (4 logical CPUs).
Values are medians of three runs.

| Benchmark | Time | Memory | Allocs |
|---|---:|---:|---:|
| `BenchmarkServerPing` | 65.8 us/op | 256 B/op | 9 |
| `BenchmarkServerPingParallel` | 26.2 us/op | 257 B/op | 9 |
| `BenchmarkServerSetGet` | 149 us/op | 608 B/op | 20 |
| `BenchmarkServerConnection` | 645 us/op | 11,002 B/op | 52 |

Interpretation:

- `BenchmarkServerPing` is one client issuing sequential round trips, roughly
  15,000 requests/second.
- `BenchmarkServerPingParallel` uses multiple clients and reaches roughly
  38,000 aggregate requests/second in this environment.
- `BenchmarkServerSetGet` measures one SET plus one GET per iteration.
- `BenchmarkServerConnection` dials, performs one PING, and closes, measuring
  connection setup cost (about 1,500 connections/second).

These numbers are loopback measurements on a laptop, not production capacity.
Network latency, TLS, payload size, cache hit rate, and client concurrency will
change them substantially.

## Out of Scope

Phase 3 does not include clustering, replication, failover, consistent hashing,
persistence, authentication, TLS, or cross-node routing. Those arrive in later
phases.
