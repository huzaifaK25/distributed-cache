# Distributed Cache

# Phase 3 - Network Server

## Objective

Transform the cache from an embedded library into a standalone network-accessible service.

At the end of this phase clients should be able to connect over TCP and execute cache commands remotely.

---

# Success Criteria

The cache must:

- Accept TCP connections
- Handle concurrent clients
- Parse commands
- Execute requests safely
- Return responses
- Support graceful shutdown
- Support connection limits

---

# Expected Repository Structure

internal/

├── cache/
├── server/
│ ├── server.go
│ ├── handler.go
│ ├── protocol.go
│ ├── connection.go
│ └── config.go

---

# Task 1 - Create Server Package

Files:

server.go
config.go

Implement:

type Server struct

Required fields:

- address
- cache
- listener
- workerPool

---

# Task 2 - TCP Listener

Implement:

Start()

Stop()

Requirements:

- Listen on TCP port
- Accept connections
- Reject new connections during shutdown

---

# Task 3 - Connection Management

File:

connection.go

Track:

- connection count
- remote address
- idle timeout

Implement cleanup logic.

---

# Task 4 - Protocol Definition

File:

protocol.go

Commands:

PING
SET
GET
DEL
STATS

Examples:

PING
→ PONG

SET user:1 hello
→ OK

GET user:1
→ hello

---

# Task 5 - Request Parser

Convert raw TCP messages into command structs.

Must validate:

- argument count
- command type
- malformed input

---

# Task 6 - Request Handler

File:

handler.go

Map commands to cache operations.

Handle errors gracefully.

---

# Task 7 - Concurrent Connections

Support:

1000+ connections

Test:

multiple simultaneous clients

---

# Task 8 - Worker Pool

Implement configurable worker pool.

Config:

WorkerCount

Prevent unbounded goroutine growth.

---

# Task 9 - Graceful Shutdown

Requirements:

- stop accepting new connections
- finish in-flight requests
- close listener
- free resources

---

# Task 10 - Tests

Add:

connection tests
protocol tests
multi-client tests

---

# Task 11 - Benchmarks

Measure:

- requests/sec
- connection throughput
- memory usage

---

# Acceptance Criteria

[ ] TCP server operational

[ ] Supports all commands

[ ] Handles multiple clients

[ ] Worker pool functioning

[ ] Graceful shutdown working

[ ] Benchmarks completed

[ ] Documentation completed
