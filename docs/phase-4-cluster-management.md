# Phase 4 - Cluster Management

## Overview

Phase 4 adds cluster membership to the distributed cache. Each cache process can
run a cluster node that discovers peers, joins an existing cluster, leaves
gracefully, exchanges metadata, detects failures, and converges on a shared
membership view.

The implementation is a standalone `internal/cluster` package. It is not yet
wired into the cache server or data path; replication, routing, and failover
build on this membership layer in later phases.

## Architecture

```text
                 +-------------------+
                 |     Cluster A     |
                 |                   |
  join / leave   |  registry         |
  heartbeat      |  tombstones       |
  gossip         |  health tracker   |
                 +---------+---------+
                           |
                 length-prefixed JSON
                    over TCP RPC
                           |
      +--------------------+--------------------+
      |                                         |
+-----v--------+                         +------v-------+
|  Cluster B   |                         |  Cluster C   |
|  full-mesh   |<---- gossip / probes --->|  full-mesh   |
|  gossip      |                         |  gossip      |
+--------------+                         +--------------+
```

Each member listens on its own cluster TCP address. Every join, heartbeat,
gossip, and leave operation is one request/response RPC over a short-lived TCP
connection. The cluster is a full mesh: every node knows and probes every other
node.

## Package Layout

| File | Responsibility |
|---|---|
| `internal/cluster/node.go` | `Node`, `NodeID`, health states, normalization |
| `internal/cluster/registry.go` | Thread-safe upsert, lookup, removal, sorted listing |
| `internal/cluster/membership.go` | Configuration, lifecycle, join/leave, metadata |
| `internal/cluster/heartbeat.go` | `I_AM_ALIVE` loop, liveness refresh, health transitions |
| `internal/cluster/gossip.go` | Wire protocol, TCP transport, message handling, anti-entropy |

## Node Model

```go
type Node struct {
    ID       NodeID
    Address  string
    Status   Status
    JoinedAt time.Time
    LastSeen time.Time
    Version  uint64
}
```

- `ID` identifies one member for the lifetime of the cluster.
- `Address` is the member's cluster RPC endpoint and must be dialable by peers.
- `JoinedAt` records when the member joined.
- `LastSeen` is the most recent evidence that the member was alive.
- `Version` increases when membership metadata or health changes. It is used to
  reject stale gossip and to let a restarted node rejoin above its tombstone.

`Address` is the cluster transport address, not a cache client address. Later
phases can add a separate cache endpoint when nodes route data between each
other.

## Health States

| State | Meaning |
|---|---|
| `ONLINE` | Observed within `DegradedAfter`. |
| `DEGRADED` | No observation for at least `DegradedAfter`, but less than `OfflineAfter`. |
| `OFFLINE` | No observation for at least `OfflineAfter`. |

Health is derived from `LastSeen`:

```text
age < DegradedAfter           -> ONLINE
DegradedAfter <= age < OfflineAfter -> DEGRADED
age >= OfflineAfter           -> OFFLINE
```

Direct contact through a successfully completed heartbeat or gossip RPC is
treated as proof of life and moves a node back to `ONLINE`. A node that crashes
without leaving stays in the membership view as `OFFLINE` so operators can see
that it failed. A node that calls `Leave` is removed from the view entirely.

## Membership Lifecycle

### Start

`Start` binds the cluster listener, registers the local node, and launches the
heartbeat and gossip loops. `Addr` returns the bound address, which is useful
with `127.0.0.1:0` for tests and local clusters.

### Join

A node joins by contacting one or more seed addresses:

```go
if err := b.Join(a.Addr().String()); err != nil {
    log.Fatal(err)
}
```

The seed:

1. Validates the cluster ID and sender identity.
2. Registers the joining node as `ONLINE`.
3. Raises the joining node's version above any old tombstone.
4. Returns its complete membership snapshot and tombstones.

The joining node merges the snapshot, adopts any version the seed assigned to
it, and triggers an immediate gossip round. Membership spreads transitively, so
a new node only needs one reachable seed. If several seeds are supplied, `Join`
succeeds when at least one responds successfully.

### Heartbeats

Every `HeartbeatInterval`, each node:

1. Refreshes its own `LastSeen`.
2. Recomputes peer health from `LastSeen`.
3. Sends `i_am_alive` to every known peer concurrently.

A successful response refreshes that peer's `LastSeen` and marks it `ONLINE`.

### Gossip

Every `GossipInterval`, and immediately after membership changes, a node sends
its full membership snapshot to every known peer. Responses contain the peer's
snapshot, so one exchange is push-pull anti-entropy:

```text
node A                                  node B
  |---- gossip(nodes, tombstones) ------->|
  |<--- gossip_ack(nodes, tombstones) ----|
  |                                       |
  merge B's view                merge A's view
```

Merging prefers the most recent observation, then the higher version, then the
health rank (`ONLINE` > `DEGRADED` > `OFFLINE`). That ordering lets the freshest
direct evidence win while still resolving ties deterministically.

### Leave

`Leave` announces a graceful exit to every known peer. Each peer removes the
node and records a tombstone containing the node's version. The leaving node
then stops its loops and listener.

Tombstones prevent delayed gossip from resurrecting a node that already left. A
later join with a higher version clears the tombstone and revives the node. If a
leave notification is lost, peers still detect the stopped node through missed
heartbeats and mark it `OFFLINE`.

### Stop

`Stop` shuts down the node without announcing a leave. It is idempotent, safe to
call concurrently, and cannot be followed by `Start`. This distinction lets
tests simulate crashes and lets callers choose between graceful shutdown and an
abrupt stop.

## Wire Protocol

Messages are length-prefixed JSON over TCP:

```text
+----------------+-----------------------------+
| 4-byte length  | JSON payload                |
| big endian     | (at most MaxMessageSize)    |
+----------------+-----------------------------+
```

| Type | Direction | Purpose |
|---|---|---|
| `join` | joiner -> seed | Request registration and a membership snapshot. |
| `join_ack` | seed -> joiner | Membership snapshot and tombstones. |
| `i_am_alive` | node -> peer | Liveness probe. |
| `i_am_alive_ack` | peer -> node | Liveness confirmation. |
| `gossip` | node -> peer | Push-pull membership and health exchange. |
| `gossip_ack` | peer -> node | Reciprocal snapshot. |
| `leave` | leaving node -> peer | Graceful removal announcement. |
| `leave_ack` | peer -> leaving node | Removal confirmation. |
| `error` | peer -> node | Protocol or validation error. |

Every message carries the cluster ID, the sender's `Node`, and metadata. The
receiver rejects a mismatched cluster ID, a missing or self sender, unknown
message types, oversized frames, and malformed JSON.

## Metadata

```go
type Metadata struct {
    ClusterID string
    NodeCount int
    Version   uint64
}
```

- `ClusterID` isolates unrelated clusters.
- `NodeCount` is the size of the local membership view, including `OFFLINE`
  members.
- `Version` is the local view revision. It advances when membership or health
  changes locally and is exchanged in every message for observation and
  diagnostics.

`Cluster.Metadata()` exposes the local snapshot without leaking internal state.

## Configuration

```go
c, err := cluster.New(cluster.Config{
    ClusterID:         "cache-prod",
    NodeID:            "node-a",
    Address:           "127.0.0.1:7001",
    HeartbeatInterval: time.Second,
    GossipInterval:    2 * time.Second,
    DegradedAfter:     3 * time.Second,
    OfflineAfter:      10 * time.Second,
    RequestTimeout:    2 * time.Second,
    ShutdownTimeout:   5 * time.Second,
    MaxMessageSize:    1 << 20,
})
```

`ClusterID` and `NodeID` are required. Zero timing and size values use
`DefaultConfig`. `OfflineAfter` must be greater than `DegradedAfter`.

## Running Multiple Nodes

```go
start := func(id, address string) *cluster.Cluster {
    c, err := cluster.New(cluster.Config{
        ClusterID: "demo",
        NodeID:    cluster.NodeID(id),
        Address:   address,
    })
    if err != nil {
        log.Fatal(err)
    }
    if err := c.Start(); err != nil {
        log.Fatal(err)
    }
    return c
}

a := start("a", "127.0.0.1:7001")
b := start("b", "127.0.0.1:7002")
defer a.Stop()
defer b.Stop()

if err := b.Join(a.Addr().String()); err != nil {
    log.Fatal(err)
}
```

After convergence, `a.Nodes()` and `b.Nodes()` both contain nodes `a` and `b`.
Leave membership changes propagate to all members through gossip.

## Tests

The cluster package uses white-box tests for internal merge and heartbeat logic,
and the `tests` package uses real TCP for multi-node behavior.

`internal/cluster`:

- Registry upsert, removal, sorted listing, copies, and invalid input
- Health-age boundaries and conflict-resolution rules
- Frame encoding, decoding, and size limits
- Join metadata exchange and request validation
- Tombstone resurrection prevention and fresher-observation merging
- Heartbeat self-refresh, peer recovery, and stopped-node behavior
- Three-node convergence with simulated delay and packet loss, followed by
  offline detection

`tests`:

- Config validation and lifecycle
- Two-node join and metadata convergence
- Three-node convergence through transitive gossip
- Graceful leave with tombstone stability
- Crash detection through `DEGRADED` and `OFFLINE`
- Rejoin after leave with version advancement
- Cluster-ID isolation and unreachable seeds

Run just the cluster API tests:

```sh
go test ./internal/cluster ./tests -run "Cluster|Registry|Status|Heartbeat|Merge|Frame|Handle"
```

## Verification

Phase 4 passes:

```sh
go vet ./...
go build ./...
go test ./...
go test -race ./...
```

The tests exercise real TCP joins, heartbeats, gossip, leave notifications,
failure detection, and a simulated lossy transport.

## Out of Scope

Phase 4 does not include:

- Cache data replication or sharding
- Request routing between nodes
- Leader election or failover
- Authentication, authorization, or TLS
- Persistent membership or incarnation numbers across process restarts
- Tombstone garbage collection
- Partial-mesh or randomized gossip for very large clusters

The full-mesh design is appropriate for small clusters and keeps convergence
behavior easy to reason about. Later phases can replace peer selection and add
durable identity without changing the public membership API.
