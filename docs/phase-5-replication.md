# Phase 5 - Replication

## Objective

Prevent data loss by keeping each cached key on more than one node.

Phase 5 adds a standalone `internal/replication` layer with a primary that
durably logs and streams mutations, replicas that apply them in order, full and
incremental synchronization, acknowledgements, and recovery from durable state.
Wiring replication into the cache server and cluster discovery is left for later
phases, matching how Phase 4's membership layer was introduced.

## Architecture

```text
                 writes
                   |
             +-----v------+
             | Coordinator|  primary
             |------------|
             | Replicator |----> Cache
             | WAL        |
             +-----+------+
                   | append / snapshot
         TCP       |
   +---------------+---------------+
   |                               |
+--v-----------+             +-----v--------+
| Coordinator  |  replica    | Coordinator  |  replica
| Replicator   |             | Replicator   |
| WAL -> Cache |             | WAL -> Cache |
+--------------+             +--------------+
```

| File | Responsibility |
|---|---|
| `replicator.go` | Roles, mutation model, local WAL-backed state machine |
| `wal.go` | Checksummed append-only log, recovery, range reads, snapshot reset |
| `syncer.go` | Wire framing, snapshot chunking, replica client loop |
| `coordinator.go` | Configuration, lifecycle, primary API, metrics |
| `session.go` | Primary-side replica sessions, bootstrap, acks, live streaming |
| `session.go` | Primary-side replica sessions, bootstrap, acks, live streaming |

`cache.InMemoryCache` has two replication-only helpers: `Snapshot` copies live
entries with remaining TTLs, and `Restore` atomically replaces its contents from
a snapshot.

## Roles

- A **primary** accepts `Set`, `SetWithTTL`, `Delete`, and `Expire`. Every write
  is durably logged before it is applied locally and published to replicas.
- A **replica** accepts no writes (`ErrNotPrimary`). It connects to one primary,
  bootstraps from a snapshot, applies ordered deltas, and acknowledges each
  applied batch.

Each process runs one `Coordinator`. Primary and replica coordinators use the
same local store and WAL implementation; only the network direction differs.

## Mutation Model

```go
type Mutation struct {
    Seq       uint64          // Global primary sequence.
    Op        Op              // SET, DEL, or TTL.
    Key       string
    Value     json.RawMessage // JSON value for SET/TTL.
    ExpiresAt int64           // Unix nanoseconds; zero means no expiration.
    Timestamp int64           // Unix nanoseconds when the write was committed.
}
```

`Expire` is implemented as a `TTL` mutation carrying the current value and a new
absolute expiration. Values are canonicalized through JSON on every node, so
values must be JSON-serializable and numbers decode as `float64`. Absolute
expiration times avoid extending TTLs through network delay or clock skew.

Every mutation has a strictly increasing sequence. A replica applies `Seq ==
local+1`, ignores sequences it already has, and treats a gap as a reason to
request a fresh full snapshot.

## Write-Ahead Log

The WAL is an append-only file:

```text
"DCWAL1\n" | record | record | ...
record     = uint32 length | uint32 CRC-32 | JSON payload
```

Two record kinds exist:

- **mutation** — one ordered cache change.
- **snapshot** — a full state basis written only when a replica resets its log.

Every append is fsynced before the write is acknowledged to the caller. On
open, the WAL validates each record and truncates an incomplete or corrupt
trailing record left by a crash. A malformed record marks the recovery
boundary; records after it are discarded.

`Reset` replaces the WAL atomically through a temporary file. Replicas use it
after a full snapshot, so a restart can rebuild both the snapshot basis and all
subsequent deltas without contacting the primary first.

The primary does not compact its WAL yet. It grows for the lifetime of the data
directory; future phases can add snapshot-based compaction.

## Replication Flow

### Primary write

1. Validate the operation and encode the value as JSON.
2. Assign the next sequence and verify it fits in one transport frame.
3. Append to the WAL and fsync.
4. Apply the mutation to the local cache.
5. Publish it to every registered replica session in sequence order.

A write mutex spans steps 2–5, while each per-replica queue is bounded. If a
replica cannot keep up and its queue fills, the session is closed; the replica
reconnects and catches up from the WAL instead of silently losing a write.

### Full synchronization

1. A replica sends `hello` with its last applied sequence.
2. The primary registers the session before capturing state, so concurrent
   writes are buffered during bootstrap.
3. The primary snapshots its cache and sequence atomically, then sends
   `snapshot_start`, bounded `snapshot_chunk` frames, and `snapshot_end`.
4. The replica restores the cache, resets its WAL to the snapshot sequence, and
   acknowledges the snapshot.
5. Buffered and later mutations stream as append batches.

Full synchronization is used for a new replica, a replica whose sequence is
ahead of the primary, or a replica that cannot obtain a contiguous WAL range.

### Incremental synchronization

After bootstrap, a replica reconnects with its durable sequence. The primary
registers the live session, captures the current sequence, streams the WAL range
`last+1..current` in batches, and then serves the live queue. Duplicate mutations
caused by the registration overlap are safe because replicas deduplicate by
sequence.

### Acknowledgements and retries

The replica acknowledges the highest sequence it has durably applied. Acks are
cumulative, so one ack covers a whole batch. The primary waits per batch and
resends on timeout; a duplicate batch is applied as a no-op and acknowledged
again. After the configured retry count, the session is dropped and the replica
reconnects.

## Wire Protocol

Messages are length-prefixed JSON over TCP. The primary listens; replicas dial.

| Message | Direction | Purpose |
|---|---|---|
| `hello` | replica → primary | Identity, last durable sequence, force-full flag |
| `snapshot_start` | primary → replica | Begin a full snapshot at a sequence |
| `snapshot_chunk` | primary → replica | A bounded set of snapshot entries |
| `snapshot_end` | primary → replica | Finish the snapshot; replica commits and acks |
| `append` | primary → replica | One ordered batch of mutations |
| `ack` | replica → primary | Highest durable replicated sequence |
| `error` | either → either | Protocol or validation failure |

`MaxMessageSize` bounds every frame. Snapshot chunks and append batches are
split to fit, including batches whose value count is small but encoded size is
large.

## Metrics

`Coordinator.Stats()` aggregates:

| Field | Meaning |
|---|---|
| `Seq` | Highest durable local sequence |
| `Delivered` | Mutations acknowledged by replicas |
| `Pending` | Queued plus in-flight mutations awaiting ack |
| `Failed` | Failed send attempts and queue overflows |
| `QueueDepth` | Buffered live mutations across all replicas |
| `ReplicationLag` | Age of the oldest pending replication work |
| `SyncTime` | Duration of the most recent full synchronization |
| `Replicas` | Connected replica sessions |

`Coordinator.Replicas()` returns per-replica ID, remote address, acked sequence,
pending/delivered/failed counters, lag, and last sync time.

## Configuration

Zero values use `replication.DefaultConfig`.

| Option | Default | Behavior |
|---|---:|---|
| `Role` | required | `PRIMARY` or `REPLICA` |
| `ID` | required | Stable unique node identity |
| `Store` | required | Cache implementing `Snapshot`/`Restore` |
| `WALPath` | required | Durable log file |
| `ListenAddr` | `127.0.0.1:0` | Primary replication listener |
| `PrimaryAddr` | replica required | Upstream primary address |
| `ReconnectMin` | `100ms` | Initial replica retry delay |
| `ReconnectMax` | `2s` | Maximum replica retry delay |
| `HandshakeTimeout` | `5s` | Dial and hello deadline |
| `AckTimeout` | `5s` | Frame write and batch ack deadline |
| `MaxRetries` | `3` | Batch and snapshot attempts |
| `MaxBatch` | `64` | Mutations per append message |
| `SessionQueue` | `4096` | Buffered live mutations per replica |
| `SnapshotBatch` | `256` | Entries per snapshot chunk before size splitting |
| `MaxMessageSize` | `8 MiB` | Maximum encoded frame size |
| `ShutdownTimeout` | `5s` | Bound on coordinator shutdown |

## Usage

```go
store := cache.New(cache.Config{})

primary, err := replication.New(replication.Config{
    Role:     replication.RolePrimary,
    ID:       "node-a",
    Store:    store,
    WALPath:  "data/node-a.wal",
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

Reads are served from the local `Store`; the coordinator controls writes and
replication.

## Recovery Behavior

| Failure | Recovery |
|---|---|
| Primary restarts | WAL replay restores cache and sequence; replicas reconnect incrementally |
| Replica restarts | Snapshot record plus later mutations rebuild state; reconnect is incremental |
| Replica falls behind | Disconnect; reconnect and stream the missing WAL range |
| Primary lost its log | Replica sequence ahead of primary triggers a full snapshot |
| Ack lost after apply | Primary retries; replica skips applied sequences and acks again |
| Queue overflow | Session closes; reconnect catches up from the WAL |
| Torn WAL tail | Open truncates the incomplete record and continues from the last good sequence |

## Tests

- WAL append/replay, sequence rules, range reads, torn-tail repair, snapshot reset.
- Cache snapshot ordering, TTL preservation, replacement, and limits.
- Real-TCP primary/replica SET, DEL, TTL, and Expire replication.
- Multiple replicas and concurrent writes.
- Replica restart with WAL replay and incremental catch-up.
- Primary restart with WAL replay and sequence continuation.
- Integration coverage in `tests/replication_test.go`.

## Limitations and Next Steps

- One primary per dataset; automatic failover and leader election belong to
  Phase 6.
- The primary WAL is not compacted.
- Full snapshots are materialized in memory on both sides, and one WAL snapshot
  record is capped at 64 MiB.
- Replication is not yet connected to cluster membership, the TCP cache server,
  or routing. Replicas are configured with an explicit primary address.
- Values must be JSON-serializable; they are canonicalized on every node.

## Acceptance Criteria

- [x] Writes replicated
- [x] Replicas recover
- [x] WAL operational
- [x] Synchronization working
- [x] Consistent cluster state
