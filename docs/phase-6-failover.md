# Phase 6 - Failover

## Objective

Keep the cache available when the primary fails. Phase 6 adds a standalone
`internal/failover` layer that elects one leader from a static set of nodes,
promotes that leader's replica into the replication primary, and removes it
again after a failure or a graceful handoff.

The layer is intentionally built above `internal/replication`: it does not
replace the WAL or the replication protocol. Instead, one `failover.Node` owns a
store and WAL and creates the appropriate replication coordinator as its role
changes. Wiring failover into the TCP cache server and cluster discovery remains
a later integration step.

## Architecture

```text
                   election RPCs (TCP, JSON)
        +------------------+------------------+
        |                  |                  |
  +-----v------+     +-----v------+     +-----v------+
  |  Follower  |     | Candidate  |     |   Leader   |
  |            |     |            |     |            |
  | replica    |     | election   |     | primary    |
  | coordinator|     | RPCs only  |     | coordinator|
  +-----+------+     +------------+     +-----+------+
        |                                     |
        +-------------------------------------+
                  replication WAL
```

| File | Responsibility |
|---|---|
| `state_machine.go` | `FOLLOWER`/`CANDIDATE`/`LEADER`, durable term/vote state, randomized timeouts |
| `detector.go` | Peer health tracking (`ALIVE`, `SUSPECT`, `DEAD`) from heartbeat observations |
| `election.go` | Control framing, request-vote and append-entries RPCs, campaigns, leadership transfer |
| `leader.go` | Node configuration and lifecycle, heartbeat/lease loop, replication promotion and demotion, quorum writes, stats |

## Roles and Terms

Every node is in one of three states:

- **FOLLOWER** waits for heartbeats and starts an election when its randomized
  timeout expires.
- **CANDIDATE** has incremented its term, voted for itself, and is requesting
  votes.
- **LEADER** has won a majority and owns the replication primary.

`term` and `votedFor` are persisted before a vote is granted or a campaign
starts. The state file is written through a temporary file, fsynced, and renamed
so a crash cannot leave a partial update. A malformed state file is a startup
error rather than a silent term reset, because resetting a term could permit two
votes in the same term.

A node must be a leader for a majority of the configured membership. Membership
is static: each node lists every other node in `Config.Peers`.

## Failure Detection

The detector records the time and sequence of every successful control RPC:

| Status | Condition |
|---|---|
| `ALIVE` | Successful contact within half the failure timeout |
| `SUSPECT` | No success for half the failure timeout |
| `DEAD` | No success for the full failure timeout |

The status is observational; it does not by itself remove a node or trigger an
election. Elections are driven by the election timer, so a node that is merely
slow still steps down when it cannot renew its quorum lease. `Node.Health()`
returns every peer plus the local node, sorted by ID, with the last seen time,
last observed sequence, and last error.

## Leader Election

Election follows the Raft safety rules:

1. A follower waits a random duration in
   `[ElectionTimeoutMin, ElectionTimeoutMax)`. Randomization avoids repeated
   split votes.
2. On timeout it becomes a candidate, increments its persistent term, votes for
   itself, and sends `request_vote` to every peer with its last durable
   sequence.
3. A voter grants a vote only when:
   - the request term is at least its current term;
   - it has not voted for a different candidate in that term; and
   - the candidate's last sequence is at least the voter's last sequence.
4. A candidate that receives a majority becomes leader for that term. A higher
   term in any response forces it back to follower.
5. A follower that receives `append_entries` from a leader at the same or a
   higher term adopts that leader and resets its election timer.

One vote per term makes two leaders in the same term impossible: both would need
the same voter's vote. A leader can only be replaced in a later term.

## Leader Lease and Split-Brain Protection

A leader counts itself and renews its lease when a majority of heartbeat RPCs
succeed. The lease lasts `ElectionTimeoutMin / 2`, which is shorter than the
minimum follower timeout. A partitioned leader therefore stops acting as leader
before the majority can elect a replacement.

A write is accepted only while all of these hold:

- the node is `LEADER` for the term that started the write;
- no leadership transfer is in progress;
- the quorum lease has not expired; and
- the replication primary endpoint is running.

Heartbeats include the leader's replication address and last sequence. A
follower that cannot renew contact starts an election. A candidate must have a
durable sequence at least as long as every voter's, so a candidate that is
missing an acknowledged suffix cannot win a majority. Equal-length divergent
suffixes are handled by the full resynchronization described below; per-entry
terms remain a documented limitation.

## Quorum Writes and Data Integrity

`Set`, `SetWithTTL`, `Delete`, and `Expire` on a leader wait for a replication
quorum before returning success:

```text
write -> leader WAL fsync -> local cache -> replicate
      -> wait for quorum-1 replica acknowledgements
      -> success
```

`replication.Coordinator.WaitForAck` counts only connected replicas whose
durable sequence has reached the write. If the quorum is not reached within
`WriteTimeout`, the call fails with `ErrNoQuorum` and the leader steps down. A
failed write may still exist locally, but it was never reported as acknowledged
and will be discarded when the node rejoins as a replica.

Because replication entries carry a flat sequence and no per-entry term,
failover's replica coordinators set `replication.Config.ForceFullSync`. The first
connection after a coordinator is created requests a full snapshot. This removes
any divergent suffix left by a former leader, including a suffix at the same
sequence as a new leader's entry. Reconnects within one replica coordinator can
still use incremental synchronization.

## Promotion and Demotion

`leader.go` runs a reconciliation loop over the desired replication mode:

| Election state | Replication mode |
|---|---|
| Leader | Primary listening on `ReplicationListenAddr` |
| Follower with a known leader address | Replica connected to that primary |
| Follower without a leader | No active coordinator |

Promotion is lazy: a new leader starts its primary listener, advertises the
bound address in its next heartbeat, and followers switch to replica mode. The
old primary stops accepting writes as soon as its role changes, then stops its
coordinator. The replication address is advertised only after the listener is
bound, so `127.0.0.1:0` and other ephemeral ports work.

A node that cannot start a primary listener steps down instead of remaining a
leader that cannot make durable progress. Failed endpoint transitions are
retried after `ReplicationRetry`.

## Leadership Transfer

`TransferLeadership(targetID)` performs a graceful handoff:

1. Verify the caller is the leader with a valid quorum lease.
2. Wait until the target's reported durable sequence is caught up.
3. Send `timeout_now` to the target, which starts an election immediately.
4. Keep the `transferring` marker set. The old leader does not campaign during
   this period, even after it steps down because it saw the target's higher
   term.
5. Wait until the target's heartbeat makes the old leader a follower of the
   target, or until `TransferTimeout`.

On success the old leader keeps its store and WAL and reconfigures as a replica.
On failure the marker is cleared and normal elections resume. A transfer does
not commit or replay data itself; the existing replication path resynchronizes
the old leader from the new primary.

## Recovery

| Failure | Recovery |
|---|---|
| Leader stops or is partitioned | Followers time out, elect a caught-up candidate, and start a new replication primary |
| Minority leader | Lease expires; it steps down, rejects writes, and cannot become leader until it reaches a majority |
| Node restarts | Persistent term/vote is loaded, the node follows heartbeats, and a replica coordinator full-syncs its store and WAL |
| Old leader rejoins | Follows the new leader, discards an unacknowledged suffix, and catches up |
| Target of transfer falls behind | Transfer waits for catch-up before sending `timeout_now` |
| Transfer target fails | `ErrTransferTimeout`; the old leader clears the marker and a normal election follows |
| Control RPC timeout | Recorded as a detector error; a quorum of successes is still required to renew the lease |

`Node.Stop` is idempotent and stops the control listener, all control
connections, and the active replication coordinator. It does not transfer
leadership; call `TransferLeadership` first for a graceful shutdown.

## Wire Protocol

Control messages use TCP with a 4-byte big-endian length followed by a JSON
object. Every frame is bounded by `MaxMessageSize`.

| Message | Direction | Purpose |
|---|---|---|
| `request_vote` | candidate → peer | Term, candidate ID, last durable sequence |
| `request_vote_response` | peer → candidate | Current term, vote result, last sequence |
| `append_entries` | leader → follower | Heartbeat, leader ID, replication address, last sequence |
| `append_entries_response` | follower → leader | Current term, success, last sequence |
| `timeout_now` | leader → target | Start an election immediately for graceful transfer |
| `timeout_now_response` | target → leader | Acceptance and current term |

Messages carry `cluster_id`; a mismatch is rejected. The control plane is
unauthenticated beyond that check and must run on a trusted network.

## Usage

All nodes use the same `ClusterID` and peer list. Each node has its own store and
WAL.

```go
store := cache.New(cache.Config{})
node, err := failover.New(failover.Config{
    ClusterID:   "cache-prod",
    ID:          "node-a",
    ListenAddr:  "127.0.0.1:7101",
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

// Writes must run on the elected leader and wait for a replication quorum.
if err := node.Set("user:1", "hello"); err != nil {
    log.Fatal(err)
}

// Reads are local and may be stale on a follower.
if value, ok := node.Get("user:1"); ok {
    fmt.Println(value)
}
```

`ReplicationListenAddr` should be reachable by the peers. The bound address is
advertised, so an ephemeral `127.0.0.1:0` works for local tests.

## Configuration

Zero values use `failover.DefaultConfig`.

| Option | Default | Behavior |
|---|---:|---|
| `ClusterID` | required | Must match across the failover cluster |
| `ID` | required | Stable unique node identity |
| `ListenAddr` | `127.0.0.1:0` | Control-plane TCP listen address |
| `Peers` | empty | Every other node; membership is static |
| `Store` | required | Cache implementing `replication.Store` |
| `WALPath` | required | Node-private replication WAL |
| `StatePath` | `WALPath + ".failover"` | Durable term/vote state |
| `ReplicationListenAddr` | `127.0.0.1:0` | Primary replication listener |
| `ReplicationConfig` | replication defaults | Tuning passed through to each coordinator |
| `ElectionTimeoutMin` / `Max` | `300ms` / `600ms` | Randomized follower timeout window |
| `HeartbeatInterval` | `100ms` | Leader heartbeat cadence |
| `FailureTimeout` | `max(3 x election max, 2s)` | Detector death threshold |
| `RPCTimeout` | `ElectionTimeoutMin / 3` | One control RPC deadline; must be under half the election timeout |
| `WriteTimeout` | `2s` | Quorum acknowledgement and primary-readiness deadline |
| `TransferTimeout` | `max(3 x election max, 3s)` | Graceful handoff deadline |
| `ReplicationRetry` | `200ms` | Retry delay for a failed endpoint transition |
| `ShutdownTimeout` | `5s` | Grace period before shutdown forces control connections closed |
| `MaxMessageSize` | `1 MiB` | Maximum encoded control message |

Validation rejects a missing cluster ID, node ID, store, or WAL path; self,
duplicate, or addressless peers; an inverted election window; an election
timeout below three heartbeat intervals; an RPC timeout at or above half the
minimum election timeout; and control messages below 1024 bytes.

## Statistics

`Node.Stats()` returns:

| Field | Meaning |
|---|---|
| `ID`, `Role`, `Term`, `LeaderID` | Current election state |
| `ReplicationAddr` | Active primary address, if this node is leader |
| `Seq` | Highest durable local sequence |
| `ClusterSize`, `Quorum` | Static membership and majority size |
| `HasQuorum` | Leader currently has a live quorum lease |
| `Transferring` | Graceful handoff in progress |
| `LastQuorum` | Last successful majority heartbeat |
| `Replication` | Nested `replication.Stats` |

`Node.LastError()` returns the most recent background error.

## Tests

- Persistent term/vote round trips, malformed state rejection, and restart
  loading.
- Vote rules: one vote per term, idempotent repeats, log up-to-date checks,
  higher-term demotion, and persisted state.
- Append-entry leader adoption, stale-term rejection, and higher-term resets.
- Configuration validation, defaults, quorum arithmetic, lease duration,
  randomized timeouts, control framing, and detector status transitions.
- Three-node real-TCP test that kills the primary and verifies a new term and
  leader, preserves pre-failover writes, commits post-failover writes, detects
  the dead node, restarts it with an empty store, and checks that it full-syncs
  both data sets.
- Graceful leadership transfer followed by writes, deletes, and a single-leader
  check.
- Minority-quorum test that verifies the leader steps down, rejects writes, and
  cannot re-elect itself.
- The suite passes under `go test -race ./...`.

## Limitations and Next Steps

- Membership is static; adding or removing nodes requires a restart. There is
  no joint-consensus reconfiguration.
- Failover is not wired into `cmd/cache`, `internal/server`, or
  `internal/cluster`; applications must use `failover.Node` directly.
- Reads are served from the local store and may be stale on followers. Linearizable
  reads are not implemented.
- Replication entries carry sequences but not terms. Full resync on each new
  replica coordinator mitigates divergence; a future protocol could add log
  terms and an explicit truncation RPC.
- Every new replica coordinator performs one full snapshot. Reconnects within
  the same coordinator use incremental sync.
- The control plane is unauthenticated apart from `ClusterID` and relies on a
  trusted network.
- The leader lease assumes reasonably bounded scheduling and clock behavior;
  the lease is deliberately half the minimum election timeout.
- The primary WAL is not compacted, and full snapshots are materialized in
  memory on both sides.

## Acceptance Criteria

- [x] Cluster remains operational after primary failure
- [x] Failure detector tracks node death
- [x] FOLLOWER, CANDIDATE, and LEADER states
- [x] Raft-style leader election with persistent term and vote
- [x] Replica promotion into the replication primary
- [x] Single active leader through majority quorum and leader lease
- [x] Graceful leadership transfer
- [x] Node recovery and full resynchronization
- [x] Kill-primary, recovery, and data-integrity tests
