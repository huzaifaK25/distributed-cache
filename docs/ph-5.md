# Distributed Cache

# Phase 5 - Replication

## Objective

Prevent data loss through replication.

Each key must exist on multiple nodes.

---

# Success Criteria

Writes replicate reliably.

---

# Expected Structure

internal/replication/

├── replicator.go
├── syncer.go
├── wal.go
└── coordinator.go

---

# Task 1 - Replication Model

Implement:

Primary
Replica

roles.

---

# Task 2 - Replication Messages

Replicate:

SET
DEL
TTL

operations.

---

# Task 3 - Write-Ahead Log

Create WAL.

Store every mutation.

---

# Task 4 - Replica Sync

New replica joins.

Perform full synchronization.

---

# Task 5 - Incremental Sync

After bootstrap:

ship deltas only.

---

# Task 6 - Replication Acknowledgements

Track:

- delivered
- pending
- failed

---

# Task 7 - Health Metrics

Measure:

replication lag

queue depth

sync time

---

# Task 8 - Recovery

Replay WAL on restart.

---

# Task 9 - Tests

Verify:

- data consistency
- replica recovery
- WAL replay

---

# Acceptance Criteria

[ ] Writes replicated

[ ] Replicas recover

[ ] WAL operational

[ ] Synchronization working

[ ] Consistent cluster state
