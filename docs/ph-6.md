# Distributed Cache

# Phase 6 - Failover

## Objective

Maintain availability when nodes fail.

---

# Success Criteria

Cluster remains operational after primary failure.

---

# Expected Structure

internal/failover/

├── election.go
├── leader.go
├── detector.go
└── state_machine.go

---

# Task 1 - Failure Detector

Monitor heartbeats.

Detect node death.

---

# Task 2 - Election States

FOLLOWER
CANDIDATE
LEADER

---

# Task 3 - Leader Election

Implement Raft-style election.

---

# Task 4 - Leader Promotion

Replica becomes leader.

---

# Task 5 - Split-Brain Protection

Guarantee:

single active leader.

---

# Task 6 - Leadership Transfer

Support graceful handoff.

---

# Task 7 - Recovery

Node rejoins cluster.

Resynchronize state.

---

# Task 8 - Tests

Kill primary.

Verify:

- election
- recovery
- data integrity

---

# Acceptance Criteria

[ ] Automatic failover

[ ] Leader election

[ ] Split-brain protection

[ ] Node recovery

[ ] No data corruption
