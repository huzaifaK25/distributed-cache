# Distributed Cache

# Phase 7 - Partitioning & Consistent Hashing

## Objective

Distribute keys across nodes.

Enable horizontal scaling.

---

# Success Criteria

Keys automatically routed to correct node.

---

# Expected Structure

internal/partitioning/

├── hashring.go
├── vnode.go
├── partition.go
└── migration.go

---

# Task 1 - Consistent Hash Ring

Implement hash ring.

---

# Task 2 - Virtual Nodes

Add configurable virtual nodes.

---

# Task 3 - Key Routing

Map key → partition → node.

---

# Task 4 - Data Ownership

Determine responsible node.

---

# Task 5 - Rebalancing

Node joins cluster.

Move minimal keys.

---

# Task 6 - Partition Migration

Transfer ownership safely.

---

# Task 7 - Node Removal

Redistribute partitions.

---

# Task 8 - Metrics

Track:

- partition count
- migration duration
- moved keys

---

# Task 9 - Tests

Verify:

- routing correctness
- migration correctness
- rebalance correctness

---

# Acceptance Criteria

[ ] Consistent hashing operational

[ ] Virtual nodes implemented

[ ] Rebalancing working

[ ] Partition migration working

[ ] Horizontal scaling enabled
