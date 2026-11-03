# Distributed Cache

# Phase 9 - Chaos Engineering

## Objective

Validate resilience under failure.

---

# Success Criteria

System survives adverse conditions.

---

# Expected Structure

internal/chaos/

├── faults.go
├── latency.go
├── partition.go
└── runner.go

---

# Task 1 - Latency Injection

Add artificial latency.

---

# Task 2 - Packet Loss

Simulate packet drops.

---

# Task 3 - Node Crash

Terminate nodes randomly.

---

# Task 4 - Network Partition

Split cluster.

---

# Task 5 - Resource Pressure

Simulate:

CPU spikes

Memory pressure

---

# Task 6 - Recovery Validation

Verify:

cluster stability.

---

# Task 7 - SLO Monitoring

Track:

availability

latency

error rate

---

# Acceptance Criteria

[ ] Fault injection operational

[ ] Crash recovery verified

[ ] Partition recovery verified

[ ] SLO reports generated
