# Distributed Cache

# Phase 4 - Cluster Management

## Objective

Create cluster membership and node discovery.

Multiple cache nodes should be aware of one another.

---

# Success Criteria

Nodes should:

- discover peers
- join cluster
- leave cluster
- detect health
- exchange metadata

---

# Expected Structure

internal/cluster/

├── node.go
├── registry.go
├── heartbeat.go
├── membership.go
└── gossip.go

---

# Task 1 - Node Model

Define:

NodeID
Address
Status
JoinedAt

---

# Task 2 - Node Registry

Maintain known nodes.

Support:

AddNode()

RemoveNode()

ListNodes()

---

# Task 3 - Cluster Metadata

Track:

- cluster id
- node count
- cluster version

---

# Task 4 - Heartbeat System

Periodic heartbeat messages.

Every node sends:

I_AM_ALIVE

---

# Task 5 - Health Tracking

States:

ONLINE
OFFLINE
DEGRADED

---

# Task 6 - Node Join

Implement join request.

New node should:

- register
- receive membership list

---

# Task 7 - Node Leave

Graceful exit workflow.

---

# Task 8 - Gossip Protocol

Exchange:

- membership
- health data
- version updates

---

# Task 9 - Consistency Checks

Verify:

all nodes converge on same view.

---

# Task 10 - Tests

Simulate:

- joins
- leaves
- network delays

---

# Acceptance Criteria

[ ] Cluster membership works

[ ] Heartbeats functioning

[ ] Gossip operational

[ ] Nodes synchronize state

[ ] Health status tracked
