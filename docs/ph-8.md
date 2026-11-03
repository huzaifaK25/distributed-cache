# Distributed Cache

# Phase 8 - Observability

## Objective

Make the system observable and debuggable.

---

# Success Criteria

Every request can be traced and measured.

---

# Expected Structure

internal/observability/

├── metrics.go
├── logging.go
├── tracing.go
└── dashboard.go

---

# Task 1 - Structured Logging

Adopt structured logs.

Fields:

requestID
nodeID
operation

---

# Task 2 - Metrics

Expose:

requests_total

cache_hits

cache_misses

evictions

---

# Task 3 - Prometheus

Expose metrics endpoint.

---

# Task 4 - Tracing

Integrate OpenTelemetry.

---

# Task 5 - Request Correlation

Assign trace IDs.

---

# Task 6 - Grafana

Create dashboards.

---

# Task 7 - Alerting

Create alerts:

high latency

replication lag

node failures

---

# Acceptance Criteria

[ ] Metrics exposed

[ ] Prometheus integrated

[ ] Grafana dashboards

[ ] Traces visible

[ ] Alerting configured
