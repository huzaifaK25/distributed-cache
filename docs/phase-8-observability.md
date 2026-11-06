# Phase 8 - Observability

## Objective

Make every request traceable and measurable. Phase 8 adds a standalone
`internal/observability` package, instruments the TCP request path, exposes a
Prometheus endpoint, exports OpenTelemetry traces, and ships a Grafana dashboard
and alert rules for latency, replication lag, and node failures.

Unlike the earlier cluster, replication, failover, and partitioning layers, the
request instrumentation is wired into `cmd/cache` and `internal/server` so the
running server is observable out of the box.

## Architecture

```text
                         +-----------------------------+
 client -- TCP line ---> | server.connection           |
                         |  parse -> worker -> write   |
                         +--------------+--------------+
                                        |
                                        v
                         +-----------------------------+
                         | observability.Telemetry     |
                         |  start / finish one request |
                         +------+-------+-------+------+
                                |       |       |
                     +----------+  +----+---+  +--------+
                     | Metrics  |  | Logger|  | Tracer |
                     +----+-----+  +---+----+  +---+----+
                          |            |           |
                    /metrics         stdout    stdout / OTLP
                          |
                    +-----+------+      +------------------+
                    | Prometheus |----->| Grafana + alerts |
                    +------------+      +------------------+
```

| File | Responsibility |
|---|---|
| `metrics.go` | Prometheus registry, required counters, latency histogram, replication and node-health collectors, HTTP handler |
| `logging.go` | JSON/text `slog` logger, request IDs, context correlation helpers |
| `tracing.go` | OpenTelemetry provider, exporters, sampling, per-request telemetry lifecycle |
| `dashboard.go` | Embedded Grafana dashboard, alert rules, Prometheus scrape configuration |
| `assets/` | Canonical dashboard and rule files |

The implementation uses the official `prometheus/client_golang` and
`go.opentelemetry.io/otel` modules rather than custom text or protobuf
encoders.

## Structured Logging

Logs use `log/slog` with a JSON handler by default. Every record includes
`nodeID`; every request record also includes the correlation fields required by
this phase.

| Field | Meaning |
|---|---|
| `requestID` | Random 128-bit request identifier, lower-case hex. |
| `nodeID` | Server node identity. |
| `operation` | `PING`, `SET`, `GET`, `DEL`, `STATS`, or `PARSE` for a malformed line. |
| `traceID` | OpenTelemetry trace ID, when tracing is enabled. |
| `spanID` | OpenTelemetry span ID, when tracing is enabled. |
| `status` | `ok`, `not_found`, `error`, or `busy`. |
| `durationMs` | End-to-end duration including worker queueing and response write. |
| `responseBytes` | Size of the protocol response. |
| `writeOK` | Whether the response write succeeded. |
| `remoteAddr` | Client TCP address. |

Example request record:

```json
{
  "time": "2026-11-06T12:00:00.000Z",
  "level": "INFO",
  "msg": "request completed",
  "nodeID": "node-a",
  "requestID": "8a891f60d9a4f1c5ad30eb2f70f4ac39",
  "operation": "GET",
  "traceID": "3f1eaf96818bb7f4ef843a90df61373d",
  "spanID": "e96b5e62e2a08de2",
  "status": "ok",
  "durationMs": 0.153,
  "responseBytes": 5,
  "writeOK": true,
  "remoteAddr": "127.0.0.1:51234"
}
```

`Logger.RequestLogger` adds request and trace fields from a context. Request IDs
are also stored in the context, so lower layers can correlate their own logs
without threading the ID through every function signature.

Keys and values are deliberately not logged or attached to span attributes.
They can contain user data and have unbounded cardinality.

## Metrics

The required metrics are exposed with their exact names:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `requests_total` | counter | `node_id`, `operation`, `status` | Commands handled, including parse failures. |
| `cache_hits` | counter | none | Cache reads that found a live value. |
| `cache_misses` | counter | none | Cache reads that did not find a live value. |
| `evictions` | counter | none | Entries removed by the LRU size limit. |

Additional metrics support latency, capacity, replication, and cluster alerts:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `request_duration_seconds` | histogram | `node_id`, `operation` | End-to-end request latency; also exports `_bucket`, `_sum`, and `_count`. |
| `requests_in_flight` | gauge | `node_id` | Requests currently executing or waiting for a worker. |
| `node_failures_total` | counter | `node_id` | Transitions recorded by `Metrics.RecordNodeFailure`. |
| `replication_lag_seconds` | gauge | none | Age of the oldest pending replication work. |
| `replication_pending` | gauge | none | Mutations queued or in flight awaiting acknowledgement. |
| `replication_queue_depth` | gauge | none | Live mutations buffered across replicas. |
| `replication_replicas` | gauge | none | Connected replicas. |
| `replication_delivered_total` | counter | none | Mutations acknowledged by replicas. |
| `replication_failed_total` | counter | none | Failed batches and queue overflows. |
| `replication_active` | gauge | none | 1 when a replication coordinator is active. |
| `node_up` | gauge | `node_id` | 1 for `ONLINE`/`ALIVE`, 0 otherwise. |
| `node_status` | gauge | `node_id`, `status` | One series per node and health state, valued 1. |

`cache_hits`, `cache_misses`, and `evictions` are collected from the cache's
cumulative counters at scrape time. The server therefore performs no duplicate
hit/miss accounting on the request path. `requests_total` and the latency
histogram are recorded by the telemetry layer when each request finishes.

The cache, replication, and health collectors are custom `prometheus.Collector`
implementations so each scrape takes one consistent snapshot of its source.
`NewMetrics` uses a private registry per instance; this keeps tests isolated and
lets an embedding application add its own collectors without touching global
state.

Example exposition:

```text
# TYPE requests_total counter
requests_total{node_id="node-a",operation="GET",status="ok"} 42
requests_total{node_id="node-a",operation="GET",status="not_found"} 7
# TYPE cache_hits counter
cache_hits 42
# TYPE cache_misses counter
cache_misses 7
# TYPE evictions counter
evictions 0
# TYPE request_duration_seconds histogram
request_duration_seconds_bucket{node_id="node-a",operation="GET",le="0.001"} 49
```

## Prometheus Endpoint

`cmd/cache` starts an HTTP server with:

| Path | Purpose |
|---|---|
| `/metrics` | Prometheus text/OpenMetrics exposition. |
| `/healthz` | Liveness probe returning `200 ok`. |

The default address is `127.0.0.1:9090`; pass an empty `-metrics-addr` to disable
the endpoint. `Metrics.Handler()` can also be mounted in an existing HTTP mux:

```go
mux := http.NewServeMux()
mux.Handle("/metrics", metrics.Handler())
```

`internal/observability/assets/prometheus.yml` is a minimal scrape
configuration. It scrapes `localhost:9090`, evaluates `alerts.yml`, and adds a
`service="distributed-cache"` label.

The endpoint has no authentication. Keep it on a private interface or protect it
with a reverse proxy when it is reachable from untrusted networks.

## Tracing

`NewTracer` creates an OpenTelemetry `TracerProvider` with these resource
attributes:

- `service.name` - defaults to `distributed-cache`.
- `service.instance.id` - the configured node ID.
- `cache.node_id` - the configured node ID.

The default sampler is parent-based trace-ID ratio sampling. The command-line
default samples every new trace (`-trace-sample 1.0`).

Three exporters are available:

| Exporter | Behavior |
|---|---|
| `none` | Spans and trace IDs are still created but nothing is exported. |
| `stdout` | One JSON span object per line, written to stderr by default. |
| `otlp` | OTLP/HTTP export to `-otel-endpoint`, with optional TLS. |

Each request creates one server span named `cache.<operation>`, for example
`cache.get` or `cache.parse`. Span attributes include:

- `cache.request_id`
- `cache.node_id`
- `cache.operation`
- `client.address`
- `cache.status`
- `cache.response_bytes`
- `cache.write_ok`

Span status is `Ok` for successful and not-found responses and `Error` for
protocol errors and worker-queue rejection. The tracer is flushed during
shutdown with a five-second deadline.

### Request Correlation

The request flow is:

1. `server.connection` reads one command line.
2. `observability.Telemetry` generates a request ID and starts a root span.
3. The span context and request ID are stored in the request context.
4. The request completes, metrics and logs are recorded, and the span ends.
5. The log record carries `requestID`, `traceID`, and `spanID` together, so a
   log line can be used to find the trace in Jaeger, Tempo, or any OTLP backend.

The telemetry layer supports a parent context, so a future transport can pass a
W3C `traceparent` into `StartRequest` and make the cache span a child of the
caller's trace. The current TCP protocol has no header frame, so each command is
a new root trace.

## Grafana Dashboard

`internal/observability/assets/grafana-dashboard.json` imports directly into
Grafana 10 or newer. The dashboard is parameterized by a Prometheus datasource
and includes:

| Panel | Query focus |
|---|---|
| Requests per second | `requests_total` by node. |
| Cache hit ratio | `cache_hits` vs `cache_misses`. |
| Requests in flight | `requests_in_flight`. |
| Request latency p95 / p99 | `request_duration_seconds_bucket`. |
| Cache hits and misses | Hit and miss rates. |
| Evictions per second | `evictions`. |
| Requests by status | Success, not-found, error, and busy rates. |
| Errors by operation | Error rate by command. |
| Replication lag | `replication_lag_seconds`. |
| Replication backlog | Pending and queued mutations. |
| Node availability | `node_up` per node. |
| Replication throughput | Delivered and failed replication rates. |

To install, import the JSON in Grafana and select the Prometheus datasource when
prompted. The same file can be written from Go with
`observability.WriteAssets(dir)` or read with `observability.GrafanaDashboard()`.

## Alerting

`alerts.yml` defines the following rules:

| Alert | Expression | For | Severity |
|---|---|---|---|
| `HighRequestLatencyP95` | p95 latency > 100 ms | 2m | warning |
| `HighRequestLatencyP99` | p99 latency > 500 ms | 2m | critical |
| `ReplicationLagHigh` | lag > 2 s | 1m | warning |
| `ReplicationLagCritical` | lag > 10 s | 2m | critical |
| `NodeDown` | `node_up == 0` | 1m | critical |
| `AllCacheNodesDown` | `sum(node_up) == 0` | 30s | critical |
| `HighRequestErrorRate` | error ratio > 5% | 5m | warning |
| `RequestQueueSaturation` | busy responses observed | 2m | warning |

The high-latency rules use the histogram buckets rather than a summary, so they
can be aggregated across nodes. The replication rules read the snapshot that the
embedding application supplies through `MetricsConfig.ReplicationStats`. The
node rules read `MetricsConfig.NodeHealth`.

## Server Integration

`server.Config.Telemetry` accepts an optional bundle. When it is nil, the server
does no instrumentation:

```go
telemetry := observability.NewTelemetry(metrics, logger, tracer)
s := server.New(store, server.Config{
	Address:   "127.0.0.1:6379",
	Telemetry: telemetry,
})
```

`cmd/cache` creates the cache, logger, metrics registry, and tracer, starts the
TCP server, and starts the metrics endpoint. The process stops both servers on
`SIGINT`/`SIGTERM` and flushes spans before exiting.

Replication and failover metrics are supplied as callbacks so the observability
package stays independent of those lifecycle layers:

```go
metrics := observability.NewMetrics(observability.MetricsConfig{
	NodeID:     node.ID(),
	CacheStats: store.Stats,
	ReplicationStats: func() (replication.Stats, bool) {
		stats := node.Stats()
		return stats.Replication, stats.Replication.Role != ""
	},
	NodeHealth: func() []observability.NodeHealth {
		health := node.Health()
		nodes := make([]observability.NodeHealth, 0, len(health))
		for _, peer := range health {
			nodes = append(nodes, observability.NodeHealth{
				ID:     peer.ID,
				Status: string(peer.Status),
			})
		}
		return nodes
	},
})
```

## Configuration

`MetricsConfig`:

| Option | Default | Behavior |
|---|---|---|
| `NodeID` | `local` | Label for request metrics and fallback health entry. |
| `CacheStats` | nil | Cache statistics snapshot callback. |
| `ReplicationStats` | nil | Replication snapshot and active flag callback. |
| `NodeHealth` | nil | Cluster/failover health callback; defaults to this node ONLINE. |

`LoggingConfig`:

| Option | Default | Behavior |
|---|---|---|
| `NodeID` | `local` | Always attached as `nodeID`. |
| `Output` | `os.Stdout` | Log destination. |
| `Level` | `info` | Minimum enabled level. |
| `Text` | `false` | JSON by default; set true for logfmt text. |
| `AddSource` | `false` | Include caller source location. |

`TracingConfig`:

| Option | Default | Behavior |
|---|---|---|
| `ServiceName` | `distributed-cache` | `service.name` resource attribute. |
| `NodeID` | service name | `service.instance.id` and span attribute. |
| `Exporter` | `none` | `none`, `stdout`, or `otlp`. |
| `Output` | `os.Stderr` | Destination for stdout spans. |
| `OTLPEndpoint` | empty | `host:port` or full OTLP/HTTP URL. |
| `OTLPInsecure` | `false` | Disable TLS for a host:port endpoint. |
| `SampleRatio` | `1.0` | Fraction of new traces to sample. |
| `ExporterOverride` | nil | Injected exporter for tests and embedding. |

## Behavior Notes

- Instrumentation observes the whole command path: parse, worker queue wait,
  cache operation, and response write.
- Parse failures are logged, counted, and traced with operation `PARSE`.
- `Request.Finish` is idempotent and thread-safe; a retry cannot double-count.
- The observability bundle is optional. Existing users of `server.New` keep the
  original behavior without a telemetry dependency.
- Cache counters are cumulative and monotonic for the lifetime of the cache.
- `node_up` deliberately treats `DEGRADED` and `SUSPECT` as not fully up; review
  the node-failure alert threshold if a degraded node should page differently.
- Trace export failure does not fail requests. The tracer logs shutdown errors
  but request handling stays independent of the collector.
- Prometheus and OpenTelemetry peers are external services. The cache only adds
  the endpoint, exporters, and assets.

## Tests

The package and integration tests cover:

- exact exposition of the four required counters and request labels;
- cache hit, miss, and eviction values from the real cache;
- replication lag, backlog, throughput, and active-state metrics;
- node health, fallback health, and failure counters;
- JSON and text logs, level filtering, request IDs, and trace correlation;
- request ID generation and context propagation;
- OpenTelemetry span export, names, attributes, status, and sampling;
- idempotent telemetry finish and nil-safety;
- dashboard JSON validity, required panels, alert names, and scrape config;
- embedded asset writing;
- a real TCP server producing metrics, logs, and spans for successful, missing,
  and malformed commands.

## Limitations

- The TCP protocol has no trace header, so client trace context is not yet
  propagated.
- The standalone server wires cache and request metrics. Cluster, replication,
  and failover values require the embedding application to provide callbacks.
- Metrics are per process; aggregation and retention are Prometheus concerns.
- The endpoint has no authentication or TLS; deploy it on a trusted network.
- There are no Prometheus exemplars linking histogram buckets to trace IDs.
- Grafana provisioning and Alertmanager routing are deployment concerns; this
  phase ships the dashboard JSON, alert rules, and scrape configuration.
- The dashboard uses a datasource input and must be imported once per Grafana
  instance unless provisioning is added.

## Acceptance Criteria

- [x] Metrics exposed
- [x] Prometheus integrated
- [x] Grafana dashboards
- [x] Traces visible
- [x] Alerting configured
