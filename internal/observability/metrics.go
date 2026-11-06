// Package observability adds structured logging, Prometheus metrics,
// OpenTelemetry tracing, and request correlation to the cache.
package observability

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/replication"
)

// Request status label values exposed by requests_total.
const (
	RequestOK       = "ok"
	RequestNotFound = "not_found"
	RequestError    = "error"
	RequestBusy     = "busy"
)

// NodeHealth is one node's health as reported by a cluster or failover layer.
//
// Status values from the built-in layers are ONLINE, DEGRADED, and OFFLINE for
// cluster membership and ALIVE, SUSPECT, and DEAD for failover peers. node_up
// is 1 only for ONLINE and ALIVE nodes.
type NodeHealth struct {
	ID     string
	Status string
}

// MetricsConfig configures the Prometheus collectors.
//
// All source callbacks are optional. When a callback is nil, the corresponding
// metrics are still exported with zero values (except node health, which
// defaults to this node being ONLINE).
type MetricsConfig struct {
	// NodeID labels request metrics and is the default health entry when
	// NodeHealth is nil.
	NodeID string

	// CacheStats returns a cumulative cache statistics snapshot.
	CacheStats func() cache.CacheStats

	// ReplicationStats returns a replication statistics snapshot. The bool
	// reports whether replication is currently active.
	ReplicationStats func() (replication.Stats, bool)

	// NodeHealth returns the health of every known node.
	NodeHealth func() []NodeHealth
}

// Metrics owns the Prometheus registry and all cache collectors.
//
// It is safe for concurrent use. A nil *Metrics disables observation.
type Metrics struct {
	nodeID   string
	registry *prometheus.Registry

	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	inFlight        *prometheus.GaugeVec
	nodeFailures    *prometheus.CounterVec
}

// NewMetrics builds an isolated registry with the cache, replication, node,
// and Go runtime collectors registered.
func NewMetrics(config MetricsConfig) *Metrics {
	if config.NodeID == "" {
		config.NodeID = "local"
	}

	m := &Metrics{
		nodeID:   config.NodeID,
		registry: prometheus.NewRegistry(),
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "requests_total",
				Help: "Total client requests handled, by operation and status.",
			},
			[]string{"node_id", "operation", "status"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "request_duration_seconds",
				Help:    "End-to-end client request duration in seconds.",
				Buckets: []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"node_id", "operation"},
		),
		inFlight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "requests_in_flight",
				Help: "Client requests currently being executed.",
			},
			[]string{"node_id"},
		),
		nodeFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "node_failures_total",
				Help: "Observed transitions of a node to a failed state.",
			},
			[]string{"node_id"},
		),
	}

	m.registry.MustRegister(
		collectors.NewGoCollector(),
		m.requestsTotal,
		m.requestDuration,
		m.inFlight,
		m.nodeFailures,
		newCacheStatsCollector(config.CacheStats),
		newReplicationStatsCollector(config.ReplicationStats),
		newNodeHealthCollector(config.NodeHealth, config.NodeID),
	)

	return m
}

// Registry returns the Prometheus registry backing this Metrics value.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// NodeID returns the node label used by this Metrics value.
func (m *Metrics) NodeID() string {
	if m == nil {
		return ""
	}
	return m.nodeID
}

// Handler returns an HTTP handler that exposes the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// ObserveRequest records one completed request.
func (m *Metrics) ObserveRequest(operation, status string, duration time.Duration) {
	if m == nil {
		return
	}
	if operation == "" {
		operation = "UNKNOWN"
	}
	if status == "" {
		status = RequestOK
	}
	if duration < 0 {
		duration = 0
	}

	m.requestsTotal.WithLabelValues(m.nodeID, operation, status).Inc()
	m.requestDuration.WithLabelValues(m.nodeID, operation).Observe(duration.Seconds())
}

// IncInFlight records that a request started executing.
func (m *Metrics) IncInFlight() {
	if m == nil {
		return
	}
	m.inFlight.WithLabelValues(m.nodeID).Inc()
}

// DecInFlight records that a request finished executing.
func (m *Metrics) DecInFlight() {
	if m == nil {
		return
	}
	m.inFlight.WithLabelValues(m.nodeID).Dec()
}

// RecordNodeFailure increments the failure counter for nodeID.
func (m *Metrics) RecordNodeFailure(nodeID string) {
	if m == nil {
		return
	}
	if nodeID == "" {
		nodeID = m.nodeID
	}
	m.nodeFailures.WithLabelValues(nodeID).Inc()
}

// cacheStatsCollector reads cumulative cache counters at scrape time so the
// server does not have to duplicate accounting for every operation.
type cacheStatsCollector struct {
	stats func() cache.CacheStats

	hits      *prometheus.Desc
	misses    *prometheus.Desc
	evictions *prometheus.Desc
}

func newCacheStatsCollector(stats func() cache.CacheStats) *cacheStatsCollector {
	return &cacheStatsCollector{
		stats:     stats,
		hits:      prometheus.NewDesc("cache_hits", "Cache reads that found a live value.", nil, nil),
		misses:    prometheus.NewDesc("cache_misses", "Cache reads that did not find a live value.", nil, nil),
		evictions: prometheus.NewDesc("evictions", "Entries removed by the LRU size limit.", nil, nil),
	}
}

func (c *cacheStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.hits
	ch <- c.misses
	ch <- c.evictions
}

func (c *cacheStatsCollector) Collect(ch chan<- prometheus.Metric) {
	var stats cache.CacheStats
	if c.stats != nil {
		stats = c.stats()
	}

	ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(stats.Hits))
	ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(stats.Misses))
	ch <- prometheus.MustNewConstMetric(c.evictions, prometheus.CounterValue, float64(stats.Evictions))
}

// replicationStatsCollector exposes replication lag and progress at scrape time.
type replicationStatsCollector struct {
	stats func() (replication.Stats, bool)

	descs []*prometheus.Desc
}

func newReplicationStatsCollector(stats func() (replication.Stats, bool)) *replicationStatsCollector {
	return &replicationStatsCollector{
		stats: stats,
		descs: []*prometheus.Desc{
			prometheus.NewDesc("replication_lag_seconds", "Age of the oldest pending replication work.", nil, nil),
			prometheus.NewDesc("replication_pending", "Mutations queued or in flight awaiting acknowledgement.", nil, nil),
			prometheus.NewDesc("replication_queue_depth", "Live mutations buffered across replicas.", nil, nil),
			prometheus.NewDesc("replication_replicas", "Connected replicas.", nil, nil),
			prometheus.NewDesc("replication_delivered_total", "Mutations acknowledged by replicas.", nil, nil),
			prometheus.NewDesc("replication_failed_total", "Failed replication batches and queue overflows.", nil, nil),
			prometheus.NewDesc("replication_active", "Whether a replication coordinator is currently active.", nil, nil),
		},
	}
}

func (c *replicationStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *replicationStatsCollector) Collect(ch chan<- prometheus.Metric) {
	var (
		stats  replication.Stats
		active bool
	)
	if c.stats != nil {
		stats, active = c.stats()
	}

	activeValue := 0.0
	if active {
		activeValue = 1
	}

	ch <- prometheus.MustNewConstMetric(c.descs[0], prometheus.GaugeValue, stats.ReplicationLag.Seconds())
	ch <- prometheus.MustNewConstMetric(c.descs[1], prometheus.GaugeValue, float64(stats.Pending))
	ch <- prometheus.MustNewConstMetric(c.descs[2], prometheus.GaugeValue, float64(stats.QueueDepth))
	ch <- prometheus.MustNewConstMetric(c.descs[3], prometheus.GaugeValue, float64(stats.Replicas))
	ch <- prometheus.MustNewConstMetric(c.descs[4], prometheus.CounterValue, float64(stats.Delivered))
	ch <- prometheus.MustNewConstMetric(c.descs[5], prometheus.CounterValue, float64(stats.Failed))
	ch <- prometheus.MustNewConstMetric(c.descs[6], prometheus.GaugeValue, activeValue)
}

// nodeHealthCollector exports one node_up and one node_status series per node.
type nodeHealthCollector struct {
	health func() []NodeHealth
	selfID string

	up     *prometheus.Desc
	status *prometheus.Desc
}

func newNodeHealthCollector(health func() []NodeHealth, selfID string) *nodeHealthCollector {
	return &nodeHealthCollector{
		health: health,
		selfID: selfID,
		up:     prometheus.NewDesc("node_up", "Whether a node is ONLINE or ALIVE (1) or not (0).", []string{"node_id"}, nil),
		status: prometheus.NewDesc("node_status", "Current node health state as a labelled gauge.", []string{"node_id", "status"}, nil),
	}
}

func (c *nodeHealthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.status
}

func (c *nodeHealthCollector) Collect(ch chan<- prometheus.Metric) {
	for _, node := range c.snapshot() {
		if node.ID == "" {
			continue
		}
		up := 0.0
		if healthyStatus(node.Status) {
			up = 1
		}
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, up, node.ID)
		ch <- prometheus.MustNewConstMetric(c.status, prometheus.GaugeValue, 1, node.ID, normalizeStatus(node.Status))
	}
}

func (c *nodeHealthCollector) snapshot() []NodeHealth {
	if c.health != nil {
		if health := c.health(); len(health) > 0 {
			return health
		}
	}
	return []NodeHealth{{ID: c.selfID, Status: "ONLINE"}}
}

// healthyStatus reports whether status means the node is serving normally.
func healthyStatus(status string) bool {
	switch normalizeStatus(status) {
	case "ONLINE", "ALIVE":
		return true
	default:
		return false
	}
}

// normalizeStatus upper-cases a health state for stable metric labels.
func normalizeStatus(status string) string {
	status = strings.ToUpper(strings.TrimSpace(status))
	if status == "" {
		return "UNKNOWN"
	}
	return status
}
