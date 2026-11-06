package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/replication"
)

// scrape returns the Prometheus text exposition for m.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics endpoint status = %d, want %d", recorder.Code, http.StatusOK)
	}
	return recorder.Body.String()
}

func TestMetricsExposeRequiredSeries(t *testing.T) {
	store := cache.New(cache.Config{MaxItems: 1})
	t.Cleanup(store.StopCleanupWorker)

	if err := store.Set("a", "one"); err != nil {
		t.Fatal(err)
	}
	if _, found := store.Get("a"); !found {
		t.Fatal("expected cache hit")
	}
	if _, found := store.Get("missing"); found {
		t.Fatal("expected cache miss")
	}
	if err := store.Set("b", "two"); err != nil {
		t.Fatal(err)
	}
	if err := store.Set("c", "three"); err != nil {
		t.Fatal(err)
	}

	metrics := NewMetrics(MetricsConfig{
		NodeID:     "node-a",
		CacheStats: store.Stats,
	})
	metrics.ObserveRequest("GET", RequestOK, 2*time.Millisecond)
	metrics.ObserveRequest("GET", RequestNotFound, 3*time.Millisecond)
	metrics.ObserveRequest("SET", RequestError, 4*time.Millisecond)
	metrics.IncInFlight()
	metrics.DecInFlight()

	body := scrape(t, metrics)
	for _, want := range []string{
		`requests_total{node_id="node-a",operation="GET",status="ok"} 1`,
		`requests_total{node_id="node-a",operation="GET",status="not_found"} 1`,
		`requests_total{node_id="node-a",operation="SET",status="error"} 1`,
		`request_duration_seconds_count{node_id="node-a",operation="GET"} 2`,
		`requests_in_flight{node_id="node-a"} 0`,
		"cache_hits 1",
		"cache_misses 1",
		"evictions 2",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output does not contain %q\n%s", want, body)
		}
	}
}

func TestMetricsReplicationAndNodeHealth(t *testing.T) {
	metrics := NewMetrics(MetricsConfig{
		NodeID: "node-a",
		ReplicationStats: func() (replication.Stats, bool) {
			return replication.Stats{
				ReplicationLag: 3500 * time.Millisecond,
				Pending:        7,
				QueueDepth:     2,
				Replicas:       1,
				Delivered:      9,
				Failed:         1,
			}, true
		},
		NodeHealth: func() []NodeHealth {
			return []NodeHealth{
				{ID: "node-a", Status: "ONLINE"},
				{ID: "node-b", Status: "DEAD"},
			}
		},
	})

	body := scrape(t, metrics)
	for _, want := range []string{
		"replication_lag_seconds 3.5",
		"replication_pending 7",
		"replication_queue_depth 2",
		"replication_replicas 1",
		"replication_delivered_total 9",
		"replication_failed_total 1",
		"replication_active 1",
		`node_up{node_id="node-a"} 1`,
		`node_up{node_id="node-b"} 0`,
		`node_status{node_id="node-a",status="ONLINE"} 1`,
		`node_status{node_id="node-b",status="DEAD"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output does not contain %q\n%s", want, body)
		}
	}
}

func TestMetricsRecordNodeFailure(t *testing.T) {
	metrics := NewMetrics(MetricsConfig{NodeID: "node-a"})
	metrics.RecordNodeFailure("node-b")
	metrics.RecordNodeFailure("node-b")
	metrics.RecordNodeFailure("")

	body := scrape(t, metrics)
	for _, want := range []string{
		`node_failures_total{node_id="node-b"} 2`,
		`node_failures_total{node_id="node-a"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output does not contain %q\n%s", want, body)
		}
	}
}

func TestMetricsDefaultsAndNilSafety(t *testing.T) {
	metrics := NewMetrics(MetricsConfig{})
	body := scrape(t, metrics)

	for _, want := range []string{
		"cache_hits 0",
		"cache_misses 0",
		"evictions 0",
		"replication_lag_seconds 0",
		"replication_active 0",
		`node_up{node_id="local"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("default metrics output does not contain %q\n%s", want, body)
		}
	}

	var nilMetrics *Metrics
	nilMetrics.ObserveRequest("GET", RequestOK, time.Millisecond)
	nilMetrics.IncInFlight()
	nilMetrics.DecInFlight()
	nilMetrics.RecordNodeFailure("")

	recorder := httptest.NewRecorder()
	nilMetrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil metrics status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestMetricsEmptyHealthFallsBackToSelf(t *testing.T) {
	metrics := NewMetrics(MetricsConfig{
		NodeID:     "node-a",
		NodeHealth: func() []NodeHealth { return nil },
	})

	body := scrape(t, metrics)
	if !strings.Contains(body, `node_up{node_id="node-a"} 1`) {
		t.Fatalf("empty health did not fall back to self\n%s", body)
	}
}
