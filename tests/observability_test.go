package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/observability"
	"distributed-cache/internal/server"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// lockedBuffer is a goroutine-safe log sink for request instrumentation tests.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// scrapeServerMetrics renders the Prometheus exposition for metrics.
func scrapeServerMetrics(t *testing.T, metrics *observability.Metrics) string {
	t.Helper()

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", recorder.Code, http.StatusOK)
	}
	return recorder.Body.String()
}

func TestServerObservability(t *testing.T) {
	store := cache.New(cache.Config{})
	t.Cleanup(store.StopCleanupWorker)

	var logs lockedBuffer
	logger := observability.NewLogger(observability.LoggingConfig{
		NodeID: "node-obs",
		Output: &logs,
	})

	exporter := tracetest.NewInMemoryExporter()
	tracer, err := observability.NewTracer(context.Background(), observability.TracingConfig{
		NodeID:           "node-obs",
		ExporterOverride: exporter,
		SampleRatio:      1,
	})
	if err != nil {
		t.Fatalf("NewTracer() returned error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tracer.Shutdown(ctx)
	})

	metrics := observability.NewMetrics(observability.MetricsConfig{
		NodeID:     "node-obs",
		CacheStats: store.Stats,
	})
	telemetry := observability.NewTelemetry(metrics, logger, tracer)

	s := startServer(t, store, server.Config{Telemetry: telemetry})
	client := dialClient(t, s.Addr().String())

	tests := []struct {
		command string
		want    string
	}{
		{command: "PING", want: "PONG"},
		{command: "SET obs:key hello", want: "OK"},
		{command: "GET obs:key", want: "hello"},
		{command: "GET obs:missing", want: "NOT_FOUND"},
		{command: "DEL obs:key", want: "OK"},
		{command: "NOPE", want: `ERR unknown command "NOPE"`},
	}
	for _, tt := range tests {
		if got := client.send(tt.command); got != tt.want {
			t.Fatalf("%s = %q, want %q", tt.command, got, tt.want)
		}
	}

	wantSeries := []string{
		`requests_total{node_id="node-obs",operation="PING",status="ok"} 1`,
		`requests_total{node_id="node-obs",operation="SET",status="ok"} 1`,
		`requests_total{node_id="node-obs",operation="GET",status="ok"} 1`,
		`requests_total{node_id="node-obs",operation="GET",status="not_found"} 1`,
		`requests_total{node_id="node-obs",operation="DEL",status="ok"} 1`,
		`requests_total{node_id="node-obs",operation="PARSE",status="error"} 1`,
		"cache_hits 1",
		"cache_misses 1",
		"evictions 0",
	}

	var body string
	if !waitFor(t, 2*time.Second, func() bool {
		body = scrapeServerMetrics(t, metrics)
		for _, want := range wantSeries {
			if !strings.Contains(body, want) {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("metrics did not reach expected state\nwant:\n%s\ngot:\n%s", strings.Join(wantSeries, "\n"), body)
	}

	// Every request must have a structured log with correlation fields.
	var entries []map[string]any
	if !waitFor(t, 2*time.Second, func() bool {
		entries = parseLogEntries(t, logs.String())
		return len(entries) == len(tests)
	}) {
		t.Fatalf("got %d log entries, want %d\n%s", len(entries), len(tests), logs.String())
	}

	operations := make(map[string]bool, len(entries))
	for _, entry := range entries {
		for _, field := range []string{
			observability.FieldRequestID,
			observability.FieldNodeID,
			observability.FieldOperation,
			observability.FieldTraceID,
			observability.FieldSpanID,
		} {
			value, _ := entry[field].(string)
			if value == "" {
				t.Errorf("log entry is missing %s: %v", field, entry)
			}
		}
		if entry[observability.FieldNodeID] != "node-obs" {
			t.Errorf("nodeID = %v, want node-obs", entry[observability.FieldNodeID])
		}
		if operation, _ := entry[observability.FieldOperation].(string); operation != "" {
			operations[operation] = true
		}
	}
	for _, want := range []string{"PING", "SET", "GET", "DEL", "PARSE"} {
		if !operations[want] {
			t.Errorf("no request log with operation %q\n%s", want, logs.String())
		}
	}

	// Force-flush until every span has been exported.
	var spans tracetest.SpanStubs
	if !waitFor(t, 3*time.Second, func() bool {
		if err := tracer.ForceFlush(context.Background()); err != nil {
			t.Fatalf("ForceFlush() returned error: %v", err)
		}
		spans = exporter.GetSpans()
		return len(spans) == len(tests)
	}) {
		t.Fatalf("exported %d spans, want %d", len(spans), len(tests))
	}

	spanNames := make(map[string]bool, len(spans))
	for _, span := range spans {
		if !span.SpanContext.IsValid() {
			t.Errorf("span %q has an invalid trace context", span.Name)
		}
		spanNames[span.Name] = true
	}
	for _, want := range []string{"cache.ping", "cache.set", "cache.get", "cache.del", "cache.parse"} {
		if !spanNames[want] {
			t.Errorf("no exported span named %q", want)
		}
	}
}

// parseLogEntries decodes every non-empty JSON log line.
func parseLogEntries(t *testing.T, output string) []map[string]any {
	t.Helper()

	var entries []map[string]any
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		entries = append(entries, entry)
	}
	return entries
}
