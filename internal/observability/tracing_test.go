package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// newTestTracer returns a tracer that exports to an in-memory recorder.
func newTestTracer(t *testing.T, nodeID string) (*Tracer, *tracetest.InMemoryExporter) {
	t.Helper()

	exporter := tracetest.NewInMemoryExporter()
	tracer, err := NewTracer(context.Background(), TracingConfig{
		NodeID:           nodeID,
		ExporterOverride: exporter,
		SampleRatio:      1,
	})
	if err != nil {
		t.Fatalf("NewTracer() returned error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tracer.Shutdown(ctx)
	})
	return tracer, exporter
}

// forceFlush exports buffered spans.
func forceFlush(t *testing.T, tracer *Tracer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tracer.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush() returned error: %v", err)
	}
}

// spanAttribute returns a string attribute from a recorded span.
func spanAttribute(span tracetest.SpanStub, key string) (string, bool) {
	for _, attr := range span.Attributes {
		if string(attr.Key) == key {
			return attr.Value.AsString(), true
		}
	}
	return "", false
}

func TestTracerExportsRequestSpan(t *testing.T) {
	tracer, exporter := newTestTracer(t, "node-a")

	ctx, span := tracer.StartRequest(context.Background(), "req-1", "GET")
	if !span.SpanContext().IsValid() {
		t.Fatal("StartRequest returned an invalid span context")
	}
	if got := trace.SpanContextFromContext(ctx).SpanID(); got != span.SpanContext().SpanID() {
		t.Fatalf("context span ID = %s, want %s", got, span.SpanContext().SpanID())
	}
	span.End()
	forceFlush(t, tracer)

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	recorded := spans[0]
	if recorded.Name != "cache.get" {
		t.Errorf("span name = %q, want cache.get", recorded.Name)
	}
	if recorded.SpanKind != trace.SpanKindServer {
		t.Errorf("span kind = %v, want server", recorded.SpanKind)
	}
	for key, want := range map[string]string{
		"cache.request_id": "req-1",
		"cache.node_id":    "node-a",
		"cache.operation":  "GET",
	} {
		got, ok := spanAttribute(recorded, key)
		if !ok {
			t.Errorf("span is missing attribute %q", key)
		} else if got != want {
			t.Errorf("span attribute %q = %q, want %q", key, got, want)
		}
	}
}

func TestTelemetryRequestLifecycle(t *testing.T) {
	tracer, exporter := newTestTracer(t, "node-a")

	var logs bytes.Buffer
	logger := NewLogger(LoggingConfig{NodeID: "node-a", Output: &logs})
	metrics := NewMetrics(MetricsConfig{NodeID: "node-a"})
	telemetry := NewTelemetry(metrics, logger, tracer)

	request := telemetry.StartRequestWithID(context.Background(), "req-1", "GET", "127.0.0.1:1234")
	if request == nil {
		t.Fatal("StartRequestWithID returned nil")
	}
	if request.RequestID() != "req-1" {
		t.Errorf("RequestID() = %q, want req-1", request.RequestID())
	}
	if request.TraceID() == "" {
		t.Fatal("TraceID() is empty with tracing enabled")
	}
	if got := RequestIDFromContext(request.Context()); got != "req-1" {
		t.Errorf("context request ID = %q, want req-1", got)
	}

	request.Finish("NOT_FOUND", true)
	// Finish is idempotent so a retry cannot double-count a request.
	request.Finish("NOT_FOUND", true)

	body := scrape(t, metrics)
	for _, want := range []string{
		`requests_total{node_id="node-a",operation="GET",status="not_found"} 1`,
		`request_duration_seconds_count{node_id="node-a",operation="GET"} 1`,
		`requests_in_flight{node_id="node-a"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output does not contain %q\n%s", want, body)
		}
	}

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry); err != nil {
		t.Fatalf("decode request log: %v", err)
	}
	for key, want := range map[string]any{
		FieldNodeID:    "node-a",
		FieldRequestID: "req-1",
		FieldOperation: "GET",
		"status":       "not_found",
		"remoteAddr":   "127.0.0.1:1234",
	} {
		if entry[key] != want {
			t.Errorf("log field %q = %v, want %v", key, entry[key], want)
		}
	}
	if entry[FieldTraceID] != request.TraceID() {
		t.Errorf("log traceID = %v, want %s", entry[FieldTraceID], request.TraceID())
	}

	forceFlush(t, tracer)
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	recorded := spans[0]
	if recorded.Status.Code != codes.Ok {
		t.Errorf("span status = %v, want Ok", recorded.Status.Code)
	}
	for key, want := range map[string]string{
		"cache.status":   "not_found",
		"client.address": "127.0.0.1:1234",
	} {
		got, ok := spanAttribute(recorded, key)
		if !ok || got != want {
			t.Errorf("span attribute %q = %q, want %q", key, got, want)
		}
	}
	writeOK := false
	for _, attr := range recorded.Attributes {
		if string(attr.Key) == "cache.write_ok" {
			writeOK = attr.Value.AsBool()
		}
	}
	if !writeOK {
		t.Error("span cache.write_ok = false, want true")
	}
}

func TestTelemetryWriteFailureIsAnError(t *testing.T) {
	tracer, exporter := newTestTracer(t, "node-a")
	metrics := NewMetrics(MetricsConfig{NodeID: "node-a"})
	telemetry := NewTelemetry(metrics, nil, tracer)

	request := telemetry.StartRequestWithID(context.Background(), "req-2", "SET", "")
	request.Finish("OK", false)

	body := scrape(t, metrics)
	if !strings.Contains(body, `requests_total{node_id="node-a",operation="SET",status="error"} 1`) {
		t.Fatalf("write failure was not counted as an error\n%s", body)
	}

	forceFlush(t, tracer)
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	if spans[0].Status.Code != codes.Error {
		t.Fatalf("span status = %v, want Error", spans[0].Status.Code)
	}
}

func TestTelemetryNilSafety(t *testing.T) {
	var telemetry *Telemetry
	request := telemetry.StartRequest(context.Background(), "GET", "")
	if request != nil {
		t.Fatal("nil Telemetry returned a request")
	}

	request.Finish("OK", true)
	if request.Context() == nil {
		t.Fatal("nil Request Context() returned nil")
	}
	if request.RequestID() != "" || request.Operation() != "" ||
		request.TraceID() != "" || request.SpanID() != "" {
		t.Fatal("nil Request accessors returned non-empty values")
	}
}

func TestNewTracerRejectsInvalidConfig(t *testing.T) {
	if _, err := NewTracer(context.Background(), TracingConfig{Exporter: "jaeger"}); !errors.Is(err, ErrInvalidTracingConfig) {
		t.Fatalf("unsupported exporter error = %v, want ErrInvalidTracingConfig", err)
	}
	if _, err := NewTracer(context.Background(), TracingConfig{Exporter: TraceExporterOTLP}); !errors.Is(err, ErrInvalidTracingConfig) {
		t.Fatalf("missing OTLP endpoint error = %v, want ErrInvalidTracingConfig", err)
	}
}

func TestTracerWithoutExporterStillAssignsIDs(t *testing.T) {
	tracer, err := NewTracer(context.Background(), TracingConfig{Exporter: TraceExporterNone})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tracer.Shutdown(ctx)
	}()

	ctx, span := tracer.StartRequest(context.Background(), "req-1", "PING")
	defer span.End()
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("trace ID was not assigned when no exporter is configured")
	}
}

func TestResponseStatus(t *testing.T) {
	tests := map[string]string{
		"PONG":                RequestOK,
		"OK":                  RequestOK,
		"hello world":         RequestOK,
		"NOT_FOUND":           RequestNotFound,
		"ERR server busy":     RequestBusy,
		"ERR unknown command": RequestError,
	}
	for response, want := range tests {
		if got := ResponseStatus(response); got != want {
			t.Errorf("ResponseStatus(%q) = %q, want %q", response, got, want)
		}
	}
}

func TestSpanName(t *testing.T) {
	tests := map[string]string{
		"GET":     "cache.get",
		"parse":   "cache.parse",
		"":        "cache.request",
		"  PING ": "cache.ping",
	}
	for operation, want := range tests {
		if got := spanName(operation); got != want {
			t.Errorf("spanName(%q) = %q, want %q", operation, got, want)
		}
	}
}
