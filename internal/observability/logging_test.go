package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// decodeLog decodes one JSON log line.
func decodeLog(t *testing.T, line []byte) map[string]any {
	t.Helper()

	var entry map[string]any
	if err := json.Unmarshal(line, &entry); err != nil {
		t.Fatalf("decode log line %q: %v", line, err)
	}
	return entry
}

func TestLoggerAlwaysIncludesNodeID(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(LoggingConfig{NodeID: "node-a", Output: &output})

	logger.Log().Info("hello", "custom", 42)

	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != 1 {
		t.Fatalf("wrote %d lines, want 1: %q", len(lines), output.String())
	}

	entry := decodeLog(t, lines[0])
	if entry["nodeID"] != "node-a" {
		t.Errorf("nodeID = %v, want node-a", entry["nodeID"])
	}
	if entry["msg"] != "hello" {
		t.Errorf("msg = %v, want hello", entry["msg"])
	}
	if entry["custom"] != float64(42) {
		t.Errorf("custom = %v, want 42", entry["custom"])
	}
}

func TestRequestLoggerIncludesCorrelationFields(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(LoggingConfig{NodeID: "node-a", Output: &output})

	logger.RequestLogger(context.Background(), "req-1", "GET").Info("request completed", "status", "ok")

	entry := decodeLog(t, bytes.TrimSpace(output.Bytes()))
	if entry[FieldNodeID] != "node-a" {
		t.Errorf("nodeID = %v, want node-a", entry[FieldNodeID])
	}
	if entry[FieldRequestID] != "req-1" {
		t.Errorf("requestID = %v, want req-1", entry[FieldRequestID])
	}
	if entry[FieldOperation] != "GET" {
		t.Errorf("operation = %v, want GET", entry[FieldOperation])
	}
	if _, present := entry[FieldTraceID]; present {
		t.Errorf("traceID must be absent without an active span: %v", entry[FieldTraceID])
	}
}

func TestRequestLoggerUsesContextRequestID(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(LoggingConfig{NodeID: "node-a", Output: &output})
	ctx := WithRequestID(context.Background(), "req-context")

	logger.RequestLogger(ctx, "", "PING").Info("request completed")

	entry := decodeLog(t, bytes.TrimSpace(output.Bytes()))
	if entry[FieldRequestID] != "req-context" {
		t.Errorf("requestID = %v, want req-context", entry[FieldRequestID])
	}
}

func TestRequestLoggerAddsTraceAndSpanIDs(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer, err := NewTracer(context.Background(), TracingConfig{
		NodeID:           "node-a",
		ExporterOverride: exporter,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tracer.Shutdown(ctx)
	})

	ctx, span := tracer.StartRequest(context.Background(), "req-1", "GET")
	defer span.End()

	var output bytes.Buffer
	logger := NewLogger(LoggingConfig{NodeID: "node-a", Output: &output})
	logger.RequestLogger(ctx, "req-1", "GET").Info("request completed")

	entry := decodeLog(t, bytes.TrimSpace(output.Bytes()))
	spanContext := span.SpanContext()
	if entry[FieldTraceID] != spanContext.TraceID().String() {
		t.Errorf("traceID = %v, want %s", entry[FieldTraceID], spanContext.TraceID())
	}
	if entry[FieldSpanID] != spanContext.SpanID().String() {
		t.Errorf("spanID = %v, want %s", entry[FieldSpanID], spanContext.SpanID())
	}
}

func TestLoggerTextFormatAndLevel(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(LoggingConfig{
		NodeID: "node-a",
		Output: &output,
		Level:  slog.LevelWarn,
		Text:   true,
	})

	logger.Log().Info("hidden")
	logger.Log().Warn("visible", "status", "error")

	text := output.String()
	if strings.Contains(text, "hidden") {
		t.Fatalf("info log was not filtered: %q", text)
	}
	for _, want := range []string{"visible", "level=WARN", "nodeID=node-a", "status=error"} {
		if !strings.Contains(text, want) {
			t.Errorf("text log does not contain %q: %q", want, text)
		}
	}
}

func TestNilLoggerIsSafe(t *testing.T) {
	var logger *Logger
	if logger.NodeID() != "" {
		t.Fatal("nil logger returned a node ID")
	}
	logger.Log().Info("discarded")
	logger.With("key", "value").Info("discarded")
	logger.RequestLogger(context.Background(), "req-1", "GET").Info("discarded")
}

func TestNewRequestIDIsRandomHex(t *testing.T) {
	seen := make(map[string]struct{}, 128)
	for range 128 {
		requestID := NewRequestID()
		if len(requestID) != 32 {
			t.Fatalf("request ID %q length = %d, want 32", requestID, len(requestID))
		}
		if strings.Trim(requestID, "0123456789abcdef") != "" {
			t.Fatalf("request ID %q is not lower-case hex", requestID)
		}
		if _, duplicate := seen[requestID]; duplicate {
			t.Fatalf("request ID %q was generated twice", requestID)
		}
		seen[requestID] = struct{}{}
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input string
		want  slog.Level
	}{
		{input: "debug", want: slog.LevelDebug},
		{input: "INFO", want: slog.LevelInfo},
		{input: " warn ", want: slog.LevelWarn},
		{input: "warning", want: slog.LevelWarn},
		{input: "error", want: slog.LevelError},
		{input: "", want: slog.LevelInfo},
	}

	for _, tt := range tests {
		level, err := ParseLogLevel(tt.input)
		if err != nil {
			t.Errorf("ParseLogLevel(%q) returned error: %v", tt.input, err)
			continue
		}
		if level != tt.want {
			t.Errorf("ParseLogLevel(%q) = %v, want %v", tt.input, level, tt.want)
		}
	}

	if _, err := ParseLogLevel("verbose"); err == nil {
		t.Fatal("ParseLogLevel(verbose) returned nil error")
	}
}
