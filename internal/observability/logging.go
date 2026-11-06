package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Structured log field names required for request correlation.
const (
	FieldRequestID = "requestID"
	FieldNodeID    = "nodeID"
	FieldOperation = "operation"
	FieldTraceID   = "traceID"
	FieldSpanID    = "spanID"
)

// LoggingConfig controls the structured logger.
type LoggingConfig struct {
	// NodeID is attached to every record and defaults to "local".
	NodeID string

	// Output receives log records and defaults to os.Stdout.
	Output io.Writer

	// Level is the minimum enabled level. The zero value is slog.LevelInfo.
	Level slog.Level

	// Text emits logfmt-style text instead of JSON.
	Text bool

	// AddSource includes the caller's source location.
	AddSource bool
}

// Logger is a structured logger that always includes nodeID and can derive
// request and trace identifiers from a request context.
//
// A nil *Logger discards records.
type Logger struct {
	nodeID string
	log    *slog.Logger
}

// NewLogger builds a JSON logger by default. Log records always carry nodeID.
func NewLogger(config LoggingConfig) *Logger {
	if config.NodeID == "" {
		config.NodeID = "local"
	}
	if config.Output == nil {
		config.Output = os.Stdout
	}

	options := &slog.HandlerOptions{
		Level:     config.Level,
		AddSource: config.AddSource,
	}

	var handler slog.Handler
	if config.Text {
		handler = slog.NewTextHandler(config.Output, options)
	} else {
		handler = slog.NewJSONHandler(config.Output, options)
	}

	return &Logger{
		nodeID: config.NodeID,
		log:    slog.New(handler).With(FieldNodeID, config.NodeID),
	}
}

// NodeID returns the node identity attached to every record.
func (l *Logger) NodeID() string {
	if l == nil {
		return ""
	}
	return l.nodeID
}

// Log returns the underlying slog logger with nodeID attached.
func (l *Logger) Log() *slog.Logger {
	if l == nil || l.log == nil {
		return discardLogger
	}
	return l.log
}

// With returns a logger with additional structured fields.
func (l *Logger) With(args ...any) *slog.Logger {
	return l.Log().With(args...)
}

// RequestLogger returns a logger carrying requestID, operation, and the trace
// and span identifiers from ctx. It is the standard way to log one request.
func (l *Logger) RequestLogger(ctx context.Context, requestID, operation string) *slog.Logger {
	logger := l.Log()

	if requestID == "" && ctx != nil {
		requestID = RequestIDFromContext(ctx)
	}
	if requestID != "" {
		logger = logger.With(FieldRequestID, requestID)
	}
	if operation != "" {
		logger = logger.With(FieldOperation, operation)
	}
	if ctx != nil {
		if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
			logger = logger.With(
				FieldTraceID, spanContext.TraceID().String(),
				FieldSpanID, spanContext.SpanID().String(),
			)
		}
	}
	return logger
}

// requestIDContextKey is private so only this package can populate the value.
type requestIDContextKey struct{}

// WithRequestID returns a context carrying a request identifier.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestIDContextKey{}, requestID)
}

// RequestIDFromContext returns the request identifier stored in ctx.
func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

// requestCounter is the fallback source when crypto/rand is unavailable.
var requestCounter atomic.Uint64

// NewRequestID returns a cryptographically random 128-bit identifier in hex.
func NewRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}

	return fmt.Sprintf("%016x%016x", uint64(time.Now().UnixNano()), requestCounter.Add(1))
}

// ParseLogLevel converts a level name into a slog level.
func ParseLogLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("observability: unknown log level %q", level)
	}
}

// discardLogger is used by nil loggers so callers never need a nil check.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
