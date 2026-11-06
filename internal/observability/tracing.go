package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ErrInvalidTracingConfig is returned for unsupported exporter settings.
var ErrInvalidTracingConfig = errors.New("observability: invalid tracing config")

// TraceExporter selects where finished spans are sent.
type TraceExporter string

const (
	// TraceExporterNone creates spans and trace IDs without exporting them.
	TraceExporterNone TraceExporter = "none"

	// TraceExporterStdout writes one JSON object per span to Output.
	TraceExporterStdout TraceExporter = "stdout"

	// TraceExporterOTLP sends spans to an OTLP/HTTP collector.
	TraceExporterOTLP TraceExporter = "otlp"
)

// TracingConfig controls the OpenTelemetry tracer provider.
type TracingConfig struct {
	// ServiceName is the OpenTelemetry service.name resource attribute.
	ServiceName string

	// NodeID is attached to every span and defaults to the service name.
	NodeID string

	// Exporter selects the span exporter. The zero value does not export spans.
	Exporter TraceExporter

	// Output receives stdout spans and defaults to os.Stderr so trace output
	// does not interleave with JSON application logs.
	Output io.Writer

	// OTLPEndpoint is a collector address such as "localhost:4318" or a full
	// URL such as "https://collector.example.com/v1/traces".
	OTLPEndpoint string

	// OTLPInsecure disables TLS for a host:port endpoint.
	OTLPInsecure bool

	// SampleRatio samples this fraction of new traces. Zero defaults to 1.0.
	SampleRatio float64

	// ExporterOverride injects an exporter, primarily for tests and embedding.
	// When set, Exporter and OTLPEndpoint are ignored.
	ExporterOverride sdktrace.SpanExporter
}

// Tracer owns the OpenTelemetry tracer provider.
type Tracer struct {
	nodeID   string
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
}

// NewTracer builds a tracer provider and its configured exporter.
func NewTracer(ctx context.Context, config TracingConfig) (*Tracer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config.ServiceName == "" {
		config.ServiceName = "distributed-cache"
	}
	if config.NodeID == "" {
		config.NodeID = config.ServiceName
	}
	if config.SampleRatio <= 0 {
		config.SampleRatio = 1
	}
	if config.SampleRatio > 1 {
		config.SampleRatio = 1
	}

	exporter, exporterKind, err := buildExporter(ctx, config)
	if err != nil {
		return nil, err
	}

	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", config.ServiceName),
			attribute.String("service.instance.id", config.NodeID),
			attribute.String("cache.node_id", config.NodeID),
		)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(config.SampleRatio))),
	}

	if exporter != nil {
		if exporterKind == TraceExporterStdout {
			// A development exporter should make spans visible immediately.
			options = append(options, sdktrace.WithSyncer(exporter))
		} else {
			options = append(options, sdktrace.WithBatcher(exporter))
		}
	}

	provider := sdktrace.NewTracerProvider(options...)
	return &Tracer{
		nodeID:   config.NodeID,
		provider: provider,
		tracer:   provider.Tracer("distributed-cache/observability"),
	}, nil
}

// buildExporter validates the exporter configuration and constructs it.
func buildExporter(ctx context.Context, config TracingConfig) (sdktrace.SpanExporter, TraceExporter, error) {
	if config.ExporterOverride != nil {
		return config.ExporterOverride, TraceExporterOTLP, nil
	}

	switch config.Exporter {
	case "", TraceExporterNone:
		return nil, TraceExporterNone, nil

	case TraceExporterStdout:
		output := config.Output
		if output == nil {
			output = os.Stderr
		}
		exporter, err := stdouttrace.New(stdouttrace.WithWriter(output))
		if err != nil {
			return nil, "", fmt.Errorf("observability: stdout trace exporter: %w", err)
		}
		return exporter, TraceExporterStdout, nil

	case TraceExporterOTLP:
		if config.OTLPEndpoint == "" {
			return nil, "", fmt.Errorf("%w: otlp exporter requires an endpoint", ErrInvalidTracingConfig)
		}

		options := make([]otlptracehttp.Option, 0, 2)
		if strings.Contains(config.OTLPEndpoint, "://") {
			options = append(options, otlptracehttp.WithEndpointURL(config.OTLPEndpoint))
		} else {
			options = append(options, otlptracehttp.WithEndpoint(config.OTLPEndpoint))
			if config.OTLPInsecure {
				options = append(options, otlptracehttp.WithInsecure())
			}
		}

		exporter, err := otlptracehttp.New(ctx, options...)
		if err != nil {
			return nil, "", fmt.Errorf("observability: otlp trace exporter: %w", err)
		}
		return exporter, TraceExporterOTLP, nil

	default:
		return nil, "", fmt.Errorf("%w: unsupported exporter %q", ErrInvalidTracingConfig, config.Exporter)
	}
}

// StartRequest starts a server span for one cache request.
func (t *Tracer) StartRequest(ctx context.Context, requestID, operation string) (context.Context, trace.Span) {
	if t == nil || t.tracer == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		return ctx, trace.SpanFromContext(ctx)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	return t.tracer.Start(ctx, spanName(operation),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("cache.request_id", requestID),
			attribute.String("cache.node_id", t.nodeID),
			attribute.String("cache.operation", operation),
		),
	)
}

// ForceFlush exports buffered spans immediately.
func (t *Tracer) ForceFlush(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.ForceFlush(ctx)
}

// Shutdown flushes buffered spans and releases exporter resources.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

// spanName returns a stable, low-cardinality OpenTelemetry span name.
func spanName(operation string) string {
	operation = strings.TrimSpace(operation)
	if operation == "" {
		return "cache.request"
	}
	return "cache." + strings.ToLower(operation)
}

// Telemetry bundles the three observability signals used by the server.
type Telemetry struct {
	Metrics *Metrics
	Logger  *Logger
	Tracer  *Tracer
}

// NewTelemetry returns nil when no component is configured.
func NewTelemetry(metrics *Metrics, logger *Logger, tracer *Tracer) *Telemetry {
	if metrics == nil && logger == nil && tracer == nil {
		return nil
	}
	return &Telemetry{Metrics: metrics, Logger: logger, Tracer: tracer}
}

// Request captures one in-flight request's observability state.
type Request struct {
	telemetry *Telemetry
	ctx       context.Context
	span      trace.Span

	requestID  string
	operation  string
	remoteAddr string
	traceID    string
	spanID     string
	started    time.Time
	finished   atomic.Bool
}

// StartRequest begins an observed request and assigns a new request ID.
func (t *Telemetry) StartRequest(ctx context.Context, operation, remoteAddr string) *Request {
	return t.StartRequestWithID(ctx, NewRequestID(), operation, remoteAddr)
}

// StartRequestWithID begins an observed request with a caller-supplied ID.
func (t *Telemetry) StartRequestWithID(ctx context.Context, requestID, operation, remoteAddr string) *Request {
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if requestID == "" {
		requestID = NewRequestID()
	}
	if operation == "" {
		operation = "UNKNOWN"
	}

	request := &Request{
		telemetry:  t,
		ctx:        ctx,
		requestID:  requestID,
		operation:  operation,
		remoteAddr: remoteAddr,
		started:    time.Now(),
	}

	if t.Tracer != nil {
		request.ctx, request.span = t.Tracer.StartRequest(ctx, requestID, operation)
		if request.span != nil {
			spanContext := request.span.SpanContext()
			if spanContext.IsValid() {
				request.traceID = spanContext.TraceID().String()
				request.spanID = spanContext.SpanID().String()
			}
		}
		if remoteAddr != "" && request.span != nil {
			request.span.SetAttributes(attribute.String("client.address", remoteAddr))
		}
	}

	request.ctx = WithRequestID(request.ctx, requestID)
	if t.Metrics != nil {
		t.Metrics.IncInFlight()
	}
	return request
}

// Context returns the request context with the active span and request ID.
func (r *Request) Context() context.Context {
	if r == nil || r.ctx == nil {
		return context.Background()
	}
	return r.ctx
}

// RequestID returns the correlation identifier for this request.
func (r *Request) RequestID() string {
	if r == nil {
		return ""
	}
	return r.requestID
}

// Operation returns the command operation being observed.
func (r *Request) Operation() string {
	if r == nil {
		return ""
	}
	return r.operation
}

// TraceID returns the OpenTelemetry trace ID, or "" when tracing is disabled.
func (r *Request) TraceID() string {
	if r == nil {
		return ""
	}
	return r.traceID
}

// SpanID returns the OpenTelemetry span ID, or "" when tracing is disabled.
func (r *Request) SpanID() string {
	if r == nil {
		return ""
	}
	return r.spanID
}

// Finish records metrics, writes the request log, and ends the span.
//
// It is safe to call on a nil request. Callers invoke it once after the
// response write attempt; repeated calls are ignored.
func (r *Request) Finish(response string, writeOK bool) {
	if r == nil || !r.finished.CompareAndSwap(false, true) {
		return
	}

	duration := time.Since(r.started)
	status := ResponseStatus(response)
	if !writeOK {
		status = RequestError
	}

	if r.telemetry != nil && r.telemetry.Metrics != nil {
		r.telemetry.Metrics.DecInFlight()
		r.telemetry.Metrics.ObserveRequest(r.operation, status, duration)
	}

	if r.telemetry != nil && r.telemetry.Logger != nil {
		logger := r.telemetry.Logger.RequestLogger(r.ctx, r.requestID, r.operation)
		attributes := []any{
			"status", status,
			"durationMs", float64(duration.Nanoseconds()) / 1e6,
			"responseBytes", len(response),
			"writeOK", writeOK,
		}
		if r.remoteAddr != "" {
			attributes = append(attributes, "remoteAddr", r.remoteAddr)
		}

		switch status {
		case RequestError:
			logger.Error("request failed", attributes...)
		case RequestBusy:
			logger.Warn("request rejected", attributes...)
		default:
			logger.Info("request completed", attributes...)
		}
	}

	if r.span != nil {
		r.span.SetAttributes(
			attribute.String("cache.status", status),
			attribute.Int("cache.response_bytes", len(response)),
			attribute.Bool("cache.write_ok", writeOK),
		)
		if status == RequestError || status == RequestBusy {
			r.span.SetStatus(codes.Error, response)
		} else {
			r.span.SetStatus(codes.Ok, "")
		}
		r.span.End()
	}
}

// ResponseStatus classifies a protocol response for metrics and logs.
func ResponseStatus(response string) string {
	switch {
	case response == "NOT_FOUND":
		return RequestNotFound
	case response == "ERR server busy":
		return RequestBusy
	case strings.HasPrefix(response, "ERR "):
		return RequestError
	default:
		return RequestOK
	}
}
