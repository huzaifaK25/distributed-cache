// Command cache runs the TCP cache server with metrics, structured logs, and
// OpenTelemetry traces.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/observability"
	"distributed-cache/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("cache: %v", err)
	}
}

func run() error {
	// Server and cache settings.
	serverConfig := server.DefaultConfig()
	var (
		maxItems        int
		cleanupInterval time.Duration
	)

	// Observability settings.
	defaultNodeID, err := os.Hostname()
	if err != nil || defaultNodeID == "" {
		defaultNodeID = "cache-node"
	}
	var (
		nodeID       string
		metricsAddr  string
		logLevel     string
		logText      bool
		traceExport  string
		otelEndpoint string
		otelInsecure bool
		traceSample  float64
	)

	flag.StringVar(&serverConfig.Address, "addr", serverConfig.Address, "TCP listen address")
	flag.IntVar(&serverConfig.MaxConnections, "max-conns", serverConfig.MaxConnections, "maximum simultaneous clients")
	flag.IntVar(&serverConfig.WorkerCount, "workers", serverConfig.WorkerCount, "request worker goroutines")
	flag.IntVar(&serverConfig.QueueSize, "queue-size", serverConfig.QueueSize, "maximum queued requests")
	flag.DurationVar(&serverConfig.IdleTimeout, "idle-timeout", serverConfig.IdleTimeout, "close idle client connections after this duration")
	flag.DurationVar(&serverConfig.WriteTimeout, "write-timeout", serverConfig.WriteTimeout, "maximum time to write one response")
	flag.DurationVar(&serverConfig.ShutdownTimeout, "shutdown-timeout", serverConfig.ShutdownTimeout, "maximum graceful shutdown wait")
	flag.IntVar(&serverConfig.MaxRequestSize, "max-request-size", serverConfig.MaxRequestSize, "maximum command-line bytes")
	flag.IntVar(&maxItems, "max-items", 0, "maximum cache entries (<=0 means unlimited)")
	flag.DurationVar(&cleanupInterval, "cleanup-interval", 30*time.Second, "expired-key sweep interval (<=0 disables the worker)")

	flag.StringVar(&nodeID, "node-id", defaultNodeID, "node identity used in logs, metrics, and traces")
	flag.StringVar(&metricsAddr, "metrics-addr", "127.0.0.1:9090", "Prometheus metrics and health HTTP listen address (empty disables)")
	flag.StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn, or error")
	flag.BoolVar(&logText, "log-text", false, "emit text logs instead of JSON")
	flag.StringVar(&traceExport, "trace-exporter", "stdout", "trace exporter: none, stdout, or otlp")
	flag.StringVar(&otelEndpoint, "otel-endpoint", "localhost:4318", "OTLP/HTTP trace collector endpoint")
	flag.BoolVar(&otelInsecure, "otel-insecure", true, "disable TLS when exporting traces to an OTLP host:port endpoint")
	flag.Float64Var(&traceSample, "trace-sample", 1.0, "fraction of new traces to sample (0 < ratio <= 1)")
	flag.Parse()

	level, err := observability.ParseLogLevel(logLevel)
	if err != nil {
		return err
	}

	// The cache owns the data; metrics read its cumulative counters at scrape
	// time instead of duplicating per-operation accounting.
	store := cache.New(cache.Config{
		MaxItems:        maxItems,
		CleanupInterval: cleanupInterval,
	})
	defer store.StopCleanupWorker()

	logger := observability.NewLogger(observability.LoggingConfig{
		NodeID: nodeID,
		Output: os.Stdout,
		Level:  level,
		Text:   logText,
	})

	metrics := observability.NewMetrics(observability.MetricsConfig{
		NodeID:     nodeID,
		CacheStats: store.Stats,
	})

	tracer, err := observability.NewTracer(context.Background(), observability.TracingConfig{
		ServiceName:  "distributed-cache",
		NodeID:       nodeID,
		Exporter:     observability.TraceExporter(traceExport),
		Output:       os.Stderr,
		OTLPEndpoint: otelEndpoint,
		OTLPInsecure: otelInsecure,
		SampleRatio:  traceSample,
	})
	if err != nil {
		return fmt.Errorf("configure tracing: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracer.Shutdown(ctx); err != nil {
			logger.Log().Error("trace shutdown failed", "error", err)
		}
	}()

	serverConfig.Telemetry = observability.NewTelemetry(metrics, logger, tracer)

	srv := server.New(store, serverConfig)
	if err := srv.Start(); err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	logger.Log().Info("cache server listening",
		"address", srv.Addr().String(),
		"workers", serverConfig.WorkerCount,
		"maxConnections", serverConfig.MaxConnections,
	)

	metricsServer, err := startMetricsServer(metricsAddr, metrics, logger)
	if err != nil {
		_ = srv.Stop()
		return err
	}
	defer shutdownMetrics(metricsServer, logger)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals

	logger.Log().Info("shutting down")
	if err := srv.Stop(); err != nil {
		logger.Log().Error("server shutdown failed", "error", err)
		return err
	}
	logger.Log().Info("stopped")
	return nil
}

// startMetricsServer exposes Prometheus metrics and a liveness endpoint.
func startMetricsServer(addr string, metrics *observability.Metrics, logger *observability.Logger) (*http.Server, error) {
	if addr == "" {
		return nil, nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen %s: %w", addr, err)
	}

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	logger.Log().Info("metrics listening", "address", listener.Addr().String(), "path", "/metrics")
	go func() {
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Log().Error("metrics server stopped", "error", err)
		}
	}()
	return httpServer, nil
}

// shutdownMetrics stops the metrics endpoint without blocking shutdown.
func shutdownMetrics(httpServer *http.Server, logger *observability.Logger) {
	if httpServer == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Log().Error("metrics shutdown failed", "error", err)
		_ = httpServer.Close()
	}
}
