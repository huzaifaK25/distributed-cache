package server

import (
	"runtime"
	"time"

	"distributed-cache/internal/observability"
)

// Defaults used when a Config field is left at its zero value.
const (
	defaultAddress         = "127.0.0.1:6379"
	defaultMaxConnections  = 4096
	defaultQueueSize       = 4096
	defaultIdleTimeout     = 5 * time.Minute
	defaultWriteTimeout    = 10 * time.Second
	defaultShutdownTimeout = 10 * time.Second
	defaultMaxRequestSize  = 64 * 1024 // 64 KiB per command line.
)

// Config controls the TCP server's limits and timeouts.
//
// Zero values are replaced by defaults in New. Use DefaultConfig to start from
// the defaults and change only the fields that matter.
type Config struct {
	// Address is the TCP listen address, for example "127.0.0.1:6379".
	Address string

	// MaxConnections caps simultaneously registered clients.
	MaxConnections int

	// WorkerCount is the number of request-execution goroutines.
	WorkerCount int

	// QueueSize bounds requests waiting for a worker. When the queue is full,
	// clients receive an ERR server busy response instead of unbounded memory use.
	QueueSize int

	// IdleTimeout closes a connection that sends no request for this long.
	IdleTimeout time.Duration

	// WriteTimeout bounds a single response write.
	WriteTimeout time.Duration

	// ShutdownTimeout bounds how long Stop waits for in-flight work.
	ShutdownTimeout time.Duration

	// MaxRequestSize is the maximum command-line length in bytes.
	MaxRequestSize int

	// Telemetry records request metrics, logs, and traces. It is optional;
	// when nil the server does no request instrumentation.
	Telemetry *observability.Telemetry
}

// DefaultConfig returns production-oriented server defaults.
func DefaultConfig() Config {
	workers := runtime.GOMAXPROCS(0) * 4
	if workers < 4 {
		workers = 4
	}

	return Config{
		Address:         defaultAddress,
		MaxConnections:  defaultMaxConnections,
		WorkerCount:     workers,
		QueueSize:       defaultQueueSize,
		IdleTimeout:     defaultIdleTimeout,
		WriteTimeout:    defaultWriteTimeout,
		ShutdownTimeout: defaultShutdownTimeout,
		MaxRequestSize:  defaultMaxRequestSize,
	}
}

// withDefaults fills unset fields with their default values.
func (c Config) withDefaults() Config {
	defaults := DefaultConfig()

	if c.Address == "" {
		c.Address = defaults.Address
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = defaults.MaxConnections
	}
	if c.WorkerCount <= 0 {
		c.WorkerCount = defaults.WorkerCount
	}
	if c.QueueSize <= 0 {
		c.QueueSize = defaults.QueueSize
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaults.IdleTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaults.WriteTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaults.ShutdownTimeout
	}
	if c.MaxRequestSize <= 0 {
		c.MaxRequestSize = defaults.MaxRequestSize
	}
	return c
}
