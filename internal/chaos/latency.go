package chaos

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// LatencyConfig configures artificial network latency.
type LatencyConfig struct {
	// Min is the minimum delay applied to each read and write.
	Min time.Duration

	// Max is the maximum delay. Zero means the same value as Min.
	Max time.Duration

	// Seed makes the jitter sequence reproducible. Zero uses the current time.
	Seed uint64
}

// Latency injects a uniformly distributed delay into every network operation.
//
// The delay is applied to each Read and Write individually. A request followed
// by a response therefore observes roughly two delays, matching a round trip.
type Latency struct {
	mu  sync.Mutex
	min time.Duration
	max time.Duration
	rng *randomSource
}

// NewLatency validates config and creates a latency injector.
func NewLatency(config LatencyConfig) (*Latency, error) {
	if config.Min < 0 {
		return nil, fmt.Errorf("%w: latency min must not be negative", ErrInvalidConfig)
	}
	if config.Max == 0 {
		config.Max = config.Min
	}
	if config.Max < config.Min {
		return nil, fmt.Errorf("%w: latency max must be at least min", ErrInvalidConfig)
	}

	return &Latency{
		min: config.Min,
		max: config.Max,
		rng: newRandomSource(config.Seed),
	}, nil
}

// Delay returns the next artificial delay.
func (l *Latency) Delay() time.Duration {
	if l == nil {
		return 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.max <= l.min {
		return l.min
	}
	return l.min + time.Duration(l.rng.Float64()*float64(l.max-l.min))
}

// Sleep waits for the next artificial delay or until ctx is done.
func (l *Latency) Sleep(ctx context.Context) error {
	if l == nil {
		return nil
	}
	delay := l.Delay()
	if delay <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Wrap returns a connection that delays reads and writes.
func (l *Latency) Wrap(conn net.Conn) net.Conn {
	if l == nil || conn == nil {
		return conn
	}
	return &latencyConn{Conn: conn, latency: l}
}

// latencyConn applies latency around every underlying network operation.
type latencyConn struct {
	net.Conn
	latency *Latency
}

// Read delays before reading from the wrapped connection.
func (c *latencyConn) Read(p []byte) (int, error) {
	if err := c.latency.Sleep(context.Background()); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

// Write delays before writing to the wrapped connection.
func (c *latencyConn) Write(p []byte) (int, error) {
	if err := c.latency.Sleep(context.Background()); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

// LatencyListener wraps every accepted connection with latency.
type LatencyListener struct {
	net.Listener
	Latency *Latency
}

// NewLatencyListener returns a listener whose accepted sockets are delayed.
func NewLatencyListener(listener net.Listener, latency *Latency) net.Listener {
	if listener == nil {
		return nil
	}
	return &LatencyListener{Listener: listener, Latency: latency}
}

// Accept accepts one connection and applies latency to it.
func (l *LatencyListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if l.Latency == nil {
		return conn, nil
	}
	return l.Latency.Wrap(conn), nil
}
