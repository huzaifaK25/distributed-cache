// Package server exposes the cache over a line-oriented TCP protocol.
package server

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"distributed-cache/internal/cache"
)

// Lifecycle errors.
var (
	// ErrAlreadyStarted is returned when Start is called twice.
	ErrAlreadyStarted = errors.New("server: already started")

	// ErrServerClosed is returned when Start is called after Stop.
	ErrServerClosed = errors.New("server: closed")

	// ErrShutdownTimeout is returned when in-flight work outlives
	// Config.ShutdownTimeout. Connections are force-closed in that case.
	ErrShutdownTimeout = errors.New("server: shutdown timeout")
)

// serverState is the server lifecycle position.
type serverState uint8

const (
	stateNew serverState = iota
	stateRunning
	stateStopping
	stateStopped
)

// Stats is a point-in-time snapshot of server activity.
type Stats struct {
	ActiveConnections   int
	TotalConnections    uint64 // Connections registered successfully.
	RejectedConnections uint64 // Connections refused by the connection limit.
	Requests            uint64 // Command lines read from clients.
}

// ConnectionInfo describes one active client connection.
type ConnectionInfo struct {
	RemoteAddr  string
	ConnectedAt time.Time
	LastActive  time.Time
}

// Server is a TCP cache service.
//
// Create it with New, call Start to listen, and call Stop for a graceful
// shutdown. A Server instance cannot be restarted after Stop.
type Server struct {
	address    string
	cache      cache.Cache
	listener   net.Listener
	workerPool *workerPool

	config  Config
	handler *Handler

	stateMu   sync.Mutex
	state     serverState
	stopping  chan struct{}
	stoppedCh chan struct{}
	stopErr   error

	acceptWG sync.WaitGroup
	connWG   sync.WaitGroup

	connMu sync.Mutex
	conns  map[*connection]struct{}

	totalConnections    atomic.Uint64
	rejectedConnections atomic.Uint64
	requests            atomic.Uint64
}

// New creates a server around c. It does not open a socket or start workers
// until Start. New panics if c is nil.
func New(c cache.Cache, config Config) *Server {
	if c == nil {
		panic("server: nil cache")
	}
	config = config.withDefaults()

	return &Server{
		address:   config.Address,
		cache:     c,
		config:    config,
		handler:   NewHandler(c),
		state:     stateNew,
		stopping:  make(chan struct{}),
		stoppedCh: make(chan struct{}),
		conns:     make(map[*connection]struct{}),
	}
}

// Start opens the TCP listener and begins accepting clients.
//
// It returns once the listener is ready; accepting happens in the background.
// Start returns ErrAlreadyStarted on a second call and ErrServerClosed after
// Stop.
func (s *Server) Start() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	switch s.state {
	case stateRunning:
		return ErrAlreadyStarted
	case stateStopping, stateStopped:
		return ErrServerClosed
	}

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.address, err)
	}

	s.listener = listener
	s.address = listener.Addr().String()
	s.workerPool = newWorkerPool(s.config.WorkerCount, s.config.QueueSize)
	s.state = stateRunning
	s.acceptWG.Add(1)
	go s.acceptLoop()
	return nil
}

// Addr returns the bound listener address, or nil before Start.
//
// It is useful with an address of "127.0.0.1:0", where the kernel chooses a
// free port.
func (s *Server) Addr() net.Addr {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Stats returns a snapshot of connection and request counters.
func (s *Server) Stats() Stats {
	s.connMu.Lock()
	active := len(s.conns)
	s.connMu.Unlock()

	return Stats{
		ActiveConnections:   active,
		TotalConnections:    s.totalConnections.Load(),
		RejectedConnections: s.rejectedConnections.Load(),
		Requests:            s.requests.Load(),
	}
}

// Connections returns a snapshot of active client connections.
func (s *Server) Connections() []ConnectionInfo {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	conns := make([]ConnectionInfo, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, ConnectionInfo{
			RemoteAddr:  conn.remoteAddr,
			ConnectedAt: conn.connectedAt,
			LastActive:  conn.lastActiveAt(),
		})
	}
	return conns
}

// acceptLoop registers clients and hands each one to a connection goroutine.
// The goroutine count is bounded by Config.MaxConnections.
func (s *Server) acceptLoop() {
	defer s.acceptWG.Done()

	for {
		rawConn, err := s.listener.Accept()
		if err != nil {
			if s.isStopping() {
				return
			}

			// A transient accept failure should not silently kill the server.
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return
		}

		if s.isStopping() {
			_ = rawConn.Close()
			return
		}

		conn := newConnection(rawConn)
		if !s.registerConnection(conn) {
			if s.isStopping() {
				_ = rawConn.Close()
				return
			}
			s.rejectedConnections.Add(1)
			s.rejectConnection(conn)
			continue
		}

		s.totalConnections.Add(1)
		s.connWG.Add(1)
		go s.serveConnection(conn)
	}
}

// registerConnection reserves a connection slot, enforcing MaxConnections.
func (s *Server) registerConnection(conn *connection) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	if s.isStopping() || len(s.conns) >= s.config.MaxConnections {
		return false
	}

	s.conns[conn] = struct{}{}
	return true
}

// unregisterConnection releases a connection slot.
func (s *Server) unregisterConnection(conn *connection) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

// isStopping reports whether shutdown has begun.
func (s *Server) isStopping() bool {
	select {
	case <-s.stopping:
		return true
	default:
		return false
	}
}

// Stop gracefully shuts the server down.
//
// It stops accepting clients, unblocks idle reads, lets in-flight requests
// complete, closes remaining connections, and drains the worker pool. Stop is
// idempotent and safe to call from multiple goroutines. If work outlives
// Config.ShutdownTimeout, connections are force-closed and ErrShutdownTimeout
// is returned.
func (s *Server) Stop() error {
	s.stateMu.Lock()
	switch s.state {
	case stateStopped:
		done := s.stoppedCh
		s.stateMu.Unlock()
		<-done
		return s.shutdownResult()

	case stateStopping:
		done := s.stoppedCh
		s.stateMu.Unlock()
		<-done
		return s.shutdownResult()

	case stateNew:
		s.state = stateStopping
		close(s.stopping)
		s.stateMu.Unlock()

		s.finishStop(nil)
		return nil
	}

	s.state = stateStopping
	close(s.stopping)
	listener := s.listener
	s.stateMu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	s.acceptWG.Wait()

	conns := s.snapshotConnections()
	for _, conn := range conns {
		// Wake connection goroutines blocked on an idle read. Requests that
		// were already read continue through the worker pool and are written
		// before the connection exits.
		_ = conn.SetReadDeadline(time.Now())
	}

	if !waitGroupWithTimeout(&s.connWG, s.config.ShutdownTimeout) {
		for _, conn := range conns {
			_ = conn.Close()
		}
		s.workerPool.Stop()
		s.connWG.Wait()
		s.finishStop(ErrShutdownTimeout)
		return ErrShutdownTimeout
	}

	s.closeRemainingConnections()
	s.workerPool.Stop()
	s.finishStop(nil)
	return nil
}

// shutdownResult returns the outcome recorded by the goroutine that performed
// the shutdown.
func (s *Server) shutdownResult() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.stopErr
}

// finishStop records the outcome and wakes any concurrent Stop callers.
func (s *Server) finishStop(err error) {
	s.stateMu.Lock()
	s.stopErr = err
	s.state = stateStopped
	close(s.stoppedCh)
	s.stateMu.Unlock()
}

// snapshotConnections copies the active connection set.
func (s *Server) snapshotConnections() []*connection {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	conns := make([]*connection, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	return conns
}

// closeRemainingConnections closes any connection handlers still registered.
func (s *Server) closeRemainingConnections() {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	for conn := range s.conns {
		_ = conn.Close()
	}
}

// waitGroupWithTimeout waits for wg and reports whether it completed in time.
func waitGroupWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
