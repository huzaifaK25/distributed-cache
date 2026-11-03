package tests

import (
	"net"
	"sync"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/server"
)

// gatedCache blocks Get calls until the test releases them. It makes in-flight
// request behavior deterministic without adding test hooks to the server.
type gatedCache struct {
	cache.Cache
	once        sync.Once
	releaseOnce sync.Once
	started     chan struct{}
	release     chan struct{}
}

func newGatedCache(inner cache.Cache) *gatedCache {
	return &gatedCache{
		Cache:   inner,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *gatedCache) Get(key string) (any, bool) {
	g.once.Do(func() { close(g.started) })
	<-g.release
	return g.Cache.Get(key)
}

// open releases all blocked Gets and is safe to call more than once.
func (g *gatedCache) open() {
	g.releaseOnce.Do(func() { close(g.release) })
}

func TestServerGracefulShutdownCompletesInFlightRequest(t *testing.T) {
	inner := cache.New(cache.Config{})
	t.Cleanup(inner.StopCleanupWorker)

	gate := newGatedCache(inner)
	s := startServer(t, gate, server.Config{
		Address:         "127.0.0.1:0",
		ShutdownTimeout: 2 * time.Second,
	})
	// Register after startServer so the gate opens before cleanup calls Stop.
	t.Cleanup(gate.open)
	addr := s.Addr().String()
	client := dialClient(t, addr)

	if err := client.sendRaw("GET key\n"); err != nil {
		t.Fatalf("write GET: %v", err)
	}

	select {
	case <-gate.started:
	case <-time.After(2 * time.Second):
		t.Fatal("request never reached the cache")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- s.Stop() }()

	// Stop must wait for the blocked, already-read request.
	select {
	case err := <-stopDone:
		t.Fatalf("Stop() returned before the in-flight request completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// The listener is closed as soon as shutdown begins, even while the
	// in-flight request is still running.
	if conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("listener accepted a connection during shutdown")
	}

	gate.open()

	if got := client.readLine(); got != "NOT_FOUND" {
		t.Fatalf("in-flight GET response = %q, want NOT_FOUND", got)
	}

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not finish after the request completed")
	}

	client.expectClosed()

	// The server must not be reusable after shutdown.
	if err := s.Stop(); err != nil {
		t.Fatalf("second Stop() returned error: %v", err)
	}
	if conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("listener still accepts connections after Stop")
	}
}

func TestServerStopClosesIdleConnections(t *testing.T) {
	s, _, addr := startTestServer(t, server.Config{IdleTimeout: time.Minute})

	client := dialClient(t, addr)
	if got := client.send("PING"); got != "PONG" {
		t.Fatalf("PING = %q, want PONG", got)
	}

	start := time.Now()
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Stop() took %v with an idle connection", elapsed)
	}

	client.expectClosed()
}

func TestServerConcurrentStop(t *testing.T) {
	s, _, addr := startTestServer(t, server.Config{})

	client := dialClient(t, addr)
	if got := client.send("PING"); got != "PONG" {
		t.Fatalf("PING = %q, want PONG", got)
	}

	const callers = 8
	errs := make(chan error, callers)
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- s.Stop()
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Stop() returned error: %v", err)
		}
	}
}
