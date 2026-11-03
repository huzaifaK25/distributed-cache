package tests

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/server"
)

func TestServerConnectionLimit(t *testing.T) {
	s, _, addr := startTestServer(t, server.Config{MaxConnections: 1})

	first := dialClient(t, addr)
	if got := first.send("PING"); got != "PONG" {
		t.Fatalf("first client PING = %q, want PONG", got)
	}

	second, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	defer second.Close()

	if err := second.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	line, err := bufio.NewReader(second).ReadString('\n')
	if err != nil {
		t.Fatalf("read rejection: %v", err)
	}
	if got := strings.TrimSpace(line); got != "ERR too many connections" {
		t.Fatalf("rejection = %q, want ERR too many connections", got)
	}

	if !waitFor(t, time.Second, func() bool {
		stats := s.Stats()
		return stats.RejectedConnections == 1 && stats.TotalConnections == 1
	}) {
		t.Fatalf("Stats() = %+v, want one active and one rejected connection", s.Stats())
	}
	if active := s.Stats().ActiveConnections; active != 1 {
		t.Fatalf("ActiveConnections = %d, want 1", active)
	}
}

func TestServerIdleTimeoutClosesConnection(t *testing.T) {
	s, _, addr := startTestServer(t, server.Config{IdleTimeout: 100 * time.Millisecond})

	c := dialClient(t, addr)
	if got := c.send("PING"); got != "PONG" {
		t.Fatalf("PING = %q, want PONG", got)
	}

	time.Sleep(400 * time.Millisecond)
	c.expectClosed()

	if !waitFor(t, 2*time.Second, func() bool {
		return s.Stats().ActiveConnections == 0
	}) {
		t.Fatalf("ActiveConnections = %d, want 0 after idle timeout", s.Stats().ActiveConnections)
	}
}

func TestServerRejectsOversizedRequest(t *testing.T) {
	_, _, addr := startTestServer(t, server.Config{MaxRequestSize: 64})

	c := dialClient(t, addr)
	if err := c.sendRaw(strings.Repeat("A", 128) + "\n"); err != nil {
		t.Fatalf("write oversized request: %v", err)
	}

	if got := c.readLine(); got != "ERR request too large" {
		t.Fatalf("oversized request response = %q, want ERR request too large", got)
	}

	// The oversized line cannot be resynchronized, so the server closes.
	c.expectClosed()
}

func TestServerManyConcurrentConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1000-connection integration test in short mode")
	}

	const connectionCount = 1000

	s, _, addr := startTestServer(t, server.Config{
		MaxConnections: connectionCount + 100,
		WorkerCount:    32,
		IdleTimeout:    30 * time.Second,
	})

	type rawClient struct {
		conn   net.Conn
		reader *bufio.Reader
	}

	clients := make([]rawClient, 0, connectionCount)
	defer func() {
		for _, client := range clients {
			_ = client.conn.Close()
		}
	}()

	for i := 0; i < connectionCount; i++ {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial connection %d: %v", i, err)
		}
		clients = append(clients, rawClient{conn: conn, reader: bufio.NewReader(conn)})
	}

	if !waitFor(t, 10*time.Second, func() bool {
		return s.Stats().ActiveConnections == connectionCount
	}) {
		t.Fatalf("ActiveConnections = %d, want %d", s.Stats().ActiveConnections, connectionCount)
	}

	errs := make(chan error, connectionCount)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			response, err := roundTrip(clients[i].conn, clients[i].reader, "PING")
			if err != nil {
				errs <- fmt.Errorf("connection %d: %w", i, err)
				return
			}
			if response != "PONG" {
				errs <- fmt.Errorf("connection %d response = %q, want PONG", i, response)
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	for _, client := range clients {
		_ = client.conn.Close()
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return s.Stats().ActiveConnections == 0
	}) {
		t.Fatalf("ActiveConnections = %d, want 0 after clients closed", s.Stats().ActiveConnections)
	}
}

func TestServerLifecycle(t *testing.T) {
	c := cache.New(cache.Config{})
	t.Cleanup(c.StopCleanupWorker)

	s := server.New(c, server.Config{Address: "127.0.0.1:0"})
	if addr := s.Addr(); addr != nil {
		t.Fatalf("Addr() before Start = %v, want nil", addr)
	}

	if err := s.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	if s.Addr() == nil {
		t.Fatal("Addr() after Start returned nil")
	}
	if err := s.Start(); !errors.Is(err, server.ErrAlreadyStarted) {
		t.Fatalf("second Start() error = %v, want ErrAlreadyStarted", err)
	}

	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() returned error: %v", err)
	}
	if err := s.Start(); !errors.Is(err, server.ErrServerClosed) {
		t.Fatalf("Start() after Stop error = %v, want ErrServerClosed", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("second Stop() returned error: %v", err)
	}
}

func TestServerStopBeforeStart(t *testing.T) {
	c := cache.New(cache.Config{})
	t.Cleanup(c.StopCleanupWorker)

	s := server.New(c, server.Config{Address: "127.0.0.1:0"})
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() before Start returned error: %v", err)
	}
	if err := s.Start(); !errors.Is(err, server.ErrServerClosed) {
		t.Fatalf("Start() after Stop error = %v, want ErrServerClosed", err)
	}
}

func TestServerStartFailure(t *testing.T) {
	c := cache.New(cache.Config{})
	t.Cleanup(c.StopCleanupWorker)

	s := server.New(c, server.Config{Address: "not-a-valid-address"})
	if err := s.Start(); err == nil {
		_ = s.Stop()
		t.Fatal("Start() with an invalid address succeeded")
	}

	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() after failed Start returned error: %v", err)
	}
}
