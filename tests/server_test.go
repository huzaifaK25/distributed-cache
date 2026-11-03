package tests

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/server"
)

// startServer starts s around c and guarantees it is stopped during cleanup.
func startServer(t *testing.T, c cache.Cache, config server.Config) *server.Server {
	t.Helper()

	if config.Address == "" {
		config.Address = "127.0.0.1:0"
	}

	s := server.New(c, config)
	if err := s.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Stop(); err != nil && !errors.Is(err, server.ErrShutdownTimeout) {
			t.Errorf("Stop() returned error: %v", err)
		}
	})
	return s
}

// startTestServer creates an isolated cache and server.
func startTestServer(t *testing.T, config server.Config) (*server.Server, *cache.InMemoryCache, string) {
	t.Helper()

	c := cache.New(cache.Config{})
	t.Cleanup(c.StopCleanupWorker)

	s := startServer(t, c, config)
	return s, c, s.Addr().String()
}

// testClient is a small line-protocol client for integration tests.
type testClient struct {
	t      *testing.T
	conn   net.Conn
	reader *bufio.Reader
}

func dialClient(t *testing.T, addr string) *testClient {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &testClient{t: t, conn: conn, reader: bufio.NewReader(conn)}
}

func (c *testClient) send(line string) string {
	c.t.Helper()

	if err := c.sendRaw(line + "\n"); err != nil {
		c.t.Fatalf("write %q: %v", line, err)
	}
	return c.readLine()
}

func (c *testClient) sendRaw(raw string) error {
	c.t.Helper()

	if err := c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	_, err := io.WriteString(c.conn, raw)
	return err
}

func (c *testClient) readLine() string {
	c.t.Helper()

	if err := c.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		c.t.Fatalf("set read deadline: %v", err)
	}
	line, err := c.reader.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read response: %v", err)
	}

	line = strings.TrimSuffix(line, "\n")
	return strings.TrimSuffix(line, "\r")
}

func (c *testClient) expectClosed() {
	c.t.Helper()

	if err := c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		c.t.Fatalf("set read deadline: %v", err)
	}
	_, err := c.reader.ReadByte()
	if err == nil {
		c.t.Fatal("expected the server to close the connection")
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		c.t.Fatalf("connection is still open: %v", err)
	}
}

// roundTrip sends one command and reads one response using raw connections.
func roundTrip(conn net.Conn, reader *bufio.Reader, line string) (string, error) {
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return "", err
	}
	if _, err := io.WriteString(conn, line+"\n"); err != nil {
		return "", err
	}

	response, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	response = strings.TrimSuffix(response, "\n")
	return strings.TrimSuffix(response, "\r"), nil
}

func TestServerPing(t *testing.T) {
	s, _, addr := startTestServer(t, server.Config{})
	c := dialClient(t, addr)

	if got := c.send("PING"); got != "PONG" {
		t.Fatalf("PING = %q, want PONG", got)
	}
	if got := c.send("ping"); got != "PONG" {
		t.Fatalf("lowercase ping = %q, want PONG", got)
	}

	// CRLF framing must work as well as LF.
	if err := c.sendRaw("PING\r\n"); err != nil {
		t.Fatalf("write CRLF PING: %v", err)
	}
	if got := c.readLine(); got != "PONG" {
		t.Fatalf("CRLF PING = %q, want PONG", got)
	}

	if !waitFor(t, time.Second, func() bool {
		return s.Stats().TotalConnections == 1
	}) {
		t.Fatalf("TotalConnections = %d, want 1", s.Stats().TotalConnections)
	}
	if requests := s.Stats().Requests; requests != 3 {
		t.Fatalf("Requests = %d, want 3", requests)
	}

	connections := s.Connections()
	if len(connections) != 1 {
		t.Fatalf("Connections() returned %d entries, want 1", len(connections))
	}
	if connections[0].RemoteAddr == "" {
		t.Fatal("Connections() returned an empty remote address")
	}
	if connections[0].LastActive.Before(connections[0].ConnectedAt) {
		t.Fatal("LastActive is before ConnectedAt")
	}
}

func TestServerSetGetDeleteAndStats(t *testing.T) {
	s, c, addr := startTestServer(t, server.Config{})
	client := dialClient(t, addr)

	if got := client.send("SET user:1 hello"); got != "OK" {
		t.Fatalf("SET = %q, want OK", got)
	}
	if got := client.send("GET user:1"); got != "hello" {
		t.Fatalf("GET = %q, want hello", got)
	}
	if got := client.send("SET note hello world"); got != "OK" {
		t.Fatalf("SET with spaces = %q, want OK", got)
	}
	if got := client.send("GET note"); got != "hello world" {
		t.Fatalf("GET note = %q, want hello world", got)
	}
	if got := client.send("GET missing"); got != "NOT_FOUND" {
		t.Fatalf("GET missing = %q, want NOT_FOUND", got)
	}
	if got := client.send("DEL user:1"); got != "OK" {
		t.Fatalf("DEL = %q, want OK", got)
	}
	if got := client.send("GET user:1"); got != "NOT_FOUND" {
		t.Fatalf("GET after DEL = %q, want NOT_FOUND", got)
	}
	if got := client.send("DEL user:1"); got != "OK" {
		t.Fatalf("idempotent DEL = %q, want OK", got)
	}

	wantStats := "hits=2 misses=2 sets=2 deletes=2 expired=0 evictions=0"
	if got := client.send("STATS"); got != wantStats {
		t.Fatalf("STATS = %q, want %q", got, wantStats)
	}

	// The server operates on the supplied cache rather than a copy.
	if value, found := c.Get("note"); !found || value != "hello world" {
		t.Fatalf("cache.Get(note) = (%v, %v), want (hello world, true)", value, found)
	}
	if stats := s.Stats(); stats.Requests != 9 {
		t.Fatalf("Requests = %d, want 9", stats.Requests)
	}
}

func TestServerProtocolErrors(t *testing.T) {
	_, _, addr := startTestServer(t, server.Config{})
	c := dialClient(t, addr)

	tests := []struct {
		line string
		want string
	}{
		{line: "NOPE", want: `ERR unknown command "NOPE"`},
		{line: "PING extra", want: "ERR usage: PING"},
		{line: "GET", want: "ERR usage: GET <key>"},
		{line: "GET key extra", want: "ERR usage: GET <key>"},
		{line: "SET key", want: "ERR usage: SET <key> <value>"},
		{line: "", want: "ERR empty command"},
	}

	for _, tt := range tests {
		if got := c.send(tt.line); got != tt.want {
			t.Errorf("response to %q = %q, want %q", tt.line, got, tt.want)
		}
	}

	// A malformed command must not desynchronize or close the connection.
	if got := c.send("PING"); got != "PONG" {
		t.Fatalf("PING after errors = %q, want PONG", got)
	}
}

func TestServerConcurrentClients(t *testing.T) {
	const (
		clients    = 100
		iterations = 25
	)

	s, _, addr := startTestServer(t, server.Config{
		MaxConnections: 200,
		WorkerCount:    8,
	})

	errs := make(chan error, clients)
	var wg sync.WaitGroup

	for clientID := 0; clientID < clients; clientID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				errs <- fmt.Errorf("client %d dial: %w", id, err)
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)

			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("client:%d:%d", id, i)
				value := fmt.Sprintf("value-%d-%d", id, i)

				if got, err := roundTrip(conn, reader, "SET "+key+" "+value); err != nil || got != "OK" {
					errs <- fmt.Errorf("client %d SET: response=%q err=%v", id, got, err)
					return
				}
				if got, err := roundTrip(conn, reader, "GET "+key); err != nil || got != value {
					errs <- fmt.Errorf("client %d GET: response=%q err=%v", id, got, err)
					return
				}
				if got, err := roundTrip(conn, reader, "DEL "+key); err != nil || got != "OK" {
					errs <- fmt.Errorf("client %d DEL: response=%q err=%v", id, got, err)
					return
				}
			}
		}(clientID)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	wantRequests := uint64(clients * iterations * 3)
	if !waitFor(t, 2*time.Second, func() bool {
		return s.Stats().Requests == wantRequests
	}) {
		t.Fatalf("Requests = %d, want %d", s.Stats().Requests, wantRequests)
	}
	if total := s.Stats().TotalConnections; total != clients {
		t.Fatalf("TotalConnections = %d, want %d", total, clients)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return s.Stats().ActiveConnections == 0
	}) {
		t.Fatalf("ActiveConnections = %d, want 0", s.Stats().ActiveConnections)
	}
}
