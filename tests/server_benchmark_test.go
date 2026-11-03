package tests

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/server"
)

// startBenchmarkServer starts a server for benchmarks and returns its address.
func startBenchmarkServer(b *testing.B) string {
	b.Helper()

	c := cache.New(cache.Config{})
	b.Cleanup(c.StopCleanupWorker)

	s := server.New(c, server.Config{
		Address:        "127.0.0.1:0",
		WorkerCount:    16,
		MaxConnections: 64,
	})
	if err := s.Start(); err != nil {
		b.Fatalf("Start() returned error: %v", err)
	}
	b.Cleanup(func() {
		if err := s.Stop(); err != nil {
			b.Errorf("Stop() returned error: %v", err)
		}
	})

	return s.Addr().String()
}

// dialBenchmarkClient opens a connection with a long deadline for benchmarks.
func dialBenchmarkClient(b *testing.B, addr string) (net.Conn, *bufio.Reader) {
	b.Helper()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		b.Fatalf("dial %s: %v", addr, err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(time.Hour)); err != nil {
		b.Fatalf("set deadline: %v", err)
	}
	return conn, bufio.NewReader(conn)
}

// benchRequest writes a preformatted request and reads one response.
func benchRequest(conn net.Conn, reader *bufio.Reader, request string) (string, error) {
	if _, err := io.WriteString(conn, request); err != nil {
		return "", err
	}
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	response = strings.TrimSuffix(response, "\n")
	return strings.TrimSuffix(response, "\r"), nil
}

func BenchmarkServerPing(b *testing.B) {
	addr := startBenchmarkServer(b)
	conn, reader := dialBenchmarkClient(b, addr)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		response, err := benchRequest(conn, reader, "PING\n")
		if err != nil {
			b.Fatalf("PING: %v", err)
		}
		if response != "PONG" {
			b.Fatalf("PING response = %q, want PONG", response)
		}
	}
}

func BenchmarkServerPingParallel(b *testing.B) {
	addr := startBenchmarkServer(b)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(time.Hour)); err != nil {
			b.Errorf("set deadline: %v", err)
			return
		}
		reader := bufio.NewReader(conn)

		for pb.Next() {
			response, err := benchRequest(conn, reader, "PING\n")
			if err != nil {
				b.Errorf("PING: %v", err)
				return
			}
			if response != "PONG" {
				b.Errorf("PING response = %q, want PONG", response)
				return
			}
		}
	})
}

func BenchmarkServerSetGet(b *testing.B) {
	addr := startBenchmarkServer(b)
	conn, reader := dialBenchmarkClient(b, addr)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		response, err := benchRequest(conn, reader, "SET benchmark value\n")
		if err != nil || response != "OK" {
			b.Fatalf("SET: response=%q err=%v", response, err)
		}
		response, err = benchRequest(conn, reader, "GET benchmark\n")
		if err != nil || response != "value" {
			b.Fatalf("GET: response=%q err=%v", response, err)
		}
	}
}

func BenchmarkServerConnection(b *testing.B) {
	addr := startBenchmarkServer(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			_ = conn.Close()
			b.Fatalf("set deadline: %v", err)
		}

		response, err := benchRequest(conn, bufio.NewReader(conn), "PING\n")
		_ = conn.Close()
		if err != nil {
			b.Fatalf("PING: %v", err)
		}
		if response != "PONG" {
			b.Fatalf("PING response = %q, want PONG", response)
		}
	}
}
