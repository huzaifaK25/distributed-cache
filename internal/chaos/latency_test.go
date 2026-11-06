package chaos

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestLatencyValidation(t *testing.T) {
	if _, err := NewLatency(LatencyConfig{Min: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewLatency(negative min) error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewLatency(LatencyConfig{
		Min: 10 * time.Millisecond,
		Max: 5 * time.Millisecond,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewLatency(max < min) error = %v, want ErrInvalidConfig", err)
	}

	latency, err := NewLatency(LatencyConfig{Min: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewLatency() returned error: %v", err)
	}
	for i := 0; i < 10; i++ {
		if delay := latency.Delay(); delay != 5*time.Millisecond {
			t.Fatalf("Delay() = %s, want 5ms when max is omitted", delay)
		}
	}

	var nilLatency *Latency
	if delay := nilLatency.Delay(); delay != 0 {
		t.Fatalf("nil Delay() = %s, want 0", delay)
	}
	if err := nilLatency.Sleep(context.Background()); err != nil {
		t.Fatalf("nil Sleep() returned error: %v", err)
	}
	conn := newMemoryConn("")
	if got := nilLatency.Wrap(conn); got != conn {
		t.Fatal("nil Latency.Wrap() did not return the original connection")
	}
	if got := latency.Wrap(nil); got != nil {
		t.Fatal("Wrap(nil) returned a non-nil connection")
	}
}

func TestLatencyDelayBounds(t *testing.T) {
	const min = 5 * time.Millisecond
	const max = 15 * time.Millisecond

	latency, err := NewLatency(LatencyConfig{Min: min, Max: max, Seed: 9})
	if err != nil {
		t.Fatalf("NewLatency() returned error: %v", err)
	}

	seenDifferent := false
	first := latency.Delay()
	for i := 0; i < 200; i++ {
		delay := latency.Delay()
		if delay < min || delay > max {
			t.Fatalf("Delay() = %s, want in [%s, %s]", delay, min, max)
		}
		if delay != first {
			seenDifferent = true
		}
	}
	if !seenDifferent {
		t.Fatal("Delay() produced no jitter across 200 samples")
	}
}

func TestLatencySleepHonorsContext(t *testing.T) {
	latency, err := NewLatency(LatencyConfig{Min: time.Second})
	if err != nil {
		t.Fatalf("NewLatency() returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	err = latency.Sleep(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep(canceled context) error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("Sleep(canceled context) took %s, want prompt return", elapsed)
	}
}

func TestLatencyWrapDelaysReadsAndWrites(t *testing.T) {
	conn := newMemoryConn("response")
	latency, err := NewLatency(LatencyConfig{Min: 10 * time.Millisecond, Max: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewLatency() returned error: %v", err)
	}
	wrapped := latency.Wrap(conn)
	if wrapped == conn {
		t.Fatal("Wrap() returned the original connection")
	}

	started := time.Now()
	buffer := make([]byte, len("response"))
	read, err := wrapped.Read(buffer)
	if err != nil {
		t.Fatalf("Read() returned error: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 10*time.Millisecond {
		t.Fatalf("Read() took %s, want at least 10ms", elapsed)
	}
	if string(buffer[:read]) != "response" {
		t.Fatalf("Read() = %q, want response", buffer[:read])
	}

	started = time.Now()
	if _, err := wrapped.Write([]byte("request")); err != nil {
		t.Fatalf("Write() returned error: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 10*time.Millisecond {
		t.Fatalf("Write() took %s, want at least 10ms", elapsed)
	}
	if got := conn.writtenString(); got != "request" {
		t.Fatalf("underlying connection received %q, want request", got)
	}
}

func TestLatencyListenerWrapsAcceptedConnections(t *testing.T) {
	if got := NewLatencyListener(nil, nil); got != nil {
		t.Fatal("NewLatencyListener(nil) returned a non-nil listener")
	}

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() returned error: %v", err)
	}

	latency, err := NewLatency(LatencyConfig{Min: time.Millisecond})
	if err != nil {
		t.Fatalf("NewLatency() returned error: %v", err)
	}
	listener := NewLatencyListener(raw, latency)
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	client, err := net.DialTimeout("tcp", raw.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("Dial() returned error: %v", err)
	}
	defer client.Close()

	select {
	case server := <-accepted:
		defer server.Close()
		if _, ok := server.(*latencyConn); !ok {
			t.Fatalf("Accept() returned %T, want *latencyConn", server)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept() did not return an accepted connection")
	}
}
