package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// transportCall records one request sent through a fakeTransport.
type transportCall struct {
	Address string
	Message wireMessage
}

// fakeTransport captures requests and delegates responses to a test function.
type fakeTransport struct {
	mu      sync.Mutex
	calls   []transportCall
	respond func(context.Context, string, wireMessage) (wireMessage, error)
}

func (f *fakeTransport) RoundTrip(ctx context.Context, address string, message wireMessage) (wireMessage, error) {
	f.mu.Lock()
	f.calls = append(f.calls, transportCall{Address: address, Message: message})
	f.mu.Unlock()

	if f.respond == nil {
		return wireMessage{}, errors.New("fake transport: no response configured")
	}
	return f.respond(ctx, address, message)
}

func (f *fakeTransport) snapshot() []transportCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]transportCall(nil), f.calls...)
}

// newTestCluster creates an unstarted cluster with common defaults.
func newTestCluster(t *testing.T, config Config) *Cluster {
	t.Helper()

	if config.ClusterID == "" {
		config.ClusterID = "test-cluster"
	}
	if config.NodeID == "" {
		config.NodeID = "self"
	}

	c, err := New(config)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })
	return c
}
