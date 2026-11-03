package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// simulatedTransport routes requests directly between clusters, adding delay
// and deterministic loss to gossip and heartbeat traffic.
type simulatedTransport struct {
	mu        sync.RWMutex
	peers     map[string]*Cluster
	delay     time.Duration
	dropEvery uint64
	calls     atomic.Uint64
}

func newSimulatedTransport(delay time.Duration, dropEvery uint64) *simulatedTransport {
	return &simulatedTransport{
		peers:     make(map[string]*Cluster),
		delay:     delay,
		dropEvery: dropEvery,
	}
}

func (s *simulatedTransport) add(address string, c *Cluster) {
	s.mu.Lock()
	s.peers[address] = c
	s.mu.Unlock()
}

func (s *simulatedTransport) remove(address string) {
	s.mu.Lock()
	delete(s.peers, address)
	s.mu.Unlock()
}

func (s *simulatedTransport) RoundTrip(ctx context.Context, address string, message wireMessage) (wireMessage, error) {
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return wireMessage{}, ctx.Err()
		case <-timer.C:
		}
	}

	s.mu.RLock()
	peer := s.peers[address]
	s.mu.RUnlock()
	if peer == nil {
		return wireMessage{}, fmt.Errorf("simulated network: no route to %s", address)
	}

	call := s.calls.Add(1)
	lossy := message.Type == messageGossip || message.Type == messageHeartbeat
	if s.dropEvery > 0 && lossy && call%s.dropEvery == 0 {
		return wireMessage{}, errors.New("simulated network: message dropped")
	}

	return peer.handleMessage(message), nil
}

func startSimulatedNode(t *testing.T, config Config, transport transport) *Cluster {
	t.Helper()

	c, err := New(config)
	if err != nil {
		t.Fatalf("New(%s) returned error: %v", config.NodeID, err)
	}
	c.transport = transport

	if err := c.Start(); err != nil {
		t.Fatalf("Start(%s) returned error: %v", config.NodeID, err)
	}
	t.Cleanup(func() { _ = c.Stop() })
	return c
}

func simulatedConfig(id NodeID) Config {
	return Config{
		ClusterID:         "simulated-cluster",
		NodeID:            id,
		Address:           "127.0.0.1:0",
		HeartbeatInterval: 20 * time.Millisecond,
		GossipInterval:    25 * time.Millisecond,
		DegradedAfter:     300 * time.Millisecond,
		OfflineAfter:      800 * time.Millisecond,
		RequestTimeout:    500 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
		MaxMessageSize:    1 << 20,
	}
}

func eventually(t *testing.T, timeout time.Duration, message string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, message)
}

func allNodesOnline(c *Cluster) bool {
	for _, node := range c.Nodes() {
		if node.Status != StatusOnline {
			return false
		}
	}
	return true
}

func TestClusterConvergesWithDelayedAndDroppedMessages(t *testing.T) {
	transport := newSimulatedTransport(2*time.Millisecond, 4)

	a := startSimulatedNode(t, simulatedConfig("a"), transport)
	b := startSimulatedNode(t, simulatedConfig("b"), transport)
	c := startSimulatedNode(t, simulatedConfig("c"), transport)

	transport.add(a.Addr().String(), a)
	transport.add(b.Addr().String(), b)
	transport.add(c.Addr().String(), c)

	if err := b.Join(a.Addr().String()); err != nil {
		t.Fatalf("b.Join(a) returned error: %v", err)
	}
	// C joins B, so A only learns about C through gossip.
	if err := c.Join(b.Addr().String()); err != nil {
		t.Fatalf("c.Join(b) returned error: %v", err)
	}

	eventually(t, 5*time.Second, "all three nodes converged while messages were delayed and dropped", func() bool {
		for _, node := range []*Cluster{a, b, c} {
			nodes := node.Nodes()
			if len(nodes) != 3 || !allNodesOnline(node) {
				return false
			}
			if nodes[0].ID != "a" || nodes[1].ID != "b" || nodes[2].ID != "c" {
				return false
			}
		}
		return true
	})

	// A stopped node must become OFFLINE even while gossip is still lossy.
	transport.remove(a.Addr().String())
	if err := a.Stop(); err != nil {
		t.Fatalf("a.Stop() returned error: %v", err)
	}

	eventually(t, 5*time.Second, "stopped node was not detected as OFFLINE", func() bool {
		for _, node := range []*Cluster{b, c} {
			stopped, found := node.Node("a")
			if !found || stopped.Status != StatusOffline {
				return false
			}
		}
		return true
	})
}
