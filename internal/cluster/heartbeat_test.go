package cluster

import (
	"context"
	"testing"
	"time"
)

func TestUpdateHealthTransitions(t *testing.T) {
	now := time.Now()
	c := newTestCluster(t, Config{
		HeartbeatInterval: time.Hour,
		GossipInterval:    time.Hour,
		DegradedAfter:     50 * time.Millisecond,
		OfflineAfter:      200 * time.Millisecond,
	})

	peer := Node{
		ID:       "peer",
		Address:  "127.0.0.1:7001",
		Status:   StatusOnline,
		JoinedAt: now,
		LastSeen: now.Add(-75 * time.Millisecond),
		Version:  1,
	}
	if err := c.registry.AddNode(peer); err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	before := c.Metadata().Version
	c.updateHealth(now)

	got, found := c.Node("peer")
	if !found {
		t.Fatal("peer missing after updateHealth()")
	}
	if got.Status != StatusDegraded {
		t.Fatalf("status = %s, want DEGRADED", got.Status)
	}
	if got.Version <= peer.Version {
		t.Fatalf("version = %d, want > %d after a health change", got.Version, peer.Version)
	}

	got.LastSeen = now.Add(-300 * time.Millisecond)
	if err := c.registry.AddNode(got); err != nil {
		t.Fatalf("AddNode() for offline transition returned error: %v", err)
	}
	c.updateHealth(now)

	got, _ = c.Node("peer")
	if got.Status != StatusOffline {
		t.Fatalf("status = %s, want OFFLINE", got.Status)
	}
	if c.Metadata().Version <= before {
		t.Fatal("metadata version did not advance after health transitions")
	}
}

func TestHeartbeatRefreshesSelfAndPeer(t *testing.T) {
	now := time.Now()
	c := newTestCluster(t, Config{
		HeartbeatInterval: time.Hour,
		GossipInterval:    time.Hour,
		DegradedAfter:     50 * time.Millisecond,
		OfflineAfter:      200 * time.Millisecond,
	})

	peer := Node{
		ID:       "peer",
		Address:  "127.0.0.1:7001",
		Status:   StatusOnline,
		JoinedAt: now.Add(-time.Second),
		LastSeen: now.Add(-time.Second),
		Version:  1,
	}
	if err := c.registry.AddNode(peer); err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	fake := &fakeTransport{
		respond: func(_ context.Context, address string, message wireMessage) (wireMessage, error) {
			if address != peer.Address {
				t.Errorf("heartbeat sent to %s, want %s", address, peer.Address)
			}
			if message.Type != messageHeartbeat {
				t.Errorf("message type = %s, want %s", message.Type, messageHeartbeat)
			}
			return wireMessage{
				Type:      messageHeartbeatAck,
				ClusterID: c.config.ClusterID,
				Sender:    peer,
			}, nil
		},
	}
	c.transport = fake

	c.heartbeatOnce(now)

	self := c.Self()
	if !self.LastSeen.Equal(now) {
		t.Fatalf("self LastSeen = %s, want %s", self.LastSeen, now)
	}
	if self.Status != StatusOnline {
		t.Fatalf("self status = %s, want ONLINE", self.Status)
	}
	if _, found := c.registry.Node(c.config.NodeID); !found {
		t.Fatal("self was not registered by the heartbeat loop")
	}

	got, _ := c.Node("peer")
	if got.Status != StatusOnline {
		t.Fatalf("peer status = %s, want ONLINE after a successful heartbeat", got.Status)
	}
	if got.LastSeen.Before(now) {
		t.Fatalf("peer LastSeen = %s, want at or after %s", got.LastSeen, now)
	}

	calls := fake.snapshot()
	if len(calls) != 1 {
		t.Fatalf("heartbeat sent %d requests, want 1", len(calls))
	}
	if calls[0].Message.ClusterID != c.config.ClusterID {
		t.Fatalf("heartbeat cluster ID = %q, want %q", calls[0].Message.ClusterID, c.config.ClusterID)
	}
	if calls[0].Message.Metadata.ClusterID != c.config.ClusterID {
		t.Fatalf("heartbeat metadata cluster ID = %q, want %q",
			calls[0].Message.Metadata.ClusterID, c.config.ClusterID)
	}
}

func TestHeartbeatSkipsWhenStopped(t *testing.T) {
	c := newTestCluster(t, Config{
		HeartbeatInterval: time.Hour,
		GossipInterval:    time.Hour,
	})
	fake := &fakeTransport{}
	c.transport = fake

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop() returned error: %v", err)
	}

	c.heartbeatOnce(time.Now())
	if calls := fake.snapshot(); len(calls) != 0 {
		t.Fatalf("heartbeat after Stop sent %d requests, want 0", len(calls))
	}
}
