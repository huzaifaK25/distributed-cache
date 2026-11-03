package cluster

import (
	"context"
	"testing"
	"time"
)

func TestHandleMessageJoinExchangesMetadata(t *testing.T) {
	now := time.Now()
	c := newTestCluster(t, Config{NodeID: "a"})
	if err := c.registry.AddNode(c.Self()); err != nil {
		t.Fatalf("registering self: %v", err)
	}

	sender := Node{
		ID:       "b",
		Address:  "127.0.0.1:7002",
		Status:   StatusOnline,
		JoinedAt: now,
		LastSeen: now,
		Version:  1,
	}

	response := c.handleMessage(wireMessage{
		Type:      messageJoin,
		ClusterID: c.config.ClusterID,
		Sender:    sender,
		Metadata:  Metadata{ClusterID: c.config.ClusterID, NodeCount: 1, Version: 1},
	})

	if response.Type != messageJoinAck {
		t.Fatalf("response type = %s, want %s", response.Type, messageJoinAck)
	}
	if response.ClusterID != c.config.ClusterID {
		t.Fatalf("response cluster ID = %q, want %q", response.ClusterID, c.config.ClusterID)
	}
	if response.Metadata.ClusterID != c.config.ClusterID {
		t.Fatalf("metadata cluster ID = %q, want %q", response.Metadata.ClusterID, c.config.ClusterID)
	}
	if response.Metadata.NodeCount != 2 {
		t.Fatalf("metadata node count = %d, want 2", response.Metadata.NodeCount)
	}
	if len(response.Nodes) != 2 {
		t.Fatalf("join response contains %d nodes, want 2", len(response.Nodes))
	}

	got, found := c.Node("b")
	if !found {
		t.Fatal("joining node was not registered")
	}
	if got.Status != StatusOnline || got.LastSeen.IsZero() || got.LastSeen.Before(now) {
		t.Fatalf("joining node = %+v, want ONLINE with a fresh LastSeen", got)
	}
}

func TestHandleMessageRejectsInvalidRequests(t *testing.T) {
	now := time.Now()
	c := newTestCluster(t, Config{NodeID: "a"})
	sender := Node{ID: "b", Address: "127.0.0.1:7002", Status: StatusOnline, LastSeen: now}

	tests := []struct {
		name    string
		message wireMessage
	}{
		{
			name:    "wrong cluster",
			message: wireMessage{Type: messageHeartbeat, ClusterID: "other", Sender: sender},
		},
		{
			name:    "missing sender",
			message: wireMessage{Type: messageHeartbeat, ClusterID: c.config.ClusterID},
		},
		{
			name: "sender is self",
			message: wireMessage{
				Type:      messageHeartbeat,
				ClusterID: c.config.ClusterID,
				Sender:    Node{ID: "a", Address: "127.0.0.1:7001"},
			},
		},
		{
			name:    "unknown type",
			message: wireMessage{Type: messageType("nope"), ClusterID: c.config.ClusterID, Sender: sender},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := c.handleMessage(tt.message)
			if response.Type != messageError {
				t.Fatalf("response type = %s, want %s", response.Type, messageError)
			}
			if response.Error == "" {
				t.Fatal("error response has an empty message")
			}
		})
	}
}

func TestMergeViewTombstonesPreventResurrection(t *testing.T) {
	now := time.Now()
	c := newTestCluster(t, Config{NodeID: "self"})

	peer := Node{
		ID:       "peer",
		Address:  "127.0.0.1:7001",
		Status:   StatusOnline,
		JoinedAt: now,
		LastSeen: now,
		Version:  2,
	}
	if err := c.registry.AddNode(peer); err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	c.mergeView(nil, map[NodeID]uint64{"peer": 2})
	if _, found := c.Node("peer"); found {
		t.Fatal("tombstoned node is still in the registry")
	}

	// Stale gossip at or below the tombstone version must be ignored.
	c.mergeView([]Node{peer}, nil)
	if _, found := c.Node("peer"); found {
		t.Fatal("stale gossip resurrected a tombstoned node")
	}

	// A higher-versioned rejoin revives the node and clears the tombstone.
	revived := peer
	revived.Version = 3
	revived.LastSeen = now.Add(time.Millisecond)
	c.mergeView([]Node{revived}, nil)

	got, found := c.Node("peer")
	if !found {
		t.Fatal("higher-versioned rejoin did not revive the node")
	}
	if got.Version != 3 {
		t.Fatalf("revived version = %d, want 3", got.Version)
	}
	if _, exists := c.tombstones["peer"]; exists {
		t.Fatal("tombstone was not cleared by a higher-versioned rejoin")
	}
}

func TestHandleMessageLeaveRemovesNodeAndTombstones(t *testing.T) {
	now := time.Now()
	c := newTestCluster(t, Config{NodeID: "self"})

	peer := Node{
		ID:       "peer",
		Address:  "127.0.0.1:7001",
		Status:   StatusOnline,
		JoinedAt: now,
		LastSeen: now,
		Version:  2,
	}
	if err := c.registry.AddNode(peer); err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	response := c.handleMessage(wireMessage{
		Type:      messageLeave,
		ClusterID: c.config.ClusterID,
		Sender:    peer,
	})
	if response.Type != messageLeaveAck {
		t.Fatalf("response type = %s, want %s", response.Type, messageLeaveAck)
	}
	if _, found := c.Node("peer"); found {
		t.Fatal("leaving node is still in the registry")
	}
	if got := c.tombstones["peer"]; got != peer.Version {
		t.Fatalf("tombstone version = %d, want %d", got, peer.Version)
	}

	// Stale gossip must not bring the node back.
	c.mergeView([]Node{peer}, nil)
	if _, found := c.Node("peer"); found {
		t.Fatal("stale gossip resurrected a node that left")
	}

	// A later higher-versioned join is allowed to revive it.
	revived := peer
	revived.Version = peer.Version + 1
	revived.LastSeen = now.Add(time.Millisecond)
	c.mergeView([]Node{revived}, nil)
	if _, found := c.Node("peer"); !found {
		t.Fatal("higher-versioned rejoin did not revive the node")
	}
}

func TestLeaveAnnouncesAndStops(t *testing.T) {
	c := newTestCluster(t, Config{
		NodeID:            "self",
		HeartbeatInterval: time.Hour,
		GossipInterval:    time.Hour,
	})

	peer := Node{
		ID:       "peer",
		Address:  "127.0.0.1:7001",
		Status:   StatusOnline,
		JoinedAt: time.Now(),
		LastSeen: time.Now(),
		Version:  1,
	}
	fake := &fakeTransport{
		respond: func(_ context.Context, address string, message wireMessage) (wireMessage, error) {
			if address != peer.Address {
				t.Errorf("leave sent to %s, want %s", address, peer.Address)
			}
			if message.Type != messageLeave {
				t.Errorf("message type = %s, want %s", message.Type, messageLeave)
			}
			return wireMessage{
				Type:      messageLeaveAck,
				ClusterID: c.config.ClusterID,
				Sender:    peer,
			}, nil
		},
	}
	c.transport = fake

	if err := c.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	if err := c.registry.AddNode(peer); err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	if err := c.Leave(); err != nil {
		t.Fatalf("Leave() returned error: %v", err)
	}
	if c.running() {
		t.Fatal("cluster is still running after Leave()")
	}

	calls := fake.snapshot()
	if len(calls) != 1 {
		t.Fatalf("Leave() sent %d messages, want 1", len(calls))
	}
	if calls[0].Message.Sender.ID != "self" {
		t.Fatalf("leave sender = %q, want self", calls[0].Message.Sender.ID)
	}

	if err := c.Leave(); err != nil {
		t.Fatalf("second Leave() returned error: %v", err)
	}
}

func TestObserveNodeClearsTombstoneForHigherVersion(t *testing.T) {
	now := time.Now()
	c := newTestCluster(t, Config{NodeID: "self"})
	c.mergeView(nil, map[NodeID]uint64{"peer": 2})

	sender := Node{
		ID:       "peer",
		Address:  "127.0.0.1:7001",
		Status:   StatusOnline,
		JoinedAt: now,
		LastSeen: now,
		Version:  3,
	}
	response := c.handleMessage(wireMessage{
		Type:      messageHeartbeat,
		ClusterID: c.config.ClusterID,
		Sender:    sender,
	})
	if response.Type != messageHeartbeatAck {
		t.Fatalf("response type = %s, want %s", response.Type, messageHeartbeatAck)
	}

	node, found := c.Node("peer")
	if !found {
		t.Fatal("higher-versioned heartbeat did not revive the node")
	}
	if node.Status != StatusOnline {
		t.Fatalf("status = %s, want ONLINE", node.Status)
	}
	if _, exists := c.tombstones["peer"]; exists {
		t.Fatal("tombstone was not cleared")
	}
}

func TestMergeViewSelfTombstoneRaisesSelfVersion(t *testing.T) {
	c := newTestCluster(t, Config{NodeID: "self"})
	if err := c.registry.AddNode(c.Self()); err != nil {
		t.Fatalf("registering self: %v", err)
	}

	c.mergeView(nil, map[NodeID]uint64{"self": 7})
	self := c.Self()
	if self.Version <= 7 {
		t.Fatalf("self version = %d, want > 7", self.Version)
	}
	registered, found := c.Node("self")
	if !found || registered.Version != self.Version {
		t.Fatalf("registered self = (%+v, %v), want version %d", registered, found, self.Version)
	}

	// Replaying the same tombstone must not keep inflating the version.
	before := self.Version
	c.mergeView(nil, map[NodeID]uint64{"self": 7})
	if got := c.Self().Version; got != before {
		t.Fatalf("self version = %d after replay, want %d", got, before)
	}
}

func TestMergeViewPrefersNewestObservation(t *testing.T) {
	now := time.Now()
	c := newTestCluster(t, Config{NodeID: "self"})

	current := Node{
		ID:       "peer",
		Address:  "127.0.0.1:7001",
		Status:   StatusOnline,
		JoinedAt: now,
		LastSeen: now,
		Version:  5,
	}
	if err := c.registry.AddNode(current); err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	fresher := current
	fresher.LastSeen = now.Add(time.Millisecond)
	fresher.Status = StatusDegraded
	fresher.Version = 2
	c.mergeView([]Node{fresher}, nil)

	got, _ := c.Node("peer")
	if got.Status != StatusDegraded || got.Version != 2 {
		t.Fatalf("merged node = %+v, want the fresher observation", got)
	}

	stale := current
	stale.LastSeen = now.Add(-time.Millisecond)
	stale.Version = 99
	c.mergeView([]Node{stale}, nil)

	got, _ = c.Node("peer")
	if got.Status != StatusDegraded || got.Version != 2 {
		t.Fatalf("merged node = %+v, want the stale observation ignored", got)
	}
}
