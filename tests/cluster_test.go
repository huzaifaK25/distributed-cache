package tests

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"distributed-cache/internal/cluster"
)

const testClusterID = "phase-4-integration"

// clusterConfig returns a fast but stable config for local integration tests.
func clusterConfig(id string) cluster.Config {
	return cluster.Config{
		ClusterID:         testClusterID,
		NodeID:            cluster.NodeID(id),
		Address:           "127.0.0.1:0",
		HeartbeatInterval: 25 * time.Millisecond,
		GossipInterval:    30 * time.Millisecond,
		DegradedAfter:     500 * time.Millisecond,
		OfflineAfter:      1500 * time.Millisecond,
		RequestTimeout:    time.Second,
		ShutdownTimeout:   3 * time.Second,
		MaxMessageSize:    1 << 20,
	}
}

// startCluster creates and starts a cluster member that is stopped at cleanup.
func startCluster(t *testing.T, config cluster.Config) *cluster.Cluster {
	t.Helper()

	if config.Address == "" {
		config.Address = "127.0.0.1:0"
	}

	c, err := cluster.New(config)
	if err != nil {
		t.Fatalf("New(%s) returned error: %v", config.NodeID, err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start(%s) returned error: %v", config.NodeID, err)
	}
	t.Cleanup(func() {
		if err := c.Stop(); err != nil && !errors.Is(err, cluster.ErrShutdownTimeout) {
			t.Errorf("Stop(%s) returned error: %v", config.NodeID, err)
		}
	})
	return c
}

func startTestCluster(t *testing.T, id string) *cluster.Cluster {
	t.Helper()
	return startCluster(t, clusterConfig(id))
}

// sameMembers reports whether c contains exactly the given IDs in sorted order.
func sameMembers(c *cluster.Cluster, ids ...string) bool {
	nodes := c.Nodes()
	if len(nodes) != len(ids) {
		return false
	}
	for i, id := range ids {
		if string(nodes[i].ID) != id {
			return false
		}
	}
	return true
}

func allOnline(c *cluster.Cluster) bool {
	for _, node := range c.Nodes() {
		if node.Status != cluster.StatusOnline {
			return false
		}
	}
	return true
}

func TestClusterConfigValidation(t *testing.T) {
	tests := []cluster.Config{
		{NodeID: "a"},
		{ClusterID: testClusterID},
		{
			ClusterID:     testClusterID,
			NodeID:        "a",
			DegradedAfter: time.Second,
			OfflineAfter:  time.Second,
		},
	}

	for _, config := range tests {
		if _, err := cluster.New(config); !errors.Is(err, cluster.ErrInvalidConfig) {
			t.Errorf("New(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}
}

func TestClusterLifecycleAndMetadata(t *testing.T) {
	config := clusterConfig("a")
	c, err := cluster.New(config)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if addr := c.Addr(); addr != nil {
		t.Fatalf("Addr() before Start = %v, want nil", addr)
	}

	if err := c.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })

	if c.Addr() == nil {
		t.Fatal("Addr() after Start returned nil")
	}
	if err := c.Start(); !errors.Is(err, cluster.ErrAlreadyStarted) {
		t.Fatalf("second Start() error = %v, want ErrAlreadyStarted", err)
	}

	self := c.Self()
	if self.ID != "a" {
		t.Fatalf("Self().ID = %q, want a", self.ID)
	}
	if self.Address != c.Addr().String() {
		t.Fatalf("Self().Address = %q, want %q", self.Address, c.Addr().String())
	}
	if self.Status != cluster.StatusOnline {
		t.Fatalf("Self().Status = %s, want ONLINE", self.Status)
	}
	if !sameMembers(c, "a") {
		t.Fatalf("Nodes() = %v, want [a]", c.Nodes())
	}

	metadata := c.Metadata()
	if metadata.ClusterID != testClusterID {
		t.Fatalf("Metadata().ClusterID = %q, want %q", metadata.ClusterID, testClusterID)
	}
	if metadata.NodeCount != 1 {
		t.Fatalf("Metadata().NodeCount = %d, want 1", metadata.NodeCount)
	}
	if metadata.Version == 0 {
		t.Fatal("Metadata().Version = 0, want a positive view version")
	}

	// Blank and self seeds are ignored, so this is a no-op.
	if err := c.Join("", "  ", c.Addr().String()); err != nil {
		t.Fatalf("Join() with no usable seeds returned error: %v", err)
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
			errs <- c.Stop()
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

	if err := c.Start(); !errors.Is(err, cluster.ErrClosed) {
		t.Fatalf("Start() after Stop error = %v, want ErrClosed", err)
	}
	if err := c.Join("127.0.0.1:1"); !errors.Is(err, cluster.ErrNotRunning) {
		t.Fatalf("Join() after Stop error = %v, want ErrNotRunning", err)
	}
}

func TestClusterJoinAndConvergence(t *testing.T) {
	a := startTestCluster(t, "a")
	b := startTestCluster(t, "b")

	if err := b.Join(a.Addr().String()); err != nil {
		t.Fatalf("b.Join(a) returned error: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool {
		return sameMembers(a, "a", "b") && sameMembers(b, "a", "b") &&
			allOnline(a) && allOnline(b)
	}) {
		t.Fatalf("membership did not converge: a=%v b=%v", a.Nodes(), b.Nodes())
	}

	for _, c := range []*cluster.Cluster{a, b} {
		metadata := c.Metadata()
		if metadata.ClusterID != testClusterID || metadata.NodeCount != 2 {
			t.Fatalf("Metadata() = %+v, want cluster %q with 2 nodes", metadata, testClusterID)
		}
	}

	// Repeating a join with duplicate seeds must be harmless.
	if err := b.Join(a.Addr().String(), a.Addr().String()); err != nil {
		t.Fatalf("repeated Join() returned error: %v", err)
	}
}

func TestClusterThreeNodeGossipConvergence(t *testing.T) {
	a := startTestCluster(t, "a")
	b := startTestCluster(t, "b")
	c := startTestCluster(t, "c")

	if err := b.Join(a.Addr().String()); err != nil {
		t.Fatalf("b.Join(a) returned error: %v", err)
	}
	// C joins B, so A learns about C through gossip rather than a direct join.
	if err := c.Join(b.Addr().String()); err != nil {
		t.Fatalf("c.Join(b) returned error: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool {
		for _, node := range []*cluster.Cluster{a, b, c} {
			if !sameMembers(node, "a", "b", "c") || !allOnline(node) {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("membership did not converge: a=%v b=%v c=%v", a.Nodes(), b.Nodes(), c.Nodes())
	}

	for _, c := range []*cluster.Cluster{a, b, c} {
		if metadata := c.Metadata(); metadata.NodeCount != 3 {
			t.Fatalf("Metadata().NodeCount = %d, want 3", metadata.NodeCount)
		}
	}
}

func TestClusterLeaveConverges(t *testing.T) {
	a := startTestCluster(t, "a")
	b := startTestCluster(t, "b")
	c := startTestCluster(t, "c")

	if err := b.Join(a.Addr().String()); err != nil {
		t.Fatalf("b.Join(a) returned error: %v", err)
	}
	if err := c.Join(a.Addr().String()); err != nil {
		t.Fatalf("c.Join(a) returned error: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return sameMembers(a, "a", "b", "c") && sameMembers(b, "a", "b", "c") &&
			sameMembers(c, "a", "b", "c")
	}) {
		t.Fatalf("membership did not converge before leave")
	}

	if err := b.Leave(); err != nil {
		t.Fatalf("b.Leave() returned error: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool {
		return sameMembers(a, "a", "c") && sameMembers(c, "a", "c") &&
			allOnline(a) && allOnline(c)
	}) {
		t.Fatalf("membership did not converge after leave: a=%v c=%v", a.Nodes(), c.Nodes())
	}

	// Stale gossip must not resurrect the left node.
	time.Sleep(250 * time.Millisecond)
	if !sameMembers(a, "a", "c") || !sameMembers(c, "a", "c") {
		t.Fatalf("left node was resurrected: a=%v c=%v", a.Nodes(), c.Nodes())
	}
	if _, found := a.Node("b"); found {
		t.Fatal("Node(b) is still present after leave")
	}

	// Leave is idempotent.
	if err := b.Leave(); err != nil {
		t.Fatalf("second b.Leave() returned error: %v", err)
	}
}

func TestClusterTracksHealthAfterCrash(t *testing.T) {
	configA := clusterConfig("a")
	configA.DegradedAfter = 150 * time.Millisecond
	configA.OfflineAfter = 1500 * time.Millisecond

	configB := clusterConfig("b")
	configB.DegradedAfter = 150 * time.Millisecond
	configB.OfflineAfter = 1500 * time.Millisecond

	a := startCluster(t, configA)
	b := startCluster(t, configB)

	if err := b.Join(a.Addr().String()); err != nil {
		t.Fatalf("b.Join(a) returned error: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return sameMembers(a, "a", "b") && allOnline(a)
	}) {
		t.Fatalf("membership did not converge before crash")
	}

	versionBefore := a.Metadata().Version

	// Stop without Leave to simulate a crash.
	if err := b.Stop(); err != nil {
		t.Fatalf("b.Stop() returned error: %v", err)
	}

	if !waitFor(t, 3*time.Second, func() bool {
		node, found := a.Node("b")
		return found && node.Status == cluster.StatusDegraded
	}) {
		node, _ := a.Node("b")
		t.Fatalf("node b did not become DEGRADED: %+v", node)
	}

	if !waitFor(t, 5*time.Second, func() bool {
		node, found := a.Node("b")
		return found && node.Status == cluster.StatusOffline
	}) {
		node, _ := a.Node("b")
		t.Fatalf("node b did not become OFFLINE: %+v", node)
	}

	// Health changes remain part of the membership view.
	if !sameMembers(a, "a", "b") {
		t.Fatalf("Nodes() = %v, want the offline node retained for health tracking", a.Nodes())
	}
	if versionAfter := a.Metadata().Version; versionAfter <= versionBefore {
		t.Fatalf("metadata version = %d, want > %d after health changes", versionAfter, versionBefore)
	}
}

func TestClusterNodeRejoinsAfterLeave(t *testing.T) {
	a := startTestCluster(t, "a")
	b := startTestCluster(t, "b")

	if err := b.Join(a.Addr().String()); err != nil {
		t.Fatalf("b.Join(a) returned error: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return sameMembers(a, "a", "b") && sameMembers(b, "a", "b")
	}) {
		t.Fatalf("membership did not converge before leave")
	}

	if err := b.Leave(); err != nil {
		t.Fatalf("b.Leave() returned error: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return sameMembers(a, "a")
	}) {
		t.Fatalf("left node was not removed from a: %v", a.Nodes())
	}

	rejoined := startTestCluster(t, "b")
	if err := rejoined.Join(a.Addr().String()); err != nil {
		t.Fatalf("rejoined.Join(a) returned error: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool {
		if !sameMembers(a, "a", "b") || !sameMembers(rejoined, "a", "b") {
			return false
		}
		node, found := a.Node("b")
		return found && node.Status == cluster.StatusOnline
	}) {
		t.Fatalf("rejoined node did not converge: a=%v rejoined=%v", a.Nodes(), rejoined.Nodes())
	}

	// The seed raises the version above the leave tombstone, and the joining
	// node adopts that version for future gossip.
	if version := rejoined.Self().Version; version < 2 {
		t.Fatalf("rejoined version = %d, want >= 2", version)
	}
}

func TestClusterRejectsMismatchedClusterID(t *testing.T) {
	configA := clusterConfig("a")
	configA.ClusterID = "cluster-alpha"
	configB := clusterConfig("b")
	configB.ClusterID = "cluster-beta"

	a := startCluster(t, configA)
	b := startCluster(t, configB)

	if err := b.Join(a.Addr().String()); err == nil {
		t.Fatal("Join() across different cluster IDs succeeded")
	}
	if !sameMembers(a, "a") || !sameMembers(b, "b") {
		t.Fatalf("mismatched clusters exchanged membership: a=%v b=%v", a.Nodes(), b.Nodes())
	}
}

func TestClusterJoinUnreachableSeed(t *testing.T) {
	a := startTestCluster(t, "a")
	b := startTestCluster(t, "b")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a dead address: %v", err)
	}
	deadAddress := listener.Addr().String()
	_ = listener.Close()

	if err := b.Join(deadAddress); err == nil {
		t.Fatal("Join() with an unreachable seed succeeded")
	}
	if !sameMembers(b, "b") {
		t.Fatalf("failed Join() changed membership: %v", b.Nodes())
	}

	// A later join with a reachable seed must still work.
	if err := b.Join(a.Addr().String()); err != nil {
		t.Fatalf("Join() with a reachable seed returned error: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return sameMembers(a, "a", "b") && sameMembers(b, "a", "b")
	}) {
		t.Fatalf("membership did not converge after a failed join")
	}
}
