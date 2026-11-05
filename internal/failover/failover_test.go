package failover_test

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/failover"
)

const testClusterID = "phase-6-failover-test"

// reserveControlAddress returns a loopback address that was just free.
func reserveControlAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release address: %v", err)
	}
	return address
}

// clusterSpec describes one test cluster before its nodes are started.
type clusterSpec struct {
	configs []failover.Config
	stores  []*cache.InMemoryCache
}

// newClusterSpec builds a static three-node cluster with reserved addresses.
func newClusterSpec(t *testing.T, size int) clusterSpec {
	t.Helper()

	dir := t.TempDir()
	addresses := make([]string, size)
	ids := make([]string, size)
	for i := 0; i < size; i++ {
		addresses[i] = reserveControlAddress(t)
		ids[i] = fmt.Sprintf("node-%d", i)
	}

	spec := clusterSpec{
		configs: make([]failover.Config, size),
		stores:  make([]*cache.InMemoryCache, size),
	}

	for i := 0; i < size; i++ {
		peers := make([]failover.Peer, 0, size-1)
		for j := 0; j < size; j++ {
			if i == j {
				continue
			}
			peers = append(peers, failover.Peer{ID: ids[j], Address: addresses[j]})
		}

		config := failover.DefaultConfig()
		config.ClusterID = testClusterID
		config.ID = ids[i]
		config.ListenAddr = addresses[i]
		config.Peers = peers
		config.Store = cache.New(cache.Config{})
		config.WALPath = filepath.Join(dir, ids[i]+".wal")
		config.ElectionTimeoutMin = 600 * time.Millisecond
		config.ElectionTimeoutMax = 1200 * time.Millisecond
		config.HeartbeatInterval = 100 * time.Millisecond
		config.FailureTimeout = 2 * time.Second
		config.WriteTimeout = 3 * time.Second
		config.TransferTimeout = 3 * time.Second
		config.ReplicationRetry = 25 * time.Millisecond
		config.ShutdownTimeout = 3 * time.Second
		config.ReplicationConfig.ReconnectMin = 10 * time.Millisecond
		config.ReplicationConfig.ReconnectMax = 100 * time.Millisecond
		config.ReplicationConfig.HandshakeTimeout = time.Second
		config.ReplicationConfig.AckTimeout = time.Second
		config.ReplicationConfig.ShutdownTimeout = 2 * time.Second

		spec.configs[i] = config
		spec.stores[i] = config.Store.(*cache.InMemoryCache)
	}
	return spec
}

// startNode creates and starts one node, registering cleanup.
func startNode(t *testing.T, config failover.Config) *failover.Node {
	t.Helper()

	node, err := failover.New(config)
	if err != nil {
		t.Fatalf("New(%s) returned error: %v", config.ID, err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start(%s) returned error: %v", config.ID, err)
	}
	t.Cleanup(func() { _ = node.Stop() })
	return node
}

// startCluster starts every configured node.
func (s clusterSpec) start(t *testing.T) []*failover.Node {
	t.Helper()

	nodes := make([]*failover.Node, len(s.configs))
	for i, config := range s.configs {
		nodes[i] = startNode(t, config)
	}
	return nodes
}

// waitFor polls condition until it succeeds or times out.
func waitFor(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if condition() {
		return
	}
	t.Fatalf("timed out waiting for %s", description)
}

// waitForLeader waits for exactly one running leader and returns its index.
//
// The leader must remain unchanged for a stable window longer than one maximum
// election timeout. This filters the short-lived leader that can appear while
// a simultaneous candidate forces one extra term at startup.
func waitForLeader(t *testing.T, nodes []*failover.Node, stopped []bool) int {
	t.Helper()

	leader := -1
	since := time.Now()
	waitFor(t, 10*time.Second, "one stable elected leader", func() bool {
		current := -1
		leaders := 0
		for i, node := range nodes {
			if stopped != nil && stopped[i] {
				continue
			}
			if node.Role() == failover.RoleLeader {
				current = i
				leaders++
			}
		}
		if leaders != 1 {
			leader = -1
			return false
		}
		if current != leader {
			leader = current
			since = time.Now()
			return false
		}
		return time.Since(since) >= 1400*time.Millisecond
	})
	return leader
}

// waitForReplicas waits for the leader's replication endpoint to reach count.
func waitForReplicas(t *testing.T, node *failover.Node, count int) {
	t.Helper()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if node.Stats().Replication.Replicas >= count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats := node.Stats()
	t.Fatalf("timed out waiting for %d replication replicas on %s: role=%s term=%d leader=%q addr=%q replicas=%d lastErr=%v",
		count, stats.ID, stats.Role, stats.Term, stats.LeaderID, stats.ReplicationAddr,
		stats.Replication.Replicas, node.LastError())
}

// waitForValue waits for key to have want on every listed store.
func waitForValue(t *testing.T, stores []*cache.InMemoryCache, key string, want any) {
	t.Helper()

	waitFor(t, 8*time.Second, fmt.Sprintf("key %q on every store", key), func() bool {
		for _, store := range stores {
			value, found := store.Get(key)
			if !found || value != want {
				return false
			}
		}
		return true
	})
}

// waitForMissing waits for key to disappear from every listed store.
func waitForMissing(t *testing.T, stores []*cache.InMemoryCache, key string) {
	t.Helper()

	waitFor(t, 8*time.Second, fmt.Sprintf("key %q removed from every store", key), func() bool {
		for _, store := range stores {
			if _, found := store.Get(key); found {
				return false
			}
		}
		return true
	})
}

func TestFailoverRecoveryAndDataIntegrity(t *testing.T) {
	spec := newClusterSpec(t, 3)
	nodes := spec.start(t)
	stopped := make([]bool, len(nodes))

	oldLeader := waitForLeader(t, nodes, stopped)
	waitForReplicas(t, nodes[oldLeader], len(nodes)-1)
	oldTerm := nodes[oldLeader].Term()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("pre-%d", i)
		if err := nodes[oldLeader].Set(key, "value-"+key); err != nil {
			t.Fatalf("Set(%s) returned error: %v", key, err)
		}
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("pre-%d", i)
		waitForValue(t, spec.stores, key, "value-"+key)
	}
	if !nodes[oldLeader].Stats().HasQuorum {
		t.Fatal("leader did not report a quorum lease")
	}

	// Abruptly stop the primary to force an election on the remaining majority.
	oldID := nodes[oldLeader].ID()
	if err := nodes[oldLeader].Stop(); err != nil {
		t.Fatalf("Stop(old leader) returned error: %v", err)
	}
	stopped[oldLeader] = true

	liveStores := make([]*cache.InMemoryCache, 0, len(spec.stores)-1)
	for i, store := range spec.stores {
		if i != oldLeader {
			liveStores = append(liveStores, store)
		}
	}

	newLeader := waitForLeader(t, nodes, stopped)
	if newLeader == oldLeader {
		t.Fatal("stopped node was elected again")
	}
	if nodes[newLeader].Term() <= oldTerm {
		t.Fatalf("new term = %d, want greater than %d", nodes[newLeader].Term(), oldTerm)
	}
	waitForReplicas(t, nodes[newLeader], len(nodes)-2)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("pre-%d", i)
		waitForValue(t, liveStores, key, "value-"+key)
	}

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("post-%d", i)
		if err := nodes[newLeader].Set(key, "value-"+key); err != nil {
			stats := nodes[newLeader].Stats()
			t.Fatalf("Set(%s) after failover returned error: %v "+
				"(role=%s term=%d leader=%q leaseAge=%v hasQuorum=%v replicas=%d)",
				key, err, stats.Role, stats.Term, stats.LeaderID,
				time.Since(stats.LastQuorum), stats.HasQuorum, stats.Replication.Replicas)
		}
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("post-%d", i)
		waitForValue(t, liveStores, key, "value-"+key)
	}

	// The failure detector must eventually mark the stopped node dead.
	waitFor(t, 5*time.Second, "stopped node to be marked DEAD", func() bool {
		for _, health := range nodes[newLeader].Health() {
			if health.ID == oldID {
				return health.Status == failover.PeerDead
			}
		}
		return false
	})

	// Restart the old node with an empty store. It must rejoin as a follower
	// and rebuild both pre- and post-failover data from the new leader.
	restartConfig := spec.configs[oldLeader]
	restartStore := cache.New(cache.Config{})
	restartConfig.Store = restartStore
	restarted := startNode(t, restartConfig)

	waitFor(t, 8*time.Second, "restarted node to follow the new leader", func() bool {
		return restarted.Role() == failover.RoleFollower &&
			restarted.LeaderID() == nodes[newLeader].ID()
	})
	for i := 0; i < 5; i++ {
		for _, prefix := range []string{"pre", "post"} {
			key := fmt.Sprintf("%s-%d", prefix, i)
			waitForValue(t, []*cache.InMemoryCache{restartStore}, key, "value-"+key)
		}
	}

	// Exactly one running node remains leader, and every running store agrees.
	running := make([]*failover.Node, 0, len(nodes))
	runningStores := make([]*cache.InMemoryCache, 0, len(nodes))
	for i, node := range nodes {
		if stopped[i] {
			continue
		}
		running = append(running, node)
		runningStores = append(runningStores, spec.stores[i])
	}
	running = append(running, restarted)
	runningStores = append(runningStores, restartStore)

	leaders := 0
	for _, node := range running {
		if node.Role() == failover.RoleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("running leaders = %d, want 1", leaders)
	}
	for i := 0; i < 5; i++ {
		for _, prefix := range []string{"pre", "post"} {
			key := fmt.Sprintf("%s-%d", prefix, i)
			waitForValue(t, runningStores, key, "value-"+key)
		}
	}
}

func TestLeadershipTransfer(t *testing.T) {
	spec := newClusterSpec(t, 3)
	nodes := spec.start(t)

	leader := waitForLeader(t, nodes, nil)
	waitForReplicas(t, nodes[leader], len(nodes)-1)

	if err := nodes[leader].Set("before-transfer", "value"); err != nil {
		t.Fatalf("Set before transfer returned error: %v", err)
	}
	waitForValue(t, spec.stores, "before-transfer", "value")

	target := (leader + 1) % len(nodes)
	if err := nodes[leader].TransferLeadership(nodes[target].ID()); err != nil {
		for i, node := range nodes {
			stats := node.Stats()
			t.Logf("node%d role=%s term=%d leader=%q repl=%q replicas=%d quorum=%v err=%v",
				i, stats.Role, stats.Term, stats.LeaderID, stats.ReplicationAddr,
				stats.Replication.Replicas, stats.HasQuorum, node.LastError())
		}
		t.Fatalf("TransferLeadership from %d to %d returned error: %v", leader, target, err)
	}

	waitFor(t, 5*time.Second, "target to become leader", func() bool {
		return nodes[target].Role() == failover.RoleLeader
	})
	waitFor(t, 5*time.Second, "old leader to follow the target", func() bool {
		return nodes[leader].Role() == failover.RoleFollower &&
			nodes[leader].LeaderID() == nodes[target].ID()
	})
	waitForReplicas(t, nodes[target], len(nodes)-1)

	if err := nodes[target].Set("after-transfer", "value"); err != nil {
		t.Fatalf("Set after transfer returned error: %v", err)
	}
	waitForValue(t, spec.stores, "after-transfer", "value")

	if err := nodes[target].Delete("before-transfer"); err != nil {
		t.Fatalf("Delete after transfer returned error: %v", err)
	}
	waitForMissing(t, spec.stores, "before-transfer")

	leaders := 0
	for _, node := range nodes {
		if node.Role() == failover.RoleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders after transfer = %d, want 1", leaders)
	}
}

func TestLeaderStepsDownWithoutQuorum(t *testing.T) {
	spec := newClusterSpec(t, 3)
	nodes := spec.start(t)

	leader := waitForLeader(t, nodes, nil)
	waitForReplicas(t, nodes[leader], len(nodes)-1)

	for i := range nodes {
		if i == leader {
			continue
		}
		if err := nodes[i].Stop(); err != nil {
			t.Fatalf("Stop(follower %d) returned error: %v", i, err)
		}
	}

	waitFor(t, 5*time.Second, "leader to step down without a majority", func() bool {
		return nodes[leader].Role() == failover.RoleFollower
	})
	if nodes[leader].Stats().HasQuorum {
		t.Fatal("minority node still reports a quorum lease")
	}

	err := nodes[leader].Set("should-fail", "value")
	if !errors.Is(err, failover.ErrNotLeader) && !errors.Is(err, failover.ErrNoQuorum) {
		t.Fatalf("Set without quorum error = %v, want ErrNotLeader or ErrNoQuorum", err)
	}

	// The surviving minority must never elect itself. Wait past a full
	// election window so a failed campaign has definitely run.
	time.Sleep(1400 * time.Millisecond)
	if nodes[leader].Role() == failover.RoleLeader {
		t.Fatal("minority node became leader")
	}
}
