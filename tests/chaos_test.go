package tests

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"distributed-cache/internal/chaos"
	"distributed-cache/internal/cluster"
	"distributed-cache/internal/server"
)

const chaosClusterID = "phase-9-chaos"

// chaosClusterNode owns one cluster incarnation. Stop drops the incarnation
// without announcing leave; Recover creates a fresh process-like instance on
// the same fixed address and rejoins a live seed.
type chaosClusterNode struct {
	config cluster.Config

	mu       sync.Mutex
	current  *cluster.Cluster
	seeds    []string
	stops    int
	recovers int
}

// newChaosClusterNode creates a stopped node wrapper.
func newChaosClusterNode(config cluster.Config, seeds ...string) *chaosClusterNode {
	return &chaosClusterNode{
		config: config,
		seeds:  append([]string(nil), seeds...),
	}
}

// start creates and starts a cluster incarnation, joining seeds when supplied.
func (n *chaosClusterNode) start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.current != nil {
		return errors.New("chaos node already running")
	}

	c, err := cluster.New(n.config)
	if err != nil {
		return err
	}
	if err := c.Start(); err != nil {
		return err
	}
	if len(n.seeds) > 0 {
		if err := c.Join(n.seeds...); err != nil {
			_ = c.Stop()
			return err
		}
	}

	n.current = c
	return nil
}

// stop terminates the current incarnation without a graceful leave.
func (n *chaosClusterNode) stop() error {
	n.mu.Lock()
	c := n.current
	n.current = nil
	n.mu.Unlock()

	if c == nil {
		return nil
	}
	err := c.Stop()

	n.mu.Lock()
	n.stops++
	n.mu.Unlock()
	return err
}

// recover starts a replacement incarnation and rejoins a live seed.
func (n *chaosClusterNode) recover() error {
	if err := n.start(); err != nil {
		return err
	}

	n.mu.Lock()
	n.recovers++
	n.mu.Unlock()
	return nil
}

// healthy reports whether the incarnation is running and sees three online
// members.
func (n *chaosClusterNode) healthy() bool {
	n.mu.Lock()
	c := n.current
	n.mu.Unlock()

	if c == nil {
		return false
	}
	nodes := c.Nodes()
	if len(nodes) != 3 {
		return false
	}
	for _, node := range nodes {
		if node.Status != cluster.StatusOnline {
			return false
		}
	}
	return true
}

// setSeeds replaces the recovery seed list.
func (n *chaosClusterNode) setSeeds(seeds ...string) {
	n.mu.Lock()
	n.seeds = append([]string(nil), seeds...)
	n.mu.Unlock()
}

// cluster returns the current incarnation, or nil when stopped.
func (n *chaosClusterNode) cluster() *cluster.Cluster {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.current
}

// counters returns the number of simulated crash and recovery cycles.
func (n *chaosClusterNode) counters() (stops, recovers int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stops, n.recovers
}

// reserveChaosAddress returns a loopback address that was just free.
func reserveChaosAddress(t *testing.T) string {
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

// chaosClusterConfig returns fast membership settings with fixed addressing.
func chaosClusterConfig(id, address string) cluster.Config {
	config := clusterConfig(id)
	config.ClusterID = chaosClusterID
	config.Address = address
	config.DegradedAfter = 200 * time.Millisecond
	config.OfflineAfter = 2 * time.Second
	config.ShutdownTimeout = 2 * time.Second
	return config
}

func TestChaosRunnerRecoversCrashedClusterNode(t *testing.T) {
	addresses := []string{
		reserveChaosAddress(t),
		reserveChaosAddress(t),
		reserveChaosAddress(t),
	}
	ids := []string{"chaos-a", "chaos-b", "chaos-c"}

	nodes := make([]*chaosClusterNode, len(ids))
	for i := range nodes {
		var seeds []string
		if i > 0 {
			seeds = []string{addresses[0]}
		}
		nodes[i] = newChaosClusterNode(chaosClusterConfig(ids[i], addresses[i]), seeds...)
		if err := nodes[i].start(); err != nil {
			t.Fatalf("start %s: %v", ids[i], err)
		}
	}
	t.Cleanup(func() {
		for i, node := range nodes {
			if err := node.stop(); err != nil && !errors.Is(err, cluster.ErrShutdownTimeout) {
				t.Errorf("stop %s: %v", ids[i], err)
			}
		}
	})

	// The original seed can crash later too; give it live peers for recovery.
	nodes[0].setSeeds(addresses[1], addresses[2])

	if !waitFor(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			if !node.healthy() {
				return false
			}
		}
		return true
	}) {
		for i, node := range nodes {
			if c := node.cluster(); c != nil {
				t.Logf("%s view: %v", ids[i], c.Nodes())
			}
		}
		t.Fatal("cluster did not converge before the chaos run")
	}

	var crashed []string
	action := func(_ context.Context, runner *chaos.Runner) error {
		ids, err := runner.CrashRandom(1)
		crashed = append(crashed, ids...)
		return err
	}

	runner, err := chaos.NewRunner(chaos.RunnerConfig{
		Targets: []chaos.Target{
			{ID: ids[0], Stop: nodes[0].stop, Recover: nodes[0].recover, Healthy: nodes[0].healthy},
			{ID: ids[1], Stop: nodes[1].stop, Recover: nodes[1].recover, Healthy: nodes[1].healthy},
			{ID: ids[2], Stop: nodes[2].stop, Recover: nodes[2].recover, Healthy: nodes[2].healthy},
		},
		ProbeInterval: 50 * time.Millisecond,
		Seed:          19,
		Recovery: chaos.RecoveryConfig{
			Timeout:   8 * time.Second,
			StableFor: 500 * time.Millisecond,
			Interval:  50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}

	report, err := runner.Run(context.Background(), chaos.Scenario{
		Name:     "real-cluster-crash",
		Duration: 600 * time.Millisecond,
		Steps: []chaos.Step{{
			At:     100 * time.Millisecond,
			Name:   "crash-one",
			Action: action,
		}},
	})
	if err != nil {
		t.Fatalf("Run() returned error: %v", err)
	}
	if len(crashed) != 1 {
		t.Fatalf("crashed targets = %v, want exactly one", crashed)
	}
	if !report.Recovery.Recovered {
		t.Fatalf("Recovery = %+v, want recovered", report.Recovery)
	}
	if report.SLO.HealthChecks == 0 {
		t.Fatal("HealthChecks = 0, want health sampling during the scenario")
	}
	if report.SLO.HealthyChecks >= report.SLO.HealthChecks {
		t.Fatalf("health = %d/%d, want a fault-induced failure",
			report.SLO.HealthyChecks, report.SLO.HealthChecks)
	}

	if !waitFor(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			if !node.healthy() {
				return false
			}
		}
		return true
	}) {
		for i, node := range nodes {
			if c := node.cluster(); c != nil {
				t.Logf("%s view after recovery: %v", ids[i], c.Nodes())
			}
		}
		t.Fatal("cluster did not reconverge after the chaos run")
	}

	crashedIndex := -1
	for i, id := range ids {
		stops, recovers := nodes[i].counters()
		switch {
		case id == crashed[0]:
			crashedIndex = i
			if stops != 1 || recovers != 1 {
				t.Errorf("%s stop/recover calls = %d/%d, want 1/1", id, stops, recovers)
			}
		default:
			if stops != 0 || recovers != 0 {
				t.Errorf("%s stop/recover calls = %d/%d, want 0/0", id, stops, recovers)
			}
		}

		c := nodes[i].cluster()
		if c == nil {
			t.Fatalf("%s has no running cluster after recovery", id)
		}
		if got := c.Addr().String(); got != addresses[i] {
			t.Errorf("%s address = %q, want fixed address %q", id, got, addresses[i])
		}
		recovered, found := c.Node(cluster.NodeID(crashed[0]))
		if !found || recovered.Status != cluster.StatusOnline {
			t.Errorf("%s sees crashed node as %+v (found=%t), want ONLINE", id, recovered, found)
		}
	}
	if crashedIndex < 0 {
		t.Fatalf("crashed target %q is not among the configured nodes", crashed[0])
	}

	kinds := make(map[chaos.EventKind]int)
	for _, event := range report.Events {
		kinds[event.Kind]++
	}
	if kinds[chaos.EventCrash] == 0 || kinds[chaos.EventRecover] == 0 {
		t.Fatalf("events = %+v, want crash and recover events", report.Events)
	}
}

func TestChaosLatencyAndPartitionOnServerTraffic(t *testing.T) {
	_, _, address := startTestServer(t, server.Config{})

	raw, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	latency, err := chaos.NewLatency(chaos.LatencyConfig{
		Min:  5 * time.Millisecond,
		Max:  10 * time.Millisecond,
		Seed: 5,
	})
	if err != nil {
		t.Fatalf("NewLatency() returned error: %v", err)
	}
	partition := chaos.NewPartition()
	conn := partition.Wrap(latency.Wrap(raw), "client", "server")
	client := &testClient{t: t, conn: conn, reader: bufio.NewReader(conn)}

	started := time.Now()
	if got := client.send("PING"); got != "PONG" {
		t.Fatalf("PING = %q, want PONG", got)
	}
	if elapsed := time.Since(started); elapsed < 10*time.Millisecond {
		t.Fatalf("latency-wrapped round trip took %s, want at least two 5ms delays", elapsed)
	}

	if got := client.send("SET chaos value"); got != "OK" {
		t.Fatalf("SET = %q, want OK", got)
	}
	if got := client.send("GET chaos"); got != "value" {
		t.Fatalf("GET = %q, want value", got)
	}

	if err := partition.Split([]string{"client"}, []string{"server"}); err != nil {
		t.Fatalf("Split() returned error: %v", err)
	}
	if _, err := conn.Write([]byte("PING\n")); !errors.Is(err, chaos.ErrPartitioned) {
		t.Fatalf("partitioned write error = %v, want ErrPartitioned", err)
	}

	partition.Heal()
	if got := client.send("PING"); got != "PONG" {
		t.Fatalf("PING after heal = %q, want PONG", got)
	}
}
