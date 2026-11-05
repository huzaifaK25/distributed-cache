package partitioning_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"distributed-cache/internal/partitioning"
)

type fakeMigrator struct {
	calls  int
	failAt int
	keys   uint64
	moves  []partitioning.Move
}

func (m *fakeMigrator) Migrate(ctx context.Context, move partitioning.Move) (partitioning.MigrationResult, error) {
	if err := ctx.Err(); err != nil {
		return partitioning.MigrationResult{}, err
	}
	m.calls++
	m.moves = append(m.moves, move)
	if m.failAt > 0 && m.calls == m.failAt {
		return partitioning.MigrationResult{}, errors.New("injected migration failure")
	}
	return partitioning.MigrationResult{Keys: m.keys}, nil
}

type fakeGate struct {
	partitions   []partitioning.PartitionID
	calls        int
	releaseCalls int
	err          error
}

func (g *fakeGate) Quiesce(ctx context.Context, partitions []partitioning.PartitionID) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.calls++
	g.partitions = append([]partitioning.PartitionID(nil), partitions...)
	if g.err != nil {
		return nil, g.err
	}
	return func() {
		g.releaseCalls++
	}, nil
}

type blockingMigrator struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *blockingMigrator) Migrate(ctx context.Context, _ partitioning.Move) (partitioning.MigrationResult, error) {
	m.once.Do(func() {
		close(m.started)
	})
	select {
	case <-m.release:
		return partitioning.MigrationResult{Keys: 1}, nil
	case <-ctx.Done():
		return partitioning.MigrationResult{}, ctx.Err()
	}
}

func newPartitioner(t *testing.T, nodes []partitioning.Node, partitions uint32, virtualNodes int) *partitioning.Partitioner {
	t.Helper()

	p, err := partitioning.New(partitioning.Config{
		Nodes:        nodes,
		Partitions:   partitions,
		VirtualNodes: virtualNodes,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func snapshotRoutes(t *testing.T, p *partitioning.Partitioner, keys []string) []partitioning.Route {
	t.Helper()

	routes := make([]partitioning.Route, len(keys))
	for i, key := range keys {
		route, ok := p.Route(key)
		if !ok {
			t.Fatalf("Route(%q) reported an empty cluster", key)
		}
		routes[i] = route
	}
	return routes
}

func TestPartitionerRoutesKeyToPartitionAndOwner(t *testing.T) {
	p := newPartitioner(t, testNodes(), 256, 64)
	if got := p.Partitions(); got != 256 {
		t.Fatalf("Partitions = %d, want 256", got)
	}

	for _, key := range testKeyList(2000) {
		route, ok := p.Route(key)
		if !ok {
			t.Fatalf("Route(%q) reported an empty cluster", key)
		}
		if got := p.PartitionOf(key); route.Partition != got {
			t.Fatalf("Route(%q).Partition = %d, want %d", key, route.Partition, got)
		}
		owner, ok := p.OwnerOf(route.Partition)
		if !ok || owner != route.Node {
			t.Fatalf("OwnerOf(%d) = %+v, %v; want %+v", route.Partition, owner, ok, route.Node)
		}
		if route.Node.ID == "" {
			t.Fatalf("Route(%q) returned an empty owner", key)
		}
	}

	if _, ok := p.OwnerOf(256); ok {
		t.Fatal("OwnerOf accepted an out-of-range partition")
	}
	for partition := partitioning.PartitionID(0); partition < 256; partition++ {
		if _, ok := p.OwnerOf(partition); !ok {
			t.Fatalf("partition %d has no owner", partition)
		}
	}
}

func TestPartitionerPartitionDistribution(t *testing.T) {
	nodes := []partitioning.Node{
		{ID: "node-a"},
		{ID: "node-b"},
		{ID: "node-c"},
		{ID: "node-d"},
	}
	p := newPartitioner(t, nodes, 1024, 128)

	counts := make(map[partitioning.NodeID]int, len(nodes))
	for partition := partitioning.PartitionID(0); partition < 1024; partition++ {
		owner, ok := p.OwnerOf(partition)
		if !ok {
			t.Fatalf("partition %d has no owner", partition)
		}
		counts[owner.ID]++
	}
	for _, node := range nodes {
		share := float64(counts[node.ID]) / 1024
		if share < 0.10 || share > 0.40 {
			t.Errorf("node %q owns %.1f%% of partitions; want a balanced share", node.ID, share*100)
		}
	}
}

func TestPartitionerPlanAddNodeMovesOnlyToNewNode(t *testing.T) {
	p := newPartitioner(t, testNodes(), 256, 64)
	before := snapshotRoutes(t, p, testKeyList(1000))
	beforeOwners := make(map[partitioning.PartitionID]partitioning.NodeID, 256)
	for partition := partitioning.PartitionID(0); partition < 256; partition++ {
		owner, ok := p.OwnerOf(partition)
		if !ok {
			t.Fatalf("partition %d has no owner before apply", partition)
		}
		beforeOwners[partition] = owner.ID
	}
	generation := p.Generation()

	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d", Address: "127.0.0.1:7004"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	if plan.Generation() != generation {
		t.Fatalf("plan generation = %d, want %d", plan.Generation(), generation)
	}
	if got, want := plan.NextGeneration(), generation+1; got != want {
		t.Fatalf("next generation = %d, want %d", got, want)
	}
	if plan.PartitionsMoved() == 0 {
		t.Fatal("adding a node moved no partitions")
	}
	if plan.PartitionsMoved() >= 256 {
		t.Fatalf("adding a node moved %d of 256 partitions", plan.PartitionsMoved())
	}
	if added := plan.AddedNodes(); len(added) != 1 || added[0].ID != "node-d" {
		t.Fatalf("AddedNodes = %+v", added)
	}
	if removed := plan.RemovedNodes(); len(removed) != 0 {
		t.Fatalf("RemovedNodes = %+v", removed)
	}
	for _, move := range plan.Moves() {
		if move.Source.ID == "" || move.Target.ID != "node-d" || move.Source.ID == move.Target.ID {
			t.Fatalf("unexpected add move: %+v", move)
		}
	}

	// Planning must not change live routing.
	if got := snapshotRoutes(t, p, testKeyList(1000)); fmt.Sprint(got) != fmt.Sprint(before) {
		t.Fatal("planning changed live routes before Apply")
	}
	if p.Generation() != generation {
		t.Fatal("planning changed the live generation")
	}

	migrator := &fakeMigrator{keys: 3}
	result, err := p.Apply(context.Background(), plan, migrator)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !result.Applied || result.Generation != generation+1 {
		t.Fatalf("Apply result = %+v", result)
	}
	if result.Moves != plan.PartitionsMoved() {
		t.Fatalf("Apply moved %d partitions, want %d", result.Moves, plan.PartitionsMoved())
	}
	if result.MovedKeys != uint64(3*plan.PartitionsMoved()) {
		t.Fatalf("Apply moved %d keys, want %d", result.MovedKeys, 3*plan.PartitionsMoved())
	}
	if got := p.Nodes(); len(got) != 4 {
		t.Fatalf("node count after apply = %d, want 4", len(got))
	}

	// Every partition that changed owner now belongs to node-d.
	for partition := partitioning.PartitionID(0); partition < 256; partition++ {
		owner, ok := p.OwnerOf(partition)
		if !ok {
			t.Fatalf("partition %d has no owner after apply", partition)
		}
		if beforeOwners[partition] != owner.ID && owner.ID != "node-d" {
			t.Fatalf("partition %d moved from %q to %q, not to node-d",
				partition, beforeOwners[partition], owner.ID)
		}
	}
	if p.Ring() == nil || p.Ring().TokenCount() != 4*64 {
		t.Fatalf("ring after apply has %d tokens, want %d", p.Ring().TokenCount(), 4*64)
	}
}

func TestPartitionerPlanRemoveNodeRedistributesAllPartitions(t *testing.T) {
	p := newPartitioner(t, testNodes(), 256, 64)
	plan, err := p.PlanRemoveNode("node-b")
	if err != nil {
		t.Fatalf("PlanRemoveNode: %v", err)
	}
	if plan.PartitionsMoved() == 0 {
		t.Fatal("removing a node moved no partitions")
	}
	removed := plan.RemovedNodes()
	if len(removed) != 1 || removed[0].ID != "node-b" {
		t.Fatalf("RemovedNodes = %+v", removed)
	}
	if added := plan.AddedNodes(); len(added) != 0 {
		t.Fatalf("AddedNodes = %+v", added)
	}
	for _, move := range plan.Moves() {
		if move.Source.ID != "node-b" || move.Target.ID == "node-b" || move.Target.ID == "" {
			t.Fatalf("unexpected remove move: %+v", move)
		}
	}

	if _, err := p.Apply(context.Background(), plan, &fakeMigrator{keys: 1}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for partition := partitioning.PartitionID(0); partition < 256; partition++ {
		owner, ok := p.OwnerOf(partition)
		if !ok || owner.ID == "node-b" {
			t.Fatalf("partition %d owner after removal = %+v, %v", partition, owner, ok)
		}
	}
	for _, key := range testKeyList(1000) {
		if route, ok := p.Route(key); !ok || route.Node.ID == "node-b" {
			t.Fatalf("Route(%q) after removal = %+v, %v", key, route, ok)
		}
	}
}

func TestPartitionerNoOpPlanCommitsWithoutMigrator(t *testing.T) {
	p := newPartitioner(t, testNodes(), 64, 32)
	plan, err := p.PlanNodes(p.Nodes())
	if err != nil {
		t.Fatalf("PlanNodes: %v", err)
	}
	if plan.PartitionsMoved() != 0 {
		t.Fatalf("identical membership moved %d partitions", plan.PartitionsMoved())
	}
	if len(plan.AddedNodes()) != 0 || len(plan.RemovedNodes()) != 0 {
		t.Fatalf("identical membership reported membership changes: +%v -%v", plan.AddedNodes(), plan.RemovedNodes())
	}

	generation := p.Generation()
	result, err := p.Apply(context.Background(), plan, nil)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !result.Applied || result.Generation != generation+1 {
		t.Fatalf("Apply result = %+v", result)
	}
	if stats := p.Stats(); stats.Rebalances != 1 || stats.Migrations != 0 {
		t.Fatalf("stats after no-op apply = %+v", stats)
	}
}

func TestPartitionerStalePlanIsRejected(t *testing.T) {
	p := newPartitioner(t, testNodes(), 64, 32)
	first, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode(node-d): %v", err)
	}
	second, err := p.PlanAddNode(partitioning.Node{ID: "node-e"})
	if err != nil {
		t.Fatalf("PlanAddNode(node-e): %v", err)
	}

	if _, err := p.Apply(context.Background(), first, &fakeMigrator{keys: 1}); err != nil {
		t.Fatalf("Apply first plan: %v", err)
	}
	if _, err := p.Apply(context.Background(), second, &fakeMigrator{keys: 1}); !errors.Is(err, partitioning.ErrStalePlan) {
		t.Fatalf("Apply stale plan error = %v", err)
	}
	if _, err := p.Apply(context.Background(), first, &fakeMigrator{keys: 1}); !errors.Is(err, partitioning.ErrStalePlan) {
		t.Fatalf("Apply reused plan error = %v", err)
	}

	for _, node := range p.Nodes() {
		if node.ID == "node-e" {
			t.Fatal("stale plan added node-e")
		}
	}
}

func TestPartitionerFailedMigrationKeepsOldOwnership(t *testing.T) {
	p := newPartitioner(t, testNodes(), 256, 64)
	keys := testKeyList(1000)
	before := snapshotRoutes(t, p, keys)
	generation := p.Generation()
	nodesBefore := fmt.Sprint(p.Nodes())

	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	if plan.PartitionsMoved() < 2 {
		t.Fatalf("test needs at least two moves, got %d", plan.PartitionsMoved())
	}

	migrator := &fakeMigrator{keys: 7, failAt: 2}
	result, err := p.Apply(context.Background(), plan, migrator)
	if !errors.Is(err, partitioning.ErrMigrationFailed) {
		t.Fatalf("Apply error = %v, want ErrMigrationFailed", err)
	}
	if result.Applied {
		t.Fatal("failed apply reported Applied")
	}
	if result.Generation != generation {
		t.Fatalf("failed apply generation = %d, want %d", result.Generation, generation)
	}
	if result.MovedKeys != 7 {
		t.Fatalf("failed apply moved %d keys, want 7", result.MovedKeys)
	}
	if migrator.calls != 2 {
		t.Fatalf("migrator calls = %d, want 2", migrator.calls)
	}
	if p.Generation() != generation {
		t.Fatalf("generation after failure = %d, want %d", p.Generation(), generation)
	}
	if got := fmt.Sprint(p.Nodes()); got != nodesBefore {
		t.Fatalf("membership after failure = %s, want %s", got, nodesBefore)
	}
	if got := snapshotRoutes(t, p, keys); fmt.Sprint(got) != fmt.Sprint(before) {
		t.Fatal("failed apply changed live routes")
	}
	stats := p.Stats()
	if stats.FailedMigrations != 1 || stats.Migrations != 1 {
		t.Fatalf("stats after failure = %+v", stats)
	}
}

func TestPartitionerApplyGateAndMetrics(t *testing.T) {
	p := newPartitioner(t, testNodes(), 256, 64)
	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}

	gate := &fakeGate{}
	migrator := &fakeMigrator{keys: 5}
	result, err := p.ApplyWithOptions(context.Background(), plan, partitioning.ApplyOptions{
		Migrator: migrator,
		Gate:     gate,
	})
	if err != nil {
		t.Fatalf("ApplyWithOptions: %v", err)
	}
	if !result.Applied || len(result.CleanupErrors) != 0 {
		t.Fatalf("ApplyWithOptions result = %+v", result)
	}
	if gate.calls != 1 || gate.releaseCalls != 1 {
		t.Fatalf("gate calls = %d, releases = %d", gate.calls, gate.releaseCalls)
	}
	if len(gate.partitions) != plan.PartitionsMoved() {
		t.Fatalf("gate quiesced %d partitions, want %d", len(gate.partitions), plan.PartitionsMoved())
	}
	for i := 1; i < len(gate.partitions); i++ {
		if gate.partitions[i-1] >= gate.partitions[i] {
			t.Fatalf("gate partitions are not sorted and unique: %v", gate.partitions)
		}
	}

	moveCount := uint64(plan.PartitionsMoved())
	stats := p.Stats()
	if stats.Nodes != 4 || stats.Partitions != 256 || stats.VirtualNodes != 64 || stats.Tokens != 4*64 {
		t.Fatalf("partition stats = %+v", stats)
	}
	if stats.Rebalances != 1 || stats.Migrations != moveCount || stats.MovedKeys != 5*moveCount {
		t.Fatalf("migration stats = %+v", stats)
	}
	if stats.Generation != plan.NextGeneration() {
		t.Fatalf("stats generation = %d, want %d", stats.Generation, plan.NextGeneration())
	}
	if stats.LastRebalance.IsZero() {
		t.Fatal("LastRebalance was not recorded")
	}
	if stats.MigrationDuration < 0 || stats.LastMigrationDuration < 0 {
		t.Fatalf("negative migration durations: %+v", stats)
	}
}

func TestPartitionerApplyGateFailureDoesNotMigrate(t *testing.T) {
	p := newPartitioner(t, testNodes(), 64, 32)
	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	generation := p.Generation()

	gateErr := errors.New("writes are still active")
	gate := &fakeGate{err: gateErr}
	migrator := &fakeMigrator{keys: 1}
	result, err := p.ApplyWithOptions(context.Background(), plan, partitioning.ApplyOptions{
		Migrator: migrator,
		Gate:     gate,
	})
	if !errors.Is(err, gateErr) {
		t.Fatalf("ApplyWithOptions error = %v, want %v", err, gateErr)
	}
	if result.Applied || p.Generation() != generation {
		t.Fatalf("gate failure applied a plan: result=%+v generation=%d", result, p.Generation())
	}
	if migrator.calls != 0 {
		t.Fatalf("migrator ran %d times after gate failure", migrator.calls)
	}
	if gate.releaseCalls != 0 {
		t.Fatalf("gate release ran %d times without a successful quiesce", gate.releaseCalls)
	}
}

func TestPartitionerApplyRejectsMissingMigratorAndCancellation(t *testing.T) {
	p := newPartitioner(t, testNodes(), 64, 32)
	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	generation := p.Generation()

	if _, err := p.Apply(context.Background(), plan, nil); !errors.Is(err, partitioning.ErrNoMigrator) {
		t.Fatalf("Apply without migrator error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	migrator := &fakeMigrator{keys: 1}
	result, err := p.Apply(ctx, plan, migrator)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply canceled error = %v", err)
	}
	if result.Applied || p.Generation() != generation || migrator.calls != 0 {
		t.Fatalf("canceled apply changed state: result=%+v generation=%d calls=%d",
			result, p.Generation(), migrator.calls)
	}
}

func TestPartitionerRejectsInvalidPlansAndMembership(t *testing.T) {
	p := newPartitioner(t, testNodes(), 64, 32)
	foreign := newPartitioner(t, testNodes(), 64, 32)
	foreignPlan, err := foreign.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("foreign PlanAddNode: %v", err)
	}
	if _, err := p.Apply(context.Background(), foreignPlan, &fakeMigrator{keys: 1}); !errors.Is(err, partitioning.ErrInvalidPlan) {
		t.Fatalf("foreign plan error = %v", err)
	}
	if _, err := p.Apply(context.Background(), nil, nil); !errors.Is(err, partitioning.ErrInvalidPlan) {
		t.Fatalf("nil plan error = %v", err)
	}
	if _, err := p.PlanAddNode(partitioning.Node{ID: "node-a"}); !errors.Is(err, partitioning.ErrNodeExists) {
		t.Fatalf("duplicate add error = %v", err)
	}
	if _, err := p.PlanRemoveNode("missing"); !errors.Is(err, partitioning.ErrNodeNotFound) {
		t.Fatalf("unknown removal error = %v", err)
	}
	if _, err := p.PlanNodes([]partitioning.Node{{ID: "node-a"}, {ID: "node-a"}}); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("duplicate PlanNodes error = %v", err)
	}

	single := newPartitioner(t, []partitioning.Node{{ID: "only"}}, 16, 8)
	if _, err := single.PlanRemoveNode("only"); !errors.Is(err, partitioning.ErrLastNode) {
		t.Fatalf("remove last node error = %v", err)
	}
	if _, err := single.PlanNodes(nil); !errors.Is(err, partitioning.ErrLastNode) {
		t.Fatalf("PlanNodes(nil) error = %v", err)
	}
}

func TestPartitionerCanBootstrapFromEmptyMembership(t *testing.T) {
	p := newPartitioner(t, nil, 32, 16)
	if _, ok := p.Route("user:1"); ok {
		t.Fatal("empty partitioner routed a key")
	}
	if p.Generation() != 0 {
		t.Fatalf("initial generation = %d", p.Generation())
	}

	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-a"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	if plan.PartitionsMoved() != 0 {
		t.Fatalf("bootstrap plan moved %d partitions", plan.PartitionsMoved())
	}
	result, err := p.Apply(context.Background(), plan, nil)
	if err != nil {
		t.Fatalf("Apply bootstrap: %v", err)
	}
	if !result.Applied {
		t.Fatal("bootstrap plan was not applied")
	}
	route, ok := p.Route("user:1")
	if !ok || route.Node.ID != "node-a" {
		t.Fatalf("Route after bootstrap = %+v, %v", route, ok)
	}
}

func TestPartitionerConfigValidationAndDefaults(t *testing.T) {
	if _, err := partitioning.New(partitioning.Config{Partitions: partitioning.MaxPartitions + 1}); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("oversized partition config error = %v", err)
	}
	if _, err := partitioning.New(partitioning.Config{VirtualNodes: -1}); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("negative virtual-node config error = %v", err)
	}
	if _, err := partitioning.New(partitioning.Config{Nodes: []partitioning.Node{{ID: ""}}}); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("empty node config error = %v", err)
	}

	config := partitioning.DefaultConfig()
	if config.VirtualNodes != partitioning.DefaultVirtualNodes || config.Partitions != partitioning.DefaultPartitions || config.Hash == nil {
		t.Fatalf("DefaultConfig = %+v", config)
	}

	p := newPartitioner(t, []partitioning.Node{{ID: "node-a"}}, 0, 0)
	stats := p.Stats()
	if stats.Partitions != partitioning.DefaultPartitions || stats.VirtualNodes != partitioning.DefaultVirtualNodes {
		t.Fatalf("defaulted stats = %+v", stats)
	}
}

func TestPartitionerConcurrentRoutingDuringApply(t *testing.T) {
	p := newPartitioner(t, testNodes(), 128, 64)
	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}

	keys := testKeyList(3000)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := worker; i < len(keys); i += 8 {
				if _, ok := p.Route(keys[i]); !ok {
					t.Errorf("Route(%q) reported an empty cluster during apply", keys[i])
					return
				}
			}
		}(worker)
	}

	close(start)
	result, err := p.Apply(context.Background(), plan, &fakeMigrator{keys: 1})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	wg.Wait()
	if !result.Applied {
		t.Fatal("concurrent apply was not committed")
	}
}

func TestPartitionerPlanBuildIsConsistentWithConcurrentApply(t *testing.T) {
	p := newPartitioner(t, testNodes(), 64, 32)
	first, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	if first.PartitionsMoved() == 0 {
		t.Fatal("expected moves when adding node-d")
	}

	migrator := &blockingMigrator{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	applyDone := make(chan error, 1)
	go func() {
		_, applyErr := p.Apply(context.Background(), first, migrator)
		applyDone <- applyErr
	}()
	select {
	case <-migrator.started:
	case <-time.After(5 * time.Second):
		t.Fatal("Apply did not start the first migration")
	}

	type planResult struct {
		plan *partitioning.Plan
		err  error
	}
	planDone := make(chan planResult, 1)
	go func() {
		second, planErr := p.PlanAddNode(partitioning.Node{ID: "node-e"})
		planDone <- planResult{plan: second, err: planErr}
	}()

	// Apply holds the plan lock while migrations are in flight. The second
	// plan must wait for the commit instead of silently dropping node-d.
	close(migrator.release)
	select {
	case err := <-applyDone:
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Apply did not finish")
	}
	var planned planResult
	select {
	case planned = <-planDone:
	case <-time.After(5 * time.Second):
		t.Fatal("PlanAddNode did not finish after the commit")
	}
	if planned.err != nil {
		t.Fatalf("PlanAddNode(node-e): %v", planned.err)
	}

	if planned.plan.Generation() != first.NextGeneration() {
		t.Fatalf("second plan generation = %d, want %d",
			planned.plan.Generation(), first.NextGeneration())
	}
	hasD := false
	for _, node := range planned.plan.Nodes() {
		if node.ID == "node-d" {
			hasD = true
		}
	}
	if !hasD {
		t.Fatalf("second plan dropped concurrently added node-d: %+v", planned.plan.Nodes())
	}
}

func TestPartitionerApplyDurationIsReported(t *testing.T) {
	p := newPartitioner(t, testNodes(), 64, 32)
	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}

	result, err := p.Apply(context.Background(), plan, &fakeMigrator{keys: 1})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Duration < 0 {
		t.Fatalf("Apply duration = %s, want non-negative", result.Duration)
	}
	if result.Duration > time.Minute {
		t.Fatalf("Apply duration = %s, unexpectedly large", result.Duration)
	}
}
