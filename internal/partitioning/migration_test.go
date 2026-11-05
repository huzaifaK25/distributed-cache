package partitioning_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/partitioning"
)

type failingStore struct {
	*cache.InMemoryCache
	err error
}

func (s *failingStore) Set(string, any) error {
	return s.err
}

func (s *failingStore) SetWithTTL(string, any, time.Duration) error {
	return s.err
}

type deleteFailingStore struct {
	*cache.InMemoryCache
	err error
}

func (s *deleteFailingStore) Delete(string) error {
	return s.err
}

func newMemoryStores(nodes []partitioning.Node) map[partitioning.NodeID]*cache.InMemoryCache {
	stores := make(map[partitioning.NodeID]*cache.InMemoryCache, len(nodes))
	for _, node := range nodes {
		stores[node.ID] = cache.New(cache.Config{})
	}
	return stores
}

func registerStores(t *testing.T, migrator *partitioning.StoreMigrator, stores map[partitioning.NodeID]*cache.InMemoryCache) {
	t.Helper()

	for id, store := range stores {
		if err := migrator.RegisterStore(id, store); err != nil {
			t.Fatalf("RegisterStore(%q): %v", id, err)
		}
	}
}

func populateStores(t *testing.T, p *partitioning.Partitioner, stores map[partitioning.NodeID]*cache.InMemoryCache, keys []string) []partitioning.Route {
	t.Helper()

	routes := make([]partitioning.Route, len(keys))
	for i, key := range keys {
		route, ok := p.Route(key)
		if !ok {
			t.Fatalf("Route(%q) reported an empty cluster", key)
		}
		store, exists := stores[route.Node.ID]
		if !exists {
			t.Fatalf("no store for owner %q", route.Node.ID)
		}
		if err := store.Set(key, testValue(key)); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
		routes[i] = route
	}
	return routes
}

func testValue(key string) string {
	return "value:" + key
}

func TestStoreMigratorRebalancesDataToNewOwner(t *testing.T) {
	nodes := testNodes()
	p := newPartitioner(t, nodes, 128, 64)
	stores := newMemoryStores(nodes)

	keys := testKeyList(2000)
	initialRoutes := populateStores(t, p, stores, keys)
	totalBefore := 0
	for _, store := range stores {
		totalBefore += len(store.Snapshot())
	}
	if totalBefore != len(keys) {
		t.Fatalf("initial stored keys = %d, want %d", totalBefore, len(keys))
	}

	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d", Address: "127.0.0.1:7004"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	stores["node-d"] = cache.New(cache.Config{})

	migrator, err := partitioning.NewStoreMigrator(p)
	if err != nil {
		t.Fatalf("NewStoreMigrator: %v", err)
	}
	registerStores(t, migrator, stores)

	result, err := p.Apply(context.Background(), plan, migrator)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !result.Applied {
		t.Fatal("rebalance was not applied")
	}
	if len(result.CleanupErrors) != 0 {
		t.Fatalf("cleanup errors: %v", result.CleanupErrors)
	}
	if result.MovedKeys == 0 {
		t.Fatal("rebalance moved no keys")
	}
	if stats := p.Stats(); stats.Migrations != uint64(result.Moves) || stats.MovedKeys != result.MovedKeys {
		t.Fatalf("stats = %+v, result = %+v", stats, result)
	}

	movedKeys := uint64(0)
	for i, key := range keys {
		route, ok := p.Route(key)
		if !ok {
			t.Fatalf("Route(%q) after rebalance reported an empty cluster", key)
		}
		if route.Node != initialRoutes[i].Node {
			if initialRoutes[i].Partition != route.Partition {
				t.Fatalf("key %q changed partition from %d to %d", key, initialRoutes[i].Partition, route.Partition)
			}
			movedKeys++
		}

		value, found := stores[route.Node.ID].Get(key)
		if !found || value != testValue(key) {
			t.Fatalf("key %q not readable from owner %q: value=%v found=%v", key, route.Node.ID, value, found)
		}
		for id, store := range stores {
			if id == route.Node.ID {
				continue
			}
			if _, stale := store.Get(key); stale {
				t.Fatalf("key %q has a stale copy on %q", key, id)
			}
		}
	}
	if movedKeys != result.MovedKeys {
		t.Fatalf("observed %d moved keys, Apply reported %d", movedKeys, result.MovedKeys)
	}

	totalAfter := 0
	for _, store := range stores {
		totalAfter += len(store.Snapshot())
	}
	if totalAfter != len(keys) {
		t.Fatalf("stored keys after rebalance = %d, want %d", totalAfter, len(keys))
	}
	if len(stores["node-d"].Snapshot()) == 0 {
		t.Fatal("new node received no partition data")
	}
}

func TestStoreMigratorRemovesNodeAndRedistributesData(t *testing.T) {
	nodes := testNodes()
	p := newPartitioner(t, nodes, 128, 64)
	stores := newMemoryStores(nodes)

	keys := testKeyList(1500)
	populateStores(t, p, stores, keys)

	plan, err := p.PlanRemoveNode("node-b")
	if err != nil {
		t.Fatalf("PlanRemoveNode: %v", err)
	}
	migrator, err := partitioning.NewStoreMigrator(p)
	if err != nil {
		t.Fatalf("NewStoreMigrator: %v", err)
	}
	registerStores(t, migrator, stores)

	result, err := p.Apply(context.Background(), plan, migrator)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !result.Applied || len(result.CleanupErrors) != 0 {
		t.Fatalf("Apply result = %+v", result)
	}

	for _, node := range p.Nodes() {
		if node.ID == "node-b" {
			t.Fatal("removed node is still a member")
		}
	}
	if remaining := len(stores["node-b"].Snapshot()); remaining != 0 {
		t.Fatalf("removed node still stores %d keys", remaining)
	}

	for _, key := range keys {
		route, ok := p.Route(key)
		if !ok {
			t.Fatalf("Route(%q) after removal reported an empty cluster", key)
		}
		value, found := stores[route.Node.ID].Get(key)
		if !found || value != testValue(key) {
			t.Fatalf("key %q not readable from new owner %q", key, route.Node.ID)
		}
	}

	total := 0
	for _, store := range stores {
		total += len(store.Snapshot())
	}
	if total != len(keys) {
		t.Fatalf("stored keys after removal = %d, want %d", total, len(keys))
	}
}

func TestStoreMigratorPreservesTTL(t *testing.T) {
	p := newPartitioner(t, testNodes()[:2], 64, 32)
	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-c"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	moves := plan.Moves()
	if len(moves) == 0 {
		t.Fatal("expected node-c to take over at least one partition")
	}
	move := moves[0]

	storageNodes := append(testNodes(), partitioning.Node{ID: "node-d"})
	stores := newMemoryStores(storageNodes)
	migrator, err := partitioning.NewStoreMigrator(p)
	if err != nil {
		t.Fatalf("NewStoreMigrator: %v", err)
	}
	registerStores(t, migrator, stores)

	key := keyForPartition(t, p, move.Partition, "ttl")
	if err := stores[move.Source.ID].SetWithTTL(key, testValue(key), time.Hour); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}

	result, err := p.Apply(context.Background(), plan, migrator)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !result.Applied {
		t.Fatal("TTL rebalance was not applied")
	}

	value, found := stores[move.Target.ID].Get(key)
	if !found || value != testValue(key) {
		t.Fatalf("migrated key not found on target: value=%v found=%v", value, found)
	}
	if _, stale := stores[move.Source.ID].Get(key); stale {
		t.Fatal("migrated key still exists on source")
	}

	var ttl time.Duration
	for _, entry := range stores[move.Target.ID].Snapshot() {
		if entry.Key == key {
			ttl = entry.TTL
			break
		}
	}
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("migrated TTL = %s, want (0, 1h]", ttl)
	}
}

func TestStoreMigratorFailureLeavesSourcesIntact(t *testing.T) {
	nodes := testNodes()
	p := newPartitioner(t, nodes, 64, 32)
	stores := newMemoryStores(nodes)

	keys := testKeyList(500)
	initialRoutes := populateStores(t, p, stores, keys)

	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	if plan.PartitionsMoved() == 0 {
		t.Fatal("expected moves when adding node-d")
	}

	// Ensure at least one moved partition contains data, so the failing
	// target store is exercised.
	failingKey := keyForPartition(t, p, plan.Moves()[0].Partition, "failure")
	if err := stores[plan.Moves()[0].Source.ID].Set(failingKey, testValue(failingKey)); err != nil {
		t.Fatalf("Set failing key: %v", err)
	}

	failing := &failingStore{
		InMemoryCache: cache.New(cache.Config{}),
		err:           errors.New("injected write failure"),
	}
	migrator, err := partitioning.NewStoreMigrator(p)
	if err != nil {
		t.Fatalf("NewStoreMigrator: %v", err)
	}
	registerStores(t, migrator, stores)
	if err := migrator.RegisterStore("node-d", failing); err != nil {
		t.Fatalf("RegisterStore(failing): %v", err)
	}

	generation := p.Generation()
	result, err := p.Apply(context.Background(), plan, migrator)
	if !errors.Is(err, partitioning.ErrMigrationFailed) {
		t.Fatalf("Apply error = %v, want ErrMigrationFailed", err)
	}
	if result.Applied {
		t.Fatal("failed migration was applied")
	}
	if p.Generation() != generation {
		t.Fatalf("generation after failure = %d, want %d", p.Generation(), generation)
	}

	// No source cleanup may run before commit, so every original owner still
	// holds every original key.
	for i, key := range keys {
		value, found := stores[initialRoutes[i].Node.ID].Get(key)
		if !found || value != testValue(key) {
			t.Fatalf("source key %q was lost after failed migration", key)
		}
	}
	if value, found := stores[plan.Moves()[0].Source.ID].Get(failingKey); !found || value != testValue(failingKey) {
		t.Fatal("failing key was lost from its source")
	}
	if stats := p.Stats(); stats.FailedMigrations != 1 {
		t.Fatalf("FailedMigrations = %d, want 1", stats.FailedMigrations)
	}
}

func TestStoreMigratorCleanupFailureKeepsCommittedOwnership(t *testing.T) {
	nodes := testNodes()
	p := newPartitioner(t, nodes, 64, 32)
	stores := newMemoryStores(nodes)
	populateStores(t, p, stores, testKeyList(300))

	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	if plan.PartitionsMoved() == 0 {
		t.Fatal("expected moves when adding node-d")
	}
	stores["node-d"] = cache.New(cache.Config{})

	move := plan.Moves()[0]
	key := keyForPartition(t, p, move.Partition, "cleanup")
	if err := stores[move.Source.ID].Set(key, testValue(key)); err != nil {
		t.Fatalf("Set cleanup key: %v", err)
	}

	migrator, err := partitioning.NewStoreMigrator(p)
	if err != nil {
		t.Fatalf("NewStoreMigrator: %v", err)
	}
	registerStores(t, migrator, stores)
	if err := migrator.RegisterStore(move.Source.ID, &deleteFailingStore{
		InMemoryCache: stores[move.Source.ID],
		err:           errors.New("injected delete failure"),
	}); err != nil {
		t.Fatalf("RegisterStore(delete failing): %v", err)
	}

	generation := p.Generation()
	result, err := p.Apply(context.Background(), plan, migrator)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !result.Applied || result.Generation != generation+1 {
		t.Fatalf("cleanup failure prevented commit: %+v", result)
	}
	if len(result.CleanupErrors) == 0 {
		t.Fatal("cleanup failure was not reported")
	}

	route, ok := p.Route(key)
	if !ok {
		t.Fatal("key has no owner after cleanup failure")
	}
	if value, found := stores[route.Node.ID].Get(key); !found || value != testValue(key) {
		t.Fatal("committed owner cannot read migrated key")
	}
	if _, stale := stores[move.Source.ID].Get(key); !stale {
		t.Fatal("source key was removed despite the injected delete failure")
	}
}

func TestStoreMigratorUnknownStoreDoesNotCommit(t *testing.T) {
	nodes := testNodes()
	p := newPartitioner(t, nodes, 64, 32)
	stores := newMemoryStores(nodes)
	keys := testKeyList(200)
	initialRoutes := populateStores(t, p, stores, keys)

	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	if plan.PartitionsMoved() == 0 {
		t.Fatal("expected moves when adding node-d")
	}
	migrator, err := partitioning.NewStoreMigrator(p)
	if err != nil {
		t.Fatalf("NewStoreMigrator: %v", err)
	}
	registerStores(t, migrator, stores)

	generation := p.Generation()
	result, err := p.Apply(context.Background(), plan, migrator)
	if !errors.Is(err, partitioning.ErrMigrationFailed) || !errors.Is(err, partitioning.ErrUnknownStore) {
		t.Fatalf("Apply error = %v, want migration and unknown-store errors", err)
	}
	if result.Applied || p.Generation() != generation {
		t.Fatalf("unknown store applied a plan: result=%+v generation=%d", result, p.Generation())
	}
	for i, key := range keys {
		if _, found := stores[initialRoutes[i].Node.ID].Get(key); !found {
			t.Fatalf("source key %q was lost", key)
		}
	}
}

func TestStoreMigratorCanceledContextDoesNotCommit(t *testing.T) {
	nodes := testNodes()
	p := newPartitioner(t, nodes, 64, 32)
	stores := newMemoryStores(nodes)
	populateStores(t, p, stores, testKeyList(100))

	plan, err := p.PlanAddNode(partitioning.Node{ID: "node-d"})
	if err != nil {
		t.Fatalf("PlanAddNode: %v", err)
	}
	stores["node-d"] = cache.New(cache.Config{})
	migrator, err := partitioning.NewStoreMigrator(p)
	if err != nil {
		t.Fatalf("NewStoreMigrator: %v", err)
	}
	registerStores(t, migrator, stores)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	generation := p.Generation()
	result, err := p.Apply(ctx, plan, migrator)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply error = %v, want context.Canceled", err)
	}
	if result.Applied || p.Generation() != generation {
		t.Fatalf("canceled apply committed: result=%+v generation=%d", result, p.Generation())
	}
}

func TestStoreMigratorValidation(t *testing.T) {
	p := newPartitioner(t, testNodes(), 16, 8)
	if _, err := partitioning.NewStoreMigrator(nil); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("NewStoreMigrator(nil) error = %v", err)
	}

	migrator, err := partitioning.NewStoreMigrator(p)
	if err != nil {
		t.Fatalf("NewStoreMigrator: %v", err)
	}
	store := cache.New(cache.Config{})
	if err := migrator.RegisterStore("", store); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("RegisterStore empty id error = %v", err)
	}
	if err := migrator.RegisterStore("node-a", nil); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("RegisterStore nil error = %v", err)
	}

	ctx := context.Background()
	if _, err := migrator.Migrate(ctx, partitioning.Move{}); !errors.Is(err, partitioning.ErrInvalidMove) {
		t.Fatalf("empty move error = %v", err)
	}
	if _, err := migrator.Migrate(ctx, partitioning.Move{
		Partition: 1,
		Source:    partitioning.Node{ID: "node-a"},
		Target:    partitioning.Node{ID: "node-a"},
	}); !errors.Is(err, partitioning.ErrInvalidMove) {
		t.Fatalf("same-source move error = %v", err)
	}
	if _, err := migrator.Migrate(ctx, partitioning.Move{
		Partition: 1,
		Source:    partitioning.Node{ID: "node-a"},
		Target:    partitioning.Node{ID: "node-b"},
	}); !errors.Is(err, partitioning.ErrUnknownStore) {
		t.Fatalf("unknown store error = %v", err)
	}
}

func keyForPartition(t *testing.T, p *partitioning.Partitioner, partition partitioning.PartitionID, prefix string) string {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		key := fmt.Sprintf("%s:%d", prefix, i)
		if p.PartitionOf(key) == partition {
			return key
		}
	}
	t.Fatalf("no key found for partition %d", partition)
	return ""
}
