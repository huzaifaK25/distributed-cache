package partitioning_test

import (
	"errors"
	"fmt"
	"testing"

	"distributed-cache/internal/partitioning"
)

func testNodes() []partitioning.Node {
	return []partitioning.Node{
		{ID: "node-a", Address: "127.0.0.1:7001"},
		{ID: "node-b", Address: "127.0.0.1:7002"},
		{ID: "node-c", Address: "127.0.0.1:7003"},
	}
}

func testKeyList(count int) []string {
	keys := make([]string, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
	}
	return keys
}

func TestRingRoutingIsDeterministic(t *testing.T) {
	nodes := testNodes()
	ring, err := partitioning.NewRing(nodes, 100)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	if ring.TokenCount() != len(nodes)*100 {
		t.Fatalf("TokenCount = %d, want %d", ring.TokenCount(), len(nodes)*100)
	}

	shuffled := []partitioning.Node{nodes[2], nodes[0], nodes[1]}
	other, err := partitioning.NewRing(shuffled, 100)
	if err != nil {
		t.Fatalf("NewRing shuffled: %v", err)
	}

	valid := make(map[partitioning.NodeID]string, len(nodes))
	for _, node := range nodes {
		valid[node.ID] = node.Address
	}

	for _, key := range testKeyList(2000) {
		owner, ok := ring.Lookup(key)
		if !ok {
			t.Fatalf("Lookup(%q) reported an empty ring", key)
		}
		if address, exists := valid[owner.ID]; !exists || address != owner.Address {
			t.Fatalf("Lookup(%q) = %+v, not a configured node", key, owner)
		}

		repeated, _ := ring.Lookup(key)
		if repeated != owner {
			t.Fatalf("Lookup(%q) changed from %+v to %+v", key, owner, repeated)
		}

		fromOther, _ := other.Lookup(key)
		if fromOther != owner {
			t.Fatalf("Lookup(%q) depends on node order: %+v vs %+v", key, owner, fromOther)
		}

		byHash, ok := ring.LookupHash(partitioning.DefaultHash(key))
		if !ok || byHash != owner {
			t.Fatalf("LookupHash(%q) = %+v, %v; want %+v", key, byHash, ok, owner)
		}
	}
}

func TestRingVirtualNodesSpreadLoad(t *testing.T) {
	const (
		virtualNodes = 200
		keyCount     = 4000
	)
	nodes := []partitioning.Node{
		{ID: "node-a"},
		{ID: "node-b"},
		{ID: "node-c"},
		{ID: "node-d"},
	}

	ring, err := partitioning.NewRing(nodes, virtualNodes)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	if got, want := ring.TokenCount(), len(nodes)*virtualNodes; got != want {
		t.Fatalf("TokenCount = %d, want %d", got, want)
	}

	counts := make(map[partitioning.NodeID]int, len(nodes))
	for _, key := range testKeyList(keyCount) {
		owner, ok := ring.Lookup(key)
		if !ok {
			t.Fatalf("Lookup(%q) reported an empty ring", key)
		}
		counts[owner.ID]++
	}

	for _, node := range nodes {
		share := float64(counts[node.ID]) / keyCount
		if share < 0.10 || share > 0.40 {
			t.Errorf("node %q owns %.1f%% of keys; want a balanced share", node.ID, share*100)
		}
	}
}

func TestRingAddAndRemoveMovesMinimalKeys(t *testing.T) {
	nodes := testNodes()
	before, err := partitioning.NewRing(nodes, 128)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}

	added, err := before.AddNode(partitioning.Node{ID: "node-d", Address: "127.0.0.1:7004"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	keys := testKeyList(4000)
	movedToNewNode := 0
	for _, key := range keys {
		oldOwner, _ := before.Lookup(key)
		newOwner, _ := added.Lookup(key)
		if oldOwner != newOwner {
			if newOwner.ID != "node-d" {
				t.Fatalf("adding node-d moved %q from %q to %q", key, oldOwner.ID, newOwner.ID)
			}
			movedToNewNode++
		}
	}
	if movedToNewNode == 0 {
		t.Fatal("adding a node moved no keys")
	}
	if movedToNewNode > len(keys)/2 {
		t.Fatalf("adding a node moved %d of %d keys; expected a small fraction", movedToNewNode, len(keys))
	}

	removed, err := added.RemoveNode("node-d")
	if err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	for _, key := range keys {
		oldOwner, _ := added.Lookup(key)
		newOwner, _ := removed.Lookup(key)
		if oldOwner != newOwner && oldOwner.ID != "node-d" {
			t.Fatalf("removing node-d moved %q from %q to %q", key, oldOwner.ID, newOwner.ID)
		}
	}
}

func TestRingLookupNReturnsDistinctOwners(t *testing.T) {
	ring, err := partitioning.NewRing(testNodes(), 64)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}

	primary, ok := ring.Lookup("user:42")
	if !ok {
		t.Fatal("Lookup reported an empty ring")
	}
	owners := ring.LookupN("user:42", 10)
	if len(owners) != 3 {
		t.Fatalf("LookupN returned %d owners, want 3", len(owners))
	}
	if owners[0] != primary {
		t.Fatalf("LookupN first owner = %+v, want primary %+v", owners[0], primary)
	}
	seen := make(map[partitioning.NodeID]struct{}, len(owners))
	for _, owner := range owners {
		if _, duplicate := seen[owner.ID]; duplicate {
			t.Fatalf("LookupN returned duplicate owner %q", owner.ID)
		}
		seen[owner.ID] = struct{}{}
	}

	if got := ring.LookupN("user:42", 2); len(got) != 2 {
		t.Fatalf("LookupN(2) returned %d owners", len(got))
	}
	if got := ring.LookupN("user:42", 0); got != nil {
		t.Fatalf("LookupN(0) = %+v, want nil", got)
	}

	empty, err := partitioning.NewRing(nil, 8)
	if err != nil {
		t.Fatalf("NewRing(empty): %v", err)
	}
	if _, ok := empty.Lookup("user:42"); ok {
		t.Fatal("empty ring Lookup returned a node")
	}
	if owners := empty.LookupN("user:42", 2); owners != nil {
		t.Fatalf("empty ring LookupN = %+v, want nil", owners)
	}
}

func TestRingCollisionTieBreakIsDeterministic(t *testing.T) {
	constant := func(string) uint64 { return 42 }
	ring, err := partitioning.NewRingWithHasher([]partitioning.Node{
		{ID: "node-bravo"},
		{ID: "node-alpha"},
	}, 4, constant)
	if err != nil {
		t.Fatalf("NewRingWithHasher: %v", err)
	}

	owner, ok := ring.Lookup("anything")
	if !ok {
		t.Fatal("Lookup reported an empty ring")
	}
	if owner.ID != "node-alpha" {
		t.Fatalf("collision owner = %q, want deterministic node-alpha", owner.ID)
	}

	owners := ring.LookupN("anything", 2)
	if len(owners) != 2 || owners[0].ID != "node-alpha" || owners[1].ID != "node-bravo" {
		t.Fatalf("LookupN collision result = %+v", owners)
	}
}

func TestRingValidation(t *testing.T) {
	nodes := testNodes()

	if _, err := partitioning.NewRing(nodes, 0); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("NewRing with zero virtual nodes error = %v", err)
	}
	if _, err := partitioning.NewRing([]partitioning.Node{{ID: ""}}, 8); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("NewRing with empty node ID error = %v", err)
	}
	if _, err := partitioning.NewRing([]partitioning.Node{{ID: "a"}, {ID: "a"}}, 8); !errors.Is(err, partitioning.ErrInvalidConfig) {
		t.Fatalf("NewRing with duplicate node error = %v", err)
	}

	ring, err := partitioning.NewRing(nodes, 8)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	if _, err := ring.AddNode(nodes[0]); !errors.Is(err, partitioning.ErrNodeExists) {
		t.Fatalf("AddNode duplicate error = %v", err)
	}
	if _, err := ring.RemoveNode("missing"); !errors.Is(err, partitioning.ErrNodeNotFound) {
		t.Fatalf("RemoveNode missing error = %v", err)
	}

	sorted := ring.Nodes()
	for i := 1; i < len(sorted); i++ {
		if sorted[i-1].ID >= sorted[i].ID {
			t.Fatalf("Nodes() is not sorted: %+v", sorted)
		}
	}
}

func BenchmarkRingLookup(b *testing.B) {
	ring, err := partitioning.NewRing(testNodes(), 128)
	if err != nil {
		b.Fatalf("NewRing: %v", err)
	}

	keys := testKeyList(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.Lookup(keys[i%len(keys)])
	}
}
