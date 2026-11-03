package cluster

import (
	"errors"
	"testing"
	"time"
)

func TestRegistryUpsertListAndRemove(t *testing.T) {
	registry := NewRegistry()

	added := []Node{
		{ID: "b", Address: "127.0.0.1:7002", Status: StatusOnline},
		{ID: "a", Address: "127.0.0.1:7001", Status: StatusOnline},
		{ID: "c", Address: "127.0.0.1:7003", Status: StatusDegraded},
	}
	for _, node := range added {
		if err := registry.AddNode(node); err != nil {
			t.Fatalf("AddNode(%s) returned error: %v", node.ID, err)
		}
	}

	nodes := registry.ListNodes()
	if len(nodes) != 3 {
		t.Fatalf("ListNodes() returned %d nodes, want 3", len(nodes))
	}
	if got := []NodeID{nodes[0].ID, nodes[1].ID, nodes[2].ID}; got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("ListNodes() IDs = %v, want [a b c]", got)
	}
	if nodes[0].JoinedAt.IsZero() || nodes[0].LastSeen.IsZero() {
		t.Fatal("AddNode() did not fill timestamps")
	}

	updated := Node{
		ID:       "a",
		Address:  "127.0.0.1:7101",
		Status:   StatusOnline,
		JoinedAt: time.Now(),
		LastSeen: time.Now(),
		Version:  2,
	}
	if err := registry.AddNode(updated); err != nil {
		t.Fatalf("updating AddNode() returned error: %v", err)
	}
	if node, found := registry.Node("a"); !found || node.Address != updated.Address || node.Version != 2 {
		t.Fatalf("Node(a) = (%+v, %v), want the updated record", node, found)
	}

	if !registry.RemoveNode("a") {
		t.Fatal("RemoveNode(a) = false, want true")
	}
	if registry.RemoveNode("a") {
		t.Fatal("second RemoveNode(a) = true, want false")
	}
	if _, found := registry.Node("a"); found {
		t.Fatal("Node(a) found after removal")
	}
	if got := registry.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestRegistryRejectsInvalidNodes(t *testing.T) {
	registry := NewRegistry()

	tests := []Node{
		{Address: "127.0.0.1:7001", Status: StatusOnline},
		{ID: "a", Status: StatusOnline},
		{ID: "a", Address: "127.0.0.1:7001", Status: Status("UNKNOWN")},
	}

	for _, node := range tests {
		err := registry.AddNode(node)
		if !errors.Is(err, ErrInvalidNode) {
			t.Errorf("AddNode(%+v) error = %v, want ErrInvalidNode", node, err)
		}
	}
	if got := registry.Len(); got != 0 {
		t.Fatalf("Len() = %d after invalid inserts, want 0", got)
	}
}

func TestRegistryReturnsCopies(t *testing.T) {
	registry := NewRegistry()
	if err := registry.AddNode(Node{ID: "a", Address: "127.0.0.1:7001"}); err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	nodes := registry.ListNodes()
	nodes[0].Address = "changed"
	node, _ := registry.Node("a")
	if node.Address == "changed" {
		t.Fatal("mutating ListNodes() result changed the registry")
	}

	node.Address = "also-changed"
	node, _ = registry.Node("a")
	if node.Address == "also-changed" {
		t.Fatal("mutating Node() result changed the registry")
	}
}

func TestStatusForAge(t *testing.T) {
	const (
		degradedAfter = 100 * time.Millisecond
		offlineAfter  = 300 * time.Millisecond
	)

	tests := []struct {
		name string
		age  time.Duration
		want Status
	}{
		{name: "fresh", age: 0, want: StatusOnline},
		{name: "just below degraded", age: degradedAfter - time.Nanosecond, want: StatusOnline},
		{name: "at degraded", age: degradedAfter, want: StatusDegraded},
		{name: "just below offline", age: offlineAfter - time.Nanosecond, want: StatusDegraded},
		{name: "at offline", age: offlineAfter, want: StatusOffline},
		{name: "long offline", age: time.Hour, want: StatusOffline},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := statusForAge(tt.age, degradedAfter, offlineAfter); got != tt.want {
				t.Fatalf("statusForAge(%s) = %s, want %s", tt.age, got, tt.want)
			}
		})
	}
}

func TestShouldReplaceNode(t *testing.T) {
	base := Node{
		ID:       "peer",
		Address:  "127.0.0.1:7001",
		Status:   StatusOnline,
		LastSeen: time.Now(),
		Version:  2,
	}

	fresher := base
	fresher.LastSeen = base.LastSeen.Add(time.Millisecond)
	fresher.Version = 1
	fresher.Status = StatusDegraded
	if !shouldReplaceNode(fresher, base) {
		t.Fatal("a fresher observation should replace an older one")
	}

	stale := base
	stale.LastSeen = base.LastSeen.Add(-time.Millisecond)
	stale.Version = 99
	if shouldReplaceNode(stale, base) {
		t.Fatal("an older observation should not replace a fresher one")
	}

	higherVersion := base
	higherVersion.Version = base.Version + 1
	if !shouldReplaceNode(higherVersion, base) {
		t.Fatal("a higher version should win when LastSeen is equal")
	}

	higherStatus := base
	higherStatus.Status = StatusDegraded
	lowerStatus := base
	lowerStatus.Status = StatusOffline
	if !shouldReplaceNode(higherStatus, lowerStatus) {
		t.Fatal("a higher health rank should win when versions and times are equal")
	}

	if shouldReplaceNode(base, base) {
		t.Fatal("identical nodes should not replace each other")
	}
}
