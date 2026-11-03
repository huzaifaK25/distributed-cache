package cluster

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrInvalidNode is returned when a node has no ID or address, or an unknown
// health status.
var ErrInvalidNode = errors.New("cluster: invalid node")

// Registry is a concurrency-safe collection of known nodes.
//
// AddNode is an upsert: adding an existing ID replaces that node's record.
// Returned nodes are copies, so callers cannot mutate registry state.
type Registry struct {
	mu    sync.RWMutex
	nodes map[NodeID]Node
}

// NewRegistry returns an empty node registry.
func NewRegistry() *Registry {
	return &Registry{nodes: make(map[NodeID]Node)}
}

// AddNode inserts or replaces a node.
func (r *Registry) AddNode(node Node) error {
	normalized, err := normalizeNode(node, time.Now())
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidNode, err)
	}

	r.mu.Lock()
	r.nodes[normalized.ID] = normalized
	r.mu.Unlock()
	return nil
}

// RemoveNode deletes a node and reports whether it was present.
func (r *Registry) RemoveNode(id NodeID) bool {
	r.mu.Lock()
	_, found := r.nodes[id]
	delete(r.nodes, id)
	r.mu.Unlock()
	return found
}

// Node returns a copy of the node with the given ID.
func (r *Registry) Node(id NodeID) (Node, bool) {
	r.mu.RLock()
	node, found := r.nodes[id]
	r.mu.RUnlock()
	return node, found
}

// ListNodes returns all known nodes sorted by NodeID.
func (r *Registry) ListNodes() []Node {
	r.mu.RLock()
	nodes := make([]Node, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodes = append(nodes, node)
	}
	r.mu.RUnlock()

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes
}

// Len returns the number of known nodes.
func (r *Registry) Len() int {
	r.mu.RLock()
	count := len(r.nodes)
	r.mu.RUnlock()
	return count
}
