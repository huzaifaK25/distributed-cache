package partitioning

import (
	"errors"
	"fmt"
	"sort"
)

// Ring and node errors.
var (
	ErrNodeExists   = errors.New("partitioning: node already exists")
	ErrNodeNotFound = errors.New("partitioning: node not found")
)

// DefaultVirtualNodes is the number of ring points created per physical node.
const DefaultVirtualNodes = 128

// NodeID uniquely identifies a node in the hash ring.
type NodeID string

// Node is one physical cache node and its routable address.
//
// Address is carried through routing and migration plans so callers can reach
// the owner without maintaining a separate lookup table. The partitioning
// package itself never dials it.
type Node struct {
	ID      NodeID `json:"id"`
	Address string `json:"address,omitempty"`
}

// String returns the node ID.
func (n Node) String() string {
	return string(n.ID)
}

// Ring is an immutable consistent hash ring.
//
// Every physical node contributes VirtualNodes points. A lookup hashes the key
// and walks clockwise to the first point, so adding or removing one node only
// changes the keys in the ranges that node now owns.
//
// Ring values are safe for concurrent use because none of their fields are
// mutated after construction. AddNode and RemoveNode return a new Ring.
type Ring struct {
	tokens       []vnode
	nodes        []Node
	byID         map[NodeID]Node
	virtualNodes int
	hash         Hasher
}

// NewRing builds a ring with virtualNodes points per node using DefaultHash.
func NewRing(nodes []Node, virtualNodes int) (*Ring, error) {
	return NewRingWithHasher(nodes, virtualNodes, DefaultHash)
}

// NewRingWithHasher builds a ring with a custom hash function.
//
// A nil hasher selects DefaultHash. virtualNodes must be positive. Node IDs
// must be non-empty and unique; node order does not affect routing.
func NewRingWithHasher(nodes []Node, virtualNodes int, hash Hasher) (*Ring, error) {
	if virtualNodes <= 0 {
		return nil, fmt.Errorf("%w: virtual nodes must be positive", ErrInvalidConfig)
	}
	if hash == nil {
		hash = DefaultHash
	}

	normalized, err := normalizeNodes(nodes)
	if err != nil {
		return nil, err
	}

	byID := make(map[NodeID]Node, len(normalized))
	for _, node := range normalized {
		byID[node.ID] = node
	}

	return &Ring{
		tokens:       buildTokens(normalized, virtualNodes, hash),
		nodes:        normalized,
		byID:         byID,
		virtualNodes: virtualNodes,
		hash:         hash,
	}, nil
}

// Lookup returns the node responsible for key.
func (r *Ring) Lookup(key string) (Node, bool) {
	if r == nil || len(r.tokens) == 0 {
		return Node{}, false
	}
	return r.LookupHash(r.hash(key))
}

// LookupHash returns the node responsible for a precomputed ring hash.
func (r *Ring) LookupHash(hash uint64) (Node, bool) {
	if r == nil || len(r.tokens) == 0 {
		return Node{}, false
	}

	index := lookupToken(r.tokens, hash)
	if index == len(r.tokens) {
		index = 0
	}
	return r.byID[r.tokens[index].nodeID], true
}

// LookupN returns up to count distinct nodes responsible for key, in clockwise
// ring order starting with the primary owner.
//
// The result is useful for replica placement. It contains at most the number
// of physical nodes on the ring. An empty ring returns nil.
func (r *Ring) LookupN(key string, count int) []Node {
	if r == nil || count <= 0 || len(r.tokens) == 0 {
		return nil
	}
	return r.lookupN(r.hash(key), count)
}

// lookupN implements LookupN for a precomputed hash.
func (r *Ring) lookupN(hash uint64, count int) []Node {
	if count > len(r.byID) {
		count = len(r.byID)
	}

	owners := make([]Node, 0, count)
	seen := make(map[NodeID]struct{}, count)
	start := lookupToken(r.tokens, hash)

	for offset := 0; offset < len(r.tokens) && len(owners) < count; offset++ {
		token := r.tokens[(start+offset)%len(r.tokens)]
		if _, duplicate := seen[token.nodeID]; duplicate {
			continue
		}
		seen[token.nodeID] = struct{}{}
		owners = append(owners, r.byID[token.nodeID])
	}
	return owners
}

// AddNode returns a new ring containing node.
func (r *Ring) AddNode(node Node) (*Ring, error) {
	if err := validateNode(node); err != nil {
		return nil, err
	}
	if r == nil {
		return NewRingWithHasher([]Node{node}, DefaultVirtualNodes, DefaultHash)
	}
	if _, exists := r.byID[node.ID]; exists {
		return nil, fmt.Errorf("%w: %q", ErrNodeExists, node.ID)
	}

	nodes := r.Nodes()
	nodes = append(nodes, node)
	return NewRingWithHasher(nodes, r.virtualNodes, r.hash)
}

// RemoveNode returns a new ring without the node identified by id.
func (r *Ring) RemoveNode(id NodeID) (*Ring, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, id)
	}
	if _, exists := r.byID[id]; !exists {
		return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, id)
	}

	nodes := make([]Node, 0, len(r.nodes)-1)
	for _, node := range r.nodes {
		if node.ID != id {
			nodes = append(nodes, node)
		}
	}
	return NewRingWithHasher(nodes, r.virtualNodes, r.hash)
}

// Nodes returns a copy of the physical nodes, sorted by ID.
func (r *Ring) Nodes() []Node {
	if r == nil {
		return nil
	}
	return append([]Node(nil), r.nodes...)
}

// TokenCount returns the number of virtual nodes on the ring.
func (r *Ring) TokenCount() int {
	if r == nil {
		return 0
	}
	return len(r.tokens)
}

// VirtualNodes returns the configured virtual-node count per physical node.
func (r *Ring) VirtualNodes() int {
	if r == nil {
		return 0
	}
	return r.virtualNodes
}

// Hasher returns the hash function used by the ring.
func (r *Ring) Hasher() Hasher {
	if r == nil {
		return DefaultHash
	}
	return r.hash
}

// normalizeNodes validates, de-duplicates, and sorts nodes by ID.
func normalizeNodes(nodes []Node) ([]Node, error) {
	normalized := make([]Node, 0, len(nodes))
	seen := make(map[NodeID]struct{}, len(nodes))
	for _, node := range nodes {
		if err := validateNode(node); err != nil {
			return nil, err
		}
		if _, duplicate := seen[node.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrInvalidConfig, node.ID)
		}
		seen[node.ID] = struct{}{}
		normalized = append(normalized, node)
	}

	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].ID < normalized[j].ID
	})
	return normalized, nil
}

// validateNode rejects nodes that cannot be routed.
func validateNode(node Node) error {
	if node.ID == "" {
		return fmt.Errorf("%w: node id must not be empty", ErrInvalidConfig)
	}
	return nil
}
