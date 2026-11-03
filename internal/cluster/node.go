// Package cluster implements TCP-based cluster membership, health tracking,
// and gossip for cache nodes.
package cluster

import (
	"fmt"
	"time"
)

// NodeID uniquely identifies a cluster member for its lifetime.
type NodeID string

// Status is a node's observed health.
type Status string

const (
	StatusOnline   Status = "ONLINE"
	StatusDegraded Status = "DEGRADED"
	StatusOffline  Status = "OFFLINE"
)

// valid reports whether s is a defined health state.
func (s Status) valid() bool {
	switch s {
	case StatusOnline, StatusDegraded, StatusOffline:
		return true
	default:
		return false
	}
}

// rank orders health states for deterministic conflict resolution.
func (s Status) rank() int {
	switch s {
	case StatusOnline:
		return 2
	case StatusDegraded:
		return 1
	case StatusOffline:
		return 0
	default:
		return -1
	}
}

// Node is one cluster member and its last known state.
type Node struct {
	ID       NodeID    `json:"id"`
	Address  string    `json:"address"` // Address of the node's cluster endpoint.
	Status   Status    `json:"status"`
	JoinedAt time.Time `json:"joined_at"`
	LastSeen time.Time `json:"last_seen"`
	Version  uint64    `json:"version"` // Increases when membership metadata changes.
}

// normalizeNode fills optional fields and rejects unusable nodes.
func normalizeNode(node Node, now time.Time) (Node, error) {
	if node.ID == "" {
		return Node{}, fmt.Errorf("node id must not be empty")
	}
	if node.Address == "" {
		return Node{}, fmt.Errorf("node %q address must not be empty", node.ID)
	}

	if node.Status == "" {
		node.Status = StatusOnline
	}
	if !node.Status.valid() {
		return Node{}, fmt.Errorf("node %q has invalid status %q", node.ID, node.Status)
	}
	if node.JoinedAt.IsZero() {
		node.JoinedAt = now
	}
	if node.LastSeen.IsZero() {
		node.LastSeen = now
	}

	return node, nil
}

// statusForAge maps time since the last observation to a health state.
func statusForAge(age, degradedAfter, offlineAfter time.Duration) Status {
	switch {
	case age >= offlineAfter:
		return StatusOffline
	case age >= degradedAfter:
		return StatusDegraded
	default:
		return StatusOnline
	}
}
