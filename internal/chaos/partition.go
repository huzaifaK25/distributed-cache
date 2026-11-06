package chaos

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

// ErrPartitioned is returned when an operation crosses a blocked link.
var ErrPartitioned = errors.New("chaos: network link partitioned")

// Partition gates communication between named nodes.
//
// A partition is a set of groups. Links inside one group remain available;
// every link between different groups is blocked. Connections wrapped with
// Wrap report ErrPartitioned while their link is blocked and resume working
// after Heal, which makes it easy to verify partition recovery.
type Partition struct {
	mu        sync.RWMutex
	idToGroup map[string]int
	groups    [][]string
}

// NewPartition returns a healed partition controller.
func NewPartition() *Partition {
	return &Partition{}
}

// Split creates a network partition from groups of node IDs.
//
// At least two non-empty groups are required. An ID may appear in only one
// group, and duplicate IDs are rejected. Splitting again replaces the current
// partition.
func (p *Partition) Split(groups ...[]string) error {
	if p == nil {
		return fmt.Errorf("%w: nil partition", ErrInvalidConfig)
	}
	if len(groups) < 2 {
		return fmt.Errorf("%w: a partition requires at least two groups", ErrInvalidConfig)
	}

	idToGroup := make(map[string]int)
	copied := make([][]string, 0, len(groups))
	for groupIndex, group := range groups {
		if len(group) == 0 {
			return fmt.Errorf("%w: partition group %d is empty", ErrInvalidConfig, groupIndex)
		}

		copiedGroup := make([]string, 0, len(group))
		for _, id := range group {
			if id == "" {
				return fmt.Errorf("%w: partition id must not be empty", ErrInvalidConfig)
			}
			if _, duplicate := idToGroup[id]; duplicate {
				return fmt.Errorf("%w: id %q appears in more than one group", ErrInvalidConfig, id)
			}
			idToGroup[id] = groupIndex
			copiedGroup = append(copiedGroup, id)
		}
		copied = append(copied, copiedGroup)
	}

	p.mu.Lock()
	p.idToGroup = idToGroup
	p.groups = copied
	p.mu.Unlock()
	return nil
}

// Heal removes the current partition.
func (p *Partition) Heal() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.idToGroup = nil
	p.groups = nil
	p.mu.Unlock()
}

// Allowed reports whether a link from one node to another may carry traffic.
//
// Unlisted nodes are unconstrained. When no partition is active every link is
// allowed.
func (p *Partition) Allowed(from, to string) bool {
	if p == nil {
		return true
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.idToGroup) == 0 {
		return true
	}
	fromGroup, fromKnown := p.idToGroup[from]
	toGroup, toKnown := p.idToGroup[to]
	if !fromKnown || !toKnown {
		return true
	}
	return fromGroup == toGroup
}

// Groups returns a deep copy of the current partition groups.
func (p *Partition) Groups() [][]string {
	if p == nil {
		return nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.groups) == 0 {
		return nil
	}
	groups := make([][]string, len(p.groups))
	for i, group := range p.groups {
		groups[i] = append([]string(nil), group...)
	}
	return groups
}

// IsSplit reports whether a partition is active.
func (p *Partition) IsSplit() bool {
	if p == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.groups) > 0
}

// Wrap returns a connection that fails reads and writes across a blocked link.
func (p *Partition) Wrap(conn net.Conn, from, to string) net.Conn {
	if p == nil || conn == nil {
		return conn
	}
	return &partitionConn{Conn: conn, partition: p, from: from, to: to}
}

// partitionConn enforces one named link on top of a byte stream.
type partitionConn struct {
	net.Conn
	partition *Partition
	from      string
	to        string
}

// Read fails while the link is partitioned.
func (c *partitionConn) Read(p []byte) (int, error) {
	if !c.partition.Allowed(c.from, c.to) {
		return 0, fmt.Errorf("%w: %s -> %s", ErrPartitioned, c.from, c.to)
	}
	return c.Conn.Read(p)
}

// Write fails while the link is partitioned.
func (c *partitionConn) Write(p []byte) (int, error) {
	if !c.partition.Allowed(c.from, c.to) {
		return 0, fmt.Errorf("%w: %s -> %s", ErrPartitioned, c.from, c.to)
	}
	return c.Conn.Write(p)
}
