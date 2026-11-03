package cluster

import (
	"sync"
	"time"
)

// heartbeatLoop periodically refreshes this node's liveness, recomputes peer
// health from LastSeen, and sends I_AM_ALIVE RPCs to known peers.
func (c *Cluster) heartbeatLoop() {
	defer c.loopWG.Done()

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.heartbeatOnce(time.Now())
		}
	}
}

// heartbeatOnce performs one full heartbeat cycle.
func (c *Cluster) heartbeatOnce(now time.Time) {
	if c.ctx.Err() != nil {
		return
	}

	c.refreshSelf(now)
	c.updateHealth(now)
	c.sendHeartbeats()
}

// refreshSelf records local liveness in the registry. Every node is its own
// freshest source of truth while it is running.
func (c *Cluster) refreshSelf(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	self := c.self
	self.Status = StatusOnline
	self.LastSeen = now
	c.self = self
	_ = c.registry.AddNode(self)
}

// updateHealth derives peer health from how long ago each node was observed.
// Status changes are versioned so they converge through gossip.
func (c *Cluster) updateHealth(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodes := c.registry.ListNodes()
	changed := false
	for _, node := range nodes {
		if node.ID == c.config.NodeID {
			continue
		}

		status := statusForAge(
			now.Sub(node.LastSeen),
			c.config.DegradedAfter,
			c.config.OfflineAfter,
		)
		if status == node.Status {
			continue
		}

		node.Status = status
		node.Version++
		if err := c.registry.AddNode(node); err == nil {
			changed = true
		}
	}
	if changed {
		c.viewVersion++
	}
}

// sendHeartbeats probes every known peer concurrently. A successful response
// is direct evidence that the peer is online.
func (c *Cluster) sendHeartbeats() {
	peers := c.peerNodes()
	if len(peers) == 0 {
		return
	}

	message := c.newMessage(messageHeartbeat)
	var wg sync.WaitGroup

	for _, peer := range peers {
		if c.ctx.Err() != nil {
			break
		}

		wg.Add(1)
		go func(peer Node) {
			defer wg.Done()

			response, err := c.roundTrip(peer.Address, message)
			if err != nil || response.Type == messageError {
				return
			}
			if response.ClusterID != c.config.ClusterID || response.Sender.ID != peer.ID {
				return
			}
			_ = c.observeNode(response.Sender, time.Now())
		}(peer)
	}
	wg.Wait()
}
