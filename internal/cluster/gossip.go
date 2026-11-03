package cluster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

// Cluster message types. Heartbeats use the I_AM_ALIVE name from the phase
// specification; the remaining messages implement join, leave, and gossip.
type messageType string

const (
	messageJoin         messageType = "join"
	messageJoinAck      messageType = "join_ack"
	messageHeartbeat    messageType = "i_am_alive"
	messageHeartbeatAck messageType = "i_am_alive_ack"
	messageGossip       messageType = "gossip"
	messageGossipAck    messageType = "gossip_ack"
	messageLeave        messageType = "leave"
	messageLeaveAck     messageType = "leave_ack"
	messageError        messageType = "error"
)

const frameHeaderSize = 4

// wireMessage is exchanged over the cluster TCP listener.
type wireMessage struct {
	Type      messageType       `json:"type"`
	ClusterID string            `json:"cluster_id"`
	Sender    Node              `json:"sender"`
	Metadata  Metadata          `json:"metadata"`
	Nodes     []Node            `json:"nodes,omitempty"`
	Left      map[NodeID]uint64 `json:"left,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// transport delivers one request and returns one response.
type transport interface {
	RoundTrip(ctx context.Context, address string, message wireMessage) (wireMessage, error)
}

// tcpTransport implements transport over a single request/response TCP frame.
type tcpTransport struct {
	maxMessageSize int
}

// newTCPTransport creates a transport that enforces the configured frame limit.
func newTCPTransport(maxMessageSize int) *tcpTransport {
	return &tcpTransport{maxMessageSize: maxMessageSize}
}

// RoundTrip dials address, sends one frame, and reads one response frame.
func (t *tcpTransport) RoundTrip(ctx context.Context, address string, message wireMessage) (wireMessage, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return wireMessage{}, err
	}
	defer conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	if requested, ok := ctx.Deadline(); ok {
		deadline = requested
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return wireMessage{}, err
	}

	// Closing the socket also unblocks a read when the caller cancels early.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if err := writeFrame(conn, message, t.maxMessageSize); err != nil {
		return wireMessage{}, err
	}
	return readFrame(conn, t.maxMessageSize)
}

// writeFrame encodes message as a 4-byte length followed by JSON bytes.
func writeFrame(w io.Writer, message wireMessage, maxMessageSize int) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode cluster message: %w", err)
	}
	if len(payload) == 0 || len(payload) > maxMessageSize || uint64(len(payload)) > math.MaxUint32 {
		return fmt.Errorf("cluster message size %d exceeds limit %d", len(payload), maxMessageSize)
	}

	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

// readFrame reads and decodes one length-prefixed JSON message.
func readFrame(r io.Reader, maxMessageSize int) (wireMessage, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return wireMessage{}, err
	}

	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || uint64(size) > uint64(maxMessageSize) {
		return wireMessage{}, fmt.Errorf("cluster message size %d exceeds limit %d", size, maxMessageSize)
	}

	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return wireMessage{}, err
	}

	var message wireMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return wireMessage{}, fmt.Errorf("decode cluster message: %w", err)
	}
	return message, nil
}

// acceptLoop accepts cluster RPC connections until the listener closes.
func (c *Cluster) acceptLoop() {
	defer c.acceptWG.Done()

	for {
		conn, err := c.listener.Accept()
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}

			// A transient accept failure should not silently disable
			// membership traffic.
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return
		}
		if c.ctx.Err() != nil {
			_ = conn.Close()
			return
		}

		c.registerConnection(conn)
		c.connWG.Add(1)
		go c.handleConnection(conn)
	}
}

// handleConnection reads one cluster message and writes one response.
func (c *Cluster) handleConnection(conn net.Conn) {
	defer c.connWG.Done()
	defer c.unregisterConnection(conn)
	defer conn.Close()
	defer func() { _ = recover() }()

	if err := conn.SetDeadline(time.Now().Add(c.config.RequestTimeout)); err != nil {
		return
	}
	message, err := readFrame(conn, c.config.MaxMessageSize)
	if err != nil {
		return
	}

	response := c.handleMessage(message)
	_ = writeFrame(conn, response, c.config.MaxMessageSize)
}

// handleMessage validates and dispatches one decoded message.
func (c *Cluster) handleMessage(message wireMessage) wireMessage {
	if message.ClusterID != c.config.ClusterID {
		return c.errorMessage("cluster id %q does not match %q", message.ClusterID, c.config.ClusterID)
	}
	if message.Sender.ID == "" || message.Sender.Address == "" {
		return c.errorMessage("sender id and address are required")
	}
	if message.Sender.ID == c.config.NodeID {
		return c.errorMessage("sender is this node")
	}

	now := time.Now()
	switch message.Type {
	case messageJoin:
		if err := c.acceptJoin(message.Sender, now); err != nil {
			return c.errorMessage("%v", err)
		}
		c.triggerGossip()
		return c.snapshotMessage(messageJoinAck)

	case messageHeartbeat:
		if err := c.observeNode(message.Sender, now); err != nil {
			return c.errorMessage("%v", err)
		}
		return c.newMessage(messageHeartbeatAck)

	case messageGossip:
		if err := c.observeNode(message.Sender, now); err != nil {
			return c.errorMessage("%v", err)
		}
		c.mergeView(message.Nodes, message.Left)
		return c.snapshotMessage(messageGossipAck)

	case messageLeave:
		c.acceptLeave(message.Sender)
		c.triggerGossip()
		return c.newMessage(messageLeaveAck)

	default:
		return c.errorMessage("unknown message type %q", message.Type)
	}
}

// acceptJoin registers a joining node, clearing any tombstone for it.
func (c *Cluster) acceptJoin(sender Node, now time.Time) error {
	if sender.ID == c.config.NodeID {
		return fmt.Errorf("node cannot join itself")
	}

	normalized, err := normalizeNode(sender, now)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	version := normalized.Version
	if current, found := c.registry.Node(normalized.ID); found && current.Version >= version {
		version = current.Version + 1
	}
	if tombstoneVersion, left := c.tombstones[normalized.ID]; left && tombstoneVersion >= version {
		version = tombstoneVersion + 1
	}
	delete(c.tombstones, normalized.ID)

	normalized.Version = version
	normalized.Status = StatusOnline
	normalized.LastSeen = now

	if err := c.registry.AddNode(normalized); err != nil {
		return err
	}
	c.viewVersion++
	return nil
}

// observeNode records direct evidence that sender is alive.
func (c *Cluster) observeNode(sender Node, now time.Time) error {
	if sender.ID == c.config.NodeID {
		return nil
	}

	normalized, err := normalizeNode(sender, now)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	changed := false
	if tombstoneVersion, left := c.tombstones[normalized.ID]; left {
		if normalized.Version <= tombstoneVersion {
			return nil
		}
		// A higher-versioned incarnation clears the old tombstone.
		delete(c.tombstones, normalized.ID)
		changed = true
	}

	current, exists := c.registry.Node(normalized.ID)
	version := normalized.Version
	if exists && current.Version > version {
		version = current.Version
	}
	if exists && current.Address != normalized.Address && version <= current.Version {
		version = current.Version + 1
	}

	statusChanged := !exists || current.Status != StatusOnline
	if statusChanged {
		version++
	}

	normalized.Status = StatusOnline
	normalized.LastSeen = now
	normalized.Version = version
	if exists {
		normalized.JoinedAt = current.JoinedAt
	}

	if err := c.registry.AddNode(normalized); err != nil {
		return err
	}
	if changed || !exists || statusChanged || current.Address != normalized.Address {
		c.viewVersion++
	}
	return nil
}

// acceptLeave removes a node and records a tombstone so stale gossip cannot
// resurrect it. A later join with a higher version can rejoin.
func (c *Cluster) acceptLeave(sender Node) {
	if sender.ID == c.config.NodeID {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	version := sender.Version
	if current, found := c.registry.Node(sender.ID); found {
		if current.Version > version {
			version = current.Version
		}
		c.registry.RemoveNode(sender.ID)
		c.viewVersion++
	}
	if existing, found := c.tombstones[sender.ID]; !found || version > existing {
		c.tombstones[sender.ID] = version
	}
}

// mergeView applies a remote membership snapshot and tombstone map.
func (c *Cluster) mergeView(nodes []Node, left map[NodeID]uint64) {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	changed := false
	for id, version := range left {
		if id == c.config.NodeID {
			// A peer remembers this incarnation as gone. If it is running and
			// hears that tombstone, reclaim the identity above it.
			if version >= c.self.Version {
				self := c.self
				self.Version = version + 1
				c.self = self
				_ = c.registry.AddNode(self)
				changed = true
			}
			continue
		}
		if existing, found := c.tombstones[id]; !found || version > existing {
			c.tombstones[id] = version
			changed = true
		}
		tombstoneVersion := c.tombstones[id]
		if node, found := c.registry.Node(id); found && node.Version <= tombstoneVersion {
			c.registry.RemoveNode(id)
			changed = true
		}
	}

	for _, incoming := range nodes {
		if incoming.ID == "" || incoming.ID == c.config.NodeID {
			continue
		}
		normalized, err := normalizeNode(incoming, now)
		if err != nil {
			continue
		}

		if tombstoneVersion, left := c.tombstones[normalized.ID]; left {
			if normalized.Version <= tombstoneVersion {
				continue
			}
			// A higher-versioned join explicitly revives the node.
			delete(c.tombstones, normalized.ID)
			changed = true
		}

		current, exists := c.registry.Node(normalized.ID)
		if !exists {
			if err := c.registry.AddNode(normalized); err == nil {
				changed = true
			}
			continue
		}
		if shouldReplaceNode(normalized, current) {
			if err := c.registry.AddNode(normalized); err == nil {
				changed = true
			}
		}
	}

	if changed {
		c.viewVersion++
	}
}

// adoptSelfVersion accepts a newer version assigned to this node by a seed
// during join. This keeps reincarnations above any tombstone they must clear.
func (c *Cluster) adoptSelfVersion(nodes []Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range nodes {
		if node.ID != c.config.NodeID || node.Version <= c.self.Version {
			continue
		}

		self := c.self
		self.Version = node.Version
		c.self = self
		_ = c.registry.AddNode(self)
		return
	}
}

// shouldReplaceNode resolves two views of one node. Recent observation wins;
// version and health rank break ties deterministically.
func shouldReplaceNode(incoming, current Node) bool {
	if incoming.LastSeen.After(current.LastSeen) {
		return true
	}
	if incoming.LastSeen.Before(current.LastSeen) {
		return false
	}
	if incoming.Version != current.Version {
		return incoming.Version > current.Version
	}
	return incoming.Status.rank() > current.Status.rank()
}

// newMessage builds a request containing this node's identity and metadata.
func (c *Cluster) newMessage(kind messageType) wireMessage {
	c.mu.Lock()
	defer c.mu.Unlock()

	return wireMessage{
		Type:      kind,
		ClusterID: c.config.ClusterID,
		Sender:    c.self,
		Metadata: Metadata{
			ClusterID: c.config.ClusterID,
			NodeCount: c.registry.Len(),
			Version:   c.viewVersion,
		},
	}
}

// snapshotMessage builds a message containing the full membership view.
func (c *Cluster) snapshotMessage(kind messageType) wireMessage {
	c.mu.Lock()
	defer c.mu.Unlock()

	message := wireMessage{
		Type:      kind,
		ClusterID: c.config.ClusterID,
		Sender:    c.self,
		Metadata: Metadata{
			ClusterID: c.config.ClusterID,
			NodeCount: c.registry.Len(),
			Version:   c.viewVersion,
		},
		Nodes: c.registry.ListNodes(),
	}
	if len(c.tombstones) > 0 {
		message.Left = make(map[NodeID]uint64, len(c.tombstones))
		for id, version := range c.tombstones {
			message.Left[id] = version
		}
	}
	return message
}

// errorMessage builds a protocol error response.
func (c *Cluster) errorMessage(format string, args ...any) wireMessage {
	message := c.newMessage(messageError)
	message.Error = fmt.Sprintf(format, args...)
	return message
}

// roundTrip sends one RPC using the cluster's request timeout.
func (c *Cluster) roundTrip(address string, message wireMessage) (wireMessage, error) {
	ctx, cancel := context.WithTimeout(c.ctx, c.config.RequestTimeout)
	defer cancel()
	return c.transport.RoundTrip(ctx, address, message)
}

// triggerGossip requests an immediate gossip round, coalescing bursts.
func (c *Cluster) triggerGossip() {
	select {
	case c.gossipNow <- struct{}{}:
	default:
	}
}

// gossipLoop periodically exchanges full membership snapshots with peers.
func (c *Cluster) gossipLoop() {
	defer c.loopWG.Done()

	ticker := time.NewTicker(c.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.sendGossip()
		case <-c.gossipNow:
			c.sendGossip()
		}
	}
}

// sendGossip performs push-pull anti-entropy with every known peer in
// parallel. A successful response also refreshes the peer's local health.
func (c *Cluster) sendGossip() {
	peers := c.peerNodes()
	if len(peers) == 0 {
		return
	}

	message := c.snapshotMessage(messageGossip)
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
			c.mergeView(response.Nodes, response.Left)
		}(peer)
	}
	wg.Wait()
}

// registerConnection adds an inbound connection to the shutdown set.
func (c *Cluster) registerConnection(conn net.Conn) {
	c.connMu.Lock()
	c.conns[conn] = struct{}{}
	c.connMu.Unlock()
}

// unregisterConnection removes an inbound connection from the shutdown set.
func (c *Cluster) unregisterConnection(conn net.Conn) {
	c.connMu.Lock()
	delete(c.conns, conn)
	c.connMu.Unlock()
}

// closeConnections force-closes inbound connections during shutdown.
func (c *Cluster) closeConnections() {
	c.connMu.Lock()
	for conn := range c.conns {
		_ = conn.Close()
	}
	c.connMu.Unlock()
}
