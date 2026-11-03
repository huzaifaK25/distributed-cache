package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Lifecycle and configuration errors.
var (
	ErrInvalidConfig   = errors.New("cluster: invalid config")
	ErrNotRunning      = errors.New("cluster: not running")
	ErrAlreadyStarted  = errors.New("cluster: already started")
	ErrClosed          = errors.New("cluster: closed")
	ErrShutdownTimeout = errors.New("cluster: shutdown timeout")
)

// Config controls cluster identity, membership timing, and transport limits.
//
// Zero values are replaced by DefaultConfig values in New.
type Config struct {
	// ClusterID must match across nodes that should form one cluster.
	ClusterID string

	// NodeID uniquely identifies this node for the cluster's lifetime.
	NodeID NodeID

	// Address is the TCP listen address for cluster messages. The default is
	// "127.0.0.1:0"; Start updates Node.Address to the bound port.
	Address string

	// HeartbeatInterval controls how often I_AM_ALIVE messages are sent and
	// health is re-evaluated.
	HeartbeatInterval time.Duration

	// GossipInterval controls anti-entropy exchanges of membership and health.
	GossipInterval time.Duration

	// DegradedAfter is how long without contact before a node becomes DEGRADED.
	DegradedAfter time.Duration

	// OfflineAfter is how long without contact before a node becomes OFFLINE.
	// It must be greater than DegradedAfter.
	OfflineAfter time.Duration

	// RequestTimeout bounds one join, heartbeat, gossip, or leave RPC.
	RequestTimeout time.Duration

	// ShutdownTimeout bounds graceful drain of gossip requests and connections.
	ShutdownTimeout time.Duration

	// MaxMessageSize is the maximum encoded cluster message size.
	MaxMessageSize int
}

// DefaultConfig returns membership defaults suitable for a small cluster.
func DefaultConfig() Config {
	return Config{
		Address:           "127.0.0.1:0",
		HeartbeatInterval: time.Second,
		GossipInterval:    2 * time.Second,
		DegradedAfter:     3 * time.Second,
		OfflineAfter:      10 * time.Second,
		RequestTimeout:    2 * time.Second,
		ShutdownTimeout:   5 * time.Second,
		MaxMessageSize:    1 << 20, // 1 MiB.
	}
}

// withDefaults validates identity fields and fills timing defaults.
func (c Config) withDefaults() (Config, error) {
	defaults := DefaultConfig()

	if c.ClusterID == "" {
		return Config{}, fmt.Errorf("%w: cluster id must not be empty", ErrInvalidConfig)
	}
	if c.NodeID == "" {
		return Config{}, fmt.Errorf("%w: node id must not be empty", ErrInvalidConfig)
	}

	if c.Address == "" {
		c.Address = defaults.Address
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if c.GossipInterval <= 0 {
		c.GossipInterval = defaults.GossipInterval
	}
	if c.DegradedAfter <= 0 {
		c.DegradedAfter = defaults.DegradedAfter
	}
	if c.OfflineAfter <= 0 {
		c.OfflineAfter = defaults.OfflineAfter
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = defaults.RequestTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaults.ShutdownTimeout
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = defaults.MaxMessageSize
	}
	if c.OfflineAfter <= c.DegradedAfter {
		return Config{}, fmt.Errorf(
			"%w: offline timeout %s must be greater than degraded timeout %s",
			ErrInvalidConfig, c.OfflineAfter, c.DegradedAfter,
		)
	}

	return c, nil
}

// Metadata is the cluster-level view shared through gossip.
type Metadata struct {
	ClusterID string `json:"cluster_id"`
	NodeCount int    `json:"node_count"`
	Version   uint64 `json:"version"` // Local monotonically increasing view revision.
}

// clusterState is the lifecycle position.
type clusterState uint8

const (
	stateNew clusterState = iota
	stateRunning
	stateStopping
	stateStopped
)

// Cluster manages this node's membership view.
//
// Use New to create it, Start to open the cluster listener, Join to contact
// seed nodes, and Leave or Stop to shut it down.
type Cluster struct {
	config    Config
	registry  *Registry
	transport transport

	mu          sync.Mutex
	self        Node
	tombstones  map[NodeID]uint64
	viewVersion uint64

	stateMu   sync.Mutex
	state     clusterState
	stoppedCh chan struct{}
	stopErr   error
	leaving   atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	listener net.Listener
	acceptWG sync.WaitGroup
	loopWG   sync.WaitGroup
	connWG   sync.WaitGroup

	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	gossipNow chan struct{}
}

// New validates config and creates a stopped cluster node.
//
// No socket or goroutine is created until Start.
func New(config Config) (*Cluster, error) {
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now()

	return &Cluster{
		config:    config,
		registry:  NewRegistry(),
		transport: newTCPTransport(config.MaxMessageSize),
		self: Node{
			ID:       config.NodeID,
			Address:  config.Address,
			Status:   StatusOnline,
			JoinedAt: now,
			LastSeen: now,
			Version:  1,
		},
		tombstones:  make(map[NodeID]uint64),
		viewVersion: 1,
		state:       stateNew,
		stoppedCh:   make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
		conns:       make(map[net.Conn]struct{}),
		gossipNow:   make(chan struct{}, 1),
	}, nil
}

// Start opens the cluster listener and launches heartbeat and gossip loops.
//
// Start returns once the listener is bound. It returns ErrAlreadyStarted on a
// second call and ErrClosed after Stop.
func (c *Cluster) Start() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	switch c.state {
	case stateRunning:
		return ErrAlreadyStarted
	case stateStopping, stateStopped:
		return ErrClosed
	}

	listener, err := net.Listen("tcp", c.config.Address)
	if err != nil {
		return fmt.Errorf("cluster: listen %s: %w", c.config.Address, err)
	}

	now := time.Now()
	self := c.self
	self.Address = listener.Addr().String()
	self.Status = StatusOnline
	self.JoinedAt = now
	self.LastSeen = now
	self.Version = 1

	c.mu.Lock()
	c.self = self
	c.viewVersion = 1
	if err := c.registry.AddNode(self); err != nil {
		c.mu.Unlock()
		_ = listener.Close()
		return fmt.Errorf("cluster: register self: %w", err)
	}
	c.mu.Unlock()

	c.listener = listener
	c.state = stateRunning

	c.acceptWG.Add(1)
	go c.acceptLoop()

	c.loopWG.Add(2)
	go c.heartbeatLoop()
	go c.gossipLoop()

	return nil
}

// Addr returns the bound cluster listener address, or nil before Start.
func (c *Cluster) Addr() net.Addr {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	if c.listener == nil {
		return nil
	}
	return c.listener.Addr()
}

// Self returns this node's current record.
func (c *Cluster) Self() Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.self
}

// Nodes returns a sorted snapshot of the membership view.
func (c *Cluster) Nodes() []Node {
	return c.registry.ListNodes()
}

// Node returns one node from the membership view.
func (c *Cluster) Node(id NodeID) (Node, bool) {
	return c.registry.Node(id)
}

// Metadata returns the cluster identity, node count, and local view version.
func (c *Cluster) Metadata() Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()

	return Metadata{
		ClusterID: c.config.ClusterID,
		NodeCount: c.registry.Len(),
		Version:   c.viewVersion,
	}
}

// Join contacts each seed, merges the membership list returned by any
// successful seed, and triggers an immediate gossip round.
//
// At least one seed must succeed when seeds are supplied. Blank and self
// addresses are ignored. Join may be called repeatedly to add seed peers.
func (c *Cluster) Join(seeds ...string) error {
	if c.leaving.Load() {
		return ErrClosed
	}
	if !c.running() {
		return ErrNotRunning
	}

	unique := make([]string, 0, len(seeds))
	seen := make(map[string]struct{}, len(seeds))
	selfAddress := c.Self().Address
	for _, seed := range seeds {
		seed = strings.TrimSpace(seed)
		if seed == "" || seed == selfAddress {
			continue
		}
		if _, exists := seen[seed]; exists {
			continue
		}
		seen[seed] = struct{}{}
		unique = append(unique, seed)
	}
	if len(unique) == 0 {
		return nil
	}

	var errs []error
	succeeded := 0
	for _, seed := range unique {
		response, err := c.roundTrip(seed, c.newMessage(messageJoin))
		if err != nil {
			errs = append(errs, fmt.Errorf("seed %s: %w", seed, err))
			continue
		}
		if response.Type == messageError {
			errs = append(errs, fmt.Errorf("seed %s: %s", seed, response.Error))
			continue
		}
		if response.Type != messageJoinAck {
			errs = append(errs, fmt.Errorf("seed %s: unexpected response %q", seed, response.Type))
			continue
		}
		if response.ClusterID != c.config.ClusterID {
			errs = append(errs, fmt.Errorf("seed %s: cluster id %q does not match %q",
				seed, response.ClusterID, c.config.ClusterID))
			continue
		}

		// The seed may raise this node's version above an old tombstone.
		c.adoptSelfVersion(response.Nodes)
		c.mergeView(response.Nodes, response.Left)
		succeeded++
	}

	if succeeded == 0 {
		return fmt.Errorf("cluster: join failed: %w", errors.Join(errs...))
	}

	c.triggerGossip()
	return nil
}

// Leave announces a graceful exit to known peers, then stops the node.
//
// Notification is best-effort; peers that miss the message mark this node
// OFFLINE through heartbeat timeouts.
func (c *Cluster) Leave() error {
	if !c.leaving.CompareAndSwap(false, true) {
		return c.Stop()
	}
	if !c.running() {
		return c.Stop()
	}

	peers := c.peerNodes()
	message := c.newMessage(messageLeave)
	var errs []error
	for _, peer := range peers {
		response, err := c.roundTrip(peer.Address, message)
		if err != nil {
			errs = append(errs, fmt.Errorf("peer %s: %w", peer.ID, err))
			continue
		}
		if response.Type == messageError {
			errs = append(errs, fmt.Errorf("peer %s: %s", peer.ID, response.Error))
		}
	}

	if err := c.Stop(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Stop stops gossip, closes the listener, and waits for active handlers.
//
// Stop is idempotent and safe to call concurrently. It does not announce a
// leave; use Leave for a graceful exit.
func (c *Cluster) Stop() error {
	c.stateMu.Lock()
	switch c.state {
	case stateStopped:
		done := c.stoppedCh
		c.stateMu.Unlock()
		<-done
		return c.shutdownResult()

	case stateStopping:
		done := c.stoppedCh
		c.stateMu.Unlock()
		<-done
		return c.shutdownResult()

	case stateNew:
		c.state = stateStopping
		c.cancel()
		c.stateMu.Unlock()
		c.finishStop(nil)
		return nil
	}

	c.state = stateStopping
	c.cancel()
	listener := c.listener
	c.stateMu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	c.acceptWG.Wait()

	done := make(chan struct{})
	go func() {
		c.loopWG.Wait()
		c.connWG.Wait()
		close(done)
	}()

	timer := time.NewTimer(c.config.ShutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
		c.finishStop(nil)
		return nil
	case <-timer.C:
		c.closeConnections()
		c.loopWG.Wait()
		c.connWG.Wait()
		c.finishStop(ErrShutdownTimeout)
		return ErrShutdownTimeout
	}
}

// running reports whether the cluster is accepting messages.
func (c *Cluster) running() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.state == stateRunning
}

// shutdownResult returns the result recorded by the shutdown goroutine.
func (c *Cluster) shutdownResult() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.stopErr
}

// finishStop records the shutdown result and wakes concurrent Stop callers.
func (c *Cluster) finishStop(err error) {
	c.stateMu.Lock()
	c.stopErr = err
	c.state = stateStopped
	close(c.stoppedCh)
	c.stateMu.Unlock()
}

// peerNodes returns all known nodes except self, sorted by ID.
func (c *Cluster) peerNodes() []Node {
	nodes := c.registry.ListNodes()
	peers := nodes[:0]
	for _, node := range nodes {
		if node.ID != c.config.NodeID {
			peers = append(peers, node)
		}
	}
	return peers
}
