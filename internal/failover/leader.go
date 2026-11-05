// Package failover adds Raft-style leader election, failure detection, and
// automatic promotion of replication replicas.
//
// A Node owns one cache store and its replication WAL. Exactly one node in a
// majority partition may be leader. Leaders expose the replication primary,
// heartbeat followers, and wait for a replication quorum before reporting a
// write as successful.
package failover

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/replication"
)

// Lifecycle and protocol errors.
var (
	ErrInvalidConfig   = errors.New("failover: invalid config")
	ErrInvalidTarget   = errors.New("failover: invalid leadership target")
	ErrAlreadyStarted  = errors.New("failover: already started")
	ErrClosed          = errors.New("failover: closed")
	ErrNotLeader       = errors.New("failover: node is not the leader")
	ErrNoQuorum        = errors.New("failover: no quorum")
	ErrNotReady        = errors.New("failover: replication endpoint is not ready")
	ErrTransferring    = errors.New("failover: leadership transfer in progress")
	ErrUnknownPeer     = errors.New("failover: unknown peer")
	ErrTransferTimeout = errors.New("failover: leadership transfer timed out")
	ErrShutdownTimeout = errors.New("failover: shutdown timeout")
)

// Config controls one failover node and its replication endpoint.
//
// Zero values are replaced by DefaultConfig values in New.
type Config struct {
	// ClusterID must match across all nodes in one failover cluster.
	ClusterID string

	// ID uniquely identifies this node.
	ID string

	// ListenAddr is the TCP control-plane listen address. Peers dial the
	// address configured for this node in their Peer list.
	ListenAddr string

	// Peers lists every other node in the cluster. Membership is static.
	Peers []Peer

	// Store is the cache replicated by this node.
	Store replication.Store

	// WALPath is the replication write-ahead log. It must be private to this
	// node and must not be shared with another running Node.
	WALPath string

	// StatePath stores the durable election term and vote. It defaults to
	// WALPath + ".failover".
	StatePath string

	// ReplicationListenAddr is the address used when this node becomes the
	// replication primary. The actual bound address is advertised to peers, so
	// "127.0.0.1:0" is valid.
	ReplicationListenAddr string

	// ReplicationConfig supplies replication tuning. Role, ID, Store, WALPath,
	// ListenAddr, and PrimaryAddr are always set by the failover node.
	ReplicationConfig replication.Config

	// ElectionTimeoutMin and ElectionTimeoutMax bound the randomized follower
	// timeout before a new election starts.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is how often a leader sends AppendEntries.
	HeartbeatInterval time.Duration

	// FailureTimeout is when the local detector marks a silent peer DEAD.
	FailureTimeout time.Duration

	// RPCTimeout bounds one control RPC.
	RPCTimeout time.Duration

	// WriteTimeout bounds waiting for the replication quorum and for the
	// primary endpoint to become ready.
	WriteTimeout time.Duration

	// TransferTimeout bounds a graceful leadership handoff.
	TransferTimeout time.Duration

	// ReplicationRetry is the delay before retrying a failed replication
	// endpoint transition.
	ReplicationRetry time.Duration

	// ShutdownTimeout is the grace period before shutdown forces control
	// connections closed. Stop still waits for background goroutines to exit.
	ShutdownTimeout time.Duration

	// MaxMessageSize is the largest encoded control message.
	MaxMessageSize int
}

// DefaultConfig returns settings suitable for a small local cluster.
func DefaultConfig() Config {
	replicationConfig := replication.DefaultConfig()
	return Config{
		ListenAddr:            "127.0.0.1:0",
		ReplicationListenAddr: "127.0.0.1:0",
		ReplicationConfig:     replicationConfig,
		ElectionTimeoutMin:    300 * time.Millisecond,
		ElectionTimeoutMax:    600 * time.Millisecond,
		HeartbeatInterval:     100 * time.Millisecond,
		FailureTimeout:        2 * time.Second,
		RPCTimeout:            0, // Derived from the election timeout.
		WriteTimeout:          2 * time.Second,
		TransferTimeout:       3 * time.Second,
		ReplicationRetry:      200 * time.Millisecond,
		ShutdownTimeout:       5 * time.Second,
		MaxMessageSize:        1 << 20, // 1 MiB.
	}
}

// withDefaults validates required fields and fills timing defaults.
func (c Config) withDefaults() (Config, error) {
	defaults := DefaultConfig()

	if c.ClusterID == "" {
		return Config{}, fmt.Errorf("%w: cluster id must not be empty", ErrInvalidConfig)
	}
	if c.ID == "" {
		return Config{}, fmt.Errorf("%w: id must not be empty", ErrInvalidConfig)
	}
	if c.Store == nil {
		return Config{}, fmt.Errorf("%w: store must not be nil", ErrInvalidConfig)
	}
	if c.WALPath == "" {
		return Config{}, fmt.Errorf("%w: wal path must not be empty", ErrInvalidConfig)
	}

	seen := make(map[string]struct{}, len(c.Peers))
	for _, peer := range c.Peers {
		if peer.ID == "" {
			return Config{}, fmt.Errorf("%w: peer id must not be empty", ErrInvalidConfig)
		}
		if peer.ID == c.ID {
			return Config{}, fmt.Errorf("%w: peer list must not contain self", ErrInvalidConfig)
		}
		if peer.Address == "" {
			return Config{}, fmt.Errorf("%w: peer %q address must not be empty", ErrInvalidConfig, peer.ID)
		}
		if _, duplicate := seen[peer.ID]; duplicate {
			return Config{}, fmt.Errorf("%w: duplicate peer %q", ErrInvalidConfig, peer.ID)
		}
		seen[peer.ID] = struct{}{}
	}
	c.Peers = append([]Peer(nil), c.Peers...)

	if c.ListenAddr == "" {
		c.ListenAddr = defaults.ListenAddr
	}
	if c.ReplicationListenAddr == "" {
		c.ReplicationListenAddr = defaults.ReplicationListenAddr
	}
	if c.StatePath == "" {
		c.StatePath = c.WALPath + ".failover"
	}
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = defaults.ElectionTimeoutMin
	}
	if c.ElectionTimeoutMax <= 0 {
		c.ElectionTimeoutMax = maxDuration(defaults.ElectionTimeoutMax, 2*c.ElectionTimeoutMin)
	}
	if c.ElectionTimeoutMax < c.ElectionTimeoutMin {
		return Config{}, fmt.Errorf("%w: election timeout max must be at least min", ErrInvalidConfig)
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaults.HeartbeatInterval
	}
	// Three heartbeat intervals leave enough room for a randomized election
	// after the leader's half-timeout lease expires.
	if c.ElectionTimeoutMin < 3*c.HeartbeatInterval {
		return Config{}, fmt.Errorf("%w: election timeout must be at least three heartbeat intervals", ErrInvalidConfig)
	}
	if c.FailureTimeout <= 0 {
		c.FailureTimeout = maxDuration(3*c.ElectionTimeoutMax, defaults.FailureTimeout)
	}
	if c.RPCTimeout <= 0 {
		// Leave room for a vote or heartbeat response well inside the
		// election window; heartbeat cadence must not shrink RPC deadlines,
		// or an overloaded follower can cause spurious leader stepdowns.
		c.RPCTimeout = maxDuration(c.ElectionTimeoutMin/3, time.Millisecond)
	}
	if c.RPCTimeout >= c.ElectionTimeoutMin/2 {
		return Config{}, fmt.Errorf("%w: rpc timeout must be shorter than half the election timeout", ErrInvalidConfig)
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaults.WriteTimeout
	}
	if c.TransferTimeout <= 0 {
		c.TransferTimeout = maxDuration(3*c.ElectionTimeoutMax, defaults.TransferTimeout)
	}
	if c.ReplicationRetry <= 0 {
		c.ReplicationRetry = defaults.ReplicationRetry
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaults.ShutdownTimeout
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = defaults.MaxMessageSize
	}
	if c.MaxMessageSize < 1024 {
		return Config{}, fmt.Errorf("%w: max message size must be at least 1024 bytes", ErrInvalidConfig)
	}

	return c, nil
}

// Stats is a point-in-time failover and replication snapshot.
type Stats struct {
	ID              string
	Role            Role
	Term            uint64
	LeaderID        string
	ReplicationAddr string
	Seq             uint64
	ClusterSize     int
	Quorum          int
	HasQuorum       bool
	Transferring    bool
	LastQuorum      time.Time
	Replication     replication.Stats
}

// lifecycleState is the Node lifecycle position.
type lifecycleState uint8

const (
	stateNew lifecycleState = iota
	stateRunning
	stateStopping
	stateStopped
)

// replicationMode is the desired replication coordinator configuration.
type replicationMode struct {
	role        replication.Role
	primaryAddr string
	listenAddr  string
}

// Node is one cache node in a failover cluster.
type Node struct {
	config   Config
	detector *detector

	stateMu        sync.Mutex
	role           Role
	term           uint64
	votedFor       string
	leaderID       string
	leaderReplAddr string
	lastQuorum     time.Time
	transferring   bool

	writeMu    sync.Mutex
	transferMu sync.Mutex
	campaignMu sync.Mutex

	coordMu       sync.Mutex
	active        *replication.Coordinator
	activeMode    replicationMode
	activeListen  string
	sequenceCache atomic.Uint64

	lifecycleMu sync.Mutex
	state       lifecycleState
	stoppedCh   chan struct{}
	stopErr     error

	ctx    context.Context
	cancel context.CancelFunc

	listener net.Listener
	wg       sync.WaitGroup

	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	electionReset chan struct{}
	replWake      chan struct{}
	heartbeatNow  chan struct{}

	errMu   sync.Mutex
	lastErr error
}

// New validates config, restores durable election state, and reads the WAL
// sequence used for Raft vote comparisons.
//
// No socket or background goroutine is created until Start. The store is not
// rebuilt from the WAL here; the first replication coordinator to run does
// that replay.
func New(config Config) (*Node, error) {
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}

	state, err := loadElectionState(config.StatePath)
	if err != nil {
		return nil, err
	}
	if err := saveElectionState(config.StatePath, state); err != nil {
		return nil, err
	}

	wal, err := replication.OpenWAL(config.WALPath)
	if err != nil {
		return nil, err
	}
	seq := wal.LastSeq()
	if err := wal.Close(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{
		config:        config,
		detector:      newDetector(config.Peers, config.FailureTimeout),
		role:          RoleFollower,
		term:          state.Term,
		votedFor:      state.VotedFor,
		stoppedCh:     make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
		conns:         make(map[net.Conn]struct{}),
		electionReset: make(chan struct{}, 1),
		replWake:      make(chan struct{}, 1),
		heartbeatNow:  make(chan struct{}, 1),
	}
	node.sequenceCache.Store(seq)
	return node, nil
}

// Start opens the control listener and launches election, heartbeat, and
// replication-management loops.
func (n *Node) Start() error {
	n.lifecycleMu.Lock()
	switch n.state {
	case stateRunning:
		n.lifecycleMu.Unlock()
		return ErrAlreadyStarted
	case stateStopping, stateStopped:
		n.lifecycleMu.Unlock()
		return ErrClosed
	}

	listener, err := net.Listen("tcp", n.config.ListenAddr)
	if err != nil {
		n.lifecycleMu.Unlock()
		return fmt.Errorf("failover: listen %s: %w", n.config.ListenAddr, err)
	}

	n.listener = listener
	n.state = stateRunning
	n.wg.Add(4)
	go n.acceptLoop()
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.replicationLoop()
	n.lifecycleMu.Unlock()
	return nil
}

// Stop stops the control plane, replication coordinator, and background loops.
//
// Stop is idempotent and safe to call concurrently. It deliberately does not
// transfer leadership; use TransferLeadership for a graceful handoff.
func (n *Node) Stop() error {
	n.lifecycleMu.Lock()
	switch n.state {
	case stateStopped, stateStopping:
		done := n.stoppedCh
		n.lifecycleMu.Unlock()
		<-done
		return n.shutdownResult()

	case stateNew:
		n.state = stateStopping
		n.cancel()
		n.lifecycleMu.Unlock()
		n.stopActiveReplication()
		n.finishStop(nil)
		return nil

	case stateRunning:
		n.state = stateStopping
		n.cancel()
		listener := n.listener
		n.lifecycleMu.Unlock()

		if listener != nil {
			_ = listener.Close()
		}
		n.closeConnections()

		done := make(chan struct{})
		go func() {
			n.wg.Wait()
			close(done)
		}()

		timer := time.NewTimer(n.config.ShutdownTimeout)
		defer timer.Stop()

		select {
		case <-done:
			n.finishStop(nil)
			return nil
		case <-timer.C:
			n.closeConnections()
			n.wg.Wait()
			err := ErrShutdownTimeout
			n.finishStop(err)
			return err
		}
	}

	n.lifecycleMu.Unlock()
	return nil
}

// Addr returns the bound control-plane address, or nil before Start.
func (n *Node) Addr() net.Addr {
	n.lifecycleMu.Lock()
	defer n.lifecycleMu.Unlock()
	if n.listener == nil {
		return nil
	}
	return n.listener.Addr()
}

// ID returns this node's configured identity.
func (n *Node) ID() string {
	return n.config.ID
}

// Store returns the local cache store.
func (n *Node) Store() replication.Store {
	return n.config.Store
}

// Role returns the current election state.
func (n *Node) Role() Role {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	return n.role
}

// Term returns the current election term.
func (n *Node) Term() uint64 {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	return n.term
}

// LeaderID returns the leader this node currently follows, or itself when it
// is the leader.
func (n *Node) LeaderID() string {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	return n.leaderID
}

// ReplicationAddr returns the active primary replication address, if any.
func (n *Node) ReplicationAddr() string {
	return n.replicationAddr()
}

// Stats returns a point-in-time failover and replication snapshot.
func (n *Node) Stats() Stats {
	n.stateMu.Lock()
	stats := Stats{
		ID:           n.config.ID,
		Role:         n.role,
		Term:         n.term,
		LeaderID:     n.leaderID,
		Transferring: n.transferring,
		LastQuorum:   n.lastQuorum,
	}
	lastQuorum := n.lastQuorum
	n.stateMu.Unlock()

	stats.ClusterSize = len(n.config.Peers) + 1
	stats.Quorum = n.quorum()
	stats.Seq = n.currentSeq()
	stats.ReplicationAddr = n.replicationAddr()
	if coord := n.activeCoordinator(); coord != nil {
		stats.Replication = coord.Stats()
	}
	if stats.Role == RoleLeader && !lastQuorum.IsZero() &&
		time.Since(lastQuorum) < n.leaseDuration() {
		stats.HasQuorum = true
	}
	return stats
}

// Health returns the local detector's view of every node.
func (n *Node) Health() []PeerHealth {
	now := time.Now()
	health := n.detector.snapshot()

	self := PeerHealth{
		ID:       n.config.ID,
		Status:   PeerDead,
		LastSeq:  n.currentSeq(),
		LastSeen: now,
	}
	if addr := n.Addr(); addr != nil {
		self.Address = addr.String()
	}
	if n.running() {
		self.Status = PeerAlive
	}
	health = append(health, self)

	sort.Slice(health, func(i, j int) bool {
		return health[i].ID < health[j].ID
	})
	return health
}

// LastError returns the most recent background error.
func (n *Node) LastError() error {
	n.errMu.Lock()
	defer n.errMu.Unlock()
	return n.lastErr
}

// Get reads a key from the local store.
func (n *Node) Get(key string) (any, bool) {
	return n.config.Store.Get(key)
}

// Set stores a value on the leader and waits for a replication quorum.
func (n *Node) Set(key string, value any) error {
	return n.writeWithQuorum(func(coord *replication.Coordinator) error {
		return coord.Set(key, value)
	})
}

// SetWithTTL stores a value with an expiration and waits for a replication
// quorum.
func (n *Node) SetWithTTL(key string, value any, ttl time.Duration) error {
	if ttl <= 0 {
		return cache.ErrInvalidTTL
	}
	return n.writeWithQuorum(func(coord *replication.Coordinator) error {
		return coord.SetWithTTL(key, value, ttl)
	})
}

// Expire updates a key's TTL on the leader and waits for a replication quorum.
func (n *Node) Expire(key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cache.ErrInvalidTTL
	}

	var expired bool
	err := n.writeWithQuorum(func(coord *replication.Coordinator) error {
		var err error
		expired, err = coord.Expire(key, ttl)
		return err
	})
	return expired, err
}

// Delete removes a key on the leader and waits for a replication quorum.
func (n *Node) Delete(key string) error {
	return n.writeWithQuorum(func(coord *replication.Coordinator) error {
		return coord.Delete(key)
	})
}

// TransferLeadership gracefully hands leadership to a caught-up peer.
func (n *Node) TransferLeadership(targetID string) error {
	if targetID == n.config.ID {
		return ErrInvalidTarget
	}
	target, ok := n.peerByID(targetID)
	if !ok {
		return ErrUnknownPeer
	}

	n.transferMu.Lock()
	defer n.transferMu.Unlock()

	n.stateMu.Lock()
	if n.role != RoleLeader {
		n.stateMu.Unlock()
		return ErrNotLeader
	}
	if n.transferring {
		n.stateMu.Unlock()
		return ErrTransferring
	}
	if time.Since(n.lastQuorum) >= n.leaseDuration() {
		n.stateMu.Unlock()
		n.stepDown(n.currentTerm())
		return ErrNoQuorum
	}
	term := n.term
	n.transferring = true
	n.stateMu.Unlock()

	defer n.clearTransfer()

	deadline := time.Now().Add(n.config.TransferTimeout)
	for n.detector.lastSeq(targetID) < n.currentSeq() {
		if time.Now().After(deadline) {
			return ErrTransferTimeout
		}
		if !n.leaderLeaseValid(term) {
			return ErrNoQuorum
		}
		if !n.sleep(10 * time.Millisecond) {
			return ErrClosed
		}
	}

	response, err := n.callPeer(target, wireMessage{
		Type:     messageTimeoutNow,
		Term:     term,
		LeaderID: n.config.ID,
	})
	if err != nil {
		return err
	}
	if response.Term > term {
		n.stepDown(response.Term)
		return ErrNotLeader
	}

	for {
		n.stateMu.Lock()
		role := n.role
		leader := n.leaderID
		n.stateMu.Unlock()

		if role != RoleLeader {
			switch {
			case leader == targetID:
				return nil
			case leader != "":
				// A different node won the election; the handoff failed.
				return ErrTransferTimeout
			}
			// The target's higher term already forced this node to step
			// down, but its first heartbeat has not arrived yet. Keep
			// waiting so the caller observes a completed handoff.
		}
		if time.Now().After(deadline) {
			return ErrTransferTimeout
		}
		if !n.sleep(10 * time.Millisecond) {
			return ErrClosed
		}
	}
}

// electionLoop starts a campaign after a randomized follower timeout.
func (n *Node) electionLoop() {
	defer n.wg.Done()

	timer := time.NewTimer(randomElectionTimeout(n.config.ElectionTimeoutMin, n.config.ElectionTimeoutMax))
	defer timer.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.electionReset:
			resetTimer(timer, randomElectionTimeout(n.config.ElectionTimeoutMin, n.config.ElectionTimeoutMax))
		case <-timer.C:
			if n.Role() != RoleLeader {
				n.campaign()
			}
			timer.Reset(randomElectionTimeout(n.config.ElectionTimeoutMin, n.config.ElectionTimeoutMax))
		}
	}
}

// heartbeatLoop sends leader heartbeats and enforces the quorum lease.
func (n *Node) heartbeatLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(n.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.sendHeartbeats()
		case <-n.heartbeatNow:
			n.sendHeartbeats()
		}
	}
}

// sendHeartbeats performs one AppendEntries round and renews the leader lease
// only when a majority responds.
func (n *Node) sendHeartbeats() {
	n.stateMu.Lock()
	if n.role != RoleLeader {
		n.stateMu.Unlock()
		return
	}
	term := n.term
	if time.Since(n.lastQuorum) >= n.leaseDuration() {
		n.stateMu.Unlock()
		n.stepDown(term)
		return
	}
	n.stateMu.Unlock()

	message := wireMessage{
		Type:            messageAppendEntries,
		Term:            term,
		LeaderID:        n.config.ID,
		ReplicationAddr: n.replicationAddr(),
		LastSeq:         n.currentSeq(),
	}

	type heartbeatResult struct {
		peer     Peer
		response wireMessage
		err      error
	}
	peers := n.config.Peers
	results := make(chan heartbeatResult, len(peers))
	for _, peer := range peers {
		go func(peer Peer) {
			response, err := n.callPeer(peer, message)
			results <- heartbeatResult{peer: peer, response: response, err: err}
		}(peer)
	}

	successes := 1 // The leader counts itself.
	var higherTerm uint64
	for range peers {
		result := <-results
		if result.err != nil {
			n.detector.observe(result.peer.ID, 0, result.err)
			continue
		}
		n.detector.observe(result.peer.ID, result.response.LastSeq, nil)
		if result.response.Term > term {
			if result.response.Term > higherTerm {
				higherTerm = result.response.Term
			}
			continue
		}
		if result.response.Success && result.response.Term == term {
			successes++
			if successes >= n.quorum() {
				// Renew as soon as the quorum replies rather than waiting for
				// a slow or failed peer to finish the round.
				n.markQuorum(term)
			}
		}
	}

	if higherTerm > term {
		n.stepDown(higherTerm)
		return
	}
	if successes >= n.quorum() {
		n.markQuorum(term)
	}
}

// replicationLoop reconciles the active coordinator with the current role and
// known leader. Promotion and demotion happen here, off the election paths.
func (n *Node) replicationLoop() {
	defer n.wg.Done()

	for {
		if n.ctx.Err() != nil {
			n.stopActiveReplication()
			return
		}

		desired := n.desiredReplication()
		if n.replicationMatches(desired) {
			select {
			case <-n.ctx.Done():
				n.stopActiveReplication()
				return
			case <-n.replWake:
			}
			continue
		}

		n.stopActiveReplication()
		if n.ctx.Err() != nil {
			return
		}
		if desired.role == "" {
			continue
		}

		if err := n.startReplication(desired); err != nil {
			n.recordError(err)
			if desired.role == replication.RolePrimary {
				// A leader that cannot expose a primary endpoint cannot make
				// quorum-durable progress. Let another node try.
				n.stepDown(n.currentTerm())
			}
			timer := time.NewTimer(n.config.ReplicationRetry)
			select {
			case <-n.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			case <-n.replWake:
				timer.Stop()
			}
		}
	}
}

// desiredReplication derives the coordinator mode from election state.
func (n *Node) desiredReplication() replicationMode {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	if n.role == RoleLeader {
		return replicationMode{
			role:       replication.RolePrimary,
			listenAddr: n.config.ReplicationListenAddr,
		}
	}
	if n.leaderID != "" && n.leaderID != n.config.ID && n.leaderReplAddr != "" {
		return replicationMode{
			role:        replication.RoleReplica,
			primaryAddr: n.leaderReplAddr,
		}
	}
	return replicationMode{}
}

// replicationMatches reports whether the active coordinator already implements
// mode.
func (n *Node) replicationMatches(mode replicationMode) bool {
	n.coordMu.Lock()
	defer n.coordMu.Unlock()

	if mode.role == "" {
		return n.active == nil
	}
	return n.active != nil && n.activeMode == mode
}

// startReplication creates and starts one coordinator.
func (n *Node) startReplication(mode replicationMode) error {
	config := n.config.ReplicationConfig
	config.Role = mode.role
	config.ID = n.config.ID
	config.Store = n.config.Store
	config.WALPath = n.config.WALPath
	config.PrimaryAddr = mode.primaryAddr
	config.ForceFullSync = mode.role == replication.RoleReplica
	if mode.role == replication.RolePrimary {
		config.ListenAddr = mode.listenAddr
	} else {
		config.ListenAddr = ""
	}

	coordinator, err := replication.New(config)
	if err != nil {
		return err
	}
	if err := coordinator.Start(); err != nil {
		_ = coordinator.Stop()
		return err
	}
	if n.ctx.Err() != nil {
		_ = coordinator.Stop()
		return n.ctx.Err()
	}

	seq := coordinator.Stats().Seq
	listenAddr := ""
	if mode.role == replication.RolePrimary {
		listenAddr = coordinator.Addr().String()
	}

	n.coordMu.Lock()
	n.active = coordinator
	n.activeMode = mode
	n.activeListen = listenAddr
	n.coordMu.Unlock()
	n.sequenceCache.Store(seq)
	return nil
}

// stopActiveReplication stops and clears the active coordinator.
func (n *Node) stopActiveReplication() {
	n.coordMu.Lock()
	coordinator := n.active
	if coordinator != nil {
		n.sequenceCache.Store(coordinator.Stats().Seq)
	}
	n.active = nil
	n.activeMode = replicationMode{}
	n.activeListen = ""
	n.coordMu.Unlock()

	if coordinator != nil {
		_ = coordinator.Stop()
	}
}

// activeCoordinator returns the current coordinator without holding coordMu.
func (n *Node) activeCoordinator() *replication.Coordinator {
	n.coordMu.Lock()
	defer n.coordMu.Unlock()
	return n.active
}

// primaryCoordinator returns the active coordinator only when it is a primary.
func (n *Node) primaryCoordinator() *replication.Coordinator {
	n.coordMu.Lock()
	defer n.coordMu.Unlock()

	if n.active != nil && n.activeMode.role == replication.RolePrimary {
		return n.active
	}
	return nil
}

// replicationAddr returns the active primary listener address.
func (n *Node) replicationAddr() string {
	n.coordMu.Lock()
	defer n.coordMu.Unlock()

	if n.activeMode.role == replication.RolePrimary {
		return n.activeListen
	}
	return ""
}

// currentSeq returns the highest durable sequence known to this node.
func (n *Node) currentSeq() uint64 {
	if coordinator := n.activeCoordinator(); coordinator != nil {
		seq := coordinator.Stats().Seq
		n.sequenceCache.Store(seq)
		return seq
	}
	return n.sequenceCache.Load()
}

// writeWithQuorum serializes a leader write and waits for replica durability.
func (n *Node) writeWithQuorum(write func(*replication.Coordinator) error) error {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()

	term, coordinator, err := n.prepareWrite()
	if err != nil {
		return err
	}

	before := coordinator.Stats().Seq
	if err := write(coordinator); err != nil {
		if errors.Is(err, replication.ErrNotPrimary) || errors.Is(err, replication.ErrClosed) {
			return ErrNotLeader
		}
		return err
	}
	after := coordinator.Stats().Seq
	if after == before {
		return nil
	}

	required := n.quorum() - 1
	if required > 0 {
		if err := coordinator.WaitForAck(after, required, n.config.WriteTimeout); err != nil {
			n.recordError(fmt.Errorf("failover: quorum write through %d: %w", after, err))
			n.stepDown(term)
			return fmt.Errorf("%w: %v", ErrNoQuorum, err)
		}
	}
	if !n.isLeaderTerm(term) {
		return ErrNotLeader
	}

	n.sequenceCache.Store(after)
	return nil
}

// prepareWrite verifies leadership and waits for the primary endpoint.
func (n *Node) prepareWrite() (uint64, *replication.Coordinator, error) {
	deadline := time.Now().Add(n.config.WriteTimeout)

	for {
		if n.ctx.Err() != nil {
			return 0, nil, ErrClosed
		}

		n.stateMu.Lock()
		if n.role != RoleLeader {
			n.stateMu.Unlock()
			return 0, nil, ErrNotLeader
		}
		if n.transferring {
			n.stateMu.Unlock()
			return 0, nil, ErrTransferring
		}
		term := n.term
		if time.Since(n.lastQuorum) >= n.leaseDuration() {
			n.stateMu.Unlock()
			n.stepDown(term)
			return 0, nil, fmt.Errorf("%w: leader lease expired", ErrNoQuorum)
		}
		n.stateMu.Unlock()

		if coordinator := n.primaryCoordinator(); coordinator != nil {
			return term, coordinator, nil
		}
		if time.Now().After(deadline) {
			return 0, nil, ErrNotReady
		}
		if !n.sleep(10 * time.Millisecond) {
			return 0, nil, ErrClosed
		}
	}
}

// stepDown moves to follower, optionally adopting a newer term.
func (n *Node) stepDown(term uint64) {
	n.stateMu.Lock()
	if term < n.term {
		n.stateMu.Unlock()
		return
	}
	if term > n.term {
		if err := n.persistStateLocked(term, ""); err != nil {
			n.recordError(err)
		}
		n.term = term
		n.votedFor = ""
	}

	changed := n.role != RoleFollower || n.leaderID != ""
	n.role = RoleFollower
	n.leaderID = ""
	n.leaderReplAddr = ""
	n.stateMu.Unlock()

	n.resetElection()
	if changed {
		n.signalReplication()
	}
}

// markQuorum renews the leader lease if term is still current.
func (n *Node) markQuorum(term uint64) {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	if n.role == RoleLeader && n.term == term {
		n.lastQuorum = time.Now()
	}
}

// leaderLeaseValid reports whether this node is still leader for term with a
// live quorum lease.
func (n *Node) leaderLeaseValid(term uint64) bool {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	return n.role == RoleLeader && n.term == term &&
		time.Since(n.lastQuorum) < n.leaseDuration()
}

// isLeaderTerm reports whether this node is still leader for term.
func (n *Node) isLeaderTerm(term uint64) bool {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	return n.role == RoleLeader && n.term == term
}

// clearTransfer ends a handoff attempt. It runs when TransferLeadership
// returns, including failures, so normal elections can resume.
func (n *Node) clearTransfer() {
	n.stateMu.Lock()
	n.transferring = false
	n.stateMu.Unlock()
}

// leaseDuration is the maximum time a leader may act without a quorum. It is
// shorter than the minimum election timeout so a partitioned leader stops
// accepting writes before followers can elect a replacement.
func (n *Node) leaseDuration() time.Duration {
	return n.config.ElectionTimeoutMin / 2
}

// currentTerm returns the current term.
func (n *Node) currentTerm() uint64 {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	return n.term
}

// persistStateLocked writes election state. Caller holds stateMu.
func (n *Node) persistStateLocked(term uint64, votedFor string) error {
	return saveElectionState(n.config.StatePath, persistentState{
		Term:     term,
		VotedFor: votedFor,
	})
}

// peerByID returns a configured peer.
func (n *Node) peerByID(id string) (Peer, bool) {
	for _, peer := range n.config.Peers {
		if peer.ID == id {
			return peer, true
		}
	}
	return Peer{}, false
}

// resetElection schedules the next randomized election attempt.
func (n *Node) resetElection() {
	select {
	case n.electionReset <- struct{}{}:
	default:
	}
}

// signalReplication asks the manager to reconcile the replication mode.
func (n *Node) signalReplication() {
	select {
	case n.replWake <- struct{}{}:
	default:
	}
}

// signalHeartbeat asks the leader loop to send an immediate heartbeat.
func (n *Node) signalHeartbeat() {
	select {
	case n.heartbeatNow <- struct{}{}:
	default:
	}
}

// sleep waits for d or cancellation.
func (n *Node) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-n.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// running reports whether the node is serving.
func (n *Node) running() bool {
	n.lifecycleMu.Lock()
	defer n.lifecycleMu.Unlock()
	return n.state == stateRunning
}

// recordError stores the most recent background error.
func (n *Node) recordError(err error) {
	if err == nil {
		return
	}
	n.errMu.Lock()
	n.lastErr = err
	n.errMu.Unlock()
}

// registerConn tracks a connection so Stop can force-close it.
func (n *Node) registerConn(conn net.Conn) {
	n.connMu.Lock()
	n.conns[conn] = struct{}{}
	n.connMu.Unlock()
}

// unregisterConn removes a tracked connection.
func (n *Node) unregisterConn(conn net.Conn) {
	n.connMu.Lock()
	delete(n.conns, conn)
	n.connMu.Unlock()
}

// closeConnections force-closes every active control connection.
func (n *Node) closeConnections() {
	n.connMu.Lock()
	for conn := range n.conns {
		_ = conn.Close()
	}
	n.connMu.Unlock()
}

// shutdownResult returns the result recorded by the stopping goroutine.
func (n *Node) shutdownResult() error {
	n.lifecycleMu.Lock()
	defer n.lifecycleMu.Unlock()
	return n.stopErr
}

// finishStop records the shutdown result and wakes concurrent Stop callers.
func (n *Node) finishStop(err error) {
	n.lifecycleMu.Lock()
	n.stopErr = err
	n.state = stateStopped
	close(n.stoppedCh)
	n.lifecycleMu.Unlock()
}

// resetTimer safely resets an active timer.
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

// maxDuration returns the larger duration.
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
