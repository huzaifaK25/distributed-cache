package replication

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
)

// Lifecycle and configuration errors.
var (
	ErrInvalidConfig   = errors.New("replication: invalid config")
	ErrNotRunning      = errors.New("replication: not running")
	ErrAlreadyStarted  = errors.New("replication: already started")
	ErrClosed          = errors.New("replication: closed")
	ErrNotPrimary      = errors.New("replication: node is not a primary")
	ErrShutdownTimeout = errors.New("replication: shutdown timeout")
	ErrAckTimeout      = errors.New("replication: acknowledgement timeout")
	ErrSessionClosed   = errors.New("replication: replica session closed")
	ErrQueueFull       = errors.New("replication: replica queue full")
)

// Config controls one node's replication role and transport behavior.
//
// Zero values are replaced by DefaultConfig values in New.
type Config struct {
	// Role selects primary (writer) or replica (follower) behavior.
	Role Role

	// ID identifies this node to its primary. It must be stable and unique.
	ID string

	// Store is the cache to read and write. It must be non-nil.
	Store Store

	// WALPath is the durable write-ahead log file. It is required for both
	// roles so a restart can recover acknowledged state.
	WALPath string

	// ListenAddr is the primary's replication listener. Replicas ignore it.
	ListenAddr string

	// PrimaryAddr is the replica's upstream address. Primaries ignore it.
	PrimaryAddr string

	// ReconnectMin and ReconnectMax bound the replica's reconnect backoff.
	ReconnectMin time.Duration
	ReconnectMax time.Duration

	// HandshakeTimeout bounds dialing the primary and sending hello.
	HandshakeTimeout time.Duration

	// AckTimeout bounds one frame write and one batch acknowledgement wait.
	AckTimeout time.Duration

	// MaxRetries is the number of attempts for a batch or full snapshot.
	MaxRetries int

	// MaxBatch is the largest number of mutations sent per append message.
	MaxBatch int

	// SessionQueue is the per-replica buffer for live mutations.
	SessionQueue int

	// SnapshotBatch is the maximum entry count in one snapshot chunk.
	SnapshotBatch int

	// MaxMessageSize is the maximum encoded frame size in bytes.
	MaxMessageSize int

	// ShutdownTimeout bounds waiting for replication goroutines to exit.
	ShutdownTimeout time.Duration

	// ForceFullSync makes a replica request a full snapshot on its first
	// connection instead of an incremental delta. Failover sets this when a
	// new replica coordinator starts so a former leader's diverging WAL
	// suffix cannot survive a leadership change.
	ForceFullSync bool
}

// DefaultConfig returns replication defaults suitable for a small cluster.
func DefaultConfig() Config {
	return Config{
		ListenAddr:       "127.0.0.1:0",
		ReconnectMin:     100 * time.Millisecond,
		ReconnectMax:     2 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		AckTimeout:       5 * time.Second,
		MaxRetries:       3,
		MaxBatch:         64,
		SessionQueue:     4096,
		SnapshotBatch:    256,
		MaxMessageSize:   8 << 20, // 8 MiB.
		ShutdownTimeout:  5 * time.Second,
	}
}

// withDefaults validates required fields and fills timing defaults.
func (c Config) withDefaults() (Config, error) {
	defaults := DefaultConfig()

	if c.Role != RolePrimary && c.Role != RoleReplica {
		return Config{}, fmt.Errorf("%w: role must be %q or %q", ErrInvalidConfig, RolePrimary, RoleReplica)
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
	if c.Role == RoleReplica && c.PrimaryAddr == "" {
		return Config{}, fmt.Errorf("%w: replica requires a primary address", ErrInvalidConfig)
	}

	if c.Role == RolePrimary && c.ListenAddr == "" {
		c.ListenAddr = defaults.ListenAddr
	}
	if c.ReconnectMin <= 0 {
		c.ReconnectMin = defaults.ReconnectMin
	}
	if c.ReconnectMax <= 0 {
		c.ReconnectMax = defaults.ReconnectMax
	}
	if c.ReconnectMax < c.ReconnectMin {
		c.ReconnectMax = c.ReconnectMin
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = defaults.HandshakeTimeout
	}
	if c.AckTimeout <= 0 {
		c.AckTimeout = defaults.AckTimeout
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaults.MaxRetries
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = defaults.MaxBatch
	}
	if c.SessionQueue <= 0 {
		c.SessionQueue = defaults.SessionQueue
	}
	if c.SnapshotBatch <= 0 {
		c.SnapshotBatch = defaults.SnapshotBatch
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = defaults.MaxMessageSize
	}
	if c.MaxMessageSize < 1024 {
		return Config{}, fmt.Errorf("%w: max message size must be at least 1024 bytes", ErrInvalidConfig)
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaults.ShutdownTimeout
	}

	return c, nil
}

// Stats is a cumulative snapshot of replication activity.
type Stats struct {
	Role           Role
	Seq            uint64        // Highest durable local sequence.
	Delivered      uint64        // Mutations acknowledged by replicas.
	Pending        uint64        // Queued or in-flight mutations awaiting acknowledgement.
	Failed         uint64        // Failed batch attempts or queue overflows.
	QueueDepth     int           // Live mutations buffered across replicas.
	ReplicationLag time.Duration // Age of the oldest pending replication work.
	SyncTime       time.Duration // Duration of the most recent full synchronization.
	Replicas       int           // Currently connected replicas.
}

// ReplicaStatus describes one connected replica from the primary's view.
type ReplicaStatus struct {
	ID        string
	Address   string
	AckedSeq  uint64
	Pending   uint64
	Delivered uint64
	Failed    uint64
	Lag       time.Duration
	SyncedAt  time.Time
}

// coordinatorState is the lifecycle position.
type coordinatorState uint8

const (
	stateNew coordinatorState = iota
	stateRunning
	stateStopping
	stateStopped
)

// Coordinator runs replication for one cache node.
//
// Create it with New, start it with Start, and stop it with Stop. Only a
// primary accepts Set, SetWithTTL, Delete, and Expire calls.
type Coordinator struct {
	config Config
	rep    *replicator
	wal    *WAL

	stateMu   sync.Mutex
	state     coordinatorState
	stoppedCh chan struct{}
	stopErr   error

	ctx    context.Context
	cancel context.CancelFunc

	listener net.Listener
	writeMu  sync.Mutex
	wg       sync.WaitGroup

	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	sessionsMu sync.Mutex
	sessions   map[*replicaSession]struct{}

	forceFull atomic.Bool
	syncNanos atomic.Int64

	// Cumulative totals retained after a replica session disconnects.
	delivered atomic.Uint64
	failed    atomic.Uint64

	errMu   sync.Mutex
	lastErr error
}

// New validates config, opens the WAL, and restores the store from it.
//
// No socket or goroutine is created until Start.
func New(config Config) (*Coordinator, error) {
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}

	wal, err := OpenWAL(config.WALPath)
	if err != nil {
		return nil, err
	}
	rep, err := newReplicator(config.Store, wal)
	if err != nil {
		_ = wal.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	coordinator := &Coordinator{
		config:    config,
		rep:       rep,
		wal:       wal,
		state:     stateNew,
		stoppedCh: make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
		conns:     make(map[net.Conn]struct{}),
		sessions:  make(map[*replicaSession]struct{}),
	}
	coordinator.forceFull.Store(config.ForceFullSync)
	return coordinator, nil
}

// Start opens the primary listener or launches the replica sync loop.
func (c *Coordinator) Start() error {
	c.stateMu.Lock()

	switch c.state {
	case stateRunning:
		c.stateMu.Unlock()
		return ErrAlreadyStarted
	case stateStopping, stateStopped:
		c.stateMu.Unlock()
		return ErrClosed
	}

	var listener net.Listener
	var err error
	if c.config.Role == RolePrimary {
		listener, err = net.Listen("tcp", c.config.ListenAddr)
		if err != nil {
			c.stateMu.Unlock()
			return fmt.Errorf("replication: listen %s: %w", c.config.ListenAddr, err)
		}
		c.listener = listener
	}

	c.state = stateRunning
	c.wg.Add(1)
	if c.config.Role == RolePrimary {
		go c.acceptLoop()
	} else {
		go c.syncLoop()
	}
	c.stateMu.Unlock()
	return nil
}

// Addr returns the bound replication listener, or nil for replicas.
func (c *Coordinator) Addr() net.Addr {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	if c.listener == nil {
		return nil
	}
	return c.listener.Addr()
}

// Role returns this node's replication role.
func (c *Coordinator) Role() Role {
	return c.config.Role
}

// ID returns this node's configured identity.
func (c *Coordinator) ID() string {
	return c.config.ID
}

// Set stores a value on the primary and replicates it.
func (c *Coordinator) Set(key string, value any) error {
	return c.mutate(OpSet, key, value, 0)
}

// SetWithTTL stores a value with an expiration on the primary and replicates
// it.
func (c *Coordinator) SetWithTTL(key string, value any, ttl time.Duration) error {
	if ttl <= 0 {
		return cache.ErrInvalidTTL
	}
	return c.mutate(OpTTL, key, value, ttl)
}

// Expire updates the TTL of an existing primary key.
//
// It returns false without writing when key is absent. The value is replicated
// with its new expiration so replicas converge without a separate TTL-only
// operation.
func (c *Coordinator) Expire(key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cache.ErrInvalidTTL
	}
	if c.config.Role != RolePrimary {
		return false, ErrNotPrimary
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.writeState(); err != nil {
		return false, err
	}
	value, found := c.config.Store.Get(key)
	if !found {
		return false, nil
	}
	mutation, err := c.rep.commit(OpTTL, key, value, ttl, c.config.MaxMessageSize)
	if err != nil {
		return false, err
	}
	c.publish(mutation)
	return true, nil
}

// Delete removes a key on the primary and replicates the deletion.
func (c *Coordinator) Delete(key string) error {
	return c.mutate(OpDelete, key, nil, 0)
}

// mutate serializes one primary write through the WAL and live replicas.
func (c *Coordinator) mutate(op Op, key string, value any, ttl time.Duration) error {
	if c.config.Role != RolePrimary {
		return ErrNotPrimary
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.writeState(); err != nil {
		return err
	}
	mutation, err := c.rep.commit(op, key, value, ttl, c.config.MaxMessageSize)
	if err != nil {
		return err
	}
	c.publish(mutation)
	return nil
}

// writeState reports whether primary writes may proceed.
func (c *Coordinator) writeState() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	switch c.state {
	case stateRunning:
		return nil
	case stateNew:
		return ErrNotRunning
	default:
		return ErrClosed
	}
}

// Stats returns a snapshot of replication counters and lag.
func (c *Coordinator) Stats() Stats {
	stats := Stats{
		Role:     c.config.Role,
		Seq:      c.rep.seqValue(),
		SyncTime: time.Duration(c.syncNanos.Load()),
	}

	c.sessionsMu.Lock()
	stats.Delivered = c.delivered.Load()
	stats.Failed = c.failed.Load()
	stats.Replicas = len(c.sessions)
	for session := range c.sessions {
		stats.Delivered += session.delivered.Load()
		stats.Failed += session.failed.Load()
		stats.QueueDepth += len(session.queue)

		pending := uint64(len(session.queue)) + uint64(maxZero(session.inFlight.Load()))
		stats.Pending += pending
		if pending == 0 {
			continue
		}
		if since := session.pendingSince.Load(); since > 0 {
			if lag := time.Since(time.Unix(0, since)); lag > stats.ReplicationLag {
				stats.ReplicationLag = lag
			}
		}
	}
	c.sessionsMu.Unlock()

	return stats
}

// WaitForAck waits until at least the requested number of connected replicas
// have durably acknowledged seq.
//
// It is used by layers that require a write quorum before reporting success.
// The wait is bounded by timeout and returns ErrAckTimeout when the quorum is
// not reached. Disconnected replicas do not count even if they acknowledged
// the sequence earlier.
func (c *Coordinator) WaitForAck(seq uint64, replicas int, timeout time.Duration) error {
	if replicas <= 0 {
		return nil
	}
	if c.config.Role != RolePrimary {
		return ErrNotPrimary
	}
	if timeout <= 0 {
		timeout = c.config.AckTimeout
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if c.ackedReplicas(seq) >= replicas {
			return nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("%w: sequence %d acknowledged by %d of %d replicas",
				ErrAckTimeout, seq, c.ackedReplicas(seq), replicas)
		}

		timer := time.NewTimer(remaining)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return ErrClosed
		case <-ticker.C:
			timer.Stop()
		case <-timer.C:
			return fmt.Errorf("%w: sequence %d acknowledged by %d of %d replicas",
				ErrAckTimeout, seq, c.ackedReplicas(seq), replicas)
		}
	}
}

// ackedReplicas counts connected replicas that have acknowledged seq.
func (c *Coordinator) ackedReplicas(seq uint64) int {
	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()

	count := 0
	for session := range c.sessions {
		if session.ackedSeq.Load() >= seq {
			count++
		}
	}
	return count
}

// Replicas returns connected replicas sorted by ID.
func (c *Coordinator) Replicas() []ReplicaStatus {
	c.sessionsMu.Lock()
	statuses := make([]ReplicaStatus, 0, len(c.sessions))
	for session := range c.sessions {
		status := ReplicaStatus{
			ID:        session.id,
			Address:   session.address,
			AckedSeq:  session.ackedSeq.Load(),
			Pending:   uint64(len(session.queue)) + uint64(maxZero(session.inFlight.Load())),
			Delivered: session.delivered.Load(),
			Failed:    session.failed.Load(),
		}
		if status.Pending > 0 {
			if since := session.pendingSince.Load(); since > 0 {
				status.Lag = time.Since(time.Unix(0, since))
			}
		}
		if synced := session.syncedAt.Load(); synced > 0 {
			status.SyncedAt = time.Unix(0, synced)
		}
		statuses = append(statuses, status)
	}
	c.sessionsMu.Unlock()

	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].ID != statuses[j].ID {
			return statuses[i].ID < statuses[j].ID
		}
		return statuses[i].Address < statuses[j].Address
	})
	return statuses
}

// LastError returns the most recent background replication error, if any.
func (c *Coordinator) LastError() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.lastErr
}

// recordError stores the latest background error for observability.
func (c *Coordinator) recordError(err error) {
	if err == nil {
		return
	}
	c.errMu.Lock()
	c.lastErr = err
	c.errMu.Unlock()
}

// setSyncTime records the duration of a completed synchronization.
func (c *Coordinator) setSyncTime(duration time.Duration) {
	c.syncNanos.Store(int64(duration))
}

// publish fans a committed mutation out to every connected replica.
func (c *Coordinator) publish(mutation Mutation) {
	c.sessionsMu.Lock()
	sessions := make([]*replicaSession, 0, len(c.sessions))
	for session := range c.sessions {
		sessions = append(sessions, session)
	}
	c.sessionsMu.Unlock()

	for _, session := range sessions {
		session.enqueue(mutation)
	}
}

// addSession registers a replica before its initial synchronization begins.
func (c *Coordinator) addSession(session *replicaSession) {
	c.sessionsMu.Lock()
	c.sessions[session] = struct{}{}
	c.sessionsMu.Unlock()
}

// removeSession unregisters a disconnected replica and retains its cumulative
// counters in the coordinator totals.
func (c *Coordinator) removeSession(session *replicaSession) {
	c.sessionsMu.Lock()
	if _, active := c.sessions[session]; active {
		c.delivered.Add(session.delivered.Load())
		c.failed.Add(session.failed.Load())
		delete(c.sessions, session)
	}
	c.sessionsMu.Unlock()
}

// Stop stops replication, closes the WAL, and waits for goroutines.
//
// Stop is idempotent and safe to call concurrently.
func (c *Coordinator) Stop() error {
	c.stateMu.Lock()
	switch c.state {
	case stateStopped, stateStopping:
		done := c.stoppedCh
		c.stateMu.Unlock()
		<-done
		return c.shutdownResult()

	case stateNew:
		c.state = stateStopping
		c.cancel()
		c.stateMu.Unlock()
		err := c.wal.Close()
		c.finishStop(err)
		return err
	}

	c.state = stateStopping
	c.cancel()
	listener := c.listener
	c.stateMu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	// Connections and the replica sync loop are unblocked by cancellation.
	c.closeConnections()

	// Drain a primary write that already passed the state check.
	c.writeMu.Lock()
	c.writeMu.Unlock()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(c.config.ShutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
		err := c.wal.Close()
		c.finishStop(err)
		return err

	case <-timer.C:
		c.closeConnections()
		c.wg.Wait()
		err := errors.Join(ErrShutdownTimeout, c.wal.Close())
		c.finishStop(err)
		return err
	}
}

// shutdownResult returns the result recorded by the stopping goroutine.
func (c *Coordinator) shutdownResult() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.stopErr
}

// finishStop records the shutdown result and wakes concurrent Stop callers.
func (c *Coordinator) finishStop(err error) {
	c.stateMu.Lock()
	c.stopErr = err
	c.state = stateStopped
	close(c.stoppedCh)
	c.stateMu.Unlock()
}

// registerConn tracks a connection so shutdown can force-close it.
func (c *Coordinator) registerConn(conn net.Conn) {
	c.connMu.Lock()
	c.conns[conn] = struct{}{}
	c.connMu.Unlock()
}

// unregisterConn removes a connection from the shutdown set.
func (c *Coordinator) unregisterConn(conn net.Conn) {
	c.connMu.Lock()
	delete(c.conns, conn)
	c.connMu.Unlock()
}

// closeConnections force-closes all active replication connections.
func (c *Coordinator) closeConnections() {
	c.connMu.Lock()
	for conn := range c.conns {
		_ = conn.Close()
	}
	c.connMu.Unlock()
}

// maxZero returns v when positive and zero otherwise.
func maxZero(v int64) int64 {
	if v > 0 {
		return v
	}
	return 0
}
