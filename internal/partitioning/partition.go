// Package partitioning maps cache keys to fixed logical partitions and assigns
// those partitions to cache nodes with a consistent hash ring.
//
// The package also plans and executes ownership changes. A rebalance is split
// into a copy phase and an atomic ownership commit, so a failed migration
// leaves routing on the previous owner. See Migrator and Partitioner.Apply.
package partitioning

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Partitioning errors.
var (
	ErrInvalidConfig   = errors.New("partitioning: invalid config")
	ErrLastNode        = errors.New("partitioning: cannot remove every node")
	ErrInvalidPlan     = errors.New("partitioning: invalid rebalance plan")
	ErrStalePlan       = errors.New("partitioning: rebalance plan is stale")
	ErrNoMigrator      = errors.New("partitioning: migrations require a migrator")
	ErrMigrationFailed = errors.New("partitioning: partition migration failed")
)

// Defaults for partition routing.
const (
	DefaultPartitions = 1024
	MaxPartitions     = 1 << 16
)

// Config controls a Partitioner and its hash ring.
//
// Zero values are replaced by DefaultConfig values in New.
type Config struct {
	// Nodes is the initial cluster membership. It may be empty while a
	// cluster is bootstrapping; add nodes with PlanAddNode or PlanNodes.
	Nodes []Node

	// VirtualNodes is the number of ring points per physical node.
	VirtualNodes int

	// Partitions is the fixed number of logical partitions. Key movement is
	// measured and migrated at partition granularity, so this value should be
	// large enough to spread load but small enough to keep plans compact.
	Partitions uint32

	// Hash maps keys and virtual nodes to the ring. It must be deterministic
	// across every node in the cluster.
	Hash Hasher
}

// DefaultConfig returns partition settings suitable for a small cluster.
func DefaultConfig() Config {
	return Config{
		VirtualNodes: DefaultVirtualNodes,
		Partitions:   DefaultPartitions,
		Hash:         DefaultHash,
	}
}

// withDefaults validates the configuration and fills zero values.
func (c Config) withDefaults() (Config, error) {
	defaults := DefaultConfig()

	if c.VirtualNodes < 0 {
		return Config{}, fmt.Errorf("%w: virtual nodes must not be negative", ErrInvalidConfig)
	}
	if c.VirtualNodes == 0 {
		c.VirtualNodes = defaults.VirtualNodes
	}
	if c.Partitions == 0 {
		c.Partitions = defaults.Partitions
	}
	if c.Partitions > MaxPartitions {
		return Config{}, fmt.Errorf("%w: partitions must not exceed %d", ErrInvalidConfig, MaxPartitions)
	}
	if c.Hash == nil {
		c.Hash = defaults.Hash
	}

	nodes, err := normalizeNodes(c.Nodes)
	if err != nil {
		return Config{}, err
	}
	c.Nodes = nodes
	return c, nil
}

// PartitionID identifies one fixed logical partition.
type PartitionID uint32

// String returns the decimal partition number.
func (p PartitionID) String() string {
	return strconv.FormatUint(uint64(p), 10)
}

// Route is the result of mapping a key to its partition and owner.
type Route struct {
	Partition PartitionID
	Node      Node
}

// Move describes one partition ownership transfer.
type Move struct {
	Partition PartitionID
	Source    Node
	Target    Node
}

// assignment is an immutable routing table. Partitioner publishes a new
// pointer only after every migration in a plan succeeds.
type assignment struct {
	generation uint64
	ring       *Ring
	owners     []Node
}

// Plan is a prepared membership change.
//
// A plan is valid for the Partitioner and generation that created it. Apply
// refuses a plan after any other membership change has committed. Plans can be
// discarded without side effects; no data is copied until Apply runs.
type Plan struct {
	owner      *Partitioner
	generation uint64
	next       *assignment
	moves      []Move
	added      []Node
	removed    []Node
}

// Generation returns the assignment generation the plan was built from.
func (p *Plan) Generation() uint64 {
	if p == nil {
		return 0
	}
	return p.generation
}

// NextGeneration returns the generation that Apply commits.
func (p *Plan) NextGeneration() uint64 {
	if p == nil || p.next == nil {
		return 0
	}
	return p.next.generation
}

// Moves returns a copy of the partition moves in ascending partition order.
func (p *Plan) Moves() []Move {
	if p == nil {
		return nil
	}
	return append([]Move(nil), p.moves...)
}

// AddedNodes returns the nodes present in the plan and absent from current
// membership.
func (p *Plan) AddedNodes() []Node {
	if p == nil {
		return nil
	}
	return append([]Node(nil), p.added...)
}

// RemovedNodes returns the nodes present in current membership and absent from
// the plan.
func (p *Plan) RemovedNodes() []Node {
	if p == nil {
		return nil
	}
	return append([]Node(nil), p.removed...)
}

// PartitionsMoved returns the number of partition ownership transfers.
func (p *Plan) PartitionsMoved() int {
	if p == nil {
		return 0
	}
	return len(p.moves)
}

// Nodes returns the target membership sorted by node ID.
func (p *Plan) Nodes() []Node {
	if p == nil || p.next == nil {
		return nil
	}
	return p.next.ring.Nodes()
}

// Metrics holds cumulative partitioning counters.
type metrics struct {
	rebalances            atomic.Uint64
	migrations            atomic.Uint64
	failedMigrations      atomic.Uint64
	movedKeys             atomic.Uint64
	migrationNanos        atomic.Int64
	lastMigrationNanos    atomic.Int64
	lastRebalanceUnixNano atomic.Int64
}

// Stats is a point-in-time partitioning snapshot.
type Stats struct {
	Nodes                 int
	Partitions            uint32
	VirtualNodes          int
	Tokens                int
	Generation            uint64
	Rebalances            uint64
	Migrations            uint64
	FailedMigrations      uint64
	MovedKeys             uint64
	MigrationDuration     time.Duration // Cumulative successful and failed copy time.
	LastMigrationDuration time.Duration
	LastRebalance         time.Time
}

// Partitioner owns the ring, fixed-partition ownership table, and rebalance
// state for one cache cluster.
//
// Routing reads are lock-free. Membership changes are serialized by Apply.
type Partitioner struct {
	config  Config
	current atomic.Pointer[assignment]

	applyMu sync.Mutex
	metrics metrics
}

// New validates config and builds the initial ownership table.
func New(config Config) (*Partitioner, error) {
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}

	ring, err := NewRingWithHasher(config.Nodes, config.VirtualNodes, config.Hash)
	if err != nil {
		return nil, err
	}

	partitioner := &Partitioner{config: config}
	partitioner.current.Store(newAssignment(ring, config.Partitions, 0))
	return partitioner, nil
}

// newAssignment maps every fixed partition to its ring owner.
func newAssignment(ring *Ring, partitions uint32, generation uint64) *assignment {
	owners := make([]Node, partitions)
	for partition := uint32(0); partition < partitions; partition++ {
		owner, ok := ring.LookupHash(ring.hash(partitionKey(PartitionID(partition))))
		if ok {
			owners[partition] = owner
		}
	}
	return &assignment{
		generation: generation,
		ring:       ring,
		owners:     owners,
	}
}

// Route maps key to its logical partition and current owner.
//
// The boolean is false when the cluster has no nodes.
func (p *Partitioner) Route(key string) (Route, bool) {
	current := p.current.Load()
	partition := p.PartitionOf(key)
	owner := current.owners[partition]
	if owner.ID == "" {
		return Route{Partition: partition}, false
	}
	return Route{Partition: partition, Node: owner}, true
}

// PartitionOf returns the fixed logical partition for key.
//
// The result depends only on the configured hash and partition count, not on
// membership, so it is stable while a node joins or leaves.
func (p *Partitioner) PartitionOf(key string) PartitionID {
	return PartitionID(p.config.Hash(key) % uint64(p.config.Partitions))
}

// OwnerOf returns the current owner of partition.
func (p *Partitioner) OwnerOf(partition PartitionID) (Node, bool) {
	if partition >= PartitionID(p.config.Partitions) {
		return Node{}, false
	}
	owner := p.current.Load().owners[partition]
	if owner.ID == "" {
		return Node{}, false
	}
	return owner, true
}

// Partitions returns the configured logical partition count.
func (p *Partitioner) Partitions() uint32 {
	return p.config.Partitions
}

// Nodes returns the current physical members sorted by ID.
func (p *Partitioner) Nodes() []Node {
	return p.current.Load().ring.Nodes()
}

// Ring returns the current immutable hash ring.
func (p *Partitioner) Ring() *Ring {
	return p.current.Load().ring
}

// Generation returns the current ownership generation.
func (p *Partitioner) Generation() uint64 {
	return p.current.Load().generation
}

// Stats returns a point-in-time routing and migration snapshot.
func (p *Partitioner) Stats() Stats {
	current := p.current.Load()
	stats := Stats{
		Nodes:                 len(current.ring.nodes),
		Partitions:            p.config.Partitions,
		VirtualNodes:          p.config.VirtualNodes,
		Tokens:                current.ring.TokenCount(),
		Generation:            current.generation,
		Rebalances:            p.metrics.rebalances.Load(),
		Migrations:            p.metrics.migrations.Load(),
		FailedMigrations:      p.metrics.failedMigrations.Load(),
		MovedKeys:             p.metrics.movedKeys.Load(),
		MigrationDuration:     time.Duration(p.metrics.migrationNanos.Load()),
		LastMigrationDuration: time.Duration(p.metrics.lastMigrationNanos.Load()),
	}
	if nanos := p.metrics.lastRebalanceUnixNano.Load(); nanos > 0 {
		stats.LastRebalance = time.Unix(0, nanos)
	}
	return stats
}

// PlanAddNode prepares the ownership changes for adding node.
//
// The returned plan moves only partitions whose ring owner changes, which is
// approximately 1/(current+1) of the partition space for a balanced ring.
func (p *Partitioner) PlanAddNode(node Node) (*Plan, error) {
	if err := validateNode(node); err != nil {
		return nil, err
	}

	p.applyMu.Lock()
	defer p.applyMu.Unlock()

	nodes := p.current.Load().ring.Nodes()
	for _, existing := range nodes {
		if existing.ID == node.ID {
			return nil, fmt.Errorf("%w: %q", ErrNodeExists, node.ID)
		}
	}
	return p.planNodesLocked(append(nodes, node))
}

// PlanRemoveNode prepares the ownership changes for removing id.
func (p *Partitioner) PlanRemoveNode(id NodeID) (*Plan, error) {
	p.applyMu.Lock()
	defer p.applyMu.Unlock()

	nodes := p.current.Load().ring.Nodes()
	if len(nodes) <= 1 {
		for _, node := range nodes {
			if node.ID == id {
				return nil, fmt.Errorf("%w: %q is the last node", ErrLastNode, id)
			}
		}
		return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, id)
	}

	remaining := make([]Node, 0, len(nodes)-1)
	found := false
	for _, node := range nodes {
		if node.ID == id {
			found = true
			continue
		}
		remaining = append(remaining, node)
	}
	if !found {
		return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, id)
	}
	return p.planNodesLocked(remaining)
}

// PlanNodes prepares a membership change to exactly nodes.
//
// The plan may add, remove, or update nodes in one step. Node IDs must be
// unique; removing every node is rejected.
func (p *Partitioner) PlanNodes(nodes []Node) (*Plan, error) {
	p.applyMu.Lock()
	defer p.applyMu.Unlock()
	return p.planNodesLocked(nodes)
}

// planNodesLocked builds a plan while applyMu prevents a concurrent commit.
func (p *Partitioner) planNodesLocked(nodes []Node) (*Plan, error) {
	normalized, err := normalizeNodes(nodes)
	if err != nil {
		return nil, err
	}

	current := p.current.Load()
	if len(normalized) == 0 && len(current.ring.nodes) > 0 {
		return nil, fmt.Errorf("%w: node set must not be empty", ErrLastNode)
	}

	nextRing, err := NewRingWithHasher(normalized, p.config.VirtualNodes, p.config.Hash)
	if err != nil {
		return nil, err
	}
	next := newAssignment(nextRing, p.config.Partitions, current.generation+1)

	added, removed := diffNodes(current.ring.nodes, nextRing.nodes)
	return &Plan{
		owner:      p,
		generation: current.generation,
		next:       next,
		moves:      diffOwners(current, next),
		added:      added,
		removed:    removed,
	}, nil
}

// diffOwners returns ownership changes in ascending partition order. Partitions
// without a current owner are new capacity, not migrations.
func diffOwners(current, next *assignment) []Move {
	moves := make([]Move, 0)
	for partition, owner := range current.owners {
		if owner.ID == "" {
			continue
		}
		target := next.owners[partition]
		if owner.ID == target.ID {
			continue
		}
		moves = append(moves, Move{
			Partition: PartitionID(partition),
			Source:    owner,
			Target:    target,
		})
	}
	return moves
}

// diffNodes returns membership additions and removals sorted by ID.
func diffNodes(current, next []Node) (added, removed []Node) {
	currentIDs := make(map[NodeID]struct{}, len(current))
	for _, node := range current {
		currentIDs[node.ID] = struct{}{}
	}
	nextIDs := make(map[NodeID]struct{}, len(next))
	for _, node := range next {
		nextIDs[node.ID] = struct{}{}
	}

	for _, node := range next {
		if _, exists := currentIDs[node.ID]; !exists {
			added = append(added, node)
		}
	}
	for _, node := range current {
		if _, exists := nextIDs[node.ID]; !exists {
			removed = append(removed, node)
		}
	}
	return added, removed
}

// Apply executes plan and atomically commits its ownership table.
//
// All partition copies happen before the commit. If any copy fails, routing
// remains on the old owners and source data is left intact. After a successful
// commit, a Migrator that implements Cleaner has its Cleanup method called for
// each moved partition. Cleanup errors are reported in ApplyResult but do not
// roll the commit back.
func (p *Partitioner) Apply(ctx context.Context, plan *Plan, migrator Migrator) (ApplyResult, error) {
	return p.ApplyWithOptions(ctx, plan, ApplyOptions{Migrator: migrator})
}

// ApplyWithOptions executes plan with optional writer coordination.
func (p *Partitioner) ApplyWithOptions(ctx context.Context, plan *Plan, options ApplyOptions) (result ApplyResult, err error) {
	if plan == nil || plan.owner != p || plan.next == nil {
		return ApplyResult{}, ErrInvalidPlan
	}

	p.applyMu.Lock()
	defer p.applyMu.Unlock()

	current := p.current.Load()
	if plan.generation != current.generation {
		return ApplyResult{}, fmt.Errorf(
			"%w: plan generation %d, current generation %d",
			ErrStalePlan, plan.generation, current.generation)
	}

	result = ApplyResult{
		Generation: current.generation,
		Moves:      len(plan.moves),
	}
	start := time.Now()
	defer func() {
		result.Duration = time.Since(start)
	}()

	if len(plan.moves) > 0 && options.Migrator == nil {
		return result, ErrNoMigrator
	}

	if options.Gate != nil && len(plan.moves) > 0 {
		release, quiesceErr := options.Gate.Quiesce(ctx, movePartitions(plan.moves))
		if quiesceErr != nil {
			return result, fmt.Errorf("partitioning: quiesce moved partitions: %w", quiesceErr)
		}
		if release != nil {
			defer release()
		}
	}

	for _, move := range plan.moves {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		migrationStart := time.Now()
		migration, migrateErr := options.Migrator.Migrate(ctx, move)
		elapsed := time.Since(migrationStart)
		p.metrics.migrationNanos.Add(elapsed.Nanoseconds())
		p.metrics.lastMigrationNanos.Store(elapsed.Nanoseconds())

		if migrateErr != nil {
			p.metrics.failedMigrations.Add(1)
			return result, fmt.Errorf(
				"%w: partition %d from %q to %q: %w",
				ErrMigrationFailed, move.Partition, move.Source.ID, move.Target.ID, migrateErr)
		}

		p.metrics.migrations.Add(1)
		p.metrics.movedKeys.Add(migration.Keys)
		result.MovedKeys += migration.Keys
	}

	if err := ctx.Err(); err != nil {
		return result, err
	}

	p.current.Store(plan.next)
	p.metrics.rebalances.Add(1)
	p.metrics.lastRebalanceUnixNano.Store(time.Now().UnixNano())
	result.Applied = true
	result.Generation = plan.next.generation

	if cleaner, ok := options.Migrator.(Cleaner); ok && len(plan.moves) > 0 {
		for _, move := range plan.moves {
			if cleanupErr := cleaner.Cleanup(ctx, move); cleanupErr != nil {
				result.CleanupErrors = append(result.CleanupErrors,
					fmt.Errorf("partition %d: %w", move.Partition, cleanupErr))
			}
		}
	}

	return result, nil
}

// movePartitions returns the distinct moved partitions in ascending order.
func movePartitions(moves []Move) []PartitionID {
	seen := make(map[PartitionID]struct{}, len(moves))
	partitions := make([]PartitionID, 0, len(moves))
	for _, move := range moves {
		if _, duplicate := seen[move.Partition]; duplicate {
			continue
		}
		seen[move.Partition] = struct{}{}
		partitions = append(partitions, move.Partition)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})
	return partitions
}
