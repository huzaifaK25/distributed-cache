package partitioning

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"distributed-cache/internal/cache"
)

// Migration errors.
var (
	ErrUnknownStore = errors.New("partitioning: node store is not registered")
	ErrInvalidMove  = errors.New("partitioning: invalid partition move")
)

// MigrationResult reports the outcome of copying one partition.
type MigrationResult struct {
	Keys uint64
}

// Migrator copies partition data between nodes.
//
// Migrate must copy every live key in move.Partition from move.Source to
// move.Target, must be idempotent, and must not delete source entries. Apply
// commits ownership only after every move succeeds, then invokes Cleanup on a
// Migrator that also implements Cleaner.
//
// Implementations must respect context cancellation so a failed rebalance can
// stop promptly.
type Migrator interface {
	Migrate(ctx context.Context, move Move) (MigrationResult, error)
}

// Cleaner removes source copies after a rebalance commits.
//
// Cleanup runs after the new ownership table is visible. It is best-effort:
// a cleanup error leaves a stale copy but does not invalidate the rebalance.
type Cleaner interface {
	Cleanup(ctx context.Context, move Move) error
}

// WriteGate coordinates concurrent writers with a rebalance.
//
// Quiesce is called once with every moved partition before any data is copied.
// It must block new writes to those partitions and return a release function.
// Apply calls release after the ownership commit and cleanup, including when a
// migration fails. A nil gate means the caller guarantees that writes are
// already paused or routed through the same ownership generation.
type WriteGate interface {
	Quiesce(ctx context.Context, partitions []PartitionID) (release func(), err error)
}

// ApplyOptions configures one rebalance.
type ApplyOptions struct {
	// Migrator copies moved partitions. It is required when the plan moves at
	// least one partition.
	Migrator Migrator

	// Gate optionally pauses writes to moved partitions during the copy and
	// commit window.
	Gate WriteGate
}

// ApplyResult reports one Apply call.
type ApplyResult struct {
	// Applied reports whether the new ownership table is active.
	Applied bool

	// Generation is the active generation after the call. It remains the old
	// generation when Applied is false.
	Generation uint64

	// Moves is the number of partition transfers in the plan.
	Moves int

	// MovedKeys is the number of keys copied successfully. On a failed apply
	// it counts copies completed before the failure.
	MovedKeys uint64

	// Duration is the total preparation, migration, commit, and cleanup time.
	Duration time.Duration

	// CleanupErrors contains source-cleanup failures after a successful
	// commit. Cleanup errors do not roll back ownership.
	CleanupErrors []error
}

// Store is the subset of cache operations required by StoreMigrator.
//
// *cache.InMemoryCache implements Store.
type Store interface {
	Snapshot() []cache.SnapshotItem
	Set(key string, value any) error
	SetWithTTL(key string, value any, ttl time.Duration) error
	Delete(key string) error
}

// StoreMigrator moves partition data between in-process cache stores.
//
// It is suitable for embedded deployments and deterministic tests. Networked
// deployments can implement Migrator and Cleaner over their own RPC transport
// instead. StoreMigrator must be created with the Partitioner that produced the
// applied plan so key-to-partition mapping stays consistent.
type StoreMigrator struct {
	partitioner *Partitioner

	mu     sync.RWMutex
	stores map[NodeID]Store
}

// NewStoreMigrator returns a migrator for partitioner.
//
// Stores are registered separately so membership can grow without rebuilding
// the migrator.
func NewStoreMigrator(partitioner *Partitioner) (*StoreMigrator, error) {
	if partitioner == nil {
		return nil, fmt.Errorf("%w: partitioner must not be nil", ErrInvalidConfig)
	}
	return &StoreMigrator{
		partitioner: partitioner,
		stores:      make(map[NodeID]Store),
	}, nil
}

// RegisterStore associates a local cache store with node.
//
// Registering the same node again replaces its store, which supports restarting
// a node without rebuilding the migrator.
func (m *StoreMigrator) RegisterStore(node NodeID, store Store) error {
	if node == "" {
		return fmt.Errorf("%w: node id must not be empty", ErrInvalidConfig)
	}
	if store == nil {
		return fmt.Errorf("%w: store for %q must not be nil", ErrInvalidConfig, node)
	}

	m.mu.Lock()
	m.stores[node] = store
	m.mu.Unlock()
	return nil
}

// Migrate copies all live entries in move.Partition to the target store.
//
// Source entries are left in place. A failed copy may leave partial target
// entries; retrying the same move overwrites them. TTLs are reduced by the copy
// duration so migrated entries expire no later than they would have on the
// source.
func (m *StoreMigrator) Migrate(ctx context.Context, move Move) (MigrationResult, error) {
	source, target, err := m.storesFor(move)
	if err != nil {
		return MigrationResult{}, err
	}

	snapshotStart := time.Now()
	entries := m.partitionEntries(source, move.Partition)

	var result MigrationResult
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		ttl := entry.TTL
		if ttl < 0 {
			continue
		}
		if ttl > 0 {
			ttl -= time.Since(snapshotStart)
			if ttl <= 0 {
				continue
			}
		}

		if err := putEntry(target, entry.Key, entry.Value, ttl); err != nil {
			return result, fmt.Errorf(
				"partitioning: copy key %q to %q: %w", entry.Key, move.Target.ID, err)
		}
		result.Keys++
	}
	return result, nil
}

// Cleanup removes source entries for move.Partition after ownership commits.
func (m *StoreMigrator) Cleanup(ctx context.Context, move Move) error {
	source, _, err := m.storesFor(move)
	if err != nil {
		return err
	}

	entries := m.partitionEntries(source, move.Partition)
	var cleanupErrs []error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			cleanupErrs = append(cleanupErrs, err)
			break
		}
		if err := source.Delete(entry.Key); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("delete key %q: %w", entry.Key, err))
		}
	}
	return errors.Join(cleanupErrs...)
}

// partitionEntries returns the source snapshot entries belonging to partition.
func (m *StoreMigrator) partitionEntries(store Store, partition PartitionID) []cache.SnapshotItem {
	snapshot := store.Snapshot()
	entries := make([]cache.SnapshotItem, 0, len(snapshot))
	for _, entry := range snapshot {
		if m.partitioner.PartitionOf(entry.Key) == partition {
			entries = append(entries, entry)
		}
	}
	return entries
}

// storesFor resolves and validates the stores for one move.
func (m *StoreMigrator) storesFor(move Move) (Store, Store, error) {
	if move.Source.ID == "" || move.Target.ID == "" {
		return nil, nil, fmt.Errorf("%w: source and target must be set", ErrInvalidMove)
	}
	if move.Source.ID == move.Target.ID {
		return nil, nil, fmt.Errorf("%w: source and target are both %q", ErrInvalidMove, move.Source.ID)
	}

	m.mu.RLock()
	source, sourceOK := m.stores[move.Source.ID]
	target, targetOK := m.stores[move.Target.ID]
	m.mu.RUnlock()

	if !sourceOK {
		return nil, nil, fmt.Errorf("%w: %q", ErrUnknownStore, move.Source.ID)
	}
	if !targetOK {
		return nil, nil, fmt.Errorf("%w: %q", ErrUnknownStore, move.Target.ID)
	}
	return source, target, nil
}

// putEntry writes one migrated entry with its remaining TTL.
func putEntry(store Store, key string, value any, ttl time.Duration) error {
	if ttl > 0 {
		return store.SetWithTTL(key, value, ttl)
	}
	return store.Set(key, value)
}
