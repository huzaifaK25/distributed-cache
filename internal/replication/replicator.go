// Package replication implements primary/replica cache replication.
//
// A primary serializes writes through a write-ahead log, applies them to its
// local cache, and streams them to connected replicas. Replicas bootstrap from
// a full snapshot, then apply ordered deltas and acknowledge each batch.
package replication

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"distributed-cache/internal/cache"
)

// Role identifies how a node participates in replication.
type Role string

const (
	// RolePrimary accepts writes and streams them to replicas.
	RolePrimary Role = "PRIMARY"

	// RoleReplica applies writes received from a primary.
	RoleReplica Role = "REPLICA"
)

// Op is a replicated cache mutation kind.
type Op string

const (
	// OpSet stores a value that never expires.
	OpSet Op = "SET"

	// OpDelete removes a key.
	OpDelete Op = "DEL"

	// OpTTL stores a value with an absolute expiration time.
	OpTTL Op = "TTL"
)

// Mutation is one ordered cache change.
//
// Times are Unix nanoseconds so they survive JSON round trips without losing
// precision. ExpiresAt is zero for mutations without a TTL.
type Mutation struct {
	Seq       uint64          `json:"seq"`
	Op        Op              `json:"op"`
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value,omitempty"`
	ExpiresAt int64           `json:"expires_at,omitempty"`
	Timestamp int64           `json:"timestamp"`
}

// SnapshotEntry is one key in a full-sync snapshot.
type SnapshotEntry struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	ExpiresAt int64           `json:"expires_at,omitempty"`
}

// Store is the cache surface required by replication.
//
// *cache.InMemoryCache implements Store. Values must be JSON-serializable;
// replicas receive their canonical JSON decoding, so integers become float64.
type Store interface {
	cache.Cache
	Snapshot() []cache.SnapshotItem
	Restore(items []cache.SnapshotItem)
}

// Replication errors.
var (
	ErrInvalidMutation = errors.New("replication: invalid mutation")
	ErrValueTooLarge   = errors.New("replication: value exceeds transport limit")
	ErrSequenceGap     = errors.New("replication: replication sequence gap")
)

// Compile-time check that the in-memory cache is a valid Store.
var _ Store = (*cache.InMemoryCache)(nil)

// replicator owns the local cache, durable log, and applied sequence.
type replicator struct {
	store Store
	wal   *WAL

	mu  sync.Mutex
	seq uint64
}

// newReplicator creates a replicator and rebuilds the store from its WAL.
func newReplicator(store Store, wal *WAL) (*replicator, error) {
	r := &replicator{store: store, wal: wal}
	if err := r.recover(); err != nil {
		return nil, err
	}
	return r, nil
}

// recover replays every durable record into the store.
func (r *replicator) recover() error {
	lastSeq, err := r.wal.Replay(func(record walRecord) error {
		switch record.Kind {
		case walKindSnapshot:
			items, err := snapshotItems(record.Snapshot)
			if err != nil {
				return fmt.Errorf("replication: replay snapshot at %d: %w", record.Seq, err)
			}
			r.store.Restore(items)
			return nil

		case walKindMutation:
			if record.Mutation == nil {
				return fmt.Errorf("replication: replay seq %d: missing mutation", record.Seq)
			}
			if err := applyMutation(r.store, *record.Mutation); err != nil {
				return fmt.Errorf("replication: replay seq %d: %w", record.Seq, err)
			}
			return nil

		default:
			return fmt.Errorf("replication: replay seq %d: unknown record kind %q", record.Seq, record.Kind)
		}
	})
	if err != nil {
		return err
	}

	r.seq = lastSeq
	return nil
}

// commit assigns a sequence, appends to the WAL, and applies a local write.
func (r *replicator) commit(op Op, key string, value any, ttl time.Duration, maxFrame int) (Mutation, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return Mutation{}, fmt.Errorf("replication: encode value: %w", err)
	}

	now := time.Now()
	mutation := Mutation{
		Op:        op,
		Key:       key,
		Value:     raw,
		Timestamp: now.UnixNano(),
	}
	switch op {
	case OpSet:
		if ttl != 0 {
			return Mutation{}, fmt.Errorf("%w: SET does not accept a ttl", ErrInvalidMutation)
		}
	case OpTTL:
		if ttl <= 0 {
			return Mutation{}, cache.ErrInvalidTTL
		}
		mutation.ExpiresAt = now.Add(ttl).UnixNano()
	case OpDelete:
		mutation.Value = nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	mutation.Seq = r.seq + 1
	if err := validateMutation(mutation); err != nil {
		return Mutation{}, err
	}
	if err := checkMutationFrame(mutation, maxFrame); err != nil {
		return Mutation{}, err
	}
	if err := r.wal.Append(mutation); err != nil {
		return Mutation{}, err
	}
	if err := applyMutation(r.store, mutation); err != nil {
		return Mutation{}, err
	}

	r.seq = mutation.Seq
	return mutation, nil
}

// applyRemote durably applies one ordered mutation from a primary.
// It reports whether the mutation was new; already-applied sequences are
// idempotent no-ops so duplicate delivery during reconnect is safe.
func (r *replicator) applyRemote(mutation Mutation) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if mutation.Seq <= r.seq {
		return false, nil
	}
	if mutation.Seq != r.seq+1 {
		return false, fmt.Errorf("%w: expected %d, got %d", ErrSequenceGap, r.seq+1, mutation.Seq)
	}
	if err := validateMutation(mutation); err != nil {
		return false, err
	}
	if err := r.wal.Append(mutation); err != nil {
		return false, err
	}
	if err := applyMutation(r.store, mutation); err != nil {
		return false, err
	}

	r.seq = mutation.Seq
	return true, nil
}

// snapshot captures a consistent point-in-time view and its sequence.
func (r *replicator) snapshot() (uint64, []SnapshotEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	items := r.store.Snapshot()
	entries := make([]SnapshotEntry, 0, len(items))
	for _, item := range items {
		raw, err := json.Marshal(item.Value)
		if err != nil {
			return 0, nil, fmt.Errorf("replication: encode snapshot key %q: %w", item.Key, err)
		}

		var expiresAt int64
		if item.TTL > 0 {
			expiresAt = now.Add(item.TTL).UnixNano()
		}
		entries = append(entries, SnapshotEntry{
			Key:       item.Key,
			Value:     raw,
			ExpiresAt: expiresAt,
		})
	}
	return r.seq, entries, nil
}

// restoreSnapshot replaces the cache with an authoritative primary snapshot.
func (r *replicator) restoreSnapshot(seq uint64, entries []SnapshotEntry) error {
	items, err := snapshotItems(entries)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.wal.Reset(seq, entries); err != nil {
		return err
	}
	r.store.Restore(items)
	r.seq = seq
	return nil
}

// seqValue returns the last applied sequence.
func (r *replicator) seqValue() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

// validateMutation rejects malformed records before they reach the WAL.
func validateMutation(mutation Mutation) error {
	if mutation.Seq == 0 {
		return fmt.Errorf("%w: sequence must be positive", ErrInvalidMutation)
	}
	if mutation.Op != OpSet && mutation.Op != OpTTL && mutation.Op != OpDelete {
		return fmt.Errorf("%w: unknown op %q", ErrInvalidMutation, mutation.Op)
	}
	if mutation.Op == OpDelete {
		return nil
	}
	if len(mutation.Value) == 0 || !json.Valid(mutation.Value) {
		return fmt.Errorf("%w: %s requires a valid JSON value", ErrInvalidMutation, mutation.Op)
	}
	if mutation.Op == OpTTL && mutation.ExpiresAt <= 0 {
		return fmt.Errorf("%w: TTL requires an expiration time", ErrInvalidMutation)
	}
	if mutation.Op == OpSet && mutation.ExpiresAt != 0 {
		return fmt.Errorf("%w: SET must not have an expiration time", ErrInvalidMutation)
	}
	return nil
}

// applyMutation applies one record to the local cache. An elapsed TTL is
// treated as a delete so replay converges on the same state as the primary.
func applyMutation(store Store, mutation Mutation) error {
	switch mutation.Op {
	case OpSet:
		value, err := decodeValue(mutation.Value)
		if err != nil {
			return err
		}
		return store.Set(mutation.Key, value)

	case OpTTL:
		value, err := decodeValue(mutation.Value)
		if err != nil {
			return err
		}
		remaining := time.Until(time.Unix(0, mutation.ExpiresAt))
		if remaining <= 0 {
			return store.Delete(mutation.Key)
		}
		return store.SetWithTTL(mutation.Key, value, remaining)

	case OpDelete:
		return store.Delete(mutation.Key)

	default:
		return fmt.Errorf("%w: unknown op %q", ErrInvalidMutation, mutation.Op)
	}
}

// snapshotItems converts wire entries into cache items, dropping expired keys.
func snapshotItems(entries []SnapshotEntry) ([]cache.SnapshotItem, error) {
	now := time.Now()
	items := make([]cache.SnapshotItem, 0, len(entries))
	for _, entry := range entries {
		value, err := decodeValue(entry.Value)
		if err != nil {
			return nil, fmt.Errorf("replication: decode snapshot key %q: %w", entry.Key, err)
		}

		var ttl time.Duration
		if entry.ExpiresAt > 0 {
			ttl = time.Unix(0, entry.ExpiresAt).Sub(now)
			if ttl <= 0 {
				continue
			}
		}
		items = append(items, cache.SnapshotItem{
			Key:   entry.Key,
			Value: value,
			TTL:   ttl,
		})
	}
	return items, nil
}

// decodeValue decodes a JSON value into the canonical form stored on every
// node.
func decodeValue(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: missing value", ErrInvalidMutation)
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMutation, err)
	}
	return value, nil
}
