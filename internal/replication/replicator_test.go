package replication

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"distributed-cache/internal/cache"
)

// newTestReplicator creates an empty replicator with a temporary WAL.
func newTestReplicator(t *testing.T) (*replicator, *cache.InMemoryCache, *WAL) {
	t.Helper()

	wal, err := OpenWAL(filepath.Join(t.TempDir(), "test.wal"))
	if err != nil {
		t.Fatalf("OpenWAL returned error: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })

	store := cache.New(cache.Config{})
	rep, err := newReplicator(store, wal)
	if err != nil {
		t.Fatalf("newReplicator returned error: %v", err)
	}
	return rep, store, wal
}

func TestApplyRemoteOrderingAndDeduplication(t *testing.T) {
	rep, store, _ := newTestReplicator(t)

	first := testMutation(1, "a", "one")
	applied, err := rep.applyRemote(first)
	if err != nil || !applied {
		t.Fatalf("applyRemote(first) = %v, %v; want true, nil", applied, err)
	}

	applied, err = rep.applyRemote(first)
	if err != nil || applied {
		t.Fatalf("duplicate applyRemote = %v, %v; want false, nil", applied, err)
	}

	if _, err := rep.applyRemote(testMutation(3, "c", "three")); !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("gap applyRemote error = %v, want ErrSequenceGap", err)
	}
	if rep.seqValue() != 1 {
		t.Fatalf("sequence after gap = %d, want 1", rep.seqValue())
	}

	if _, err := rep.applyRemote(testMutation(2, "b", "two")); err != nil {
		t.Fatalf("applyRemote(second) returned error: %v", err)
	}
	if value, found := store.Get("b"); !found || value != "two" {
		t.Fatalf("Get(b) = %v, %v; want two, true", value, found)
	}
}

func TestApplyRemoteRejectsInvalidMutation(t *testing.T) {
	rep, _, _ := newTestReplicator(t)

	mutation := Mutation{
		Seq:       1,
		Op:        Op("BOGUS"),
		Key:       "key",
		Value:     json.RawMessage(`"value"`),
		Timestamp: time.Now().UnixNano(),
	}
	if _, err := rep.applyRemote(mutation); !errors.Is(err, ErrInvalidMutation) {
		t.Fatalf("applyRemote error = %v, want ErrInvalidMutation", err)
	}
	if rep.seqValue() != 0 {
		t.Fatalf("sequence after invalid mutation = %d, want 0", rep.seqValue())
	}
}

func TestRestoreSnapshotDropsExpiredEntries(t *testing.T) {
	rep, store, _ := newTestReplicator(t)

	now := time.Now()
	entries := []SnapshotEntry{
		{Key: "live", Value: json.RawMessage(`"value"`)},
		{Key: "ttl", Value: json.RawMessage(`42`), ExpiresAt: now.Add(time.Minute).UnixNano()},
		{Key: "expired", Value: json.RawMessage(`"gone"`), ExpiresAt: now.Add(-time.Second).UnixNano()},
	}
	if err := rep.restoreSnapshot(10, entries); err != nil {
		t.Fatalf("restoreSnapshot returned error: %v", err)
	}
	if rep.seqValue() != 10 {
		t.Fatalf("sequence after restore = %d, want 10", rep.seqValue())
	}
	if value, found := store.Get("live"); !found || value != "value" {
		t.Fatalf("Get(live) = %v, %v", value, found)
	}
	if value, found := store.Get("ttl"); !found || value != float64(42) {
		t.Fatalf("Get(ttl) = %v, %v", value, found)
	}
	if _, found := store.Get("expired"); found {
		t.Fatal("restore kept an expired entry")
	}
}

func TestCommitRejectsOversizedValue(t *testing.T) {
	rep, store, wal := newTestReplicator(t)

	_, err := rep.commit(OpSet, "key", strings.Repeat("x", 1024), 0, 200)
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("commit error = %v, want ErrValueTooLarge", err)
	}
	if rep.seqValue() != 0 || wal.LastSeq() != 0 {
		t.Fatalf("sequences after rejected write = %d/%d, want 0/0", rep.seqValue(), wal.LastSeq())
	}
	if _, found := store.Get("key"); found {
		t.Fatal("rejected write reached the cache")
	}
}

func TestCommitValidatesOperationAndTTL(t *testing.T) {
	rep, _, _ := newTestReplicator(t)

	if _, err := rep.commit(OpTTL, "key", "value", 0, 1<<20); !errors.Is(err, cache.ErrInvalidTTL) {
		t.Fatalf("commit TTL error = %v, want ErrInvalidTTL", err)
	}
	if _, err := rep.commit(OpSet, "key", "value", time.Second, 1<<20); !errors.Is(err, ErrInvalidMutation) {
		t.Fatalf("commit SET with ttl error = %v, want ErrInvalidMutation", err)
	}
	if _, err := rep.commit(Op("BOGUS"), "key", "value", 0, 1<<20); !errors.Is(err, ErrInvalidMutation) {
		t.Fatalf("commit unknown op error = %v, want ErrInvalidMutation", err)
	}
	if rep.seqValue() != 0 {
		t.Fatalf("sequence after rejected commits = %d, want 0", rep.seqValue())
	}
}
