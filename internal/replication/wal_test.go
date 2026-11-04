package replication

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testMutation returns a valid mutation with a string value.
func testMutation(seq uint64, key, value string) Mutation {
	raw, _ := json.Marshal(value)
	return Mutation{
		Seq:       seq,
		Op:        OpSet,
		Key:       key,
		Value:     raw,
		Timestamp: time.Now().UnixNano(),
	}
}

func TestWALAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL returned error: %v", err)
	}

	ttlExpiry := time.Now().Add(time.Minute).UnixNano()
	mutations := []Mutation{
		testMutation(1, "a", "one"),
		{Seq: 2, Op: OpTTL, Key: "b", Value: json.RawMessage(`42`), ExpiresAt: ttlExpiry, Timestamp: time.Now().UnixNano()},
		{Seq: 3, Op: OpDelete, Key: "a", Timestamp: time.Now().UnixNano()},
	}
	for _, mutation := range mutations {
		if err := wal.Append(mutation); err != nil {
			t.Fatalf("Append(%d) returned error: %v", mutation.Seq, err)
		}
	}
	if got := wal.LastSeq(); got != 3 {
		t.Fatalf("LastSeq() = %d, want 3", got)
	}

	replay := func(w *WAL) []Mutation {
		t.Helper()
		var got []Mutation
		last, err := w.Replay(func(record walRecord) error {
			if record.Kind != walKindMutation {
				t.Fatalf("record kind = %q, want mutation", record.Kind)
			}
			got = append(got, *record.Mutation)
			return nil
		})
		if err != nil {
			t.Fatalf("Replay returned error: %v", err)
		}
		if last != 3 {
			t.Fatalf("Replay last sequence = %d, want 3", last)
		}
		return got
	}

	got := replay(wal)
	if len(got) != len(mutations) {
		t.Fatalf("replayed %d mutations, want %d", len(got), len(mutations))
	}
	if string(got[1].Value) != "42" || got[1].ExpiresAt != ttlExpiry || got[1].Op != OpTTL {
		t.Fatalf("TTL mutation did not round trip: %+v", got[1])
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()

	if got := reopened.LastSeq(); got != 3 {
		t.Fatalf("reopened LastSeq() = %d, want 3", got)
	}
	if got := replay(reopened); len(got) != 3 {
		t.Fatalf("reopened replayed %d mutations, want 3", len(got))
	}
}

func TestWALRecoversTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL returned error: %v", err)
	}
	if err := wal.Append(testMutation(1, "a", "one")); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}

	// Simulate a crash midway through a record: a length header promising
	// payload bytes that never reached disk.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile returned error: %v", err)
	}
	var trailer [8]byte
	binary.BigEndian.PutUint32(trailer[0:4], 128)
	if _, err := file.Write(trailer[:]); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close trailer returned error: %v", err)
	}

	recovered, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("recovery OpenWAL returned error: %v", err)
	}
	defer recovered.Close()

	if got := recovered.LastSeq(); got != 1 {
		t.Fatalf("LastSeq() after recovery = %d, want 1", got)
	}
	repaired, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat recovered WAL returned error: %v", err)
	}
	if repaired.Size() != info.Size() {
		t.Fatalf("repaired size = %d, want %d", repaired.Size(), info.Size())
	}

	if err := recovered.Append(testMutation(2, "b", "two")); err != nil {
		t.Fatalf("Append after recovery returned error: %v", err)
	}
}

func TestWALResetReplacesHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL returned error: %v", err)
	}
	if err := wal.Append(testMutation(1, "old", "value")); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	entries := []SnapshotEntry{{
		Key:   "snapshot",
		Value: json.RawMessage(`"basis"`),
	}}
	if err := wal.Reset(10, entries); err != nil {
		t.Fatalf("Reset returned error: %v", err)
	}
	if got := wal.LastSeq(); got != 10 {
		t.Fatalf("LastSeq() after Reset = %d, want 10", got)
	}
	if err := wal.Append(testMutation(11, "after", "reset")); err != nil {
		t.Fatalf("Append after Reset returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()

	var kinds []string
	last, err := reopened.Replay(func(record walRecord) error {
		kinds = append(kinds, record.Kind)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if last != 11 {
		t.Fatalf("Replay last = %d, want 11", last)
	}
	if len(kinds) != 2 || kinds[0] != walKindSnapshot || kinds[1] != walKindMutation {
		t.Fatalf("replayed kinds = %v, want [snapshot mutation]", kinds)
	}
}

func TestWALRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL returned error: %v", err)
	}
	defer wal.Close()

	for seq := uint64(1); seq <= 5; seq++ {
		if err := wal.Append(testMutation(seq, "key", "value")); err != nil {
			t.Fatalf("Append(%d) returned error: %v", seq, err)
		}
	}

	var got []uint64
	if err := wal.Range(2, 4, func(mutation Mutation) error {
		got = append(got, mutation.Seq)
		return nil
	}); err != nil {
		t.Fatalf("Range(2,4) returned error: %v", err)
	}
	if len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Fatalf("Range(2,4) = %v, want [2 3 4]", got)
	}

	if err := wal.Range(6, 6, func(Mutation) error { return nil }); err != nil {
		t.Fatalf("Range at end returned error: %v", err)
	}
	if err := wal.Range(7, 7, func(Mutation) error { return nil }); !errors.Is(err, ErrWALRange) {
		t.Fatalf("Range beyond end error = %v, want ErrWALRange", err)
	}

	if err := wal.Reset(10, nil); err != nil {
		t.Fatalf("Reset returned error: %v", err)
	}
	if err := wal.Append(testMutation(11, "key", "value")); err != nil {
		t.Fatalf("Append after Reset returned error: %v", err)
	}
	if err := wal.Range(4, 10, func(Mutation) error { return nil }); !errors.Is(err, ErrWALRange) {
		t.Fatalf("Range across snapshot error = %v, want ErrWALRange", err)
	}
	if err := wal.Range(11, 11, func(Mutation) error { return nil }); err != nil {
		t.Fatalf("Range after snapshot returned error: %v", err)
	}
}

func TestWALRejectsDuplicateSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL returned error: %v", err)
	}
	defer wal.Close()

	if err := wal.Append(testMutation(2, "a", "one")); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := wal.Append(testMutation(2, "b", "two")); !errors.Is(err, ErrWALSequence) {
		t.Fatalf("duplicate Append error = %v, want ErrWALSequence", err)
	}
}

func TestWALValidationAndClose(t *testing.T) {
	if _, err := OpenWAL(""); err == nil {
		t.Fatal("OpenWAL(empty path) returned nil error")
	}

	invalidPath := filepath.Join(t.TempDir(), "invalid.wal")
	if err := os.WriteFile(invalidPath, []byte("not a wal"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := OpenWAL(invalidPath); err == nil {
		t.Fatal("OpenWAL(invalid header) returned nil error")
	}

	path := filepath.Join(t.TempDir(), "cache.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL returned error: %v", err)
	}
	if wal.Path() != path {
		t.Fatalf("Path() = %q, want %q", wal.Path(), path)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if err := wal.Append(testMutation(1, "a", "one")); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("Append after Close error = %v, want ErrWALClosed", err)
	}
	if _, err := wal.Replay(func(walRecord) error { return nil }); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("Replay after Close error = %v, want ErrWALClosed", err)
	}
}
