package replication

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	walMagic        = "DCWAL1\n"
	walHeaderSize   = 8 // 4-byte payload length followed by 4-byte CRC-32.
	walMaxRecord    = 64 << 20
	walKindMutation = "mutation"
	walKindSnapshot = "snapshot"
)

// WAL errors.
var (
	ErrWALClosed   = errors.New("replication: wal is closed")
	ErrWALSequence = errors.New("replication: wal sequence must increase")
	ErrWALRange    = errors.New("replication: wal does not contain the requested range")
)

// errWALTorn marks an incomplete or invalid trailing record. A crash can leave
// one of these at the end of the file; recovery truncates it.
var errWALTorn = errors.New("replication: torn wal record")

// walRecord is one durable entry: either a mutation or a full snapshot basis.
type walRecord struct {
	Kind     string          `json:"kind"`
	Seq      uint64          `json:"seq"`
	Mutation *Mutation       `json:"mutation,omitempty"`
	Snapshot []SnapshotEntry `json:"snapshot,omitempty"`
}

// WAL is an append-only, checksummed write-ahead log.
//
// Records are length-prefixed JSON with a CRC-32 trailer. Every append is
// fsynced before success is reported. OpenWAL scans and repairs a torn tail
// left by a crash, so later appends never follow a partial record.
type WAL struct {
	mu      sync.Mutex
	file    *os.File
	path    string
	size    int64
	lastSeq uint64
	closed  bool
}

// OpenWAL opens or creates path and recovers the last durable sequence.
func OpenWAL(path string) (*WAL, error) {
	if path == "" {
		return nil, errors.New("replication: wal path must not be empty")
	}

	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("replication: create wal directory: %w", err)
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("replication: open wal: %w", err)
	}

	wal := &WAL{file: file, path: path}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("replication: stat wal: %w", err)
	}

	if info.Size() == 0 {
		if _, err := file.Write([]byte(walMagic)); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("replication: initialize wal: %w", err)
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("replication: sync wal header: %w", err)
		}
		wal.size = int64(len(walMagic))
		return wal, nil
	}

	if err := wal.recoverTail(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return wal, nil
}

// recoverTail validates the file, records the last sequence, and truncates an
// incomplete or corrupt trailing record.
func (w *WAL) recoverTail() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("replication: seek wal: %w", err)
	}

	magic := make([]byte, len(walMagic))
	if _, err := io.ReadFull(w.file, magic); err != nil {
		return fmt.Errorf("replication: read wal header: %w", err)
	}
	if string(magic) != walMagic {
		return errors.New("replication: invalid wal header")
	}

	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("replication: stat wal: %w", err)
	}

	offset := int64(len(walMagic))
	lastGood := offset
	var lastSeq uint64

	for {
		record, bytesRead, err := readWALRecord(w.file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Any malformed record is treated as the crash boundary. There is
			// no way to distinguish a torn tail from mid-file corruption when
			// scanning forward, so everything from this point is discarded.
			break
		}
		if err := validateWALSequence(record, lastSeq); err != nil {
			break
		}

		lastSeq = record.Seq
		offset += bytesRead
		lastGood = offset
	}

	if info.Size() > lastGood {
		if err := w.file.Truncate(lastGood); err != nil {
			return fmt.Errorf("replication: truncate torn wal: %w", err)
		}
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("replication: sync repaired wal: %w", err)
		}
	}

	w.size = lastGood
	w.lastSeq = lastSeq
	if _, err := w.file.Seek(w.size, io.SeekStart); err != nil {
		return fmt.Errorf("replication: seek wal end: %w", err)
	}
	return nil
}

// validateWALSequence rejects records that would make replay ambiguous.
func validateWALSequence(record walRecord, lastSeq uint64) error {
	if record.Seq < lastSeq {
		return fmt.Errorf("%w: record sequence regressed", errWALTorn)
	}

	switch record.Kind {
	case walKindMutation:
		if record.Mutation == nil || record.Mutation.Seq != record.Seq || record.Seq <= lastSeq {
			return fmt.Errorf("%w: invalid mutation record", errWALTorn)
		}
	case walKindSnapshot:
		// A snapshot record is the first record after Reset, but accepting a
		// later one is harmless because it establishes a new durable basis.
	default:
		return fmt.Errorf("%w: unknown record kind %q", errWALTorn, record.Kind)
	}
	return nil
}

// Append durably records one mutation. The caller owns sequence assignment and
// must pass a sequence greater than the log's last sequence.
func (w *WAL) Append(mutation Mutation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.file == nil {
		return ErrWALClosed
	}
	if mutation.Seq <= w.lastSeq {
		return fmt.Errorf("%w: last %d, new %d", ErrWALSequence, w.lastSeq, mutation.Seq)
	}
	if err := validateMutation(mutation); err != nil {
		return err
	}

	record := walRecord{
		Kind:     walKindMutation,
		Seq:      mutation.Seq,
		Mutation: &mutation,
	}
	if err := w.appendRecord(record); err != nil {
		return err
	}
	w.lastSeq = mutation.Seq
	return nil
}

// Reset truncates the log and writes a full snapshot as the new durable basis.
// It is used when a replica bootstraps: the snapshot plus later mutations are
// sufficient to reconstruct the cache after a restart.
func (w *WAL) Reset(seq uint64, entries []SnapshotEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.file == nil {
		return ErrWALClosed
	}

	record := walRecord{Kind: walKindSnapshot, Seq: seq, Snapshot: entries}
	encoded, err := encodeWALRecord(record)
	if err != nil {
		return err
	}

	temp := w.path + ".tmp"
	_ = os.Remove(temp)

	tempFile, err := os.OpenFile(temp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("replication: create wal snapshot: %w", err)
	}

	contents := make([]byte, 0, len(walMagic)+len(encoded))
	contents = append(contents, walMagic...)
	contents = append(contents, encoded...)
	if _, err := tempFile.Write(contents); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(temp)
		return fmt.Errorf("replication: write wal snapshot: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(temp)
		return fmt.Errorf("replication: sync wal snapshot: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("replication: close wal snapshot: %w", err)
	}

	// Windows cannot replace an open file, so release the old descriptor
	// before the atomic rename.
	if err := w.file.Close(); err != nil {
		_ = os.Remove(temp)
		w.closed = true
		return fmt.Errorf("replication: close wal: %w", err)
	}
	if err := os.Rename(temp, w.path); err != nil {
		_ = os.Remove(temp)
		w.file, _ = os.OpenFile(w.path, os.O_RDWR, 0o644)
		if w.file == nil {
			w.closed = true
		}
		return fmt.Errorf("replication: replace wal: %w", err)
	}

	file, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		w.file = nil
		w.closed = true
		return fmt.Errorf("replication: reopen wal: %w", err)
	}
	w.file = file
	w.size = int64(len(contents))
	w.lastSeq = seq
	return nil
}

// Replay calls fn for every record in order and returns the last sequence.
//
// Replay is intended for startup, before the WAL is used concurrently.
func (w *WAL) Replay(fn func(walRecord) error) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.file == nil {
		return 0, ErrWALClosed
	}

	file, err := os.Open(w.path)
	if err != nil {
		return 0, fmt.Errorf("replication: open wal for replay: %w", err)
	}
	defer file.Close()

	if err := readWALMagic(file); err != nil {
		return 0, err
	}

	var lastSeq uint64
	for {
		record, _, err := readWALRecord(file)
		if errors.Is(err, io.EOF) {
			return lastSeq, nil
		}
		if err != nil {
			return lastSeq, err
		}
		if err := fn(record); err != nil {
			return lastSeq, err
		}
		lastSeq = record.Seq
	}
}

// Range streams mutations in [from, to] to fn.
//
// Range returns ErrWALRange when the log cannot supply a contiguous sequence
// starting at from; the caller should bootstrap from a full snapshot instead.
func (w *WAL) Range(from, to uint64, fn func(Mutation) error) error {
	if from > to {
		return nil
	}

	last := w.LastSeq()
	if from > last+1 {
		return fmt.Errorf("%w: need %d, log ends at %d", ErrWALRange, from, last)
	}
	if from == last+1 {
		return nil
	}

	file, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("replication: open wal for range: %w", err)
	}
	defer file.Close()

	if err := readWALMagic(file); err != nil {
		return err
	}

	expected := from
	started := false
	for {
		record, _, err := readWALRecord(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// An append may be mid-write past the requested range. Treat a
			// torn tail as the current end of available history; the final
			// expected check below still reports a real gap.
			if errors.Is(err, errWALTorn) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		if record.Seq < from {
			continue
		}
		if record.Seq > to {
			break
		}

		if !started {
			if record.Kind != walKindMutation || record.Seq != from {
				return fmt.Errorf("%w: no mutation at %d", ErrWALRange, from)
			}
			started = true
		} else if record.Kind != walKindMutation || record.Seq != expected {
			return fmt.Errorf("%w: expected %d, found %d", ErrWALRange, expected, record.Seq)
		}

		if err := fn(*record.Mutation); err != nil {
			return err
		}
		expected++
	}

	if expected <= to {
		return fmt.Errorf("%w: wanted through %d, found through %d", ErrWALRange, to, expected-1)
	}
	return nil
}

// LastSeq returns the highest durable sequence.
func (w *WAL) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeq
}

// Path returns the log file path.
func (w *WAL) Path() string {
	return w.path
}

// Close flushes and closes the log. Close is idempotent.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	if w.file == nil {
		return nil
	}

	syncErr := w.file.Sync()
	closeErr := w.file.Close()
	w.file = nil
	return errors.Join(syncErr, closeErr)
}

// appendRecord writes and fsyncs one framed record.
func (w *WAL) appendRecord(record walRecord) error {
	encoded, err := encodeWALRecord(record)
	if err != nil {
		return err
	}

	start := w.size
	if _, err := w.file.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("replication: seek wal: %w", err)
	}
	if _, err := w.file.Write(encoded); err != nil {
		w.rollback(start)
		return fmt.Errorf("replication: write wal: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		w.rollback(start)
		return fmt.Errorf("replication: sync wal: %w", err)
	}

	w.size = start + int64(len(encoded))
	return nil
}

// rollback removes a partial append so the next record starts on clean bytes.
func (w *WAL) rollback(size int64) {
	if err := w.file.Truncate(size); err == nil {
		_, _ = w.file.Seek(size, io.SeekStart)
		w.size = size
	}
}

// encodeWALRecord returns the length, checksum, and JSON payload for record.
func encodeWALRecord(record walRecord) ([]byte, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("replication: encode wal record: %w", err)
	}
	if len(payload) == 0 || len(payload) > walMaxRecord {
		return nil, fmt.Errorf("replication: wal record size %d exceeds limit %d", len(payload), walMaxRecord)
	}

	encoded := make([]byte, walHeaderSize+len(payload))
	binary.BigEndian.PutUint32(encoded[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(encoded[4:8], crc32.ChecksumIEEE(payload))
	copy(encoded[walHeaderSize:], payload)
	return encoded, nil
}

// readWALRecord reads one framed record and reports its encoded size.
func readWALRecord(r io.Reader) (walRecord, int64, error) {
	var header [walHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return walRecord{}, 0, err
	}

	size := binary.BigEndian.Uint32(header[0:4])
	checksum := binary.BigEndian.Uint32(header[4:8])
	if size == 0 || size > walMaxRecord {
		return walRecord{}, walHeaderSize, errWALTorn
	}

	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return walRecord{}, walHeaderSize, errWALTorn
	}
	if crc32.ChecksumIEEE(payload) != checksum {
		return walRecord{}, walHeaderSize + int64(size), errWALTorn
	}

	var record walRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return walRecord{}, walHeaderSize + int64(size), fmt.Errorf("%w: %v", errWALTorn, err)
	}
	return record, walHeaderSize + int64(size), nil
}

// readWALMagic validates the file preamble.
func readWALMagic(r io.Reader) error {
	magic := make([]byte, len(walMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return fmt.Errorf("replication: read wal header: %w", err)
	}
	if string(magic) != walMagic {
		return errors.New("replication: invalid wal header")
	}
	return nil
}
