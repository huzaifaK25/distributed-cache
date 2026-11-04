package replication

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"
)

// Replication wire message types.
type messageType string

const (
	messageHello         messageType = "hello"
	messageSnapshotStart messageType = "snapshot_start"
	messageSnapshotChunk messageType = "snapshot_chunk"
	messageSnapshotEnd   messageType = "snapshot_end"
	messageAppend        messageType = "append"
	messageAck           messageType = "ack"
	messageError         messageType = "error"
)

const frameHeaderSize = 4

// wireMessage is the single envelope used by the replication protocol.
type wireMessage struct {
	Type      messageType     `json:"type"`
	ID        string          `json:"id,omitempty"`
	LastSeq   uint64          `json:"last_seq,omitempty"`
	Full      bool            `json:"full,omitempty"`
	Seq       uint64          `json:"seq,omitempty"`
	Mutations []Mutation      `json:"mutations,omitempty"`
	Entries   []SnapshotEntry `json:"entries,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// checkMutationFrame rejects writes that cannot fit in one replication frame.
func checkMutationFrame(mutation Mutation, maxFrame int) error {
	return checkAppendFrame([]Mutation{mutation}, maxFrame)
}

// checkAppendFrame reports whether a batch fits in one append message.
func checkAppendFrame(mutations []Mutation, maxFrame int) error {
	if maxFrame <= 0 {
		return nil
	}
	payload, err := json.Marshal(wireMessage{
		Type:      messageAppend,
		Mutations: mutations,
	})
	if err != nil {
		return fmt.Errorf("replication: encode mutation frame: %w", err)
	}
	if len(payload) > maxFrame {
		return fmt.Errorf("%w: %d mutations encode to %d bytes, limit %d",
			ErrValueTooLarge, len(mutations), len(payload), maxFrame)
	}
	return nil
}

// chunkSnapshot splits entries into frames that respect both the entry-count
// batch size and the encoded message limit.
func chunkSnapshot(entries []SnapshotEntry, batchSize, maxFrame int) ([][]SnapshotEntry, error) {
	if batchSize <= 0 {
		batchSize = 1
	}

	// Reserve room for the envelope, type, and sequence fields.
	budget := maxFrame - 512
	if budget <= 0 {
		return nil, fmt.Errorf("%w: frame limit %d is too small", ErrValueTooLarge, maxFrame)
	}

	var chunks [][]SnapshotEntry
	var current []SnapshotEntry
	used := 0

	for _, entry := range entries {
		encoded, err := json.Marshal(entry)
		if err != nil {
			return nil, fmt.Errorf("replication: encode snapshot key %q: %w", entry.Key, err)
		}
		size := len(encoded) + 1

		// A single large entry may exceed the batch budget but still fit in
		// its own frame. Check the exact frame before rejecting it.
		if size > budget {
			single, err := json.Marshal(wireMessage{
				Type:    messageSnapshotChunk,
				Entries: []SnapshotEntry{entry},
			})
			if err != nil {
				return nil, fmt.Errorf("replication: encode snapshot key %q: %w", entry.Key, err)
			}
			if len(single) > maxFrame {
				return nil, fmt.Errorf("%w: snapshot key %q encodes to %d bytes, limit %d",
					ErrValueTooLarge, entry.Key, len(single), maxFrame)
			}

			if len(current) > 0 {
				chunks = append(chunks, current)
				current = nil
				used = 0
			}
			chunks = append(chunks, []SnapshotEntry{entry})
			continue
		}

		if len(current) > 0 && (len(current) >= batchSize || used+size > budget) {
			chunks = append(chunks, current)
			current = nil
			used = 0
		}
		current = append(current, entry)
		used += size
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks, nil
}

// writeWireMessage encodes message as a 4-byte length followed by JSON.
func writeWireMessage(w io.Writer, message wireMessage, maxMessageSize int) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("replication: encode message: %w", err)
	}
	if len(payload) == 0 || len(payload) > maxMessageSize || uint64(len(payload)) > math.MaxUint32 {
		return fmt.Errorf("%w: message size %d, limit %d", ErrValueTooLarge, len(payload), maxMessageSize)
	}

	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

// readWireMessage reads and decodes one length-prefixed JSON message.
func readWireMessage(r io.Reader, maxMessageSize int) (wireMessage, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return wireMessage{}, err
	}

	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || uint64(size) > uint64(maxMessageSize) {
		return wireMessage{}, fmt.Errorf("replication: message size %d exceeds limit %d", size, maxMessageSize)
	}

	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return wireMessage{}, err
	}

	var message wireMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return wireMessage{}, fmt.Errorf("replication: decode message: %w", err)
	}
	return message, nil
}

// writeWithDeadline sends one message with a bounded write.
func (c *Coordinator) writeWithDeadline(conn net.Conn, message wireMessage) error {
	if err := conn.SetWriteDeadline(time.Now().Add(c.config.AckTimeout)); err != nil {
		return err
	}
	return writeWireMessage(conn, message, c.config.MaxMessageSize)
}

// sendAck acknowledges the highest applied sequence.
func (c *Coordinator) sendAck(conn net.Conn, seq uint64) error {
	return c.writeWithDeadline(conn, wireMessage{Type: messageAck, Seq: seq})
}

// pendingSnapshot accumulates a chunked full-sync snapshot from the primary.
type pendingSnapshot struct {
	seq     uint64
	started time.Time
	entries []SnapshotEntry
}

// syncLoop keeps a replica connected to its primary until shutdown.
func (c *Coordinator) syncLoop() {
	defer c.wg.Done()

	delay := c.config.ReconnectMin
	for {
		if c.ctx.Err() != nil {
			return
		}

		synced, err := c.syncOnce()
		if c.ctx.Err() != nil {
			return
		}
		if err != nil {
			c.recordError(err)
		}

		if synced {
			delay = c.config.ReconnectMin
		} else {
			delay *= 2
			if delay > c.config.ReconnectMax {
				delay = c.config.ReconnectMax
			}
		}

		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// syncOnce opens one primary connection and serves it until it fails.
// The boolean reports whether at least one snapshot or append was applied.
func (c *Coordinator) syncOnce() (bool, error) {
	dialCtx, cancel := context.WithTimeout(c.ctx, c.config.HandshakeTimeout)
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "tcp", c.config.PrimaryAddr)
	cancel()
	if err != nil {
		return false, fmt.Errorf("replication: dial primary %s: %w", c.config.PrimaryAddr, err)
	}

	c.registerConn(conn)
	defer c.unregisterConn(conn)
	defer conn.Close()

	// Cancellation during Stop must unblock a pending frame read.
	stopClose := context.AfterFunc(c.ctx, func() { _ = conn.Close() })
	defer stopClose()

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	if err := conn.SetDeadline(time.Now().Add(c.config.HandshakeTimeout)); err != nil {
		return false, err
	}
	hello := wireMessage{
		Type:    messageHello,
		ID:      c.config.ID,
		LastSeq: c.rep.seqValue(),
		Full:    c.forceFull.Load(),
	}
	if err := writeWireMessage(conn, hello, c.config.MaxMessageSize); err != nil {
		return false, fmt.Errorf("replication: send hello: %w", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return false, err
	}

	synced := false
	var snapshot *pendingSnapshot

	for {
		message, err := readWireMessage(conn, c.config.MaxMessageSize)
		if err != nil {
			return synced, err
		}

		switch message.Type {
		case messageSnapshotStart:
			if snapshot != nil {
				return synced, errors.New("replication: overlapping snapshot")
			}
			snapshot = &pendingSnapshot{seq: message.Seq, started: time.Now()}

		case messageSnapshotChunk:
			if snapshot == nil {
				return synced, errors.New("replication: snapshot chunk without start")
			}
			snapshot.entries = append(snapshot.entries, message.Entries...)

		case messageSnapshotEnd:
			if snapshot == nil || message.Seq != snapshot.seq {
				return synced, errors.New("replication: snapshot end does not match start")
			}
			if err := c.rep.restoreSnapshot(snapshot.seq, snapshot.entries); err != nil {
				c.forceFull.Store(true)
				return synced, err
			}
			c.forceFull.Store(false)
			if err := c.sendAck(conn, snapshot.seq); err != nil {
				return synced, err
			}
			c.setSyncTime(time.Since(snapshot.started))
			snapshot = nil
			synced = true

		case messageAppend:
			if snapshot != nil {
				return synced, errors.New("replication: append during snapshot")
			}
			if len(message.Mutations) == 0 {
				continue
			}
			for _, mutation := range message.Mutations {
				if _, err := c.rep.applyRemote(mutation); err != nil {
					c.forceFull.Store(true)
					return synced, err
				}
			}
			if err := c.sendAck(conn, message.Mutations[len(message.Mutations)-1].Seq); err != nil {
				return synced, err
			}
			synced = true

		case messageError:
			return synced, fmt.Errorf("replication: primary: %s", message.Error)

		default:
			return synced, fmt.Errorf("replication: unexpected message type %q", message.Type)
		}
	}
}
