package replication

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// acceptLoop accepts replica connections until the listener closes.
func (c *Coordinator) acceptLoop() {
	defer c.wg.Done()

	for {
		conn, err := c.listener.Accept()
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return
		}
		if c.ctx.Err() != nil {
			_ = conn.Close()
			return
		}

		c.registerConn(conn)
		c.wg.Add(1)
		go c.handleSession(conn)
	}
}

// handleSession performs the hello handshake and serves one replica.
func (c *Coordinator) handleSession(conn net.Conn) {
	defer c.wg.Done()
	defer c.unregisterConn(conn)
	defer conn.Close()

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	if err := conn.SetDeadline(time.Now().Add(c.config.HandshakeTimeout)); err != nil {
		return
	}
	hello, err := readWireMessage(conn, c.config.MaxMessageSize)
	if err != nil {
		return
	}
	if hello.Type != messageHello || hello.ID == "" {
		_ = c.writeWithDeadline(conn, wireMessage{
			Type:  messageError,
			Error: "hello with a non-empty replica id is required",
		})
		return
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return
	}

	session := &replicaSession{
		coord:      c,
		conn:       conn,
		id:         hello.ID,
		address:    conn.RemoteAddr().String(),
		queue:      make(chan Mutation, c.config.SessionQueue),
		ackNotify:  make(chan struct{}, 1),
		done:       make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	c.addSession(session)
	defer c.removeSession(session)

	if err := session.run(hello); err != nil && c.ctx.Err() == nil {
		c.recordError(fmt.Errorf("replication: replica %s: %w", hello.ID, err))
	}
}

// replicaSession is one connected replica from the primary's perspective.
type replicaSession struct {
	coord   *Coordinator
	conn    net.Conn
	id      string
	address string

	queue      chan Mutation
	ackNotify  chan struct{}
	done       chan struct{}
	readerDone chan struct{}
	closeOnce  sync.Once

	errMu    sync.Mutex
	closeErr error

	delivered    atomic.Uint64
	failed       atomic.Uint64
	inFlight     atomic.Int64
	ackedSeq     atomic.Uint64
	pendingSince atomic.Int64
	syncedAt     atomic.Int64
}

// run starts the ack reader, synchronizes, and then streams live mutations.
func (s *replicaSession) run(hello wireMessage) error {
	s.ackedSeq.Store(hello.LastSeq)
	go s.readLoop()

	err := s.syncAndServe(hello)
	s.close()
	<-s.readerDone
	return err
}

// syncAndServe selects full or incremental synchronization before going live.
func (s *replicaSession) syncAndServe(hello wireMessage) error {
	current := s.coord.rep.seqValue()

	// A zero sequence means a new replica; a sequence ahead of the primary
	// means the primary lost its log and must re-establish authority.
	if hello.Full || hello.LastSeq == 0 || hello.LastSeq > current {
		if err := s.fullSync(); err != nil {
			return err
		}
		return s.live()
	}

	if hello.LastSeq < current {
		err := s.incremental(hello.LastSeq+1, current)
		if errors.Is(err, ErrWALRange) {
			// The primary cannot supply every intermediate mutation, so send
			// the current state instead.
			if err := s.fullSync(); err != nil {
				return err
			}
			return s.live()
		}
		if err != nil {
			return err
		}
	}

	s.syncedAt.Store(time.Now().UnixNano())
	return s.live()
}

// fullSync sends a consistent snapshot with bounded chunks and waits for the
// replica's acknowledgement. The whole snapshot is retried on timeout.
func (s *replicaSession) fullSync() error {
	seq, entries, err := s.coord.rep.snapshot()
	if err != nil {
		return err
	}
	chunks, err := chunkSnapshot(entries, s.coord.config.SnapshotBatch, s.coord.config.MaxMessageSize)
	if err != nil {
		return err
	}

	// A replica may have reported a higher sequence from an older primary
	// incarnation. Reset the local ack watermark before waiting on this one.
	s.ackedSeq.Store(0)

	for attempt := 0; attempt < s.coord.config.MaxRetries; attempt++ {
		started := time.Now()

		if err := s.write(wireMessage{Type: messageSnapshotStart, Seq: seq}); err != nil {
			s.failed.Add(1)
			return err
		}
		for _, chunk := range chunks {
			if err := s.write(wireMessage{Type: messageSnapshotChunk, Entries: chunk}); err != nil {
				s.failed.Add(1)
				return err
			}
		}
		if err := s.write(wireMessage{Type: messageSnapshotEnd, Seq: seq}); err != nil {
			s.failed.Add(1)
			return err
		}

		err := s.waitAck(seq, s.coord.config.AckTimeout)
		if err == nil {
			s.ackedSeq.Store(seq)
			s.syncedAt.Store(time.Now().UnixNano())
			s.coord.setSyncTime(time.Since(started))
			return nil
		}
		if !errors.Is(err, ErrAckTimeout) {
			return err
		}
		s.failed.Add(1)
	}

	return fmt.Errorf("%w: full snapshot through %d", ErrAckTimeout, seq)
}

// incremental streams the primary's WAL range in acknowledgement batches.
func (s *replicaSession) incremental(from, to uint64) error {
	batch := make([]Mutation, 0, s.coord.config.MaxBatch)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.sendBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	err := s.coord.wal.Range(from, to, func(mutation Mutation) error {
		batch = append(batch, mutation)
		if len(batch) >= s.coord.config.MaxBatch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}

	s.syncedAt.Store(time.Now().UnixNano())
	return nil
}

// live sends queued primary mutations until the session fails or stops.
func (s *replicaSession) live() error {
	for {
		var first Mutation
		select {
		case <-s.coord.ctx.Done():
			return s.coord.ctx.Err()
		case <-s.done:
			return s.sessionError()
		case first = <-s.queue:
		}

		batch := make([]Mutation, 1, s.coord.config.MaxBatch)
		batch[0] = first

	drain:
		for len(batch) < s.coord.config.MaxBatch {
			select {
			case <-s.done:
				return s.sessionError()
			case mutation := <-s.queue:
				batch = append(batch, mutation)
			default:
				break drain
			}
		}

		if err := s.sendBatch(batch); err != nil {
			return err
		}
	}
}

// sendBatch writes one append frame and waits for a cumulative acknowledgement.
// A timed-out batch is resent; the replica ignores sequences it already has.
func (s *replicaSession) sendBatch(batch []Mutation) error {
	if len(batch) == 0 {
		return nil
	}

	// A count-based batch can still be too large when values are big. Split
	// until each append frame fits instead of failing the session.
	if err := checkAppendFrame(batch, s.coord.config.MaxMessageSize); err != nil {
		if !errors.Is(err, ErrValueTooLarge) || len(batch) == 1 {
			return err
		}
		middle := len(batch) / 2
		if err := s.sendBatch(batch[:middle]); err != nil {
			return err
		}
		return s.sendBatch(batch[middle:])
	}

	if s.pendingSince.Load() == 0 {
		s.pendingSince.Store(time.Now().UnixNano())
	}
	s.inFlight.Store(int64(len(batch)))
	last := batch[len(batch)-1].Seq

	for attempt := 0; attempt < s.coord.config.MaxRetries; attempt++ {
		if err := s.write(wireMessage{Type: messageAppend, Mutations: batch}); err != nil {
			s.failed.Add(1)
			s.inFlight.Store(0)
			return err
		}

		err := s.waitAck(last, s.coord.config.AckTimeout)
		if err == nil {
			s.delivered.Add(uint64(len(batch)))
			s.inFlight.Store(0)
			if len(s.queue) == 0 {
				s.pendingSince.Store(0)
			}
			if s.ackedSeq.Load() < last {
				s.ackedSeq.Store(last)
			}
			return nil
		}
		if !errors.Is(err, ErrAckTimeout) {
			s.inFlight.Store(0)
			return err
		}
		s.failed.Add(1)
	}

	s.inFlight.Store(0)
	return fmt.Errorf("%w: batch through %d", ErrAckTimeout, last)
}

// write sends one frame with the configured write deadline.
func (s *replicaSession) write(message wireMessage) error {
	return s.coord.writeWithDeadline(s.conn, message)
}

// enqueue buffers a live mutation. A full queue disconnects the replica so it
// can reconnect and catch up from the WAL instead of silently losing data.
func (s *replicaSession) enqueue(mutation Mutation) {
	select {
	case <-s.done:
		return
	default:
	}

	select {
	case s.queue <- mutation:
		if s.inFlight.Load() == 0 && len(s.queue) == 1 && s.pendingSince.Load() == 0 {
			s.pendingSince.Store(time.Now().UnixNano())
		}
	case <-s.done:
	default:
		s.failed.Add(1)
		s.fail(ErrQueueFull)
	}
}

// readLoop consumes cumulative acknowledgements from the replica.
func (s *replicaSession) readLoop() {
	defer close(s.readerDone)

	for {
		message, err := readWireMessage(s.conn, s.coord.config.MaxMessageSize)
		if err != nil {
			s.fail(err)
			return
		}

		switch message.Type {
		case messageAck:
			s.observeAck(message.Seq)
		case messageError:
			s.fail(fmt.Errorf("replication: replica reported: %s", message.Error))
			return
		default:
			s.fail(fmt.Errorf("replication: unexpected replica message %q", message.Type))
			return
		}
	}
}

// observeAck advances the cumulative acknowledgement watermark.
func (s *replicaSession) observeAck(seq uint64) {
	for {
		current := s.ackedSeq.Load()
		if seq <= current {
			return
		}
		if s.ackedSeq.CompareAndSwap(current, seq) {
			select {
			case s.ackNotify <- struct{}{}:
			default:
			}
			return
		}
	}
}

// waitAck waits until the replica acknowledges seq or the timeout elapses.
func (s *replicaSession) waitAck(seq uint64, timeout time.Duration) error {
	if s.ackedSeq.Load() >= seq {
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-s.done:
			return s.sessionError()
		case <-s.ackNotify:
			if s.ackedSeq.Load() >= seq {
				return nil
			}
		case <-timer.C:
			return ErrAckTimeout
		}
	}
}

// fail records the first session error and closes the connection.
func (s *replicaSession) fail(err error) {
	if err == nil {
		err = ErrSessionClosed
	}
	s.errMu.Lock()
	if s.closeErr == nil {
		s.closeErr = err
	}
	s.errMu.Unlock()
	s.close()
}

// close unblocks both the writer and the reader. It is idempotent.
func (s *replicaSession) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		_ = s.conn.Close()
	})
}

// sessionError returns the recorded failure or ErrSessionClosed.
func (s *replicaSession) sessionError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.closeErr != nil {
		return s.closeErr
	}
	return ErrSessionClosed
}
