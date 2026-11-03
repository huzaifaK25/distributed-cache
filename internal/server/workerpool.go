package server

import (
	"errors"
	"sync"
)

// Worker-pool errors.
var (
	errPoolClosed = errors.New("server: worker pool closed")
	errPoolFull   = errors.New("server: worker pool queue full")
)

// workerPool executes request jobs on a fixed number of goroutines.
//
// A bounded queue prevents an overloaded server from accumulating unbounded
// work. Submit never blocks: it returns a queue-full error when no slot is free.
type workerPool struct {
	jobs chan func()

	mu     sync.RWMutex
	closed bool

	wg sync.WaitGroup
}

// newWorkerPool starts workers goroutines and a queue of queueSize jobs.
func newWorkerPool(workers, queueSize int) *workerPool {
	p := &workerPool{
		jobs: make(chan func(), queueSize),
	}

	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

// worker consumes jobs until the queue is closed and drained.
func (p *workerPool) worker() {
	defer p.wg.Done()

	for job := range p.jobs {
		func() {
			// A panicking request must not kill a worker or the server.
			defer func() { _ = recover() }()
			job()
		}()
	}
}

// Submit enqueues job without blocking.
func (p *workerPool) Submit(job func()) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return errPoolClosed
	}

	select {
	case p.jobs <- job:
		return nil
	default:
		return errPoolFull
	}
}

// Stop closes the queue, lets workers drain queued jobs, and waits for them.
// It is safe to call more than once.
func (p *workerPool) Stop() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
	p.mu.Unlock()

	p.wg.Wait()
}
