package server

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPoolLimitsConcurrency(t *testing.T) {
	const (
		workers = 4
		jobs    = 32
	)

	pool := newWorkerPool(workers, jobs)
	defer pool.Stop()

	var (
		active    atomic.Int32
		maxActive atomic.Int32
		wg        sync.WaitGroup
	)

	for i := 0; i < jobs; i++ {
		wg.Add(1)
		err := pool.Submit(func() {
			defer wg.Done()

			current := active.Add(1)
			for {
				observed := maxActive.Load()
				if current <= observed || maxActive.CompareAndSwap(observed, current) {
					break
				}
			}

			time.Sleep(5 * time.Millisecond)
			active.Add(-1)
		})
		if err != nil {
			t.Fatalf("Submit() returned error: %v", err)
		}
	}

	wg.Wait()

	if got := maxActive.Load(); got > workers {
		t.Fatalf("max concurrent jobs = %d, want <= %d", got, workers)
	}
	if got := maxActive.Load(); got < 2 {
		t.Fatalf("max concurrent jobs = %d, want the pool to run jobs concurrently", got)
	}
}

func TestWorkerPoolRejectsWhenQueueIsFull(t *testing.T) {
	pool := newWorkerPool(1, 1)
	defer pool.Stop()

	started := make(chan struct{})
	release := make(chan struct{})

	if err := pool.Submit(func() {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("first Submit() returned error: %v", err)
	}

	<-started // The only worker is now blocked.

	if err := pool.Submit(func() {}); err != nil {
		t.Fatalf("second Submit() returned error: %v", err)
	}
	if err := pool.Submit(func() {}); !errors.Is(err, errPoolFull) {
		t.Fatalf("third Submit() error = %v, want queue-full error", err)
	}

	close(release)
	pool.Stop()
}

func TestWorkerPoolStopDrainsQueuedJobs(t *testing.T) {
	const jobs = 50

	pool := newWorkerPool(3, jobs)

	var completed atomic.Int32
	for i := 0; i < jobs; i++ {
		if err := pool.Submit(func() {
			completed.Add(1)
		}); err != nil {
			t.Fatalf("Submit() returned error: %v", err)
		}
	}

	pool.Stop()

	if got := completed.Load(); got != jobs {
		t.Fatalf("completed jobs = %d, want %d", got, jobs)
	}
	if err := pool.Submit(func() {}); !errors.Is(err, errPoolClosed) {
		t.Fatalf("Submit() after Stop error = %v, want pool-closed error", err)
	}

	// Stop must be safe to call again.
	pool.Stop()
}

func TestWorkerPoolSurvivesPanic(t *testing.T) {
	pool := newWorkerPool(1, 2)
	defer pool.Stop()

	done := make(chan struct{})
	if err := pool.Submit(func() {
		panic("boom")
	}); err != nil {
		t.Fatalf("Submit(panic) returned error: %v", err)
	}
	if err := pool.Submit(func() {
		close(done)
	}); err != nil {
		t.Fatalf("Submit(after panic) returned error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not process a job after a panic")
	}
}
