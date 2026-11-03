package cache

import "time"

// StartCleanupWorker launches the background expiration sweeper when
// CleanupInterval is positive. Calling it while a worker is already running
// has no effect.
func (c *InMemoryCache) StartCleanupWorker() {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()

	if c.config.CleanupInterval <= 0 || c.stopCh != nil {
		return
	}

	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	go c.cleanupLoop(c.stopCh, c.doneCh)
}

// StopCleanupWorker stops the sweeper and waits for its goroutine to exit.
// It is safe to call multiple times or when no worker is running.
func (c *InMemoryCache) StopCleanupWorker() {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()

	if c.stopCh == nil {
		return
	}

	close(c.stopCh)
	<-c.doneCh // Wait for the goroutine to exit; no leak.
	c.stopCh = nil
	c.doneCh = nil
}

// cleanupLoop sweeps expired entries on every tick until stop is closed.
func (c *InMemoryCache) cleanupLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.removeExpired()
		case <-stop:
			return
		}
	}
}

// removeExpired deletes every entry whose TTL has elapsed.
//
// The sweep runs in the background, but it holds the cache mutex while scanning
// so it always observes a consistent map.
func (c *InMemoryCache) removeExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, item := range c.data {
		if item.isExpired(now) {
			c.removeLocked(key)
			c.stats.expired.Add(1)
		}
	}
}
