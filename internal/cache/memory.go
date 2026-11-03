package cache

import (
	"sync"
	"sync/atomic"
)

// InMemoryCache stores values in a map protected by an RWMutex.
//
// Reads take a shared lock and may run concurrently; writes take an exclusive
// lock. Statistics use atomics so recording a hit never upgrades Get to a
// write lock. Create instances with New and do not copy them after first use.
type InMemoryCache struct {
	mu    sync.RWMutex
	data  map[string]any
	stats counters
}

// counters holds the cumulative metrics. atomic.Uint64 guarantees alignment
// and race-free updates without taking the map lock.
type counters struct {
	hits    atomic.Uint64
	misses  atomic.Uint64
	sets    atomic.Uint64
	deletes atomic.Uint64
}

// Compile-time check that InMemoryCache satisfies Cache.
var _ Cache = (*InMemoryCache)(nil)

// New returns an empty, ready-to-use InMemoryCache.
func New() *InMemoryCache {
	return &InMemoryCache{
		data: make(map[string]any),
	}
}

// Set stores value under key, replacing any previous value.
func (c *InMemoryCache) Set(key string, value any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = value
	c.stats.sets.Add(1) // Count only after the write succeeds.
	return nil
}

// Get returns the value stored under key and whether it exists.
//
// The map lookup is protected by a read lock, while the hit/miss counter is
// updated atomically after the lock is released.
func (c *InMemoryCache) Get(key string) (any, bool) {
	c.mu.RLock()
	value, found := c.data[key]
	c.mu.RUnlock()

	if found {
		c.stats.hits.Add(1)
		return value, true
	}

	c.stats.misses.Add(1)
	return nil, false
}

// Delete removes key. Deleting a missing key is a no-op and still counts as a
// delete operation.
func (c *InMemoryCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
	c.stats.deletes.Add(1)
	return nil
}

// Stats returns a snapshot of the cumulative counters.
//
// No map lock is needed because the counters are atomic, so metrics collection
// does not block reads or writes.
func (c *InMemoryCache) Stats() CacheStats {
	return CacheStats{
		Hits:    c.stats.hits.Load(),
		Misses:  c.stats.misses.Load(),
		Sets:    c.stats.sets.Load(),
		Deletes: c.stats.deletes.Load(),
	}
}
