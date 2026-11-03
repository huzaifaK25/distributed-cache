package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

// InMemoryCache stores values in a map with TTL metadata and LRU ordering.
//
// A single Mutex guards the map and LRU list. Phase 1 used an RWMutex, but Get
// now updates LastAccessed and recency order, so every read mutates state and
// an exclusive lock is both simpler and cheaper than an unusable read lock.
//
// Create instances with New and do not copy them after first use.
type InMemoryCache struct {
	mu     sync.Mutex
	data   map[string]*CacheItem
	lru    *lruList
	stats  counters
	config Config

	workerMu sync.Mutex
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// counters holds the cumulative metrics. atomic.Uint64 guarantees alignment
// and race-free snapshots without taking the cache lock.
type counters struct {
	hits      atomic.Uint64
	misses    atomic.Uint64
	sets      atomic.Uint64
	deletes   atomic.Uint64
	expired   atomic.Uint64
	evictions atomic.Uint64
}

// Compile-time check that InMemoryCache satisfies Cache.
var _ Cache = (*InMemoryCache)(nil)

// New returns an empty cache and starts the cleanup worker when
// config.CleanupInterval is positive.
func New(config Config) *InMemoryCache {
	c := &InMemoryCache{
		data:   make(map[string]*CacheItem),
		lru:    newLRUList(),
		config: config,
	}

	c.StartCleanupWorker()
	return c
}

// Set stores value under key without expiration, replacing any existing value
// and clearing any previous TTL.
func (c *InMemoryCache) Set(key string, value any) error {
	c.set(key, value, 0)
	return nil
}

// SetWithTTL stores value under key and expires it after ttl.
func (c *InMemoryCache) SetWithTTL(key string, value any, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}

	c.set(key, value, ttl)
	return nil
}

// set stores a value with an optional TTL and updates recency metadata.
func (c *InMemoryCache) set(key string, value any, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}

	if item, found := c.data[key]; found {
		item.Value = value
		item.CreatedAt = now
		item.ExpiresAt = expiresAt
		c.touchLocked(key, item, now)
		c.stats.sets.Add(1)
		return
	}

	c.makeRoomLocked()
	c.lru.add(key)
	c.data[key] = &CacheItem{
		Value:        value,
		CreatedAt:    now,
		ExpiresAt:    expiresAt,
		LastAccessed: now,
	}
	c.stats.sets.Add(1)
}

// Get returns the value stored under key and whether it exists.
//
// Expired entries are removed, counted, and reported as a cache miss.
func (c *InMemoryCache) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, found := c.data[key]
	if !found {
		c.stats.misses.Add(1)
		return nil, false
	}

	now := time.Now()
	if item.isExpired(now) {
		c.removeLocked(key)
		c.stats.expired.Add(1)
		c.stats.misses.Add(1)
		return nil, false
	}

	c.touchLocked(key, item, now)
	c.stats.hits.Add(1)
	return item.Value, true
}

// Delete removes key. Deleting a missing key is a no-op and still counts as a
// delete operation.
func (c *InMemoryCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, found := c.data[key]; found {
		c.removeLocked(key)
	}
	c.stats.deletes.Add(1)
	return nil
}

// Stats returns a snapshot of the cumulative counters.
//
// No cache lock is needed because the counters are atomic, so metrics
// collection does not block reads or writes.
func (c *InMemoryCache) Stats() CacheStats {
	return CacheStats{
		Hits:      c.stats.hits.Load(),
		Misses:    c.stats.misses.Load(),
		Sets:      c.stats.sets.Load(),
		Deletes:   c.stats.deletes.Load(),
		Expired:   c.stats.expired.Load(),
		Evictions: c.stats.evictions.Load(),
	}
}
