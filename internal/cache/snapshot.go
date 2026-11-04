package cache

import (
	"sort"
	"time"
)

// SnapshotItem is a point-in-time copy of one live cache entry.
//
// TTL is the remaining lifetime. A zero TTL means the entry never expires;
// a negative TTL denotes an entry that had already expired when the snapshot
// was taken.
type SnapshotItem struct {
	Key   string
	Value any
	TTL   time.Duration
}

// Snapshot returns a consistent copy of every live entry, sorted by key.
//
// Expired entries encountered while copying are removed and counted exactly
// like a lazy Get miss. The returned slice is independent of the cache, but
// the values themselves are shared.
func (c *InMemoryCache) Snapshot() []SnapshotItem {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	items := make([]SnapshotItem, 0, len(c.data))
	for key, item := range c.data {
		if item.isExpired(now) {
			c.removeLocked(key)
			c.stats.expired.Add(1)
			continue
		}

		var ttl time.Duration
		if !item.ExpiresAt.IsZero() {
			ttl = item.ExpiresAt.Sub(now)
		}
		items = append(items, SnapshotItem{
			Key:   key,
			Value: item.Value,
			TTL:   ttl,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})
	return items
}

// Restore replaces the cache contents with items.
//
// Restore is intended for replication bootstrap and recovery, not for normal
// writes. Expired entries are discarded, and MaxItems still applies: when the
// snapshot is larger than this cache, the first restored entries are evicted
// in favor of later ones. Cumulative statistics are left untouched.
func (c *InMemoryCache) Restore(items []SnapshotItem) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[string]*CacheItem, len(items))
	c.lru = newLRUList()

	now := time.Now()
	for _, item := range items {
		if item.TTL < 0 {
			continue
		}

		var expiresAt time.Time
		if item.TTL > 0 {
			expiresAt = now.Add(item.TTL)
		}

		if _, exists := c.data[item.Key]; exists {
			c.removeLocked(item.Key)
		}
		c.makeRoomLocked()

		c.data[item.Key] = &CacheItem{
			Value:        item.Value,
			CreatedAt:    now,
			ExpiresAt:    expiresAt,
			LastAccessed: now,
		}
		c.lru.add(item.Key)
	}
}
