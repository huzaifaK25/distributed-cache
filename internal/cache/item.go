package cache

import "time"

// CacheItem is a cached value plus the metadata needed for TTL and LRU.
type CacheItem struct {
	Value        any
	CreatedAt    time.Time
	ExpiresAt    time.Time // Zero means the item never expires.
	LastAccessed time.Time
}

// isExpired reports whether the item has a TTL that has elapsed at now.
func (i *CacheItem) isExpired(now time.Time) bool {
	return !i.ExpiresAt.IsZero() && !now.Before(i.ExpiresAt)
}
