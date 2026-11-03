// Package cache defines the cache contract and its in-memory implementation.
package cache

// Cache is the contract every cache backend must satisfy.
//
// Methods return errors even though the in-memory backend cannot currently
// fail; future networked or replicated backends will need them.
type Cache interface {
	// Set stores value under key, replacing any existing value.
	Set(key string, value any) error

	// Get returns the value stored under key and whether it was found.
	Get(key string) (any, bool)

	// Delete removes key. Deleting a missing key is not an error.
	Delete(key string) error

	// Stats returns a snapshot of the cache's cumulative counters.
	Stats() CacheStats
}
