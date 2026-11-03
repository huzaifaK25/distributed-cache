// Package cache defines the cache contract and its in-memory implementation.
package cache

import (
	"errors"
	"time"
)

// ErrInvalidTTL is returned when SetWithTTL receives a non-positive duration.
var ErrInvalidTTL = errors.New("cache: ttl must be positive")

// Cache is the contract every cache backend must satisfy.
//
// Methods return errors even though the in-memory backend cannot currently
// fail; future networked or replicated backends will need them.
type Cache interface {
	// Set stores value under key with no expiration, replacing any existing
	// value and clearing any existing TTL.
	Set(key string, value any) error

	// SetWithTTL stores value under key and expires it after ttl.
	// A non-positive ttl returns ErrInvalidTTL.
	SetWithTTL(key string, value any, ttl time.Duration) error

	// Get returns the value stored under key and whether it was found.
	// Expired entries are removed and reported as missing.
	Get(key string) (any, bool)

	// Delete removes key. Deleting a missing key is not an error.
	Delete(key string) error

	// Stats returns a snapshot of the cache's cumulative counters.
	Stats() CacheStats
}
