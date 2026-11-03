package cache

// CacheStats is a cumulative snapshot of cache activity since creation.
//
// Each counter is read atomically, but the four reads are not one atomic
// transaction, so a snapshot can be slightly skewed under concurrent load.
type CacheStats struct {
	Hits    uint64 // Get calls that found a value.
	Misses  uint64 // Get calls that did not find a value.
	Sets    uint64 // Set calls that stored a value.
	Deletes uint64 // Delete calls, including deletes of missing keys.
}
