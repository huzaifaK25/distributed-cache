package cache

// CacheStats is a cumulative snapshot of cache activity since creation.
//
// Each counter is read atomically, but the reads are not one atomic
// transaction, so a snapshot can be slightly skewed under concurrent load.
type CacheStats struct {
	Hits      uint64 // Get calls that found a live value.
	Misses    uint64 // Get calls that did not find a live value.
	Sets      uint64 // Set and SetWithTTL calls that stored a value.
	Deletes   uint64 // Delete calls, including deletes of missing keys.
	Expired   uint64 // Entries removed because their TTL elapsed.
	Evictions uint64 // Entries removed by the LRU size limit.
}
