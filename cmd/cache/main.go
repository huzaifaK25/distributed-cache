// Command cache demonstrates the Phase 2 in-memory cache engine.
// A later phase will turn this entry point into a network server.
package main

import (
	"fmt"
	"log"
	"time"

	"distributed-cache/internal/cache"
)

func main() {
	// MaxItems enables LRU eviction; a positive CleanupInterval starts the
	// background expiration worker.
	c := cache.New(cache.Config{
		MaxItems:        2,
		CleanupInterval: time.Minute,
	})
	defer c.StopCleanupWorker()

	// A normal value without expiration.
	if err := c.Set("language", "Go"); err != nil {
		log.Fatalf("set: %v", err)
	}
	value, found := c.Get("language")
	fmt.Printf("get language: value=%v found=%v\n", value, found)

	// A value that expires after 200ms.
	if err := c.SetWithTTL("session", "token", 200*time.Millisecond); err != nil {
		log.Fatalf("set ttl: %v", err)
	}
	value, found = c.Get("session")
	fmt.Printf("get session: value=%v found=%v\n", value, found)

	time.Sleep(300 * time.Millisecond)

	// The first read after expiry removes the entry and reports a miss.
	_, found = c.Get("session")
	fmt.Printf("get expired session: found=%v\n", found)

	// MaxItems is 2, so adding B evicts the least recently used entry.
	if err := c.Set("A", 1); err != nil {
		log.Fatalf("set A: %v", err)
	}
	if err := c.Set("B", 2); err != nil {
		log.Fatalf("set B: %v", err)
	}
	_, found = c.Get("language")
	fmt.Printf("get language after eviction: found=%v\n", found)

	stats := c.Stats()
	fmt.Printf("stats: hits=%d misses=%d sets=%d deletes=%d expired=%d evictions=%d\n",
		stats.Hits, stats.Misses, stats.Sets, stats.Deletes, stats.Expired, stats.Evictions)
}
