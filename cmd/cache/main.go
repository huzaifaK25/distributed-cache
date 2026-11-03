// Command cache demonstrates the Phase 1 in-memory cache engine.
// A later phase will turn this entry point into a network server.
package main

import (
	"fmt"
	"log"

	"distributed-cache/internal/cache"
)

func main() {
	c := cache.New()

	// Write a value, then read it back.
	if err := c.Set("language", "Go"); err != nil {
		log.Fatalf("set: %v", err)
	}

	value, found := c.Get("language")
	fmt.Printf("get language: value=%v found=%v\n", value, found)

	// Delete the value and confirm it is gone.
	if err := c.Delete("language"); err != nil {
		log.Fatalf("delete: %v", err)
	}

	_, found = c.Get("language")
	fmt.Printf("after delete: found=%v\n", found)

	stats := c.Stats()
	fmt.Printf("stats: hits=%d misses=%d sets=%d deletes=%d\n",
		stats.Hits, stats.Misses, stats.Sets, stats.Deletes)
}
