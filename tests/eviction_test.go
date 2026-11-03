package tests

import (
	"fmt"
	"testing"

	"distributed-cache/internal/cache"
)

func TestLRUEviction(t *testing.T) {
	c := newTestCache(t, cache.Config{MaxItems: 3})

	for _, key := range []string{"A", "B", "C"} {
		if err := c.Set(key, key); err != nil {
			t.Fatalf("Set(%q) returned error: %v", key, err)
		}
	}

	// Make A and B more recent than C.
	c.Get("A")
	c.Get("A")
	c.Get("B")

	// The cache is full, so inserting D evicts the least recently used key, C.
	if err := c.Set("D", "D"); err != nil {
		t.Fatalf("Set(D) returned error: %v", err)
	}

	for _, key := range []string{"A", "B", "D"} {
		if _, found := c.Get(key); !found {
			t.Errorf("Get(%q) reported a missing key that should have survived", key)
		}
	}
	if _, found := c.Get("C"); found {
		t.Fatal("Get(C) found the least recently used key after eviction")
	}

	stats := c.Stats()
	if stats.Evictions != 1 {
		t.Fatalf("Evictions = %d, want 1", stats.Evictions)
	}
	if stats.Sets != 4 {
		t.Fatalf("Sets = %d, want 4", stats.Sets)
	}
}

func TestMaxItemsLimit(t *testing.T) {
	const maxItems = 3

	c := newTestCache(t, cache.Config{MaxItems: maxItems})

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		if err := c.Set(keys[i], i); err != nil {
			t.Fatalf("Set(%q) returned error: %v", keys[i], err)
		}
	}

	// With no reads between inserts, only the final three keys remain.
	for i := 0; i < len(keys)-maxItems; i++ {
		if _, found := c.Get(keys[i]); found {
			t.Errorf("Get(%q) found an evicted key", keys[i])
		}
	}
	for i := len(keys) - maxItems; i < len(keys); i++ {
		value, found := c.Get(keys[i])
		if !found {
			t.Errorf("Get(%q) reported a missing recent key", keys[i])
			continue
		}
		if value != i {
			t.Errorf("Get(%q) = %v, want %d", keys[i], value, i)
		}
	}

	stats := c.Stats()
	if want := uint64(len(keys) - maxItems); stats.Evictions != want {
		t.Fatalf("Evictions = %d, want %d", stats.Evictions, want)
	}
	if sets := stats.Sets; sets != uint64(len(keys)) {
		t.Fatalf("Sets = %d, want %d", sets, len(keys))
	}
}

func TestRecentlyAccessedItemSurvives(t *testing.T) {
	c := newTestCache(t, cache.Config{MaxItems: 3})

	for _, key := range []string{"A", "B", "C"} {
		if err := c.Set(key, key); err != nil {
			t.Fatalf("Set(%q) returned error: %v", key, err)
		}
	}

	// A becomes the most recently used key, so B is now the eviction candidate.
	c.Get("A")
	if err := c.Set("D", "D"); err != nil {
		t.Fatalf("Set(D) returned error: %v", err)
	}

	if _, found := c.Get("A"); !found {
		t.Fatal("Get(A) missed the recently accessed key")
	}
	if _, found := c.Get("C"); !found {
		t.Fatal("Get(C) missed a key that should have survived")
	}
	if _, found := c.Get("D"); !found {
		t.Fatal("Get(D) missed the newly inserted key")
	}
	if _, found := c.Get("B"); found {
		t.Fatal("Get(B) found the least recently used key after eviction")
	}

	if evictions := c.Stats().Evictions; evictions != 1 {
		t.Fatalf("Evictions = %d, want 1", evictions)
	}
}

func TestUpdatingExistingItemDoesNotEvict(t *testing.T) {
	c := newTestCache(t, cache.Config{MaxItems: 3})

	for _, key := range []string{"A", "B", "C"} {
		if err := c.Set(key, "old"); err != nil {
			t.Fatalf("Set(%q) returned error: %v", key, err)
		}
	}

	if err := c.Set("A", "new"); err != nil {
		t.Fatalf("Set(A) returned error: %v", err)
	}

	if value, found := c.Get("A"); !found || value != "new" {
		t.Fatalf("Get(A) = (%v, %v), want (new, true)", value, found)
	}
	for _, key := range []string{"B", "C"} {
		if _, found := c.Get(key); !found {
			t.Errorf("Get(%q) missed a key after updating an existing entry", key)
		}
	}
	if evictions := c.Stats().Evictions; evictions != 0 {
		t.Fatalf("Evictions = %d, want 0", evictions)
	}
}

func TestUnlimitedMaxItems(t *testing.T) {
	c := newTestCache(t, cache.Config{MaxItems: 0})

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := c.Set(key, i); err != nil {
			t.Fatalf("Set(%q) returned error: %v", key, err)
		}
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if _, found := c.Get(key); !found {
			t.Fatalf("Get(%q) missed a key with no size limit", key)
		}
	}
	if evictions := c.Stats().Evictions; evictions != 0 {
		t.Fatalf("Evictions = %d, want 0 with no limit", evictions)
	}
}
