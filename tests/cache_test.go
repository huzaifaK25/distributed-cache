package tests

import (
	"fmt"
	"sync"
	"testing"

	"distributed-cache/internal/cache"
)

func TestSetAndGet(t *testing.T) {
	c := cache.New()

	// Cover common value types plus nil, which must still report found=true.
	tests := []struct {
		name  string
		key   string
		value any
	}{
		{name: "string", key: "greeting", value: "hello"},
		{name: "integer", key: "answer", value: 42},
		{name: "nil value", key: "nil", value: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.Set(tt.key, tt.value); err != nil {
				t.Fatalf("Set(%q) returned error: %v", tt.key, err)
			}

			got, found := c.Get(tt.key)
			if !found {
				t.Fatalf("Get(%q) reported a missing key", tt.key)
			}
			if got != tt.value {
				t.Fatalf("Get(%q) = %v, want %v", tt.key, got, tt.value)
			}
		})
	}

	t.Run("overwrite", func(t *testing.T) {
		if err := c.Set("greeting", "goodbye"); err != nil {
			t.Fatalf("Set() returned error: %v", err)
		}

		got, found := c.Get("greeting")
		if !found {
			t.Fatal("Get() reported a missing key after overwrite")
		}
		if got != "goodbye" {
			t.Fatalf("Get() = %v, want goodbye", got)
		}
	})
}

func TestMissingKey(t *testing.T) {
	c := cache.New()

	value, found := c.Get("does-not-exist")
	if found {
		t.Fatal("Get() reported a missing key as found")
	}
	if value != nil {
		t.Fatalf("Get() = %v, want nil for a missing key", value)
	}

	if misses := c.Stats().Misses; misses != 1 {
		t.Fatalf("Misses = %d, want 1", misses)
	}
}

func TestDelete(t *testing.T) {
	c := cache.New()

	if err := c.Set("key", "value"); err != nil {
		t.Fatalf("Set() returned error: %v", err)
	}

	if err := c.Delete("key"); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	if _, found := c.Get("key"); found {
		t.Fatal("Get() found a key after it was deleted")
	}

	// Delete is idempotent: removing a missing key is still successful.
	if err := c.Delete("key"); err != nil {
		t.Fatalf("Delete() on a missing key returned error: %v", err)
	}

	if deletes := c.Stats().Deletes; deletes != 2 {
		t.Fatalf("Deletes = %d, want 2", deletes)
	}
}

func TestStats(t *testing.T) {
	c := cache.New()

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("Set() returned error: %v", err)
	}
	c.Get("a")       // hit
	c.Get("missing") // miss
	if err := c.Delete("a"); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	want := cache.CacheStats{Hits: 1, Misses: 1, Sets: 1, Deletes: 1}
	if got := c.Stats(); got != want {
		t.Fatalf("Stats() = %+v, want %+v", got, want)
	}

	// Stats returns a copy; changing the snapshot must not affect the cache.
	snapshot := c.Stats()
	snapshot.Hits = 999
	if got := c.Stats(); got != want {
		t.Fatalf("mutating a Stats snapshot changed cache state: got %+v", got)
	}
}

func TestConcurrentWritersAndReaders(t *testing.T) {
	const (
		writerCount     = 100
		readerCount     = 100
		writesPerWriter = 100
		readsPerReader  = 100
	)

	c := cache.New()
	start := make(chan struct{}) // Releases all goroutines together to maximize contention.
	var wg sync.WaitGroup

	// Writers own disjoint key ranges, so every write can be verified at the end.
	wg.Add(writerCount)
	for w := range writerCount {
		go func() {
			defer wg.Done()
			<-start

			for i := range writesPerWriter {
				key := fmt.Sprintf("writer-%03d-key-%03d", w, i)
				if err := c.Set(key, i); err != nil {
					t.Errorf("Set(%q) returned error: %v", key, err)
				}
			}
		}()
	}

	// Readers race the writers. A key may be absent, but a present value must
	// be the value written for that exact key.
	wg.Add(readerCount)
	for r := range readerCount {
		go func() {
			defer wg.Done()
			<-start

			for i := range readsPerReader {
				w := (r + i) % writerCount
				key := fmt.Sprintf("writer-%03d-key-%03d", w, i)
				if value, found := c.Get(key); found && value != i {
					t.Errorf("Get(%q) = %v, want %v", key, value, i)
				}
			}
		}()
	}

	close(start)
	wg.Wait()

	// Exact counts prove that no writes or counter updates were lost.
	stats := c.Stats()
	if want := uint64(writerCount * writesPerWriter); stats.Sets != want {
		t.Errorf("Sets = %d, want %d", stats.Sets, want)
	}
	if want := uint64(readerCount * readsPerReader); stats.Hits+stats.Misses != want {
		t.Errorf("Hits+Misses = %d, want %d", stats.Hits+stats.Misses, want)
	}
	if stats.Deletes != 0 {
		t.Errorf("Deletes = %d, want 0", stats.Deletes)
	}

	// Every writer used unique keys, so the final map contents are deterministic.
	for w := range writerCount {
		for i := range writesPerWriter {
			key := fmt.Sprintf("writer-%03d-key-%03d", w, i)
			value, found := c.Get(key)
			if !found {
				t.Errorf("Get(%q) reported a missing key after writers finished", key)
				continue
			}
			if value != i {
				t.Errorf("Get(%q) = %v, want %v", key, value, i)
			}
		}
	}
}
