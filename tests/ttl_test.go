package tests

import (
	"errors"
	"testing"
	"time"

	"distributed-cache/internal/cache"
)

// waitFor polls condition until it becomes true or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}

	return condition()
}

func TestKeyExpires(t *testing.T) {
	c := newTestCache(t, cache.Config{})

	if err := c.SetWithTTL("key", "value", 10*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL() returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	value, found := c.Get("key")
	if found {
		t.Fatal("Get() returned an expired key")
	}
	if value != nil {
		t.Fatalf("Get() = %v, want nil for an expired key", value)
	}

	stats := c.Stats()
	if stats.Expired != 1 {
		t.Fatalf("Expired = %d, want 1", stats.Expired)
	}
	if stats.Misses != 1 {
		t.Fatalf("Misses = %d, want 1", stats.Misses)
	}
}

func TestNonExpiredKeyAccessible(t *testing.T) {
	c := newTestCache(t, cache.Config{})

	if err := c.SetWithTTL("key", "value", 5*time.Second); err != nil {
		t.Fatalf("SetWithTTL() returned error: %v", err)
	}

	value, found := c.Get("key")
	if !found {
		t.Fatal("Get() reported a live key as missing")
	}
	if value != "value" {
		t.Fatalf("Get() = %v, want value", value)
	}
	if expired := c.Stats().Expired; expired != 0 {
		t.Fatalf("Expired = %d, want 0", expired)
	}
}

func TestTTLOverridesPreviousTTL(t *testing.T) {
	t.Run("new ttl replaces old ttl", func(t *testing.T) {
		c := newTestCache(t, cache.Config{})

		if err := c.SetWithTTL("key", "first", 10*time.Millisecond); err != nil {
			t.Fatalf("first SetWithTTL() returned error: %v", err)
		}
		if err := c.SetWithTTL("key", "second", 500*time.Millisecond); err != nil {
			t.Fatalf("second SetWithTTL() returned error: %v", err)
		}

		time.Sleep(50 * time.Millisecond) // Past the first TTL, before the second.

		value, found := c.Get("key")
		if !found {
			t.Fatal("Get() expired the key using the old TTL")
		}
		if value != "second" {
			t.Fatalf("Get() = %v, want second", value)
		}
		if expired := c.Stats().Expired; expired != 0 {
			t.Fatalf("Expired = %d, want 0", expired)
		}
	})

	t.Run("set clears previous ttl", func(t *testing.T) {
		c := newTestCache(t, cache.Config{})

		if err := c.SetWithTTL("key", "first", 10*time.Millisecond); err != nil {
			t.Fatalf("SetWithTTL() returned error: %v", err)
		}
		if err := c.Set("key", "second"); err != nil {
			t.Fatalf("Set() returned error: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		value, found := c.Get("key")
		if !found {
			t.Fatal("Get() expired a key after Set cleared its TTL")
		}
		if value != "second" {
			t.Fatalf("Get() = %v, want second", value)
		}
		if expired := c.Stats().Expired; expired != 0 {
			t.Fatalf("Expired = %d, want 0", expired)
		}
	})
}

func TestExpiredKeyReturnsMiss(t *testing.T) {
	c := newTestCache(t, cache.Config{})

	if err := c.SetWithTTL("key", "value", 10*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL() returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if value, found := c.Get("key"); found || value != nil {
		t.Fatalf("Get() = (%v, %v), want (nil, false)", value, found)
	}
	if expired := c.Stats().Expired; expired != 1 {
		t.Fatalf("Expired = %d, want 1", expired)
	}

	// The expired entry was removed on the first miss, so it is not counted again.
	if _, found := c.Get("key"); found {
		t.Fatal("Get() found an expired key on a second read")
	}
	if expired := c.Stats().Expired; expired != 1 {
		t.Fatalf("Expired = %d after second read, want 1", expired)
	}
}

func TestSetWithTTLRejectsNonPositiveTTL(t *testing.T) {
	c := newTestCache(t, cache.Config{})

	for _, ttl := range []time.Duration{0, -time.Second} {
		err := c.SetWithTTL("key", "value", ttl)
		if !errors.Is(err, cache.ErrInvalidTTL) {
			t.Fatalf("SetWithTTL(ttl=%v) error = %v, want ErrInvalidTTL", ttl, err)
		}
	}

	if sets := c.Stats().Sets; sets != 0 {
		t.Fatalf("Sets = %d, want 0 for rejected TTLs", sets)
	}
}

func TestCleanupWorkerRemovesExpiredKeys(t *testing.T) {
	c := newTestCache(t, cache.Config{CleanupInterval: 5 * time.Millisecond})

	keys := []string{"a", "b", "c"}
	for _, key := range keys {
		if err := c.SetWithTTL(key, key, 15*time.Millisecond); err != nil {
			t.Fatalf("SetWithTTL(%q) returned error: %v", key, err)
		}
	}

	// Poll metrics only; calling Get here would perform lazy expiration instead.
	if !waitFor(t, 2*time.Second, func() bool {
		return c.Stats().Expired == uint64(len(keys))
	}) {
		t.Fatalf("Expired = %d, want %d; cleanup worker did not remove keys", c.Stats().Expired, len(keys))
	}

	for _, key := range keys {
		if _, found := c.Get(key); found {
			t.Errorf("Get(%q) found a key removed by the cleanup worker", key)
		}
	}

	if expired := c.Stats().Expired; expired != uint64(len(keys)) {
		t.Fatalf("Expired = %d after reads, want %d", expired, len(keys))
	}
}

func TestCleanupWorkerStopsCleanly(t *testing.T) {
	c := newTestCache(t, cache.Config{CleanupInterval: 5 * time.Millisecond})
	c.StopCleanupWorker()

	if err := c.SetWithTTL("key", "value", 10*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL() returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if expired := c.Stats().Expired; expired != 0 {
		t.Fatalf("Expired = %d, want 0 after the worker stopped", expired)
	}

	// Lazy expiration still works after the worker is stopped.
	if _, found := c.Get("key"); found {
		t.Fatal("Get() found an expired key after the worker stopped")
	}
	if expired := c.Stats().Expired; expired != 1 {
		t.Fatalf("Expired = %d, want 1 after lazy expiration", expired)
	}

	// Stopping again must be safe.
	c.StopCleanupWorker()

	// Restarting must resume background cleanup.
	c.StartCleanupWorker()
	if err := c.SetWithTTL("second", "value", 10*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL() returned error: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return c.Stats().Expired == 2
	}) {
		t.Fatalf("Expired = %d, want 2 after restart", c.Stats().Expired)
	}
}

func TestCleanupWorkerDisabled(t *testing.T) {
	c := newTestCache(t, cache.Config{})

	if err := c.SetWithTTL("key", "value", 10*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL() returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if expired := c.Stats().Expired; expired != 0 {
		t.Fatalf("Expired = %d, want 0 when cleanup is disabled", expired)
	}

	if _, found := c.Get("key"); found {
		t.Fatal("Get() found an expired key")
	}
	if expired := c.Stats().Expired; expired != 1 {
		t.Fatalf("Expired = %d, want 1 after lazy expiration", expired)
	}
}
