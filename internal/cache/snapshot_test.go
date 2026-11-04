package cache

import (
	"testing"
	"time"
)

func TestSnapshotReturnsLiveEntriesSorted(t *testing.T) {
	c := New(Config{})
	defer c.StopCleanupWorker()

	if err := c.Set("b", "two"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	if err := c.Set("a", "one"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	if err := c.SetWithTTL("c", "three", time.Minute); err != nil {
		t.Fatalf("SetWithTTL returned error: %v", err)
	}
	if err := c.SetWithTTL("expired", "gone", time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL returned error: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	items := c.Snapshot()
	if len(items) != 3 {
		t.Fatalf("Snapshot length = %d, want 3", len(items))
	}
	if items[0].Key != "a" || items[1].Key != "b" || items[2].Key != "c" {
		t.Fatalf("Snapshot keys = %q, %q, %q", items[0].Key, items[1].Key, items[2].Key)
	}
	if items[0].Value != "one" || items[1].Value != "two" || items[2].Value != "three" {
		t.Fatalf("Snapshot values = %v, %v, %v", items[0].Value, items[1].Value, items[2].Value)
	}
	if items[0].TTL != 0 || items[2].TTL <= 0 {
		t.Fatalf("Snapshot TTLs = %v, %v", items[0].TTL, items[2].TTL)
	}
}

func TestRestoreReplacesContentsAndTTL(t *testing.T) {
	c := New(Config{})
	defer c.StopCleanupWorker()

	if err := c.Set("stale", "old"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	items := []SnapshotItem{
		{Key: "a", Value: "one"},
		{Key: "b", Value: "two", TTL: 50 * time.Millisecond},
		{Key: "expired", Value: "gone", TTL: -time.Second},
	}
	c.Restore(items)

	if _, found := c.Get("stale"); found {
		t.Fatal("Restore kept a stale key")
	}
	if value, found := c.Get("a"); !found || value != "one" {
		t.Fatalf("Get(a) = %v, %v", value, found)
	}
	if value, found := c.Get("b"); !found || value != "two" {
		t.Fatalf("Get(b) = %v, %v", value, found)
	}
	if _, found := c.Get("expired"); found {
		t.Fatal("Restore kept an expired entry")
	}

	time.Sleep(60 * time.Millisecond)
	if _, found := c.Get("b"); found {
		t.Fatal("restored TTL did not expire")
	}
}

func TestRestoreRespectsMaxItems(t *testing.T) {
	c := New(Config{MaxItems: 2})
	defer c.StopCleanupWorker()

	c.Restore([]SnapshotItem{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
		{Key: "c", Value: 3},
	})
	if got := len(c.Snapshot()); got != 2 {
		t.Fatalf("Snapshot length = %d, want 2", got)
	}
}
