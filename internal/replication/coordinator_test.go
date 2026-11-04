package replication_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/replication"
)

// testConfig returns fast but stable local replication settings.
func testConfig(role replication.Role, id, walPath string, store *cache.InMemoryCache) replication.Config {
	return replication.Config{
		Role:             role,
		ID:               id,
		Store:            store,
		WALPath:          walPath,
		ListenAddr:       "127.0.0.1:0",
		ReconnectMin:     10 * time.Millisecond,
		ReconnectMax:     100 * time.Millisecond,
		HandshakeTimeout: 2 * time.Second,
		AckTimeout:       2 * time.Second,
		MaxRetries:       3,
		MaxBatch:         8,
		SessionQueue:     512,
		SnapshotBatch:    32,
		MaxMessageSize:   1 << 20,
		ShutdownTimeout:  3 * time.Second,
	}
}

// startCoordinator creates and starts a coordinator stopped at cleanup.
func startCoordinator(t *testing.T, config replication.Config) *replication.Coordinator {
	t.Helper()

	coordinator, err := replication.New(config)
	if err != nil {
		t.Fatalf("New(%s) returned error: %v", config.ID, err)
	}
	if err := coordinator.Start(); err != nil {
		t.Fatalf("Start(%s) returned error: %v", config.ID, err)
	}
	t.Cleanup(func() {
		if err := coordinator.Stop(); err != nil && !errors.Is(err, replication.ErrShutdownTimeout) {
			t.Errorf("Stop(%s) returned error: %v", config.ID, err)
		}
	})
	return coordinator
}

// waitFor polls condition until it succeeds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

// waitValue blocks until the cache returns want for key.
func waitValue(t *testing.T, store *cache.InMemoryCache, key string, want any) {
	t.Helper()

	waitFor(t, 5*time.Second, fmt.Sprintf("key %q", key), func() bool {
		got, found := store.Get(key)
		return found && got == want
	})
}

// waitMissing blocks until the cache no longer contains key.
func waitMissing(t *testing.T, store *cache.InMemoryCache, key string) {
	t.Helper()

	waitFor(t, 5*time.Second, fmt.Sprintf("key %q to disappear", key), func() bool {
		_, found := store.Get(key)
		return !found
	})
}

func TestPrimaryReplicatesSetDeleteTTLAndExpire(t *testing.T) {
	dir := t.TempDir()
	primaryStore := cache.New(cache.Config{})
	primary := startCoordinator(t, testConfig(
		replication.RolePrimary, "primary", filepath.Join(dir, "primary.wal"), primaryStore,
	))

	if err := primary.Set("before", "full-sync"); err != nil {
		t.Fatalf("Set before replica returned error: %v", err)
	}

	replicaStore := cache.New(cache.Config{})
	replicaConfig := testConfig(
		replication.RoleReplica, "replica", filepath.Join(dir, "replica.wal"), replicaStore,
	)
	replicaConfig.PrimaryAddr = primary.Addr().String()
	replica := startCoordinator(t, replicaConfig)

	waitValue(t, replicaStore, "before", "full-sync")

	if err := primary.Set("live", "value"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	waitValue(t, replicaStore, "live", "value")

	if err := primary.Delete("live"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	waitMissing(t, replicaStore, "live")

	if err := primary.SetWithTTL("ttl", "short", 250*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL returned error: %v", err)
	}
	waitValue(t, replicaStore, "ttl", "short")
	waitMissing(t, replicaStore, "ttl")

	if err := primary.Set("expire", "value"); err != nil {
		t.Fatalf("Set expire key returned error: %v", err)
	}
	waitValue(t, replicaStore, "expire", "value")
	expired, err := primary.Expire("expire", 100*time.Millisecond)
	if err != nil || !expired {
		t.Fatalf("Expire = %v, %v; want true, nil", expired, err)
	}
	waitMissing(t, replicaStore, "expire")

	if missing, err := primary.Expire("absent", time.Second); err != nil || missing {
		t.Fatalf("Expire(absent) = %v, %v; want false, nil", missing, err)
	}
	if err := replica.Set("nope", "value"); !errors.Is(err, replication.ErrNotPrimary) {
		t.Fatalf("replica Set error = %v, want ErrNotPrimary", err)
	}

	waitFor(t, 5*time.Second, "replication acknowledgements to drain", func() bool {
		stats := primary.Stats()
		return stats.Replicas == 1 && stats.Delivered > 0 && stats.Pending == 0
	})
	stats := primary.Stats()
	if stats.SyncTime <= 0 {
		t.Fatalf("SyncTime = %v, want positive", stats.SyncTime)
	}
	if statuses := primary.Replicas(); len(statuses) != 1 || statuses[0].ID != "replica" {
		t.Fatalf("Replicas() = %+v, want one replica", statuses)
	}

	waitFor(t, 5*time.Second, "replica sequence to catch up", func() bool {
		return replica.Stats().Seq == primary.Stats().Seq
	})
}

func TestMultipleReplicasConverge(t *testing.T) {
	dir := t.TempDir()
	primaryStore := cache.New(cache.Config{})
	primary := startCoordinator(t, testConfig(
		replication.RolePrimary, "primary", filepath.Join(dir, "primary.wal"), primaryStore,
	))

	replicaStores := make([]*cache.InMemoryCache, 2)
	for i := range replicaStores {
		store := cache.New(cache.Config{})
		replicaStores[i] = store
		config := testConfig(
			replication.RoleReplica,
			fmt.Sprintf("replica-%d", i),
			filepath.Join(dir, fmt.Sprintf("replica-%d.wal", i)),
			store,
		)
		config.PrimaryAddr = primary.Addr().String()
		startCoordinator(t, config)
	}

	waitFor(t, 5*time.Second, "both replicas to connect", func() bool {
		return primary.Stats().Replicas == 2
	})

	const keyCount = 50
	var writers sync.WaitGroup
	for i := 0; i < keyCount; i++ {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			value := fmt.Sprintf("value-%d", i)
			if err := primary.Set(fmt.Sprintf("key-%02d", i), value); err != nil {
				t.Errorf("Set key %d returned error: %v", i, err)
			}
		}(i)
	}
	writers.Wait()

	for _, store := range replicaStores {
		for i := 0; i < keyCount; i++ {
			waitValue(t, store, fmt.Sprintf("key-%02d", i), fmt.Sprintf("value-%d", i))
		}
	}

	for i := 0; i < keyCount; i += 5 {
		if err := primary.Delete(fmt.Sprintf("key-%02d", i)); err != nil {
			t.Fatalf("Delete key %d returned error: %v", i, err)
		}
	}
	for _, store := range replicaStores {
		for i := 0; i < keyCount; i += 5 {
			waitMissing(t, store, fmt.Sprintf("key-%02d", i))
		}
	}

	waitFor(t, 5*time.Second, "acknowledgements to drain", func() bool {
		stats := primary.Stats()
		return stats.Delivered > 0 && stats.Pending == 0
	})
}

func TestReplicaRecoversFromWALAndIncrementalSync(t *testing.T) {
	dir := t.TempDir()
	primaryStore := cache.New(cache.Config{})
	primary := startCoordinator(t, testConfig(
		replication.RolePrimary, "primary", filepath.Join(dir, "primary.wal"), primaryStore,
	))
	if err := primary.Set("base", "one"); err != nil {
		t.Fatalf("Set base returned error: %v", err)
	}

	walPath := filepath.Join(dir, "replica.wal")
	firstStore := cache.New(cache.Config{})
	firstConfig := testConfig(replication.RoleReplica, "replica", walPath, firstStore)
	firstConfig.PrimaryAddr = primary.Addr().String()
	first := startCoordinator(t, firstConfig)
	waitValue(t, firstStore, "base", "one")
	if err := primary.Set("acknowledged", "value"); err != nil {
		t.Fatalf("Set acknowledged returned error: %v", err)
	}
	waitValue(t, firstStore, "acknowledged", "value")
	waitFor(t, 2*time.Second, "replica acknowledgement", func() bool {
		return primary.Stats().Delivered > 0
	})
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop first replica returned error: %v", err)
	}
	waitFor(t, 2*time.Second, "primary to release the replica session", func() bool {
		return primary.Stats().Replicas == 0
	})
	if delivered := primary.Stats().Delivered; delivered == 0 {
		t.Fatal("delivered history was lost when the replica disconnected")
	}

	if err := primary.Set("offline", "two"); err != nil {
		t.Fatalf("Set offline returned error: %v", err)
	}

	recoveredStore := cache.New(cache.Config{})
	recoveredConfig := testConfig(replication.RoleReplica, "replica", walPath, recoveredStore)
	recoveredConfig.PrimaryAddr = primary.Addr().String()
	recovered, err := replication.New(recoveredConfig)
	if err != nil {
		t.Fatalf("New recovered replica returned error: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Stop() })

	// New replays the WAL before any network activity.
	if value, found := recoveredStore.Get("base"); !found || value != "one" {
		t.Fatalf("recovered store Get(base) = %v, %v; want one, true", value, found)
	}
	if err := recovered.Start(); err != nil {
		t.Fatalf("Start recovered replica returned error: %v", err)
	}
	waitValue(t, recoveredStore, "offline", "two")

	// The recovered replica had a durable baseline, so it should have used
	// incremental sync rather than a full snapshot.
	if syncTime := recovered.Stats().SyncTime; syncTime != 0 {
		t.Fatalf("recovered replica SyncTime = %v, want 0 (incremental sync)", syncTime)
	}
	waitFor(t, 5*time.Second, "recovered replica sequence", func() bool {
		return recovered.Stats().Seq == primary.Stats().Seq
	})
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	valid := testConfig(
		replication.RolePrimary, "primary", filepath.Join(dir, "valid.wal"), cache.New(cache.Config{}),
	)
	if coordinator, err := replication.New(valid); err != nil {
		t.Fatalf("New(valid) returned error: %v", err)
	} else if err := coordinator.Stop(); err != nil {
		t.Fatalf("Stop(valid) returned error: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*replication.Config)
		want   error
	}{
		{name: "role", mutate: func(c *replication.Config) { c.Role = "OTHER" }, want: replication.ErrInvalidConfig},
		{name: "id", mutate: func(c *replication.Config) { c.ID = "" }, want: replication.ErrInvalidConfig},
		{name: "store", mutate: func(c *replication.Config) { c.Store = nil }, want: replication.ErrInvalidConfig},
		{name: "wal", mutate: func(c *replication.Config) { c.WALPath = "" }, want: replication.ErrInvalidConfig},
		{name: "primary address", mutate: func(c *replication.Config) {
			c.Role = replication.RoleReplica
			c.PrimaryAddr = ""
		}, want: replication.ErrInvalidConfig},
		{name: "message size", mutate: func(c *replication.Config) { c.MaxMessageSize = 10 }, want: replication.ErrInvalidConfig},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			config := testConfig(
				replication.RolePrimary, "primary", filepath.Join(t.TempDir(), "test.wal"),
				cache.New(cache.Config{}),
			)
			testCase.mutate(&config)
			if _, err := replication.New(config); !errors.Is(err, testCase.want) {
				t.Fatalf("New error = %v, want %v", err, testCase.want)
			}
		})
	}
}

func TestCoordinatorLifecycle(t *testing.T) {
	dir := t.TempDir()
	store := cache.New(cache.Config{})
	config := testConfig(replication.RolePrimary, "primary", filepath.Join(dir, "primary.wal"), store)

	coordinator, err := replication.New(config)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if coordinator.Role() != replication.RolePrimary || coordinator.ID() != "primary" {
		t.Fatalf("Role/ID = %v/%q", coordinator.Role(), coordinator.ID())
	}
	if coordinator.Addr() != nil {
		t.Fatal("Addr before Start was not nil")
	}
	if err := coordinator.Set("key", "value"); !errors.Is(err, replication.ErrNotRunning) {
		t.Fatalf("Set before Start error = %v, want ErrNotRunning", err)
	}

	if err := coordinator.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if coordinator.Addr() == nil {
		t.Fatal("Addr after Start was nil")
	}
	if err := coordinator.Start(); !errors.Is(err, replication.ErrAlreadyStarted) {
		t.Fatalf("second Start error = %v, want ErrAlreadyStarted", err)
	}
	if err := coordinator.Set("key", "value"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	if err := coordinator.SetWithTTL("ttl", "value", 0); !errors.Is(err, cache.ErrInvalidTTL) {
		t.Fatalf("SetWithTTL(0) error = %v, want ErrInvalidTTL", err)
	}

	if err := coordinator.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if err := coordinator.Stop(); err != nil {
		t.Fatalf("second Stop returned error: %v", err)
	}
	if err := coordinator.Set("key", "value"); !errors.Is(err, replication.ErrClosed) {
		t.Fatalf("Set after Stop error = %v, want ErrClosed", err)
	}
	if err := coordinator.Start(); !errors.Is(err, replication.ErrClosed) {
		t.Fatalf("Start after Stop error = %v, want ErrClosed", err)
	}
}

func TestReplicaRejectsWritesAndNilAddr(t *testing.T) {
	config := testConfig(
		replication.RoleReplica, "replica", filepath.Join(t.TempDir(), "replica.wal"),
		cache.New(cache.Config{}),
	)
	config.PrimaryAddr = "127.0.0.1:1"
	replica, err := replication.New(config)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer replica.Stop()

	if replica.Addr() != nil {
		t.Fatal("replica Addr was not nil")
	}
	if err := replica.Set("key", "value"); !errors.Is(err, replication.ErrNotPrimary) {
		t.Fatalf("replica Set error = %v, want ErrNotPrimary", err)
	}
	if _, err := replica.Expire("key", time.Second); !errors.Is(err, replication.ErrNotPrimary) {
		t.Fatalf("replica Expire error = %v, want ErrNotPrimary", err)
	}
}

func TestReplicaRetriesUnreachablePrimary(t *testing.T) {
	config := testConfig(
		replication.RoleReplica, "replica", filepath.Join(t.TempDir(), "replica.wal"),
		cache.New(cache.Config{}),
	)
	config.PrimaryAddr = "127.0.0.1:1"
	config.ReconnectMin = 5 * time.Millisecond
	config.ReconnectMax = 20 * time.Millisecond
	replica := startCoordinator(t, config)

	waitFor(t, 2*time.Second, "replica to record a connection error", func() bool {
		return replica.LastError() != nil
	})
}

func TestLargeValuesSplitBatches(t *testing.T) {
	dir := t.TempDir()
	const maxMessage = 1024
	value := strings.Repeat("x", 600)

	primaryStore := cache.New(cache.Config{})
	primaryConfig := testConfig(
		replication.RolePrimary, "primary", filepath.Join(dir, "primary.wal"), primaryStore,
	)
	primaryConfig.MaxMessageSize = maxMessage
	primaryConfig.MaxBatch = 32
	primary := startCoordinator(t, primaryConfig)

	// These keys exercise size-aware snapshot chunking during bootstrap.
	for i := 0; i < 5; i++ {
		if err := primary.Set(fmt.Sprintf("snapshot-%d", i), value); err != nil {
			t.Fatalf("snapshot Set returned error: %v", err)
		}
	}

	replicaStore := cache.New(cache.Config{})
	replicaConfig := testConfig(
		replication.RoleReplica, "replica", filepath.Join(dir, "replica.wal"), replicaStore,
	)
	replicaConfig.MaxMessageSize = maxMessage
	replicaConfig.MaxBatch = 32
	replicaConfig.PrimaryAddr = primary.Addr().String()
	startCoordinator(t, replicaConfig)

	for i := 0; i < 5; i++ {
		waitValue(t, replicaStore, fmt.Sprintf("snapshot-%d", i), value)
	}

	// A batch of 32 of these values is much larger than maxMessage, so the
	// primary must split the live append into several frames.
	for i := 0; i < 20; i++ {
		if err := primary.Set(fmt.Sprintf("live-%d", i), value); err != nil {
			t.Fatalf("live Set returned error: %v", err)
		}
	}
	for i := 0; i < 20; i++ {
		waitValue(t, replicaStore, fmt.Sprintf("live-%d", i), value)
	}
}

func TestPrimaryReplaysWALOnRestart(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "primary.wal")

	store := cache.New(cache.Config{})
	primary := startCoordinator(t, testConfig(replication.RolePrimary, "primary", walPath, store))
	if err := primary.Set("persisted", "value"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	if err := primary.Set("deleted", "value"); err != nil {
		t.Fatalf("Set deleted returned error: %v", err)
	}
	if err := primary.Delete("deleted"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if err := primary.SetWithTTL("kept-ttl", "value", time.Minute); err != nil {
		t.Fatalf("SetWithTTL kept returned error: %v", err)
	}
	if err := primary.SetWithTTL("gone-ttl", "value", 20*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL gone returned error: %v", err)
	}
	before := primary.Stats().Seq
	if err := primary.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	recoveredStore := cache.New(cache.Config{})
	recoveredConfig := testConfig(replication.RolePrimary, "primary", walPath, recoveredStore)
	recoveredConfig.ListenAddr = "127.0.0.1:0"
	recovered, err := replication.New(recoveredConfig)
	if err != nil {
		t.Fatalf("New recovered primary returned error: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Stop() })

	if value, found := recoveredStore.Get("persisted"); !found || value != "value" {
		t.Fatalf("Get(persisted) = %v, %v; want value, true", value, found)
	}
	if _, found := recoveredStore.Get("deleted"); found {
		t.Fatal("recovery resurrected a deleted key")
	}
	if _, found := recoveredStore.Get("kept-ttl"); !found {
		t.Fatal("recovery lost an unexpired TTL key")
	}
	if _, found := recoveredStore.Get("gone-ttl"); found {
		t.Fatal("recovery retained an expired TTL key")
	}
	if got := recovered.Stats().Seq; got != before {
		t.Fatalf("recovered sequence = %d, want %d", got, before)
	}

	if err := recovered.Start(); err != nil {
		t.Fatalf("Start recovered primary returned error: %v", err)
	}
	if err := recovered.Set("after", "restart"); err != nil {
		t.Fatalf("Set after restart returned error: %v", err)
	}
	if got := recovered.Stats().Seq; got != before+1 {
		t.Fatalf("sequence after restart write = %d, want %d", got, before+1)
	}
}
