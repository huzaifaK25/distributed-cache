package tests

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/replication"
)

// replicationConfig returns fast local settings for end-to-end tests.
func replicationConfig(role replication.Role, id, walPath string, store *cache.InMemoryCache) replication.Config {
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
		MaxBatch:         16,
		SessionQueue:     512,
		SnapshotBatch:    32,
		MaxMessageSize:   1 << 20,
		ShutdownTimeout:  3 * time.Second,
	}
}

// startReplicationNode creates and starts a coordinator stopped at cleanup.
func startReplicationNode(t *testing.T, config replication.Config) *replication.Coordinator {
	t.Helper()

	coordinator, err := replication.New(config)
	if err != nil {
		t.Fatalf("replication.New(%s) returned error: %v", config.ID, err)
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

func TestReplicationEndToEnd(t *testing.T) {
	dir := t.TempDir()
	primaryStore := cache.New(cache.Config{})
	primary := startReplicationNode(t, replicationConfig(
		replication.RolePrimary, "primary", filepath.Join(dir, "primary.wal"), primaryStore,
	))

	if err := primary.Set("before-join", "snapshot-value"); err != nil {
		t.Fatalf("Set before join returned error: %v", err)
	}

	replicaStore := cache.New(cache.Config{})
	replicaConfig := replicationConfig(
		replication.RoleReplica, "replica", filepath.Join(dir, "replica.wal"), replicaStore,
	)
	replicaConfig.PrimaryAddr = primary.Addr().String()
	replica := startReplicationNode(t, replicaConfig)

	if !waitFor(t, 5*time.Second, func() bool {
		value, found := replicaStore.Get("before-join")
		return found && value == "snapshot-value"
	}) {
		t.Fatal("replica did not receive the bootstrap snapshot")
	}

	if err := primary.Set("live", "value"); err != nil {
		t.Fatalf("Set live returned error: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		value, found := replicaStore.Get("live")
		return found && value == "value"
	}) {
		t.Fatal("replica did not receive the live write")
	}

	if err := primary.Delete("live"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		_, found := replicaStore.Get("live")
		return !found
	}) {
		t.Fatal("replica did not apply the delete")
	}

	if !waitFor(t, 5*time.Second, func() bool {
		stats := primary.Stats()
		return stats.Replicas == 1 && stats.Delivered >= 2 && stats.Pending == 0
	}) {
		t.Fatalf("replication acknowledgements did not drain: %+v", primary.Stats())
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return replica.Stats().Seq == primary.Stats().Seq
	}) {
		t.Fatalf("replica sequence = %d, primary sequence = %d",
			replica.Stats().Seq, primary.Stats().Seq)
	}
}
