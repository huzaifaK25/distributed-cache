package failover

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"
)

// Role is a node's position in the election state machine.
type Role string

const (
	// RoleFollower accepts heartbeats from the current leader.
	RoleFollower Role = "FOLLOWER"

	// RoleCandidate is soliciting votes for a new term.
	RoleCandidate Role = "CANDIDATE"

	// RoleLeader owns the replication primary role for its term.
	RoleLeader Role = "LEADER"
)

// persistentState is the election state that must survive a restart.
//
// Losing Term or VotedFor could allow the same node to vote twice in one term,
// which is exactly the condition that permits two leaders.
type persistentState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for,omitempty"`
}

// loadElectionState reads persisted election state.
//
// A missing file means this node has no prior election history. A malformed
// file is an error: silently resetting the term would weaken safety.
func loadElectionState(path string) (persistentState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return persistentState{}, nil
	}
	if err != nil {
		return persistentState{}, fmt.Errorf("failover: read election state: %w", err)
	}

	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return persistentState{}, fmt.Errorf("failover: decode election state: %w", err)
	}
	return state, nil
}

// saveElectionState atomically replaces the persisted election state.
func saveElectionState(path string, state persistentState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failover: encode election state: %w", err)
	}

	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failover: create election state directory: %w", err)
		}
	}

	temp := path + ".tmp"
	file, err := os.OpenFile(temp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("failover: create election state: %w", err)
	}

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(temp)
		return fmt.Errorf("failover: write election state: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(temp)
		return fmt.Errorf("failover: sync election state: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("failover: close election state: %w", err)
	}
	if err := os.Rename(temp, path); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("failover: replace election state: %w", err)
	}
	return nil
}

// randomElectionTimeout spreads simultaneous candidates over the configured
// election window so one usually wins before a split vote occurs.
func randomElectionTimeout(minimum, maximum time.Duration) time.Duration {
	if maximum <= minimum {
		return minimum
	}
	return minimum + time.Duration(rand.Int64N(int64(maximum-minimum)))
}
