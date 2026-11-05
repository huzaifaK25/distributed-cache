package failover

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"distributed-cache/internal/cache"
)

// newTestConfig returns a minimal valid config rooted in a temporary directory.
func newTestConfig(t *testing.T) Config {
	t.Helper()

	dir := t.TempDir()
	return Config{
		ClusterID:  "test-cluster",
		ID:         "node-a",
		ListenAddr: "127.0.0.1:0",
		Store:      cache.New(cache.Config{}),
		WALPath:    filepath.Join(dir, "node.wal"),
	}
}

// newTestNode creates a stopped node with no peers.
func newTestNode(t *testing.T) *Node {
	t.Helper()

	node, err := New(newTestConfig(t))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })
	return node
}

func TestElectionStatePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "election.json")

	state, err := loadElectionState(path)
	if err != nil {
		t.Fatalf("load missing state returned error: %v", err)
	}
	if state.Term != 0 || state.VotedFor != "" {
		t.Fatalf("load missing state = %+v, want zero value", state)
	}

	want := persistentState{Term: 7, VotedFor: "node-b"}
	if err := saveElectionState(path, want); err != nil {
		t.Fatalf("saveElectionState returned error: %v", err)
	}
	got, err := loadElectionState(path)
	if err != nil {
		t.Fatalf("loadElectionState returned error: %v", err)
	}
	if got != want {
		t.Fatalf("loadElectionState = %+v, want %+v", got, want)
	}

	want = persistentState{Term: 8, VotedFor: ""}
	if err := saveElectionState(path, want); err != nil {
		t.Fatalf("replace election state returned error: %v", err)
	}
	got, err = loadElectionState(path)
	if err != nil {
		t.Fatalf("loadElectionState after replace returned error: %v", err)
	}
	if got != want {
		t.Fatalf("loadElectionState after replace = %+v, want %+v", got, want)
	}
}

func TestLoadElectionStateRejectsMalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "election.json")
	if err := saveElectionState(path, persistentState{Term: 1, VotedFor: "node-a"}); err != nil {
		t.Fatalf("saveElectionState returned error: %v", err)
	}
	// Corrupting the file must not silently reset the term.
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
	if _, err := loadElectionState(path); err == nil {
		t.Fatal("loadElectionState accepted a malformed file")
	}
}

func TestRequestVoteRules(t *testing.T) {
	node := newTestNode(t)
	node.sequenceCache.Store(5)

	response := node.handleRequestVote(wireMessage{
		Type:        messageRequestVote,
		Term:        2,
		CandidateID: "node-b",
		LastSeq:     5,
	})
	if !response.VoteGranted || response.Term != 2 {
		t.Fatalf("first vote = %+v, want granted at term 2", response)
	}
	node.stateMu.Lock()
	if node.term != 2 || node.votedFor != "node-b" {
		t.Fatalf("state after vote = term %d votedFor %q, want term 2 votedFor node-b",
			node.term, node.votedFor)
	}
	node.stateMu.Unlock()

	// A second candidate cannot receive a second vote in the same term.
	response = node.handleRequestVote(wireMessage{
		Type:        messageRequestVote,
		Term:        2,
		CandidateID: "node-c",
		LastSeq:     5,
	})
	if response.VoteGranted {
		t.Fatal("second candidate was granted a vote in the same term")
	}

	// Repeating the same request is idempotent.
	response = node.handleRequestVote(wireMessage{
		Type:        messageRequestVote,
		Term:        2,
		CandidateID: "node-b",
		LastSeq:     5,
	})
	if !response.VoteGranted {
		t.Fatal("repeated vote request for the same candidate was rejected")
	}

	// A newer term is adopted, but a candidate with a shorter log is rejected.
	response = node.handleRequestVote(wireMessage{
		Type:        messageRequestVote,
		Term:        3,
		CandidateID: "node-c",
		LastSeq:     4,
	})
	if response.VoteGranted {
		t.Fatal("candidate with a shorter log was granted a vote")
	}
	if response.Term != 3 {
		t.Fatalf("response term = %d, want 3", response.Term)
	}
	node.stateMu.Lock()
	if node.votedFor != "" {
		t.Fatalf("votedFor = %q after adopting term 3, want empty", node.votedFor)
	}
	node.stateMu.Unlock()

	// The same term can then grant a vote to a candidate that is caught up.
	response = node.handleRequestVote(wireMessage{
		Type:        messageRequestVote,
		Term:        3,
		CandidateID: "node-c",
		LastSeq:     6,
	})
	if !response.VoteGranted {
		t.Fatal("caught-up candidate was not granted a vote in term 3")
	}

	state, err := loadElectionState(node.config.StatePath)
	if err != nil {
		t.Fatalf("loadElectionState returned error: %v", err)
	}
	if state.Term != 3 || state.VotedFor != "node-c" {
		t.Fatalf("persisted state = %+v, want term 3 votedFor node-c", state)
	}

	// A restart must preserve the term and vote.
	restarted, err := New(newTestConfigFrom(node.config))
	if err != nil {
		t.Fatalf("New after restart returned error: %v", err)
	}
	if restarted.Term() != 3 {
		t.Fatalf("restarted term = %d, want 3", restarted.Term())
	}
	restarted.stateMu.Lock()
	votedFor := restarted.votedFor
	restarted.stateMu.Unlock()
	if votedFor != "node-c" {
		t.Fatalf("restarted votedFor = %q, want node-c", votedFor)
	}
}

func TestHigherTermVoteDemotesLeader(t *testing.T) {
	node := newTestNode(t)

	// A higher term invalidates the leader a follower was following.
	node.stateMu.Lock()
	node.role = RoleFollower
	node.leaderID = "node-a"
	node.leaderReplAddr = "127.0.0.1:9000"
	node.term = 1
	node.stateMu.Unlock()

	response := node.handleRequestVote(wireMessage{
		Type:        messageRequestVote,
		Term:        2,
		CandidateID: "node-b",
		LastSeq:     0,
	})
	if !response.VoteGranted {
		t.Fatal("higher-term vote was not granted")
	}
	if role := node.Role(); role != RoleFollower {
		t.Fatalf("role after higher-term vote = %s, want FOLLOWER", role)
	}
	if leader := node.LeaderID(); leader != "" {
		t.Fatalf("leader after higher-term vote = %q, want empty", leader)
	}

	// A leader is demoted by the same rule.
	node.stateMu.Lock()
	node.role = RoleLeader
	node.leaderID = node.config.ID
	node.term = 3
	node.stateMu.Unlock()

	response = node.handleRequestVote(wireMessage{
		Type:        messageRequestVote,
		Term:        4,
		CandidateID: "node-c",
		LastSeq:     0,
	})
	if !response.VoteGranted {
		t.Fatal("higher-term vote to a leader was not granted")
	}
	if role := node.Role(); role != RoleFollower {
		t.Fatalf("leader role after higher-term vote = %s, want FOLLOWER", role)
	}
	if leader := node.LeaderID(); leader != "" {
		t.Fatalf("leader after demotion = %q, want empty", leader)
	}
}

func TestAppendEntriesUpdatesLeader(t *testing.T) {
	node := newTestNode(t)

	response := node.handleAppendEntries(wireMessage{
		Type:            messageAppendEntries,
		Term:            1,
		LeaderID:        "node-b",
		ReplicationAddr: "127.0.0.1:9000",
	})
	if !response.Success || response.Term != 1 {
		t.Fatalf("append response = %+v, want success at term 1", response)
	}
	node.stateMu.Lock()
	leader := node.leaderID
	replicationAddr := node.leaderReplAddr
	role := node.role
	node.stateMu.Unlock()
	if leader != "node-b" || replicationAddr != "127.0.0.1:9000" || role != RoleFollower {
		t.Fatalf("state after append = leader %q addr %q role %s", leader, replicationAddr, role)
	}

	// An older term is rejected without changing the known leader.
	response = node.handleAppendEntries(wireMessage{
		Type:     messageAppendEntries,
		Term:     0,
		LeaderID: "node-c",
	})
	if response.Success {
		t.Fatal("append from an older term was accepted")
	}
	if leader := node.LeaderID(); leader != "node-b" {
		t.Fatalf("leader after stale append = %q, want node-b", leader)
	}

	// A newer term replaces the leader and clears any vote.
	node.stateMu.Lock()
	node.votedFor = "node-b"
	node.stateMu.Unlock()
	response = node.handleAppendEntries(wireMessage{
		Type:     messageAppendEntries,
		Term:     2,
		LeaderID: "node-c",
	})
	if !response.Success || response.Term != 2 {
		t.Fatalf("append response = %+v, want success at term 2", response)
	}
	node.stateMu.Lock()
	if node.votedFor != "" || node.leaderID != "node-c" {
		t.Fatalf("state after newer append = votedFor %q leader %q", node.votedFor, node.leaderID)
	}
	node.stateMu.Unlock()
}

func TestConfigValidation(t *testing.T) {
	valid := func(t *testing.T) Config {
		t.Helper()
		return newTestConfig(t)
	}

	tests := []struct {
		name   string
		mutate func(Config) Config
	}{
		{
			name: "missing cluster id",
			mutate: func(c Config) Config {
				c.ClusterID = ""
				return c
			},
		},
		{
			name: "missing id",
			mutate: func(c Config) Config {
				c.ID = ""
				return c
			},
		},
		{
			name: "missing store",
			mutate: func(c Config) Config {
				c.Store = nil
				return c
			},
		},
		{
			name: "missing wal path",
			mutate: func(c Config) Config {
				c.WALPath = ""
				return c
			},
		},
		{
			name: "self in peers",
			mutate: func(c Config) Config {
				c.Peers = []Peer{{ID: c.ID, Address: "127.0.0.1:1"}}
				return c
			},
		},
		{
			name: "duplicate peer",
			mutate: func(c Config) Config {
				c.Peers = []Peer{
					{ID: "node-b", Address: "127.0.0.1:1"},
					{ID: "node-b", Address: "127.0.0.1:2"},
				}
				return c
			},
		},
		{
			name: "peer without address",
			mutate: func(c Config) Config {
				c.Peers = []Peer{{ID: "node-b"}}
				return c
			},
		},
		{
			name: "election window reversed",
			mutate: func(c Config) Config {
				c.ElectionTimeoutMin = time.Second
				c.ElectionTimeoutMax = time.Second / 2
				return c
			},
		},
		{
			name: "heartbeat too slow",
			mutate: func(c Config) Config {
				c.ElectionTimeoutMin = 100 * time.Millisecond
				c.ElectionTimeoutMax = 200 * time.Millisecond
				c.HeartbeatInterval = 50 * time.Millisecond
				return c
			},
		},
		{
			name: "rpc timeout too large",
			mutate: func(c Config) Config {
				c.ElectionTimeoutMin = 300 * time.Millisecond
				c.ElectionTimeoutMax = 600 * time.Millisecond
				c.RPCTimeout = 150 * time.Millisecond
				return c
			},
		},
		{
			name: "message size too small",
			mutate: func(c Config) Config {
				c.MaxMessageSize = 100
				return c
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.mutate(valid(t)).withDefaults(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("withDefaults error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	base := newTestConfig(t)
	base.ElectionTimeoutMin = time.Second
	config, err := base.withDefaults()
	if err != nil {
		t.Fatalf("withDefaults returned error: %v", err)
	}

	if config.StatePath != config.WALPath+".failover" {
		t.Fatalf("StatePath = %q, want %q", config.StatePath, config.WALPath+".failover")
	}
	if config.ElectionTimeoutMax < config.ElectionTimeoutMin {
		t.Fatalf("ElectionTimeoutMax = %v, want at least %v", config.ElectionTimeoutMax, config.ElectionTimeoutMin)
	}
	if config.RPCTimeout <= 0 || config.RPCTimeout >= config.ElectionTimeoutMin/2 {
		t.Fatalf("RPCTimeout = %v, want inside (0, %v)", config.RPCTimeout, config.ElectionTimeoutMin/2)
	}
	if config.FailureTimeout <= 0 || config.WriteTimeout <= 0 || config.TransferTimeout <= 0 {
		t.Fatalf("negative timeouts: %+v", config)
	}
	if config.MaxMessageSize < 1024 {
		t.Fatalf("MaxMessageSize = %d, want at least 1024", config.MaxMessageSize)
	}
}

func TestQuorumAndLease(t *testing.T) {
	tests := []struct {
		peers int
		want  int
	}{
		{peers: 0, want: 1},
		{peers: 1, want: 1},
		{peers: 2, want: 2},
		{peers: 3, want: 2},
		{peers: 4, want: 3},
	}
	for _, test := range tests {
		node := &Node{config: Config{Peers: make([]Peer, test.peers)}}
		if got := node.quorum(); got != test.want {
			t.Fatalf("quorum with %d peers = %d, want %d", test.peers, got, test.want)
		}
	}

	node := &Node{config: Config{ElectionTimeoutMin: 300 * time.Millisecond}}
	if got := node.leaseDuration(); got != 150*time.Millisecond {
		t.Fatalf("leaseDuration = %v, want 150ms", got)
	}
}

func TestRandomElectionTimeout(t *testing.T) {
	minimum := 100 * time.Millisecond
	maximum := 250 * time.Millisecond
	for i := 0; i < 100; i++ {
		got := randomElectionTimeout(minimum, maximum)
		if got < minimum || got >= maximum {
			t.Fatalf("randomElectionTimeout = %v, want [%v, %v)", got, minimum, maximum)
		}
	}
	if got := randomElectionTimeout(minimum, minimum); got != minimum {
		t.Fatalf("randomElectionTimeout with equal bounds = %v, want %v", got, minimum)
	}
}

func TestControlMessageFraming(t *testing.T) {
	const maxSize = 1024

	var buffer bytes.Buffer
	want := wireMessage{
		Type:            messageAppendEntries,
		ClusterID:       "cluster",
		Term:            9,
		LeaderID:        "node-a",
		ReplicationAddr: "127.0.0.1:9000",
		LastSeq:         42,
		Success:         true,
	}
	if err := writeControlMessage(&buffer, want, maxSize); err != nil {
		t.Fatalf("writeControlMessage returned error: %v", err)
	}
	got, err := readControlMessage(&buffer, maxSize)
	if err != nil {
		t.Fatalf("readControlMessage returned error: %v", err)
	}
	if got.Type != want.Type || got.ClusterID != want.ClusterID || got.Term != want.Term ||
		got.LeaderID != want.LeaderID || got.ReplicationAddr != want.ReplicationAddr ||
		got.LastSeq != want.LastSeq || !got.Success {
		t.Fatalf("read message = %+v, want %+v", got, want)
	}

	if err := writeControlMessage(&buffer, want, 16); err == nil {
		t.Fatal("writeControlMessage accepted an oversized message")
	}
	if _, err := readControlMessage(bytes.NewReader([]byte{0, 0, 0, 0}), maxSize); err == nil {
		t.Fatal("readControlMessage accepted a zero-length frame")
	}
	if _, err := readControlMessage(bytes.NewReader([]byte{0, 0, 0, 8, '{'}), maxSize); err == nil {
		t.Fatal("readControlMessage accepted a truncated frame")
	}
}

func TestTransferLeadershipValidation(t *testing.T) {
	config := newTestConfig(t)
	config.Peers = []Peer{{ID: "node-b", Address: "127.0.0.1:1"}}
	node, err := New(config)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })

	if err := node.TransferLeadership(node.ID()); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("self transfer error = %v, want ErrInvalidTarget", err)
	}
	if err := node.TransferLeadership("missing"); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("unknown target error = %v, want ErrUnknownPeer", err)
	}
	if err := node.TransferLeadership("node-b"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower transfer error = %v, want ErrNotLeader", err)
	}
}

func TestDetectorStatusTransitions(t *testing.T) {
	const failureTimeout = time.Second
	detector := newDetector([]Peer{{ID: "node-b", Address: "127.0.0.1:1"}}, failureTimeout)

	health := detector.snapshot()
	if len(health) != 1 || health[0].Status != PeerAlive {
		t.Fatalf("initial health = %+v, want one ALIVE peer", health)
	}

	detector.mu.Lock()
	detector.peers["node-b"].lastSeen = time.Now().Add(-failureTimeout * 3 / 4)
	detector.mu.Unlock()
	if status := detector.snapshot()[0].Status; status != PeerSuspect {
		t.Fatalf("status at 3/4 failure timeout = %s, want SUSPECT", status)
	}

	detector.mu.Lock()
	detector.peers["node-b"].lastSeen = time.Now().Add(-failureTimeout * 2)
	detector.mu.Unlock()
	if status := detector.snapshot()[0].Status; status != PeerDead {
		t.Fatalf("status past failure timeout = %s, want DEAD", status)
	}

	detector.observe("node-b", 9, nil)
	health = detector.snapshot()
	if health[0].Status != PeerAlive || health[0].LastSeq != 9 || health[0].LastError != "" {
		t.Fatalf("health after success = %+v, want ALIVE seq 9", health[0])
	}

	detector.observe("node-b", 0, errors.New("connection refused"))
	health = detector.snapshot()
	if health[0].Status != PeerAlive || health[0].LastError != "connection refused" {
		t.Fatalf("health after failure = %+v, want ALIVE with error", health[0])
	}

	detector.observe("unknown", 0, nil) // Must be ignored.
}

// newTestConfigFrom copies a config for a second node instance.
func newTestConfigFrom(config Config) Config {
	config.Store = cache.New(cache.Config{})
	config.Peers = nil
	return config
}
