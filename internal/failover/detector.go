package failover

import (
	"sort"
	"sync"
	"time"
)

// Peer identifies another failover control endpoint.
type Peer struct {
	ID      string
	Address string
}

// PeerStatus is the failure detector's view of one peer.
type PeerStatus string

const (
	// PeerAlive means a heartbeat or RPC succeeded recently.
	PeerAlive PeerStatus = "ALIVE"

	// PeerSuspect means contact is older than half the failure timeout.
	PeerSuspect PeerStatus = "SUSPECT"

	// PeerDead means no successful contact within the failure timeout.
	PeerDead PeerStatus = "DEAD"
)

// PeerHealth is a point-in-time failure-detector observation.
type PeerHealth struct {
	ID        string
	Address   string
	Status    PeerStatus
	LastSeen  time.Time
	LastSeq   uint64
	LastError string
}

// peerRecord is the mutable detector state for one peer.
type peerRecord struct {
	address  string
	lastSeen time.Time
	lastSeq  uint64
	lastErr  string
}

// detector tracks last successful contact with each configured peer.
type detector struct {
	mu             sync.Mutex
	peers          map[string]*peerRecord
	failureTimeout time.Duration
}

// newDetector creates health records for the configured peers. Peers start
// with a full failure-timeout grace period so an initial observation is not
// reported as dead before the first heartbeat round.
func newDetector(peers []Peer, failureTimeout time.Duration) *detector {
	now := time.Now()
	records := make(map[string]*peerRecord, len(peers))
	for _, peer := range peers {
		records[peer.ID] = &peerRecord{
			address:  peer.Address,
			lastSeen: now,
		}
	}
	return &detector{
		peers:          records,
		failureTimeout: failureTimeout,
	}
}

// observe records a successful or failed contact with peer.
func (d *detector) observe(id string, seq uint64, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	record, ok := d.peers[id]
	if !ok {
		return
	}
	if err != nil {
		record.lastErr = err.Error()
		return
	}

	record.lastSeen = time.Now()
	record.lastSeq = seq
	record.lastErr = ""
}

// lastSeq returns the highest sequence most recently reported by peer.
func (d *detector) lastSeq(id string) uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	if record, ok := d.peers[id]; ok {
		return record.lastSeq
	}
	return 0
}

// snapshot returns peer health sorted by ID.
func (d *detector) snapshot() []PeerHealth {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	health := make([]PeerHealth, 0, len(d.peers))
	for id, record := range d.peers {
		health = append(health, PeerHealth{
			ID:        id,
			Address:   record.address,
			Status:    d.status(record, now),
			LastSeen:  record.lastSeen,
			LastSeq:   record.lastSeq,
			LastError: record.lastErr,
		})
	}
	sort.Slice(health, func(i, j int) bool {
		return health[i].ID < health[j].ID
	})
	return health
}

// status derives a health state from the age of the last success.
func (d *detector) status(record *peerRecord, now time.Time) PeerStatus {
	if record.lastSeen.IsZero() {
		return PeerDead
	}

	age := now.Sub(record.lastSeen)
	switch {
	case age >= d.failureTimeout:
		return PeerDead
	case age >= d.failureTimeout/2:
		return PeerSuspect
	default:
		return PeerAlive
	}
}
