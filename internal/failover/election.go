package failover

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"
)

// Control-plane message types.
type messageType string

const (
	messageRequestVote         messageType = "request_vote"
	messageRequestVoteResponse messageType = "request_vote_response"
	messageAppendEntries       messageType = "append_entries"
	messageAppendEntriesReply  messageType = "append_entries_response"
	messageTimeoutNow          messageType = "timeout_now"
	messageTimeoutNowResponse  messageType = "timeout_now_response"
)

const controlHeaderSize = 4

// wireMessage is the single envelope for every election RPC.
type wireMessage struct {
	Type            messageType `json:"type"`
	ClusterID       string      `json:"cluster_id,omitempty"`
	Term            uint64      `json:"term,omitempty"`
	CandidateID     string      `json:"candidate_id,omitempty"`
	LeaderID        string      `json:"leader_id,omitempty"`
	ReplicationAddr string      `json:"replication_addr,omitempty"`
	LastSeq         uint64      `json:"last_seq,omitempty"`
	VoteGranted     bool        `json:"vote_granted,omitempty"`
	Success         bool        `json:"success,omitempty"`
	Error           string      `json:"error,omitempty"`
}

// acceptLoop accepts control RPC connections until the listener closes.
func (n *Node) acceptLoop() {
	defer n.wg.Done()

	for {
		conn, err := n.listener.Accept()
		if err != nil {
			if n.ctx.Err() != nil {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return
		}
		if n.ctx.Err() != nil {
			_ = conn.Close()
			return
		}

		n.registerConn(conn)
		n.wg.Add(1)
		go n.handleConnection(conn)
	}
}

// handleConnection serves one request/response control RPC.
func (n *Node) handleConnection(conn net.Conn) {
	defer n.wg.Done()
	defer n.unregisterConn(conn)
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(n.config.RPCTimeout)); err != nil {
		return
	}
	message, err := readControlMessage(conn, n.config.MaxMessageSize)
	if err != nil {
		return
	}
	if message.ClusterID != n.config.ClusterID {
		_ = writeControlMessage(conn, wireMessage{
			Type:  message.Type,
			Error: "cluster id mismatch",
		}, n.config.MaxMessageSize)
		return
	}

	response := n.handleMessage(message)
	response.ClusterID = n.config.ClusterID
	_ = writeControlMessage(conn, response, n.config.MaxMessageSize)
}

// handleMessage dispatches one control RPC and returns its response.
func (n *Node) handleMessage(message wireMessage) wireMessage {
	switch message.Type {
	case messageRequestVote:
		return n.handleRequestVote(message)
	case messageAppendEntries:
		return n.handleAppendEntries(message)
	case messageTimeoutNow:
		return n.handleTimeoutNow(message)
	default:
		return wireMessage{Type: message.Type, Error: fmt.Sprintf("unknown message type %q", message.Type)}
	}
}

// callPeer performs one control RPC with the configured timeout.
func (n *Node) callPeer(peer Peer, message wireMessage) (wireMessage, error) {
	ctx, cancel := context.WithTimeout(n.ctx, n.config.RPCTimeout)
	defer cancel()

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", peer.Address)
	if err != nil {
		return wireMessage{}, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(n.config.RPCTimeout)); err != nil {
		return wireMessage{}, err
	}
	message.ClusterID = n.config.ClusterID
	if err := writeControlMessage(conn, message, n.config.MaxMessageSize); err != nil {
		return wireMessage{}, err
	}

	response, err := readControlMessage(conn, n.config.MaxMessageSize)
	if err != nil {
		return wireMessage{}, err
	}
	if response.Error != "" {
		return response, fmt.Errorf("failover: peer %s: %s", peer.ID, response.Error)
	}
	return response, nil
}

// handleRequestVote implements the Raft vote rule: one vote per term and only
// for a candidate whose durable log is at least as complete as this node's.
func (n *Node) handleRequestVote(request wireMessage) wireMessage {
	response := wireMessage{Type: messageRequestVoteResponse}

	if request.CandidateID == "" {
		response.Error = "candidate id is required"
		return response
	}

	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	response.Term = n.term
	if request.Term < n.term {
		response.LastSeq = n.currentSeq()
		return response
	}
	if request.Term > n.term {
		if err := n.persistStateLocked(request.Term, ""); err != nil {
			response.Error = err.Error()
			return response
		}
		n.term = request.Term
		n.votedFor = ""
		// A higher term invalidates the leader this node was following.
		changed := n.role != RoleFollower || n.leaderID != "" || n.leaderReplAddr != ""
		n.role = RoleFollower
		n.leaderID = ""
		n.leaderReplAddr = ""
		n.resetElection()
		if changed {
			n.signalReplication()
		}
	}
	response.Term = n.term

	localSeq := n.currentSeq()
	response.LastSeq = localSeq
	if n.votedFor != "" && n.votedFor != request.CandidateID {
		return response
	}
	if request.LastSeq < localSeq {
		return response
	}

	if err := n.persistStateLocked(n.term, request.CandidateID); err != nil {
		response.Error = err.Error()
		return response
	}
	n.votedFor = request.CandidateID
	n.resetElection()
	response.Term = n.term
	response.VoteGranted = true
	n.detector.observe(request.CandidateID, request.LastSeq, nil)
	return response
}

// handleAppendEntries accepts a leader heartbeat, updates the known leader and
// its replication address, and resets the local election timer.
func (n *Node) handleAppendEntries(request wireMessage) wireMessage {
	response := wireMessage{Type: messageAppendEntriesReply}

	if request.LeaderID == "" {
		response.Error = "leader id is required"
		return response
	}

	n.stateMu.Lock()
	response.Term = n.term
	if request.Term < n.term {
		n.stateMu.Unlock()
		return response
	}
	if request.Term > n.term {
		if err := n.persistStateLocked(request.Term, ""); err != nil {
			response.Error = err.Error()
			n.stateMu.Unlock()
			return response
		}
		n.term = request.Term
		n.votedFor = ""
	}

	changed := n.role != RoleFollower ||
		n.leaderID != request.LeaderID ||
		n.leaderReplAddr != request.ReplicationAddr
	n.role = RoleFollower
	n.leaderID = request.LeaderID
	n.leaderReplAddr = request.ReplicationAddr
	n.transferring = false
	response.Term = n.term
	response.Success = true
	n.stateMu.Unlock()

	n.resetElection()
	seq := n.currentSeq()
	response.LastSeq = seq
	n.detector.observe(request.LeaderID, seq, nil)
	if changed {
		n.signalReplication()
	}
	return response
}

// handleTimeoutNow lets the current leader hand leadership to this node.
//
// The sender identity is not a safety condition: any peer that can reach this
// node could already start a campaign by timing out. The call is only a hint
// to begin that campaign immediately.
func (n *Node) handleTimeoutNow(request wireMessage) wireMessage {
	response := wireMessage{Type: messageTimeoutNowResponse}

	n.stateMu.Lock()
	response.Term = n.term
	if request.Term < n.term || request.LeaderID == "" {
		n.stateMu.Unlock()
		return response
	}
	if request.Term > n.term {
		if err := n.persistStateLocked(request.Term, ""); err != nil {
			response.Error = err.Error()
			n.stateMu.Unlock()
			return response
		}
		n.term = request.Term
		n.votedFor = ""
	}
	if n.role == RoleLeader {
		n.stateMu.Unlock()
		return response
	}

	response.Term = n.term
	response.Success = true
	n.stateMu.Unlock()

	n.resetElection()
	go n.campaign()
	return response
}

// campaign runs one election round. It persists a new term before requesting
// votes, then activates leadership only after a majority has granted a vote.
func (n *Node) campaign() {
	n.campaignMu.Lock()
	defer n.campaignMu.Unlock()

	n.stateMu.Lock()
	if n.role == RoleLeader || n.transferring || n.ctx.Err() != nil {
		n.stateMu.Unlock()
		return
	}

	term := n.term + 1
	if err := n.persistStateLocked(term, n.config.ID); err != nil {
		n.recordError(err)
		n.stateMu.Unlock()
		return
	}
	n.term = term
	n.votedFor = n.config.ID
	n.role = RoleCandidate
	n.leaderID = ""
	n.leaderReplAddr = ""
	n.transferring = false
	n.stateMu.Unlock()

	n.resetElection()
	n.signalReplication()

	seq := n.currentSeq()
	peers := n.config.Peers
	type voteResult struct {
		peer     Peer
		response wireMessage
		err      error
	}
	results := make(chan voteResult, len(peers))

	for _, peer := range peers {
		go func(peer Peer) {
			response, err := n.callPeer(peer, wireMessage{
				Type:        messageRequestVote,
				Term:        term,
				CandidateID: n.config.ID,
				LastSeq:     seq,
			})
			results <- voteResult{peer: peer, response: response, err: err}
		}(peer)
	}

	votes := 1
	var higherTerm uint64
	for range peers {
		result := <-results
		if result.err != nil {
			n.detector.observe(result.peer.ID, 0, result.err)
			continue
		}
		n.detector.observe(result.peer.ID, result.response.LastSeq, nil)
		if result.response.Term > term {
			if result.response.Term > higherTerm {
				higherTerm = result.response.Term
			}
			continue
		}
		if result.response.Term == term && result.response.VoteGranted {
			votes++
		}
	}

	if higherTerm > term {
		n.stepDown(higherTerm)
		return
	}
	if votes < n.quorum() {
		return
	}
	n.becomeLeader(term)
}

// becomeLeader activates this node if the election result is still current.
func (n *Node) becomeLeader(term uint64) {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()

	if n.role != RoleCandidate || n.term != term {
		return
	}
	n.role = RoleLeader
	n.leaderID = n.config.ID
	n.leaderReplAddr = ""
	n.lastQuorum = time.Now()
	n.transferring = false
	n.signalReplication()
	n.signalHeartbeat()
}

// writeControlMessage frames and writes one JSON control message.
func writeControlMessage(w io.Writer, message wireMessage, maxMessageSize int) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failover: encode message: %w", err)
	}
	if len(payload) == 0 || len(payload) > maxMessageSize || uint64(len(payload)) > math.MaxUint32 {
		return fmt.Errorf("failover: message size %d exceeds limit %d", len(payload), maxMessageSize)
	}

	var header [controlHeaderSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

// readControlMessage reads one length-prefixed JSON control message.
func readControlMessage(r io.Reader, maxMessageSize int) (wireMessage, error) {
	var header [controlHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return wireMessage{}, err
	}

	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || uint64(size) > uint64(maxMessageSize) {
		return wireMessage{}, fmt.Errorf("failover: message size %d exceeds limit %d", size, maxMessageSize)
	}

	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return wireMessage{}, err
	}

	var message wireMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return wireMessage{}, fmt.Errorf("failover: decode message: %w", err)
	}
	return message, nil
}

// quorum returns the strict majority of the configured cluster.
func (n *Node) quorum() int {
	return len(n.config.Peers)/2 + 1
}
