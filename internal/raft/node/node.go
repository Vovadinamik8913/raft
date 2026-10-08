package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Vovadinamik8913/raft/internal/raft/model"
)

const (
	ElectionTimeoutMin = 500 * time.Millisecond
	ElectionTimeoutMax = 1000 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
)

var ErrUnavailable = errors.New("raft node unavailable")

type RaftNode struct {
	ID         string
	Peers      []string
	State      *model.State
	httpClient *http.Client
	wake       chan struct{}

	// Protected by State.Mu. Only Run owns and resets the actual timer.
	electionDeadline time.Time
	votes            map[string]bool
	running          bool
	stopped          bool
}

type rpcResult struct {
	peer      string
	term      int
	vote      *model.RequestVoteResponse
	heartbeat *model.AppendEntriesResponse
	err       error
}

func NewRaftNode(id string, peers []string) (*RaftNode, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("node ID must not be empty")
	}
	unique := make([]string, 0, len(peers))
	seen := make(map[string]bool)
	for _, peer := range peers {
		u, err := url.Parse(strings.TrimSpace(peer))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
			u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("invalid peer URL %q: expected http(s)://host:port", peer)
		}
		u.Host = strings.ToLower(u.Host)
		u.Path = ""
		peer = u.String()
		if !seen[peer] {
			seen[peer] = true
			unique = append(unique, peer)
		}
	}
	return &RaftNode{
		ID: id, Peers: unique, State: model.NewState(),
		httpClient: &http.Client{Timeout: 400 * time.Millisecond},
		wake:       make(chan struct{}, 1),
	}, nil
}

// Run owns the timers and outbound RPC workers. It may be called once.
func (n *RaftNode) Run(parent context.Context) error {
	n.State.Mu.Lock()
	if n.running || n.stopped {
		n.State.Mu.Unlock()
		return ErrUnavailable
	}
	n.running = true
	n.resetDeadlineLocked()
	n.State.Mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait(); n.stop() }()
	results := make(chan rpcResult, max(1, 2*len(n.Peers)))
	heartbeatsInFlight := make(map[string]int)
	timer := time.NewTimer(ElectionTimeoutMin)
	defer timer.Stop()
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return nil
		}
		n.State.Mu.RLock()
		role, deadline := n.State.State, n.electionDeadline
		n.State.Mu.RUnlock()
		if role == model.Leader {
			timer.Stop()
		} else {
			timer.Reset(max(time.Millisecond, time.Until(deadline)))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-n.wake:
			// Incoming RPCs already updated the deadline under the state lock.
		case <-timer.C:
			term, started := n.startElection()
			if started {
				for _, peer := range n.Peers {
					n.launchRPC(ctx, &workers, results, peer, term, true)
				}
				n.sendHeartbeats(ctx, &workers, results, heartbeatsInFlight)
			}
		case <-ticker.C:
			n.sendHeartbeats(ctx, &workers, results, heartbeatsInFlight)
		case result := <-results:
			if result.vote == nil && heartbeatsInFlight[result.peer] == result.term {
				delete(heartbeatsInFlight, result.peer)
			}
			becameLeader := n.handleResult(result)
			if becameLeader {
				n.sendHeartbeats(ctx, &workers, results, heartbeatsInFlight)
			}
		}
	}
}

func (n *RaftNode) startElection() (int, bool) {
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	if n.State.State == model.Leader || time.Now().Before(n.electionDeadline) {
		return n.State.CurrentTerm, false
	}
	term := n.State.CurrentTerm + 1
	n.State.CurrentTerm = term
	n.State.VotedFor = n.ID
	n.State.State = model.Candidate
	n.State.LeaderID = ""
	n.votes = map[string]bool{n.ID: true}
	n.resetDeadlineLocked()
	if len(n.Peers) == 0 {
		n.becomeLeaderLocked()
	}
	return term, true
}

func (n *RaftNode) becomeLeaderLocked() {
	n.State.State = model.Leader
	n.State.LeaderID = n.ID
}

func (n *RaftNode) handleResult(result rpcResult) bool {
	if result.err != nil {
		return false
	}
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	term := result.responseTerm()
	if term > n.State.CurrentTerm {
		n.followHigherTermLocked(term)
		return false
	}
	if result.vote == nil || !result.vote.VoteGranted || term != result.term ||
		n.State.State != model.Candidate || n.State.CurrentTerm != result.term {
		return false
	}
	n.votes[result.peer] = true
	if len(n.votes) >= (len(n.Peers)+1)/2+1 {
		n.becomeLeaderLocked()
		return true
	}
	return false
}

func (r rpcResult) responseTerm() int {
	if r.vote != nil {
		return r.vote.Term
	}
	return r.heartbeat.Term
}

func (n *RaftNode) sendHeartbeats(ctx context.Context, workers *sync.WaitGroup, results chan<- rpcResult, inFlight map[string]int) {
	n.State.Mu.RLock()
	role, term := n.State.State, n.State.CurrentTerm
	n.State.Mu.RUnlock()
	if role != model.Leader {
		return
	}
	for _, peer := range n.Peers {
		if inFlight[peer] != term {
			inFlight[peer] = term
			n.launchRPC(ctx, workers, results, peer, term, false)
		}
	}
}

func (n *RaftNode) launchRPC(ctx context.Context, workers *sync.WaitGroup, results chan<- rpcResult, peer string, term int, vote bool) {
	workers.Add(1)
	go func() {
		defer workers.Done()
		result := rpcResult{peer: peer, term: term}
		if vote {
			result.vote = &model.RequestVoteResponse{}
			result.err = n.postRPC(ctx, peer+"/request_vote", model.RequestVoteRequest{Term: term, CandidateID: n.ID}, result.vote)
		} else {
			result.heartbeat = &model.AppendEntriesResponse{}
			result.err = n.postRPC(ctx, peer+"/append_entries", model.AppendEntriesRequest{Term: term, LeaderID: n.ID}, result.heartbeat)
		}
		select {
		case results <- result:
		case <-ctx.Done():
		}
	}()
}

func (n *RaftNode) postRPC(ctx context.Context, endpoint string, req, response any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := n.httpClient.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("RPC %s returned HTTP %d", endpoint, resp.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := decoder.Decode(response); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("invalid RPC response: trailing JSON")
	}
	return nil
}

func (n *RaftNode) resetDeadlineLocked() {
	n.electionDeadline = time.Now().Add(ElectionTimeoutMin + time.Duration(rand.Int64N(int64(ElectionTimeoutMax-ElectionTimeoutMin))))
	n.notifyLocked()
}

func (n *RaftNode) notifyLocked() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *RaftNode) followHigherTermLocked(term int) {
	n.State.CurrentTerm = term
	n.State.VotedFor = ""
	n.State.State = model.Follower
	n.State.LeaderID = ""
	n.votes = nil
	n.resetDeadlineLocked()
}

func (n *RaftNode) RequestVote(req model.RequestVoteRequest) (model.RequestVoteResponse, error) {
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	if n.stopped {
		return model.RequestVoteResponse{}, ErrUnavailable
	}
	if req.Term > n.State.CurrentTerm {
		n.followHigherTermLocked(req.Term)
	}
	resp := model.RequestVoteResponse{Term: n.State.CurrentTerm}
	// All logs are empty in Part 1; every valid candidate log is up to date.
	if req.Term == n.State.CurrentTerm && req.CandidateID != n.ID &&
		(n.State.VotedFor == "" || n.State.VotedFor == req.CandidateID) {
		n.State.VotedFor = req.CandidateID
		resp.VoteGranted = true
		n.resetDeadlineLocked()
	}
	return resp, nil
}

func (n *RaftNode) AppendEntries(req model.AppendEntriesRequest) (model.AppendEntriesResponse, error) {
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	if n.stopped {
		return model.AppendEntriesResponse{}, ErrUnavailable
	}
	if req.Term > n.State.CurrentTerm {
		n.followHigherTermLocked(req.Term)
	}
	resp := model.AppendEntriesResponse{Term: n.State.CurrentTerm}
	if req.Term == n.State.CurrentTerm && req.LeaderID != n.ID {
		n.State.State = model.Follower
		n.State.LeaderID = req.LeaderID
		n.votes = nil
		n.resetDeadlineLocked()
		// A heartbeat can only reference the empty log in Part 1.
		resp.Success = req.PrevLogIndex == 0 && req.PrevLogTerm == 0
	}
	return resp, nil
}

func (n *RaftNode) Status() model.NodeStatus {
	n.State.Mu.RLock()
	defer n.State.Mu.RUnlock()
	return model.NodeStatus{ID: n.ID, State: n.State.State.String(), Term: n.State.CurrentTerm, LeaderID: n.State.LeaderID}
}

func (n *RaftNode) stop() {
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	n.stopped = true
	n.State.State = model.Follower
	n.State.LeaderID = ""
}
