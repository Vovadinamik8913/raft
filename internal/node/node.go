package node

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Vovadinamik8913/raft/internal/client"
	"github.com/Vovadinamik8913/raft/internal/dto"
	"github.com/Vovadinamik8913/raft/internal/entity"
	"github.com/rs/zerolog/log"
)

const (
	ElectionTimeoutMin = 500 * time.Millisecond
	ElectionTimeoutMax = 1000 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
)

var ErrUnavailable = errors.New("raft node unavailable")

type RaftNode struct {
	ID     string
	Peers  []string
	State  *entity.State
	client *client.Client
	wake   chan struct{}

	electionDeadline time.Time
	votes            map[string]bool
	running          bool
	stopped          bool
}

type rpcResult struct {
	peer      string
	term      int
	vote      *dto.RequestVoteResponse
	heartbeat *dto.AppendEntriesResponse
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
		ID: id, Peers: unique, State: entity.NewState(),
		client: client.New(),
		wake:   make(chan struct{}, 1),
	}, nil
}

func (n *RaftNode) Run(parent context.Context) error {
	n.State.Mu.Lock()
	if n.running || n.stopped {
		n.State.Mu.Unlock()
		return ErrUnavailable
	}
	n.running = true
	n.resetDeadline()
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
		if role == entity.Leader {
			timer.Stop()
		} else {
			timer.Reset(max(time.Millisecond, time.Until(deadline)))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-n.wake:
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
	if n.State.State == entity.Leader || time.Now().Before(n.electionDeadline) {
		return n.State.CurrentTerm, false
	}
	term := n.State.CurrentTerm + 1
	n.State.CurrentTerm = term
	n.State.VotedFor = n.ID
	n.State.State = entity.Candidate
	n.State.LeaderID = ""
	n.votes = map[string]bool{n.ID: true}
	n.resetDeadline()
	log.Info().Int("term", term).Msg("started election")
	if len(n.Peers) == 0 {
		n.becomeLeader()
	}
	return term, true
}

func (n *RaftNode) becomeLeader() {
	n.State.State = entity.Leader
	n.State.LeaderID = n.ID
	log.Info().Int("term", n.State.CurrentTerm).Msg("became leader")
}

func (n *RaftNode) handleResult(result rpcResult) bool {
	if result.err != nil {
		return false
	}
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	term := result.responseTerm()
	if term > n.State.CurrentTerm {
		n.followHigherTerm(term)
		return false
	}
	if result.vote == nil || !result.vote.VoteGranted || term != result.term ||
		n.State.State != entity.Candidate || n.State.CurrentTerm != result.term {
		return false
	}
	n.votes[result.peer] = true
	log.Info().Str("peer", result.peer).Int("term", result.term).Msg("received vote")
	if len(n.votes) >= (len(n.Peers)+1)/2+1 {
		n.becomeLeader()
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
	if role != entity.Leader {
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
			resp, err := n.client.RequestVote(ctx, peer, dto.RequestVoteRequest{Term: term, CandidateID: n.ID})
			result.vote, result.err = &resp, err
		} else {
			resp, err := n.client.AppendEntries(ctx, peer, dto.AppendEntriesRequest{Term: term, LeaderID: n.ID})
			result.heartbeat, result.err = &resp, err
		}
		select {
		case results <- result:
		case <-ctx.Done():
		}
	}()
}

func (n *RaftNode) resetDeadline() {
	n.electionDeadline = time.Now().Add(ElectionTimeoutMin + time.Duration(rand.Int64N(int64(ElectionTimeoutMax-ElectionTimeoutMin))))
	n.notify()
}

func (n *RaftNode) notify() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *RaftNode) followHigherTerm(term int) {
	n.State.CurrentTerm = term
	n.State.VotedFor = ""
	n.State.State = entity.Follower
	n.State.LeaderID = ""
	n.votes = nil
	n.resetDeadline()
}

func (n *RaftNode) RequestVote(req dto.RequestVoteRequest) (dto.RequestVoteResponse, error) {
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	if n.stopped {
		return dto.RequestVoteResponse{}, ErrUnavailable
	}
	if req.Term > n.State.CurrentTerm {
		n.followHigherTerm(req.Term)
	}
	resp := dto.RequestVoteResponse{Term: n.State.CurrentTerm}
	if req.Term == n.State.CurrentTerm && req.CandidateID != n.ID &&
		(n.State.VotedFor == "" || n.State.VotedFor == req.CandidateID) {
		n.State.VotedFor = req.CandidateID
		resp.VoteGranted = true
		n.resetDeadline()
	}
	log.Info().Str("candidate", req.CandidateID).Int("term", req.Term).
		Bool("granted", resp.VoteGranted).Msg("vote request")
	return resp, nil
}

func (n *RaftNode) AppendEntries(req dto.AppendEntriesRequest) (dto.AppendEntriesResponse, error) {
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	if n.stopped {
		return dto.AppendEntriesResponse{}, ErrUnavailable
	}
	if req.Term > n.State.CurrentTerm {
		n.followHigherTerm(req.Term)
	}
	resp := dto.AppendEntriesResponse{Term: n.State.CurrentTerm}
	if req.Term == n.State.CurrentTerm && req.LeaderID != n.ID {
		n.State.State = entity.Follower
		n.State.LeaderID = req.LeaderID
		n.votes = nil
		n.resetDeadline()
		resp.Success = req.PrevLogIndex == 0 && req.PrevLogTerm == 0
	}
	log.Info().Str("leader", req.LeaderID).Int("term", req.Term).Msg("received heartbeat")
	return resp, nil
}

func (n *RaftNode) Status() dto.NodeStatus {
	n.State.Mu.RLock()
	defer n.State.Mu.RUnlock()
	return dto.NodeStatus{ID: n.ID, State: n.State.State.String(), Term: n.State.CurrentTerm, LeaderID: n.State.LeaderID}
}

func (n *RaftNode) stop() {
	n.State.Mu.Lock()
	defer n.State.Mu.Unlock()
	n.stopped = true
	n.State.State = entity.Follower
	n.State.LeaderID = ""
}
