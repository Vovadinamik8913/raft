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
	ElectionTimeoutMin = 5000 * time.Millisecond
	ElectionTimeoutMax = 10000 * time.Millisecond
	HeartbeatInterval  = 500 * time.Millisecond
)

var ErrUnavailable = errors.New("raft node unavailable")

type RaftNode struct {
	ID      string
	Peers   []string
	State   *entity.State
	client  *client.Client
	wake    chan struct{}
	changed chan struct{}

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
	appendReq *dto.AppendEntriesRequest
	err       error
}

type rpcRequest struct {
	vote   *dto.RequestVoteRequest
	append *dto.AppendEntriesRequest
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
		client:  client.New(),
		wake:    make(chan struct{}, 1),
		changed: make(chan struct{}),
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
			n.sendHeartbeats(ctx, &workers, results, heartbeatsInFlight)
		case <-timer.C:
			term, started := n.startElection()
			if started {
				n.State.Mu.RLock()
				lastIndex, lastTerm := n.lastLogInfo()
				n.State.Mu.RUnlock()
				for _, peer := range n.Peers {
					req := dto.RequestVoteRequest{Term: term, CandidateID: n.ID, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
					n.launchRPC(ctx, &workers, results, peer, rpcRequest{vote: &req})
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
	n.State.NextIndex = make(map[string]int, len(n.Peers))
	n.State.MatchIndex = make(map[string]int, len(n.Peers))
	for _, peer := range n.Peers {
		n.State.NextIndex[peer] = len(n.State.Log) + 1
	}
	n.advanceCommit()
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
	if result.appendReq != nil {
		if n.State.State != entity.Leader || n.State.CurrentTerm != result.term {
			return false
		}
		if result.heartbeat.Success {
			matched := result.appendReq.PrevLogIndex + len(result.appendReq.Entries)
			n.State.NextIndex[result.peer] = matched + 1
			if matched > n.State.MatchIndex[result.peer] {
				n.State.MatchIndex[result.peer] = matched
				n.advanceCommit()
			}
		} else if n.State.NextIndex[result.peer] > 1 {
			n.State.NextIndex[result.peer]--
			n.notify()
		}
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
			n.State.Mu.RLock()
			next := n.State.NextIndex[peer]
			if next < 1 {
				next = 1
			}
			prev := next - 1
			prevTerm := 0
			if prev > 0 {
				prevTerm = n.State.Log[prev-1].Term
			}
			entries := make([]entity.LogEntry, len(n.State.Log)-prev)
			copy(entries, n.State.Log[prev:])
			for i := range entries {
				entries[i].Command = append([]byte(nil), entries[i].Command...)
			}
			req := dto.AppendEntriesRequest{Term: term, LeaderID: n.ID, PrevLogIndex: prev,
				PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: n.State.CommitIndex}
			n.State.Mu.RUnlock()
			inFlight[peer] = term
			n.launchRPC(ctx, workers, results, peer, rpcRequest{append: &req})
		}
	}
}

func (n *RaftNode) launchRPC(ctx context.Context, workers *sync.WaitGroup, results chan<- rpcResult, peer string, req rpcRequest) {
	workers.Add(1)
	go func() {
		defer workers.Done()
		result := rpcResult{peer: peer, appendReq: req.append}
		if req.vote != nil {
			result.term = req.vote.Term
			resp, err := n.client.RequestVote(ctx, peer, *req.vote)
			result.vote, result.err = &resp, err
		} else {
			result.term = req.append.Term
			resp, err := n.client.AppendEntries(ctx, peer, *req.append)
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
	n.signalChanged()
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
		(n.State.VotedFor == "" || n.State.VotedFor == req.CandidateID) && n.candidateLogCurrent(req.LastLogIndex, req.LastLogTerm) {
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
	log.Info().Str("leader", req.LeaderID).Int("term", req.Term).
		Int("prev_log_index", req.PrevLogIndex).Int("prev_log_term", req.PrevLogTerm).
		Int("leader_commit", req.LeaderCommit).Interface("entries", req.Entries).
		Msg("received append entries")
	if n.stopped {
		return dto.AppendEntriesResponse{}, ErrUnavailable
	}
	if req.Term > n.State.CurrentTerm {
		n.followHigherTerm(req.Term)
	}
	resp := dto.AppendEntriesResponse{Term: n.State.CurrentTerm}
	if req.Term == n.State.CurrentTerm && req.LeaderID != n.ID {
		if n.State.State == entity.Leader || n.State.State == entity.Candidate {
			n.signalChanged()
		}
		n.State.State = entity.Follower
		n.State.LeaderID = req.LeaderID
		n.votes = nil
		n.resetDeadline()
		if req.PrevLogIndex <= len(n.State.Log) &&
			((req.PrevLogIndex == 0 && req.PrevLogTerm == 0) ||
				(req.PrevLogIndex > 0 && n.State.Log[req.PrevLogIndex-1].Term == req.PrevLogTerm)) {
			for i, entry := range req.Entries {
				index := req.PrevLogIndex + i
				if index < n.State.CommitIndex && n.State.Log[index].Term != entry.Term {
					return resp, nil
				}
			}
			resp.Success = true
			for i, entry := range req.Entries {
				index := req.PrevLogIndex + i
				if index < len(n.State.Log) && n.State.Log[index].Term != entry.Term {
					n.State.Log = n.State.Log[:index]
				}
				if index >= len(n.State.Log) {
					n.State.Log = append(n.State.Log, entity.LogEntry{Term: entry.Term,
						Command: append([]byte(nil), entry.Command...)})
				}
			}
			if req.LeaderCommit > n.State.CommitIndex {
				n.State.CommitIndex = min(req.LeaderCommit, len(n.State.Log))
				n.signalChanged()
			}
		}
	}
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
	n.signalChanged()
}
