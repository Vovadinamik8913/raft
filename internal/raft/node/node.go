package node

import (
    "bytes"
    "encoding/json"
    "log"
    "math/rand"
    "net/http"
    "sync"
    "time"

    "github.com/Vovadinamik8913/raft/internal/raft/model"
)

const (
	ElectionTimeoutMin = 500 * time.Millisecond
	ElectionTimeoutMax = 1000 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
)

type RaftNode struct {
	ID    string
	Peers []string
	State *model.State

	httpClient *http.Client

	electionTimer *time.Timer
	resetElection chan struct{}
	heartbeatStop chan struct{}
}

func NewRaftNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		ID:            id,
		Peers:         peers,
		State:         model.NewState(),
		httpClient:    &http.Client{Timeout: 500 * time.Millisecond},
		resetElection: make(chan struct{}, 1),
	}
}

func (n *RaftNode) Run() {
	n.resetElectionTimer()

	for {
		select {
		case <-n.electionTimer.C:
			go n.startElection()
		case <-n.resetElection:
			n.resetElectionTimer()
		}
	}
}

func (n *RaftNode) resetElectionTimer() {
	timeout := time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin))) + ElectionTimeoutMin
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		n.electionTimer.Reset(timeout)
	}
}

func (n *RaftNode) startElection() {
	n.State.Mu.Lock()
	n.State.State = model.Candidate
	n.State.CurrentTerm++
	n.State.VotedFor = n.ID
	term := n.State.CurrentTerm
	n.State.Mu.Unlock()

	log.Printf("[%s] Starting election for term %d", n.ID, term)
	n.resetElectionTimer() 

	votes := 1 
	var mu sync.Mutex

	for _, peer := range n.Peers {
		go func(peerURL string) {
			req := model.RequestVoteRequest{
				Term:         term,
				CandidateID:  n.ID,
				LastLogIndex: 0,
				LastLogTerm:  0,
			}
			resp, err := n.sendRequestVote(peerURL, req)
			if err != nil {
				return
			}

			n.State.Mu.Lock()
			defer n.State.Mu.Unlock()

			if resp.Term > n.State.CurrentTerm {
				n.State.CurrentTerm = resp.Term
				n.State.State = model.Follower
				n.State.VotedFor = ""
				return
			}

			if resp.VoteGranted {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}(peer)
	}

	time.Sleep(50 * time.Millisecond)

	if n.State.State == model.Candidate && votes > (len(n.Peers)+1)/2 {
		log.Printf("[%s] Became LEADER for term %d", n.ID, term)
		n.becomeLeader()
	}
}

func (n *RaftNode) becomeLeader() {
	n.State.Mu.Lock()
	n.State.State = model.Leader
	n.State.Mu.Unlock()

	n.heartbeatStop = make(chan struct{})
	go n.heartbeatLoop()
}

func (n *RaftNode) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.sendHeartbeats()
		case <-n.heartbeatStop:
			return
		}
	}
}

func (n *RaftNode) sendHeartbeats() {
	n.State.Mu.RLock()
	term := n.State.CurrentTerm
	commitIdx := 0
	n.State.Mu.RUnlock()

	for _, peer := range n.Peers {
		go func(peerURL string) {
			req := model.AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.ID,
				LeaderCommit: commitIdx,
			}
			resp, err := n.sendAppendEntries(peerURL, req)
			if err != nil {
				return
			}

			n.State.Mu.Lock()
			defer n.State.Mu.Unlock()
			if resp.Term > n.State.CurrentTerm {
				n.State.CurrentTerm = resp.Term
				n.State.State = model.Follower
				n.State.VotedFor = ""
				n.State.LeaderID = ""
				close(n.heartbeatStop)
			}
		}(peer)
	}
}


func (n *RaftNode) sendRequestVote(peerURL string, req model.RequestVoteRequest) (*model.RequestVoteResponse, error) {
	body, _ := json.Marshal(req)
	resp, err := n.httpClient.Post(peerURL+"/request_vote", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var voteResp model.RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return nil, err
	}
	return &voteResp, nil
}

func (n *RaftNode) sendAppendEntries(peerURL string, req model.AppendEntriesRequest) (*model.AppendEntriesResponse, error) {
	body, _ := json.Marshal(req)
	resp, err := n.httpClient.Post(peerURL+"/append_entries", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var aeResp model.AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&aeResp); err != nil {
		return nil, err
	}
	return &aeResp, nil
}

func (n *RaftNode) ResetElectionTimeout() {
	select {
	case n.resetElection <- struct{}{}:
	default:
		
	}
}