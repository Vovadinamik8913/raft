package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/Vovadinamik8913/raft/internal/raft/model"
	"github.com/Vovadinamik8913/raft/internal/raft/node"
)

func HandleRequestVote(n *node.RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req model.RequestVoteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		n.State.Mu.Lock()
		defer n.State.Mu.Unlock()

		log.Printf("[%s] Received RequestVote from %s for term %d",
			n.ID, req.CandidateID, req.Term)

		if req.Term > n.State.CurrentTerm {
			n.State.CurrentTerm = req.Term
			n.State.State = model.Follower
			n.State.VotedFor = ""
		}

		voteGranted := false
		if req.Term == n.State.CurrentTerm {
			if n.State.VotedFor == "" || n.State.VotedFor == req.CandidateID {
				voteGranted = true
				n.State.VotedFor = req.CandidateID
				defer n.ResetElectionTimeout()
			}
		}

		log.Printf("[%s] Voting %v for %s in term %d",
			n.ID, voteGranted, req.CandidateID, req.Term)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(model.RequestVoteResponse{
			Term:        n.State.CurrentTerm,
			VoteGranted: voteGranted,
		})
	}
}

func HandleAppendEntries(n *node.RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req model.AppendEntriesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		n.State.Mu.Lock()
		defer n.State.Mu.Unlock()

		if req.Term > n.State.CurrentTerm {
			n.State.CurrentTerm = req.Term
			n.State.State = model.Follower
			n.State.VotedFor = ""
		}

		success := false
		if req.Term == n.State.CurrentTerm {
			n.State.State = model.Follower
			n.State.LeaderID = req.LeaderID
			defer n.ResetElectionTimeout()
			success = true
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(model.AppendEntriesResponse{
			Term:    n.State.CurrentTerm,
			Success: success,
		})
	}
}

func HandleStatus(n *node.RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		n.State.Mu.RLock()
		defer n.State.Mu.RUnlock()

		status := model.NodeStatus{
			ID:       n.ID,
			State:    n.State.State.String(),
			Term:     n.State.CurrentTerm,
			LeaderID: n.State.LeaderID,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}
}