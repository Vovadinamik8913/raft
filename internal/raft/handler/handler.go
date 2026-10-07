package handler

import (
	"log/slog"
	"net/http"

	"github.com/Vovadinamik8913/raft/internal/raft/model"
	"github.com/Vovadinamik8913/raft/internal/raft/node"
	"github.com/gin-gonic/gin"
)

func NewRouter(n *node.RaftNode, logger *slog.Logger) http.Handler {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery())

	r.POST("/request_vote", handleRequestVote(n, logger))
	r.POST("/append_entries", handleAppendEntries(n, logger))
	r.GET("/status", handleStatus(n))

	return r
}

func handleRequestVote(n *node.RaftNode, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.RequestVoteRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		n.State.Mu.Lock()
		defer n.State.Mu.Unlock()

		logger.Info("received RequestVote",
			"from", req.CandidateID, "term", req.Term)

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

		logger.Info("voting",
			"granted", voteGranted,
			"candidate", req.CandidateID,
			"term", req.Term)

		c.JSON(http.StatusOK, model.RequestVoteResponse{
			Term:        n.State.CurrentTerm,
			VoteGranted: voteGranted,
		})
	}
}

func handleAppendEntries(n *node.RaftNode, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.AppendEntriesRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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

		c.JSON(http.StatusOK, model.AppendEntriesResponse{
			Term:    n.State.CurrentTerm,
			Success: success,
		})
	}
}

func handleStatus(n *node.RaftNode) gin.HandlerFunc {
	return func(c *gin.Context) {
		n.State.Mu.RLock()
		defer n.State.Mu.RUnlock()

		c.JSON(http.StatusOK, model.NodeStatus{
			ID:       n.ID,
			State:    n.State.State.String(),
			Term:     n.State.CurrentTerm,
			LeaderID: n.State.LeaderID,
		})
	}
}