package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/Vovadinamik8913/raft/internal/raft/model"
	"github.com/Vovadinamik8913/raft/internal/raft/node"
	"strings"

	"github.com/gin-gonic/gin"
)

func NewRouter(n *node.RaftNode, logger *slog.Logger) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/status", func(c *gin.Context) {
		c.JSON(http.StatusOK, n.Status())
	})
	router.POST("/request_vote", func(c *gin.Context) {
		var req model.RequestVoteRequest
		if err := decodeRequest(c, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if req.Term < 0 || strings.TrimSpace(req.CandidateID) == "" || req.LastLogIndex < 0 || req.LastLogTerm < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid RequestVote fields"})
			return
		}
		logger.Debug("received RequestVote", "candidate", req.CandidateID, "term", req.Term)
		resp, err := n.RequestVote(req)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, resp)
	})
	router.POST("/append_entries", func(c *gin.Context) {
		var req model.AppendEntriesRequest
		if err := decodeRequest(c, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if req.Term < 0 || strings.TrimSpace(req.LeaderID) == "" || req.PrevLogIndex < 0 || req.PrevLogTerm < 0 || req.LeaderCommit < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid AppendEntries fields"})
			return
		}
		if len(req.Entries) != 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Part 1 supports empty heartbeats only"})
			return
		}
		resp, err := n.AppendEntries(req)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, resp)
	})
	return router
}

func decodeRequest(c *gin.Context, target any) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	decoder := json.NewDecoder(c.Request.Body)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}
