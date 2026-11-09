package controller

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	node "github.com/Vovadinamik8913/raft/internal/core"
	"github.com/Vovadinamik8913/raft/internal/dto"
	"github.com/gin-gonic/gin"
)

type Controller struct {
	node *node.RaftNode
}

func New(n *node.RaftNode) *Controller {
	return &Controller{node: n}
}

func (ctrl *Controller) Status(c *gin.Context) {
	c.JSON(http.StatusOK, ctrl.node.Status())
}

func (ctrl *Controller) RequestVote(c *gin.Context) {
	var req dto.RequestVoteRequest
	if err := decodeRequest(c, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Term < 0 || strings.TrimSpace(req.CandidateID) == "" || req.LastLogIndex < 0 || req.LastLogTerm < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid RequestVote fields"})
		return
	}
	resp, err := ctrl.node.RequestVote(req)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (ctrl *Controller) AppendEntries(c *gin.Context) {
	var req dto.AppendEntriesRequest
	if err := decodeRequest(c, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Term < 0 || strings.TrimSpace(req.LeaderID) == "" || req.PrevLogIndex < 0 || req.PrevLogTerm < 0 || req.LeaderCommit < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid AppendEntries fields"})
		return
	}
	for _, entry := range req.Entries {
		if entry.Term < 0 || entry.Term > req.Term || len(entry.Command) == 0 || !json.Valid(entry.Command) || string(entry.Command) == "null" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid log entry"})
			return
		}
	}
	resp, err := ctrl.node.AppendEntries(req)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (ctrl *Controller) ShowLog(c *gin.Context) {
	c.JSON(http.StatusOK, ctrl.node.ShowLog())
}

func (ctrl *Controller) AddEntries(c *gin.Context) {
	var req dto.AddEntriesRequest
	if err := decodeRequest(c, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(req.Entries) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "entries must not be empty"})
		return
	}
	for _, entry := range req.Entries {
		if len(entry) == 0 || string(entry) == "null" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "entries must contain non-null JSON values"})
			return
		}
	}
	resp, err := ctrl.node.AddEntries(c.Request.Context(), req.Entries)
	if err != nil {
		if errors.Is(err, node.ErrNotLeader) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "leader_id": ctrl.node.Status().LeaderID})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, resp)
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
