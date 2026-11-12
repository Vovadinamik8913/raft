package dto

import "github.com/Vovadinamik8913/raft/internal/entity"

type AppendEntriesRequest struct {
	Term         int               `json:"term"`
	LeaderID     string            `json:"leader_id"`
	PrevLogIndex int               `json:"prev_log_index"`
	PrevLogTerm  int               `json:"prev_log_term"`
	Entries      []entity.LogEntry `json:"entries"`
	LeaderCommit int               `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}
