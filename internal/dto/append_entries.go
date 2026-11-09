package dto

import "encoding/json"

type AppendEntriesRequest struct {
	Term         int               `json:"term"`
	LeaderID     string            `json:"leader_id"`
	PrevLogIndex int               `json:"prev_log_index"`
	PrevLogTerm  int               `json:"prev_log_term"`
	Entries      []json.RawMessage `json:"entries"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}
