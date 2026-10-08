package model

import "encoding/json"

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

type RequestVoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         int               `json:"term"`
	LeaderID     string            `json:"leader_id"`
	PrevLogIndex int               `json:"prev_log_index"`
	PrevLogTerm  int               `json:"prev_log_term"`
	Entries      []json.RawMessage `json:"entries"`
	LeaderCommit int               `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type NodeStatus struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Term     int    `json:"term"`
	LeaderID string `json:"leader_id,omitempty"`
}
