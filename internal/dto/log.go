package dto

import "encoding/json"

type AddEntriesRequest struct {
	Entries []json.RawMessage `json:"entries"`
}

type AddEntriesResponse struct {
	FirstIndex  int `json:"first_index"`
	LastIndex   int `json:"last_index"`
	CommitIndex int `json:"commit_index"`
}

type LogItem struct {
	Index   int             `json:"index"`
	Term    int             `json:"term"`
	Command json.RawMessage `json:"command"`
}

type ShowLogResponse struct {
	Entries     []LogItem `json:"entries"`
	CommitIndex int       `json:"commit_index"`
}
