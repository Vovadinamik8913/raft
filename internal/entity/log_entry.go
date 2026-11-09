package entity

import "encoding/json"

type LogEntry struct {
	Term    int             `json:"term"`
	Command json.RawMessage `json:"command"`
}
