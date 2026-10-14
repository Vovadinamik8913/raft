package dto

type NodeStatus struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Term     int    `json:"term"`
	LeaderID string `json:"leader_id,omitempty"`
}
