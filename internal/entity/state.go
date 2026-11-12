package entity

import "sync"

type State struct {
	Mu sync.RWMutex

	CurrentTerm int
	VotedFor    string
	State       NodeState
	LeaderID    string
	Log         []LogEntry
	CommitIndex int
	NextIndex   map[string]int
	MatchIndex  map[string]int
}

func NewState() *State {
	return &State{
		CurrentTerm: 0,
		VotedFor:    "",
		State:       Follower,
		LeaderID:    "",
	}
}
