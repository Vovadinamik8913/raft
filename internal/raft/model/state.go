package model

import "sync"

type State struct {
	Mu sync.RWMutex

	CurrentTerm int
	VotedFor    string
	State       NodeState
	LeaderID    string
}

func NewState() *State {
	return &State{
		CurrentTerm: 0,
		VotedFor:    "",
		State:       Follower,
		LeaderID:    "",
	}
}
