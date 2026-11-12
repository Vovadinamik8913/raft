package node

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Vovadinamik8913/raft/internal/dto"
	"github.com/Vovadinamik8913/raft/internal/entity"
)

var ErrNotLeader = errors.New("node is not leader")
var ErrLeadershipLost = errors.New("leadership lost before commit")

func (n *RaftNode) lastLogInfo() (int, int) {
	index := len(n.State.Log)
	if index == 0 {
		return 0, 0
	}
	return index, n.State.Log[index-1].Term
}

func (n *RaftNode) candidateLogCurrent(index, term int) bool {
	lastIndex, lastTerm := n.lastLogInfo()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (n *RaftNode) signalChanged() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *RaftNode) advanceCommit() {
	for index := len(n.State.Log); index > n.State.CommitIndex; index-- {
		if n.State.Log[index-1].Term != n.State.CurrentTerm {
			continue
		}
		count := 1
		for _, peer := range n.Peers {
			if n.State.MatchIndex[peer] >= index {
				count++
			}
		}
		if count >= (len(n.Peers)+1)/2+1 {
			n.State.CommitIndex = index
			n.signalChanged()
			n.notify()
			return
		}
	}
}

func (n *RaftNode) AddEntries(ctx context.Context, commands []json.RawMessage) (dto.AddEntriesResponse, error) {
	n.State.Mu.Lock()
	if n.stopped {
		n.State.Mu.Unlock()
		return dto.AddEntriesResponse{}, ErrUnavailable
	}
	if n.State.State != entity.Leader {
		n.State.Mu.Unlock()
		return dto.AddEntriesResponse{}, ErrNotLeader
	}
	if len(commands) == 0 {
		n.State.Mu.Unlock()
		return dto.AddEntriesResponse{}, errors.New("entries must not be empty")
	}
	term := n.State.CurrentTerm
	first := len(n.State.Log) + 1
	for _, command := range commands {
		n.State.Log = append(n.State.Log, entity.LogEntry{Term: term,
			Command: append([]byte(nil), command...)})
	}
	last := len(n.State.Log)
	n.advanceCommit()
	n.notify()
	for {
		if n.State.CommitIndex >= last {
			resp := dto.AddEntriesResponse{FirstIndex: first, LastIndex: last, CommitIndex: n.State.CommitIndex}
			n.State.Mu.Unlock()
			return resp, nil
		}
		if n.stopped || n.State.State != entity.Leader || n.State.CurrentTerm != term {
			n.State.Mu.Unlock()
			return dto.AddEntriesResponse{}, ErrLeadershipLost
		}
		changed := n.changed
		n.State.Mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return dto.AddEntriesResponse{}, ctx.Err()
		}
		n.State.Mu.Lock()
	}
}

func (n *RaftNode) ShowLog() dto.ShowLogResponse {
	n.State.Mu.RLock()
	defer n.State.Mu.RUnlock()
	resp := dto.ShowLogResponse{Entries: make([]dto.LogItem, len(n.State.Log)), CommitIndex: n.State.CommitIndex}
	for i, entry := range n.State.Log {
		resp.Entries[i] = dto.LogItem{Index: i + 1, Term: entry.Term,
			Command: append([]byte(nil), entry.Command...)}
	}
	return resp
}
