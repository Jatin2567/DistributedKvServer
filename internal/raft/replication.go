package raft

import (
	"context"
	"fmt"
	"kvstore/pkg/types"
	"time"
)

func (r *RaftNode) Propose(cmd types.Command) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return ErrNotLeader
	}

	entry := types.LogEntry{
		Index:   len(r.log),
		Term:    r.currentTerm,
		Command: cmd,
	}

	r.log = append(r.log, entry)
	r.updateCommitIndex()

	go r.broadcastAppendEntries()

	return nil
}

var ErrNotLeader = &RaftError{"not leader"}

type RaftError struct {
	msg string
}

func (e *RaftError) Error() string {
	return e.msg
}

func (r *RaftNode) broadcastAppendEntries() {
	for _, peer := range r.peers {
		go r.sendAppendEntries(peer)
	}
}

func (r *RaftNode) sendAppendEntries(peer string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	nextIdx := r.nextIndex[peer]
	prevIdx := nextIdx - 1

	var prevTerm int
	if prevIdx >= 0 && prevIdx < len(r.log) {
		prevTerm = r.log[prevIdx].Term
	}

	entries := make([]types.LogEntry, len(r.log[nextIdx:]))
	copy(entries, r.log[nextIdx:])

	req := types.AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}

	r.mu.Unlock()

	resp, err := sendAppendEntries(peer, req)
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.becomeFollower(resp.Term)
		return
	}
	if r.state != Leader || r.currentTerm != req.Term {
		return
	}

	if resp.Success {
		if prevIdx+len(entries) < r.matchIndex[peer] {
			return
		}
		r.matchIndex[peer] = prevIdx + len(entries)
		r.nextIndex[peer] = r.matchIndex[peer] + 1
	} else {
		if r.nextIndex[peer] == nextIdx && r.nextIndex[peer] > 0 {
			r.nextIndex[peer]--
		}
	}

	r.updateCommitIndex()
}

func (r *RaftNode) HandleAppendEntries(req types.AppendEntriesRequest) types.AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := types.AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}

	if req.Term < r.currentTerm {
		return resp
	}

	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term)
	}
	resp.Term = r.currentTerm
	if req.PrevLogIndex < -1 {
		return resp
	}

	// reset role to follower on valid leader contact
	r.state = Follower

	// FIX: signal electionLoop to reset its timer — this is the core fix for
	// bug #1. Without this, followers time out and start elections even while
	// a healthy leader is sending heartbeats.
	select {
	case r.resetElection <- struct{}{}:
	default:
	}

	// consistency check
	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= len(r.log) {
			return resp
		}
		if r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			return resp
		}
	}

	// append entries (overwrite conflicting entries)
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + 1 + i
		if index < len(r.log) && r.log[index].Term != entry.Term {
			if index <= r.commitIndex {
				return resp
			}
			r.log = r.log[:index]
		}
		if index >= len(r.log) {
			r.log = append(r.log, req.Entries[i:]...)
			break
		}
	}

	// update commit index
	if req.LeaderCommit > r.commitIndex {
		lastIndex := req.PrevLogIndex + len(req.Entries)
		if req.LeaderCommit < lastIndex {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = lastIndex
		}
	}

	resp.Success = true
	resp.Term = r.currentTerm
	return resp
}

// ProposeWait acknowledges only after this exact entry has been applied.
func (r *RaftNode) ProposeWait(ctx context.Context, cmd types.Command) error {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	if len(r.log) >= 10000 {
		r.mu.Unlock()
		return fmt.Errorf("demo log full; restart to reset")
	}
	index, term := len(r.log), r.currentTerm
	r.log = append(r.log, types.LogEntry{Index: index, Term: term, Command: cmd})
	r.updateCommitIndex()
	r.mu.Unlock()
	go r.broadcastAppendEntries()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return fmt.Errorf("node stopped")
		case <-ticker.C:
			r.mu.Lock()
			applied := r.lastApplied >= index && index < len(r.log) && r.log[index].Term == term
			changed := r.currentTerm != term || r.state != Leader
			r.mu.Unlock()
			if applied {
				return nil
			}
			if changed {
				return ErrNotLeader
			}
		}
	}
}
