package raft

import "kvstore/pkg/types"

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

	if resp.Success {
		r.matchIndex[peer] = prevIdx + len(entries)
		r.nextIndex[peer] = r.matchIndex[peer] + 1
	} else {
		if r.nextIndex[peer] > 0 {
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
	r.log = append(r.log[:req.PrevLogIndex+1], req.Entries...)

	// update commit index
	if req.LeaderCommit > r.commitIndex {
		lastIndex := len(r.log) - 1
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
