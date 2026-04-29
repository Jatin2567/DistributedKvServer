package raft

import "kvstore/pkg/types"

func (r *RaftNode) lastLogIndex() int {
	return len(r.log) - 1
}

func (r *RaftNode) lastLogTerm() int {
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

func (r *RaftNode) appendEntry(entry types.LogEntry) {
	r.log = append(r.log, entry)
}