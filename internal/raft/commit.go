package raft

import (
	"log"

	"kvstore/pkg/types"
)

func (r *RaftNode) updateCommitIndex() {
	for i := len(r.log) - 1; i > r.commitIndex; i-- {
		count := 1

		for _, peer := range r.peers {
			if r.matchIndex[peer] >= i {
				count++
			}
		}

		if count > len(r.peers)/2 && r.log[i].Term == r.currentTerm {
			r.commitIndex = i
			break
		}
	}
}

func (r *RaftNode) applyLoop() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
			r.mu.Lock()
			entries := make([]types.LogEntry, 0, r.commitIndex-r.lastApplied)

			for r.lastApplied < r.commitIndex {
				r.lastApplied++
				entries = append(entries, r.log[r.lastApplied])
			}

			r.mu.Unlock()

			for _, entry := range entries {
				if _, err := r.stateMachine.Apply(entry); err != nil {
					log.Printf("[Node %s] apply error: %v", r.id, err)
				}
			}
		}
	}
}
