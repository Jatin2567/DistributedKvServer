package raft

import (
	"log"
	"time"

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
	// FIX: replace the busy-spin `default:` branch with a ticker.
	// The old code used `select { default: ... }` which meant the goroutine
	// looped at full CPU speed (100% core usage), starving the election and
	// heartbeat goroutines and causing spurious timeouts / leader flipping.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return

		case <-ticker.C:
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
