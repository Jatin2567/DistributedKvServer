package raft

import (
	"log"
	"time"

	"kvstore/pkg/types"
)

func (r *RaftNode) electionLoop() {
	for {
		timeout := randomElectionTimeout()

		select {
		case <-time.After(timeout):
			// No heartbeat received within timeout — start election
			r.mu.Lock()
			if r.state != Leader {
				r.startElection()
			}
			r.mu.Unlock()

		case <-r.resetElection:
			// FIX: heartbeat received from valid leader — reset timer by
			// looping again, which picks a fresh randomElectionTimeout()
			continue

		case <-r.stopCh:
			return
		}
	}
}

func (r *RaftNode) startElection() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id

	termStarted := r.currentTerm
	votes := 1

	log.Printf("[Node %s] became Candidate for term %d", r.id, r.currentTerm)

	lastLogIndex := len(r.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = r.log[lastLogIndex].Term
	}

	req := types.RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peer := range r.peers {
		go func(peer string) {
			resp, err := sendRequestVote(peer, req)
			if err != nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if r.state != Candidate || r.currentTerm != termStarted {
				return
			}

			if resp.Term > r.currentTerm {
				r.becomeFollower(resp.Term)
				return
			}

			if resp.VoteGranted {
				votes++
				if votes > len(r.peers)/2 {
					r.becomeLeader()
				}
			}
		}(peer)
	}
}

func (r *RaftNode) becomeLeader() {
	r.state = Leader

	log.Printf("[Node %s] became Leader for term %d", r.id, r.currentTerm)

	for _, peer := range r.peers {
		r.nextIndex[peer] = len(r.log)
		r.matchIndex[peer] = -1
	}

	go r.heartbeatLoop()
}

func (r *RaftNode) becomeFollower(term int) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""

	log.Printf("[Node %s] became Follower for term %d", r.id, term)
}

func (r *RaftNode) heartbeatLoop() {
	for {
		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		r.broadcastAppendEntries()

		select {
		case <-time.After(r.heartbeatTimeout):
		case <-r.stopCh:
			return
		}
	}
}

func (r *RaftNode) HandleRequestVote(req types.RequestVoteRequest) types.RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := types.RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}

	if req.Term < r.currentTerm {
		return resp
	}

	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term)
	}

	lastLogIndex := len(r.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = r.log[lastLogIndex].Term
	}

	upToDate := (req.LastLogTerm > lastLogTerm) ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if (r.votedFor == "" || r.votedFor == req.CandidateID) && upToDate {
		r.votedFor = req.CandidateID
		resp.VoteGranted = true

		// FIX: granting a vote also counts as activity — reset election timer
		select {
		case r.resetElection <- struct{}{}:
		default:
		}
	}

	resp.Term = r.currentTerm
	return resp
}
