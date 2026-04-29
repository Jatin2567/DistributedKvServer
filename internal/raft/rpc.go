package raft

import (
	"kvstore/internal/transport"
	"kvstore/pkg/types"
)

func sendRequestVote(peer string, req types.RequestVoteRequest) (types.RequestVoteResponse, error) {
	return transport.SendRequestVote(peer, req)
}

func sendAppendEntries(peer string, req types.AppendEntriesRequest) (types.AppendEntriesResponse, error) {
	return transport.SendAppendEntries(peer, req)
}