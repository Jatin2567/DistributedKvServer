package transport

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"kvstore/pkg/types"
)

type RaftHandler interface {
	HandleRequestVote(req types.RequestVoteRequest) types.RequestVoteResponse
	HandleAppendEntries(req types.AppendEntriesRequest) types.AppendEntriesResponse
}

type Server struct {
	handler RaftHandler
}

func NewServer(h RaftHandler) *Server {
	return &Server{handler: h}
}

func (s *Server) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/raft/request-vote", s.handleRequestVote)
	mux.HandleFunc("/raft/append-entries", s.handleAppendEntries)
}

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req types.RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.handler.HandleRequestVote(req)
	writeJSON(w, resp)
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req types.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.handler.HandleAppendEntries(req)
	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// =====================
// RPC Client Functions
// =====================

var httpClient = &http.Client{
	Timeout: 3 * time.Second,
}

func SendRequestVote(peer string, req types.RequestVoteRequest) (types.RequestVoteResponse, error) {
	var resp types.RequestVoteResponse

	data, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}

	httpResp, err := httpClient.Post("http://"+peer+"/raft/request-vote", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return resp, err
	}
	defer httpResp.Body.Close()

	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return resp, err
	}

	return resp, nil
}

func SendAppendEntries(peer string, req types.AppendEntriesRequest) (types.AppendEntriesResponse, error) {
	var resp types.AppendEntriesResponse

	data, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}

	httpResp, err := httpClient.Post("http://"+peer+"/raft/append-entries", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return resp, err
	}
	defer httpResp.Body.Close()

	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return resp, err
	}

	return resp, nil
}	