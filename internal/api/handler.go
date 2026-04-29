package api

import (
	"encoding/json"
	"net/http"

	"kvstore/internal/raft"
	"kvstore/pkg/types"
)

type Handler struct {
	node *raft.RaftNode
}

func NewHandler(node *raft.RaftNode) *Handler {
	return &Handler{node: node}
}

func (h *Handler) HandlePut(w http.ResponseWriter, r *http.Request) {
	var req types.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	cmd := types.Command{
		Type:  types.CommandPut,
		Key:   req.Key,
		Value: req.Value,
	}

	if err := h.node.Propose(cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, types.Response{Success: true})
}

func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	val, ok := h.node.Get(key)
	writeJSON(w, types.GetResponse{
		Value: val,
		Found: ok,
	})
}

func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	var req types.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	cmd := types.Command{
		Type: types.CommandDelete,
		Key:  req.Key,
	}

	if err := h.node.Propose(cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, types.Response{Success: true})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
