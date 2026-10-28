package raft

type Status struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        int    `json:"term"`
	LogLength   int    `json:"log_length"`
	CommitIndex int    `json:"commit_index"`
	Applied     int    `json:"applied"`
}

func (r *RaftNode) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{r.id, r.state.String(), r.currentTerm, len(r.log), r.commitIndex, r.lastApplied}
}
