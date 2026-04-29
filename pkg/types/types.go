package types

// =====================
// Command (State Machine Input)
// =====================

type CommandType string

const (
	CommandPut    CommandType = "PUT"
	CommandDelete CommandType = "DELETE"
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value,omitempty"`
}

// =====================
// Log Entry (Raft Log)
// =====================

type LogEntry struct {
	Index   int     `json:"index"`
	Term    int     `json:"term"`
	Command Command `json:"command"`
}

// =====================
// AppendEntries RPC (Leader → Followers)
// =====================

type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// =====================
// RequestVote RPC (Election)
// =====================

type RequestVoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// =====================
// Client API Requests / Responses
// =====================

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetResponse struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type DeleteRequest struct {
	Key string `json:"key"`
}

// =====================
// Generic Response
// =====================

type Response struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}