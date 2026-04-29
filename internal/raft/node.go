package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"kvstore/internal/statemachine"
	"kvstore/pkg/types"
)

type RaftNode struct {
	mu sync.Mutex

	// identity
	id    string
	peers []string

	// persistent state
	currentTerm int
	votedFor    string
	log         []types.LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// leader state
	nextIndex  map[string]int
	matchIndex map[string]int

	// role
	state State

	// channels
	applyCh        chan types.LogEntry
	stopCh         chan struct{}
	resetElection  chan struct{} // FIX: signals electionLoop to reset its timer

	// timeouts
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	// state machine
	stateMachine *statemachine.KVStateMachine
}

func NewRaftNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		id:    id,
		peers: peers,

		currentTerm: 0,
		votedFor:    "",
		log:         make([]types.LogEntry, 0),

		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),

		state: Follower,

		applyCh:       make(chan types.LogEntry, 100),
		stopCh:        make(chan struct{}),
		resetElection: make(chan struct{}, 1), // FIX: buffered so sender never blocks

		electionTimeout:  randomElectionTimeout(),
		heartbeatTimeout: 50 * time.Millisecond,

		stateMachine: statemachine.NewKVStateMachine(),
	}
}

func (r *RaftNode) Start() {
	log.Printf("[Node %s] starting as %s", r.id, r.state.String())

	go r.electionLoop()
	go r.applyLoop()
}

func (r *RaftNode) Stop() {
	close(r.stopCh)
	log.Printf("[Node %s] stopped", r.id)
}

func (r *RaftNode) Get(key string) (string, bool) {
	return r.stateMachine.Get(key)
}

func randomElectionTimeout() time.Duration {
	// FIX: use math/rand instead of time.Now().UnixNano() % n
	// which produced near-identical values across nodes started simultaneously
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func init() {
	// FIX: seed the global rand source once at startup
	rand.Seed(time.Now().UnixNano())
}
