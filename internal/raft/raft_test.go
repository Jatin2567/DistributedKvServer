package raft

import (
	"context"
	"kvstore/internal/transport"
	"kvstore/pkg/types"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMajorityIncludesLeader(t *testing.T) {
	n := NewRaftNode("a", []string{"b", "c", "d"})
	n.currentTerm = 1
	n.log = []types.LogEntry{{Term: 1}}
	n.matchIndex = map[string]int{"b": 0, "c": -1, "d": -1}
	n.updateCommitIndex()
	if n.commitIndex != -1 {
		t.Fatal("two of four must not commit")
	}
	n.matchIndex["c"] = 0
	n.updateCommitIndex()
	if n.commitIndex != 0 {
		t.Fatal("three of four must commit")
	}
}

func TestHeartbeatPreservesSuffix(t *testing.T) {
	n := NewRaftNode("a", nil)
	n.currentTerm = 1
	n.log = []types.LogEntry{{Term: 1}, {Term: 1}}
	response := n.HandleAppendEntries(types.AppendEntriesRequest{Term: 1, PrevLogIndex: 0, PrevLogTerm: 1, LeaderCommit: -1})
	if !response.Success || len(n.log) != 2 {
		t.Fatal("heartbeat truncated suffix")
	}
	response = n.HandleAppendEntries(types.AppendEntriesRequest{Term: 2, PrevLogIndex: -2})
	if response.Success || response.Term != 2 {
		t.Fatal("malformed request accepted or wrong term")
	}
}

func TestSingleNodeCommit(t *testing.T) {
	n := NewRaftNode("a", nil)
	n.Start()
	defer n.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for n.Status().Role != "Leader" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.ProposeWait(ctx, types.Command{Type: types.CommandPut, Key: "k", Value: "v"}); err != nil {
		t.Fatal(err)
	}
	if value, ok := n.Get("k"); !ok || value != "v" {
		t.Fatal("acknowledged before apply")
	}
}

func TestThreeNodeReplicationAndLeaderFailover(t *testing.T) {
	nodes := make([]*RaftNode, 3)
	servers := make([]*httptest.Server, 3)
	addresses := make([]string, 3)
	for i := range servers {
		servers[i] = httptest.NewUnstartedServer(nil)
		addresses[i] = servers[i].Listener.Addr().String()
	}
	for i := range nodes {
		peers := []string{}
		for j, address := range addresses {
			if i != j {
				peers = append(peers, address)
			}
		}
		nodes[i] = NewRaftNode(addresses[i], peers)
		mux := http.NewServeMux()
		transport.NewServer(nodes[i]).RegisterHandlers(mux)
		servers[i].Config.Handler = mux
		servers[i].Start()
		nodes[i].Start()
	}
	stopped := -1
	defer func() {
		for i := range nodes {
			servers[i].Close()
			if i != stopped {
				nodes[i].Stop()
			}
		}
	}()
	findLeader := func(exclude int) int {
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			for i, n := range nodes {
				if i != exclude && n.Status().Role == "Leader" {
					return i
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("no leader elected")
		return -1
	}
	write := func(i int, kind types.CommandType) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := nodes[i].ProposeWait(ctx, types.Command{Type: kind, Key: "test", Value: "replicated"}); err != nil {
			t.Fatal(err)
		}
	}
	leader := findLeader(-1)
	write(leader, types.CommandPut)
	deadline := time.Now().Add(2 * time.Second)
	for {
		count := 0
		for _, n := range nodes {
			v, ok := n.Get("test")
			if ok && strings.Contains(v, "replicated") {
				count++
			}
		}
		if count == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not replicated to all nodes")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopped = leader
	nodes[leader].Stop()
	servers[leader].Close()
	leader = findLeader(stopped)
	write(leader, types.CommandDelete)
	if _, ok := nodes[leader].Get("test"); ok {
		t.Fatal("delete not applied")
	}
	// Isolate the surviving leader: no quorum means no successful acknowledgment.
	for i := range servers {
		if i != leader {
			servers[i].Close()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := nodes[leader].ProposeWait(ctx, types.Command{Type: types.CommandPut, Key: "no-quorum"}); err == nil {
		t.Fatal("write succeeded without quorum")
	}
}
