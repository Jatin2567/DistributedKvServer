package main

import (
	"kvstore/internal/raft"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDemoAPI(t *testing.T) {
	n := raft.NewRaftNode("test", nil)
	n.Start()
	defer n.Stop()
	c := cluster{nodes: []*raft.RaftNode{n}}
	handler := c.routes()
	deadline := time.Now().Add(time.Second)
	for c.leader() == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for _, tc := range []struct {
		method, path, body string
		status             int
		contains           string
	}{
		{"GET", "/healthz", "", 200, "ok"},
		{"PUT", "/api/kv", `{"key":"demo","value":"hello"}`, 200, `"success":true`},
		{"GET", "/api/kv?key=demo", "", 200, `"value":"hello"`},
		{"DELETE", "/api/kv", `{"key":"demo"}`, 200, `"success":true`},
		{"GET", "/api/kv?key=demo", "", 200, `"found":false`},
		{"GET", "/api/kv", "", 400, "key is required"},
		{"PUT", "/api/kv", `{}`, 400, "provide a key"},
		{"PUT", "/api/kv", `{broken`, 400, "provide a key"},
		{"PUT", "/api/kv", `{"key":"x","value":"` + strings.Repeat("x", 9000) + `"}`, 400, "provide a key"},
		{"POST", "/api/kv", `{}`, 405, "Method Not Allowed"},
		{"GET", "/raft/request-vote", "", 404, "404"},
		{"GET", "/", "", 200, "One truth."},
	} {
		t.Run(tc.method+tc.path+tc.body[:min(len(tc.body), 10)], func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			handler.ServeHTTP(w, r)
			if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.contains) {
				t.Fatalf("got %d %s", w.Code, w.Body.String())
			}
		})
	}
}
