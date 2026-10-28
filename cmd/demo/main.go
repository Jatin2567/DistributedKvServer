// The demo runs three real Raft nodes over loopback HTTP in one service.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"kvstore/internal/raft"
	"kvstore/internal/transport"
	"kvstore/pkg/types"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

//go:embed web/*
var assets embed.FS

type cluster struct{ nodes []*raft.RaftNode }

func (c *cluster) leader() *raft.RaftNode {
	for _, n := range c.nodes {
		if n.Status().Role == "Leader" {
			return n
		}
	}
	return nil
}
func jsonResponse(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(value)
}

func (c *cluster) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if c.leader() == nil {
			http.Error(w, "election in progress", 503)
			return
		}
		jsonResponse(w, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/cluster", func(w http.ResponseWriter, r *http.Request) {
		statuses := []raft.Status{}
		for _, n := range c.nodes {
			statuses = append(statuses, n.Status())
		}
		jsonResponse(w, statuses)
	})
	mux.HandleFunc("GET /api/kv", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required", 400)
			return
		}
		n := c.leader()
		if n == nil {
			http.Error(w, "election in progress; retry shortly", 503)
			return
		}
		value, found := n.Get(key)
		jsonResponse(w, types.GetResponse{Value: value, Found: found})
	})
	for _, method := range []string{"PUT", "DELETE"} {
		mux.HandleFunc(method+" /api/kv", func(w http.ResponseWriter, r *http.Request) {
			var input types.PutRequest
			r.Body = http.MaxBytesReader(w, r.Body, 8192)
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil || len(input.Key) == 0 || len(input.Key) > 256 || len(input.Value) > 4096 {
				http.Error(w, "provide a key (1–256 bytes) and value (up to 4096 bytes)", 400)
				return
			}
			n := c.leader()
			if n == nil {
				http.Error(w, "election in progress; retry shortly", 503)
				return
			}
			kind := types.CommandPut
			if r.Method == "DELETE" {
				kind = types.CommandDelete
			}
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			if err := n.ProposeWait(ctx, types.Command{Type: kind, Key: input.Key, Value: input.Value}); err != nil {
				http.Error(w, "write not confirmed: "+err.Error(), 503)
				return
			}
			jsonResponse(w, types.Response{Success: true, Message: "Committed to a majority and applied"})
		})
	}
	web, _ := fs.Sub(assets, "web")
	mux.Handle("GET /", http.FileServer(http.FS(web)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; frame-ancestors 'none'")
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func main() {
	listeners := make([]net.Listener, 3)
	addresses := make([]string, 3)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}
		listeners[i] = l
		addresses[i] = l.Addr().String()
	}
	c := &cluster{}
	servers := []*http.Server{}
	for i := range listeners {
		peers := []string{}
		for j, addr := range addresses {
			if j != i {
				peers = append(peers, addr)
			}
		}
		n := raft.NewRaftNode(fmt.Sprintf("node-%d", i+1), peers)
		c.nodes = append(c.nodes, n)
		mux := http.NewServeMux()
		transport.NewServer(n).RegisterHandlers(mux)
		server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		servers = append(servers, server)
		go server.Serve(listeners[i])
	}
	for _, n := range c.nodes {
		n.Start()
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{Addr: ":" + port, Handler: c.routes(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	servers = append(servers, server)
	go func() {
		log.Printf("KV console listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, s := range servers {
		s.Shutdown(ctx)
	}
	for _, n := range c.nodes {
		n.Stop()
	}
}
