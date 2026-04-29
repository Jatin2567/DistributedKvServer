package main

import (
	"log"
	"net/http"
	"os"

	"kvstore/internal/api"
	"kvstore/internal/config"
	"kvstore/internal/raft"
	"kvstore/internal/transport"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: server <config-file>")
	}

	configPath := os.Args[1]

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Initialize Raft node
	raftNode := raft.NewRaftNode(cfg.NodeID, cfg.Peers)

	// Start Raft processes
	raftNode.Start()

	// Initialize API handler
	handler := api.NewHandler(raftNode)

	// Register routes
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)
	transport.NewServer(raftNode).RegisterHandlers(mux)

	log.Printf("Node %s starting HTTP server on %s", cfg.NodeID, cfg.HTTPAddr)

	// Start HTTP server
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
