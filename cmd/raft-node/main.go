package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/Vovadinamik8913/raft/internal/raft/handler"
	"github.com/Vovadinamik8913/raft/internal/raft/node"
)

func main() {
	id := flag.String("id", "node1", "Node ID")
	port := flag.String("port", "8000", "Port to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer URLs")
	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	n := node.NewRaftNode(*id, peers)
	go n.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/request_vote", handler.HandleRequestVote(n))
	mux.HandleFunc("/append_entries", handler.HandleAppendEntries(n))
	mux.HandleFunc("/status", handler.HandleStatus(n))

	log.Printf("[%s] Starting server on :%s", *id, *port)
	if err := http.ListenAndServe(":"+*port, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}