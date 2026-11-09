package config

import (
	"flag"
	"strings"
)

type Config struct {
	ID    string
	Port  string
	Peers []string
}

func Read() Config {
	id := flag.String("id", "node1", "Unique node ID")
	port := flag.String("port", "8000", "HTTP port to listen on")
	peers := flag.String("peers", "", "Comma-separated peer URLs")
	flag.Parse()
	return Config{ID: *id, Port: *port, Peers: parsePeers(*peers)}
}

func parsePeers(s string) []string {
	var peers []string
	for _, peer := range strings.Split(s, ",") {
		if peer = strings.TrimSpace(peer); peer != "" {
			peers = append(peers, peer)
		}
	}
	return peers
}
