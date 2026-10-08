package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Vovadinamik8913/raft/internal/raft/handler"
	"github.com/Vovadinamik8913/raft/internal/raft/node"
)

func main() {
	var (
		id        = flag.String("id", "node1", "Unique node ID")
		port      = flag.String("port", "8000", "HTTP port to listen on")
		peersFlag = flag.String("peers", "", "Comma-separated peer URLs")
	)
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("node", *id)
	n, err := node.NewRaftNode(*id, parsePeers(*peersFlag))
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	raftDone := make(chan struct{})
	go func() {
		defer close(raftDone)
		if err := n.Run(ctx); err != nil {
			logger.Error("raft stopped", "err", err)
			cancel()
		}
	}()

	srv := &http.Server{
		Addr:              ":" + *port,
		Handler:           handler.NewRouter(n, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverDone := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", srv.Addr)
		err := srv.ListenAndServe()
		serverDone <- err
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			cancel()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = srv.Close()
	}
	<-raftDone
	serverErr := <-serverDone
	if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
		logger.Error("http server failed", "err", serverErr)
		os.Exit(1)
	}
	if shutdownErr != nil {
		logger.Error("http shutdown failed", "err", shutdownErr)
		os.Exit(1)
	}
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
