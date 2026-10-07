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

	logger := newLogger("info", *id)
	peers := parsePeers(*peersFlag)
	if len(peers) == 0 {
		logger.Warn("no peers configured — running single-node cluster")
	}

	n := node.NewRaftNode(*id, peers)

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go n.Run()

	srv := &http.Server{
		Addr:              ":" + *port,
		Handler:           handler.NewRouter(n, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func parsePeers(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newLogger(level, id string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("node", id)
}