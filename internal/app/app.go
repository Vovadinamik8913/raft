package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/Vovadinamik8913/raft/internal/config"
	"github.com/Vovadinamik8913/raft/internal/node"
	"github.com/Vovadinamik8913/raft/internal/server"
	"github.com/rs/zerolog/log"
)

func Run(parent context.Context, cfg config.Config) error {
	n, err := node.NewRaftNode(cfg.ID, cfg.Peers)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	srv := server.New(cfg.Port, n)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	raftDone := make(chan error, 1)
	go func() {
		err := n.Run(ctx)
		if err != nil {
			err = fmt.Errorf("raft stopped: %w", err)
			cancel()
		}
		raftDone <- err
	}()

	serverDone := make(chan error, 1)
	go func() {
		err := server.Serve(srv)
		if err != nil {
			cancel()
		}
		serverDone <- err
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down")
	shutdownErr := shutdownServer(srv)
	raftErr := <-raftDone
	serverErr := <-serverDone
	return errors.Join(raftErr, serverErr, shutdownErr)
}
