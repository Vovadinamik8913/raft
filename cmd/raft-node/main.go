package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Vovadinamik8913/raft/internal/app"
	"github.com/Vovadinamik8913/raft/internal/config"
	"github.com/Vovadinamik8913/raft/internal/logging"
	"github.com/rs/zerolog/log"
)

func main() {
	cfg := config.Read()
	logging.Init(cfg.ID)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := app.Run(ctx, cfg)
	cancel()
	exitCode := 0
	if err != nil {
		log.Error().Err(err).Msg("raft-node failed")
		exitCode = 1
	}
	os.Exit(exitCode)
}
