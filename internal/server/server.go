package server

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Vovadinamik8913/raft/internal/node"
	"github.com/rs/zerolog/log"
)

func New(port string, n *node.RaftNode) *http.Server {
	return &http.Server{
		Addr:              ":" + port,
		Handler:           newRouter(n),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func Serve(srv *http.Server) error {
	log.Info().Str("addr", srv.Addr).Msg("http server listening")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server failed: %w", err)
	}
	return nil
}
