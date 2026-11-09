package app

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

func shutdownServer(srv *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		_ = srv.Close()
		return fmt.Errorf("http shutdown failed: %w", err)
	}
	return nil
}
