package server

import (
	"net/http"

	"github.com/Vovadinamik8913/raft/internal/controller"
	"github.com/Vovadinamik8913/raft/internal/node"
	"github.com/gin-gonic/gin"
)

func newRouter(n *node.RaftNode) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	ctrl := controller.New(n)
	router.GET("/status", ctrl.Status)
	router.POST("/request_vote", ctrl.RequestVote)
	router.POST("/append_entries", ctrl.AppendEntries)
	return router
}
