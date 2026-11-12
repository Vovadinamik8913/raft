package server

import (
	"net/http"

	"github.com/Vovadinamik8913/raft/internal/controller"
	node "github.com/Vovadinamik8913/raft/internal/core"
	"github.com/gin-gonic/gin"
)

func newRouter(n *node.RaftNode) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	ctrl := controller.New(n)
	router.GET("/status", ctrl.Status)
	router.GET("/show_log", ctrl.ShowLog)
	router.POST("/add_entries", ctrl.AddEntries)
	router.POST("/request_vote", ctrl.RequestVote)
	router.POST("/append_entries", ctrl.AppendEntries)
	return router
}
