package main

import (
	"fmt"
	"log/slog"
	"os"
	"vecdb-go/internal/api"
	"vecdb-go/internal/config"

	"github.com/gin-gonic/gin"
)

func main() {
	// Load configuration
	appConfig, err := config.LoadConfig()
	if err != nil {
		slog.Error("Error loading config", "error", err)
		os.Exit(1)
	}

	// Initialize Gin router
	router := gin.Default()

	// Set up API routes
	api.SetupRoutes(router)

	// Start the server
	addr := ":" + fmt.Sprintf("%d", appConfig.Server.Port)
	slog.Info("Server listening", "address", addr)
	if err := router.Run(addr); err != nil {
		slog.Error("Error starting server", "error", err)
		os.Exit(1)
	}
}
