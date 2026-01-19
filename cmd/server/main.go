package main

import (
	"fmt"
	"log"
	"vecdb-go/internal/api"
	"vecdb-go/internal/config"

	"github.com/gin-gonic/gin"
)

func main() {
	// Load configuration
	appConfig, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	// Initialize Gin router
	router := gin.Default()

	// Set up API routes
	api.SetupRoutes(router)

	// Start the server
	addr := ":" + fmt.Sprintf("%d", appConfig.Server.Port)
	log.Printf("Server listening on %s", addr)
	if err := router.Run(addr); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
