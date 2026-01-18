package main

import (
    "github.com/gin-gonic/gin"
    "log"
    "vecdb-go/internal/config"
    "vecdb-go/internal/api"
)

func main() {
    // Load configuration
    appConfig, err := config.LoadConfig("config.toml")
    if err != nil {
        log.Fatalf("Error loading config: %v", err)
    }

    // Initialize Gin router
    router := gin.Default()

    // Set up API routes
    api.SetupRoutes(router)

    // Start the server
    addr := ":" + string(appConfig.Server.Port)
    log.Printf("Server listening on %s", addr)
    if err := router.Run(addr); err != nil {
        log.Fatalf("Error starting server: %v", err)
    }
}