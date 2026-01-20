package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"vecdb-go/internal/api"
	"vecdb-go/internal/config"
	"vecdb-go/internal/vecdb"

	"github.com/gin-gonic/gin"
)

func main() {
	// Parse command-line flags
	mode := flag.String("mode", "dev", "Run mode (dev or test)")

	flag.Parse()

	// Determine which profile to use
	profile := "dev"
	if *mode == "test" {
		profile = "test"
	}

	// Load configuration with the selected profile
	appConfig, err := config.LoadConfigWithProfile(profile)
	if err != nil {
		slog.Error("Error loading config", "error", err, "profile", profile)
		os.Exit(1)
	}

	slog.Info("Loaded configuration", "profile", profile)

	// Configure logging level
	setupLogging(appConfig.Server.LogLevel)

	// Configure Gin mode based on log level
	setupGinMode(appConfig.Server.LogLevel)

	// Prepare database parameters from config
	// Initialize VectorDatabase
	slog.Info("Initializing vector database", "path", appConfig.Database.FilePath, "params", appConfig.Database)
	vdb, err := vecdb.NewVectorDatabase(&appConfig.Database)
	if err != nil {
		slog.Error("Error initializing vector database", "error", err)
		os.Exit(1)
	}

	// Initialize API handlers with the database
	api.Initialize(vdb)

	// Initialize Gin router
	router := gin.Default()

	// Set up API routes with configured URL suffixes
	setupRoutes(router, appConfig)

	// Start the server
	addr := fmt.Sprintf(":%d", appConfig.Server.Port)
	slog.Info("Server listening", "address", addr)
	if err := router.Run(addr); err != nil {
		slog.Error("Error starting server", "error", err)
		os.Exit(1)
	}
}

func setupLogging(logLevel string) {
	var level slog.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(handler))
}

func setupGinMode(logLevel string) {
	switch strings.ToLower(logLevel) {
	case "debug":
		gin.SetMode(gin.DebugMode)
	case "error":
		gin.SetMode(gin.ReleaseMode)
	default:
		gin.SetMode(gin.ReleaseMode)
	}
}

func setupRoutes(router *gin.Engine, cfg *config.AppConfig) {
	router.POST(cfg.Server.SearchURLSuffix, api.HandleVectorSearch)
	router.POST(cfg.Server.UpsertURLSuffix, api.HandleVectorUpsert)
}
