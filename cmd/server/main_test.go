package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"vecdb-go/internal/config"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestSetupLogging(t *testing.T) {
	tests := []struct {
		name     string
		logLevel string
		expected slog.Level
	}{
		{"debug level", "debug", slog.LevelDebug},
		{"info level", "info", slog.LevelInfo},
		{"warn level", "warn", slog.LevelWarn},
		{"warning level", "warning", slog.LevelWarn},
		{"error level", "error", slog.LevelError},
		{"default level", "unknown", slog.LevelInfo},
		{"uppercase", "DEBUG", slog.LevelDebug},
		{"mixed case", "WaRn", slog.LevelWarn},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture log output
			var buf bytes.Buffer
			handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
				Level: tt.expected,
			})
			logger := slog.New(handler)

			// Setup logging
			setupLogging(tt.logLevel)

			// Verify by checking if we can log at the expected level
			logger.Info("test message")

			// The test passes if setupLogging doesn't panic
			// More thorough testing would require exposing the handler
		})
	}
}

func TestSetupGinMode(t *testing.T) {
	tests := []struct {
		name     string
		logLevel string
		expected string
	}{
		{"debug mode", "debug", gin.DebugMode},
		{"release mode for info", "info", gin.ReleaseMode},
		{"release mode for error", "error", gin.ReleaseMode},
		{"release mode for warn", "warn", gin.ReleaseMode},
		{"release mode for unknown", "unknown", gin.ReleaseMode},
		{"uppercase debug", "DEBUG", gin.DebugMode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupGinMode(tt.logLevel)
			assert.Equal(t, tt.expected, gin.Mode())
		})
	}
}

func TestSetupRoutes(t *testing.T) {
	// Disable Gin debug output for cleaner test output
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		config         *config.AppConfig
		expectedRoutes map[string]string
	}{
		{
			name: "default routes",
			config: &config.AppConfig{
				Server: config.ServerConfig{
					SearchURLSuffix: "/search",
					UpsertURLSuffix: "/upsert",
				},
			},
			expectedRoutes: map[string]string{
				"/search": "POST",
				"/upsert": "POST",
			},
		},
		{
			name: "custom routes",
			config: &config.AppConfig{
				Server: config.ServerConfig{
					SearchURLSuffix: "/api/v1/search",
					UpsertURLSuffix: "/api/v1/upsert",
				},
			},
			expectedRoutes: map[string]string{
				"/api/v1/search": "POST",
				"/api/v1/upsert": "POST",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			setupRoutes(router, tt.config)

			// Get all registered routes
			routes := router.Routes()

			// Verify expected routes are registered
			for path, method := range tt.expectedRoutes {
				found := false
				for _, route := range routes {
					if route.Path == path && route.Method == method {
						found = true
						break
					}
				}
				assert.True(t, found, "Route %s %s should be registered", method, path)
			}

			// Verify the correct number of routes
			assert.Equal(t, len(tt.expectedRoutes), len(routes), "Should have exactly %d routes", len(tt.expectedRoutes))
		})
	}
}

func TestSetupRoutesHandlers(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := &config.AppConfig{
		Server: config.ServerConfig{
			SearchURLSuffix: "/search",
			UpsertURLSuffix: "/upsert",
		},
	}

	router := gin.New()
	setupRoutes(router, cfg)

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
	}{
		{
			name:           "search endpoint exists",
			method:         "POST",
			path:           "/search",
			expectedStatus: http.StatusBadRequest, // No VDB initialized, but endpoint exists
		},
		{
			name:           "upsert endpoint exists",
			method:         "POST",
			path:           "/upsert",
			expectedStatus: http.StatusBadRequest, // No VDB initialized, but endpoint exists
		},
		{
			name:           "non-existent endpoint",
			method:         "GET",
			path:           "/unknown",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)
		})
	}
}
