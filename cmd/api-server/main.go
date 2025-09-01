// Package main implements the REST API server for the Continuum Streamer.
// This service provides HTTP endpoints to query blockchain data stored by the streamer.
//
// Key Features:
// - REST API for tick and transaction queries
// - TimescaleDB integration for fast time-series queries
// - CORS support for web applications
// - Health check endpoints
// - Graceful shutdown handling
//
// Endpoints:
// - GET /api/v1/ticks/{id} - Get specific tick data
// - GET /api/v1/transactions/{hash} - Get transaction details
// - GET /api/v1/chain/state - Get blockchain state summary
// - GET /healthz - Health check endpoint
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zerooo111/tick-streamer/internal/api"
	"github.com/zerooo111/tick-streamer/internal/config"
)

// main is the entry point for the API server application.
// It handles:
// 1. Configuration loading
// 2. HTTP server creation and startup
// 3. Graceful shutdown with proper resource cleanup
// 4. Signal handling for clean termination
func main() {
	// Load configuration from environment variables
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create API server with:
	// - REST route handlers
	// - TimescaleDB repository connection
	// - CORS middleware
	// - Request logging and error handling
	server, err := api.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Create a context that cancels on SIGTERM/SIGINT for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP server in a separate goroutine to avoid blocking
	// This allows the main thread to handle shutdown signals
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v", err)
			cancel() // Cancel context to trigger shutdown
		}
	}()

	// Wait for shutdown signal or server error
	select {
	case <-sigChan:
		log.Println("Shutdown signal received, graceful shutdown starting...")
	case <-ctx.Done():
		log.Println("Server error detected, starting shutdown...")
	}

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown server
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
		os.Exit(1)
	}

	log.Println("Graceful shutdown completed")
}