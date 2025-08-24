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

func main() {
	// Load configuration from environment variables
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create API server
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

	// Start server in a goroutine
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