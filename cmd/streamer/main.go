package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zerooo111/tick-streamer/internal/config"
	"github.com/zerooo111/tick-streamer/internal/streamer"
)

func main() {
	// Load configuration from environment variables
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create a context that cancels on SIGTERM/SIGINT for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP server for health checks in a goroutine
	httpServer := &http.Server{
		Addr: cfg.HTTPBind,
	}

	// Register health check endpoint
	http.HandleFunc("/health", healthHandler)

	go func() {
		log.Printf("Starting HTTP server on %s", cfg.HTTPBind)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Create and start the streamer
	tickStreamer, err := streamer.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create streamer: %v", err)
	}

	// Start streaming in a goroutine
	go func() {
		if err := tickStreamer.Start(ctx); err != nil {
			log.Printf("Streamer error: %v", err)
			cancel() // Cancel context to trigger shutdown
		}
	}()

	log.Println("Continuum Streamer started successfully")
	log.Printf("Connecting to sequencer at: %s", cfg.SequencerAddr)
	log.Printf("Health check available at: http://%s/health", cfg.HTTPBind)

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutdown signal received, graceful shutdown starting...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// Cancel main context to stop streamer
	cancel()

	// Wait for streamer to finish with timeout
	done := make(chan struct{})
	go func() {
		tickStreamer.Stop()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Graceful shutdown completed")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timeout exceeded, forcing exit")
	}
}

// healthHandler handles the /health endpoint
// This is a basic implementation - we'll enhance it later with actual health checks
func healthHandler(w http.ResponseWriter, r *http.Request) {
	// For now, always return healthy
	// TODO: Check if streamer is connected and sink is available
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK\n")
}
