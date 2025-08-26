package main

import (
	"context"
	"log"
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

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutdown signal received, graceful shutdown starting...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

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
