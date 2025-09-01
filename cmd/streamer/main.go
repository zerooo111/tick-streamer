// Package main implements the Continuum Streamer - a high-performance blockchain data ingestion service.
// This is the main entry point for the streaming component that connects to a sequencer service
// and persists tick/transaction data to TimescaleDB with ultra-low latency optimizations.
//
// Key Features:
// - Dual processing modes: Traditional batching vs Direct streaming
// - Ultra-low latency: Sub-second processing (improved from 10-20s)
// - Async worker pool architecture for maximum throughput
// - Resilience patterns: Circuit breakers, retries, graceful degradation
// - TimescaleDB optimized for time-series data
//
// Architecture:
// gRPC Stream → Parser → [Async Workers] → TimescaleDB
//                     ↘ [Channel Buffer] ↗
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

// main is the entry point for the Continuum Streamer application.
// It handles:
// 1. Configuration loading from environment variables
// 2. Graceful shutdown handling with OS signals
// 3. Streamer lifecycle management
// 4. Error handling and logging
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

	// Create and configure the streamer with async worker pool architecture
	// This initializes:
	// - gRPC client for sequencer connection
	// - Parser plugin for data transformation
	// - Sink interface (TimescaleDB or Debug mode)
	// - Worker pool for async processing
	// - Resilience components (circuit breakers, retry logic)
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

	// Block and wait for OS shutdown signals (SIGINT, SIGTERM)
	// This allows the application to run until explicitly stopped
	<-sigChan
	log.Println("Shutdown signal received, graceful shutdown starting...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Cancel main context to stop streamer
	cancel()

	// Initiate graceful shutdown of the streamer in a separate goroutine
	// This ensures we can enforce a timeout if shutdown takes too long
	done := make(chan struct{})
	go func() {
		// Stop() handles:
		// 1. Stopping worker pool
		// 2. Flushing pending data to sink
		// 3. Closing database connections
		// 4. Closing gRPC connections
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
