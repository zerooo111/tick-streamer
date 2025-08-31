package streamer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zerooo111/tick-streamer/internal/batcher"
	"github.com/zerooo111/tick-streamer/internal/checkpoint"
	"github.com/zerooo111/tick-streamer/internal/config"
	"github.com/zerooo111/tick-streamer/internal/parser"
	"github.com/zerooo111/tick-streamer/internal/resilience"
	"github.com/zerooo111/tick-streamer/internal/sink"
	"github.com/zerooo111/tick-streamer/internal/validation"
	pb "github.com/zerooo111/tick-streamer/proto"
)

// Streamer handles the gRPC streaming connection only
// Phase 5: Now includes resilience patterns and error recovery
type Streamer struct {
	config *config.Config
	client pb.SequencerServiceClient
	conn   *grpc.ClientConn
	
	// Concurrent processing pipeline: stream → parser → batcher → sink
	parser  parser.Parser
	sink    sink.Sink
	batcher *batcher.Batcher
	
	// Phase 4: Checkpoint system for durability
	checkpoint checkpoint.Store
	
	// Phase 5: Resilience patterns
	retryConfig      resilience.RetryConfig
	circuitBreaker   *resilience.CircuitBreaker
	componentHealth  *resilience.ComponentHealth
	degradedMode     bool
	lastReconnectTime time.Time
	reconnectCount   int
	
	// Phase 5: Validation and reorg detection
	tickValidator    *validation.TickValidator
	reorgDetector    *validation.ReorgDetector
	
	// Metrics for monitoring
	ticksReceived     uint64
	lastProcessedTick uint64
	startTime         time.Time
	lastTickTime      time.Time
	
	// Goroutine tracking for checkpoint saves
	checkpointWg      sync.WaitGroup
	checkpointWorkers int32 // atomic counter for active checkpoint workers
}

// New creates a new Streamer instance with the given configuration
// This is a common Go pattern - a constructor function that returns a pointer to a struct
func New(cfg *config.Config) (*Streamer, error) {
	// Create parser plugin
	parserConfig := parser.ParserConfig{
		Type: "tick", // Default to tick parser
		Settings: map[string]interface{}{
			"detailed_logging": true,
			"max_tx_to_log":   3,
		},
	}
	
	dataParser, err := parser.NewParser(parserConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}
	
	// Create sink based on configuration (only ClickHouse supported)
	sinkConfig := sink.Config{
		Kind:         "clickhouse", // Only ClickHouse is supported
		MaxBatchSize: cfg.BatchRowsTx, // Use transaction batch size as max
	}
	
	dataSink, err := sink.NewSink(sinkConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink: %w", err)
	}
	
	// Create concurrent batcher for Phase 3
	// Use the larger of the two batch sizes for the batcher
	batchSize := cfg.BatchRowsTx
	if cfg.BatchRowsTick > batchSize {
		batchSize = cfg.BatchRowsTick
	}
	
	dataBatcher := batcher.New(dataSink, batchSize, cfg.BatchMaxWaitTime)
	
	// Create checkpoint store for Phase 4
	checkpointStore, err := checkpoint.NewStore(cfg.CheckpointDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to create checkpoint store: %w", err)
	}
	
	// Phase 5: Initialize resilience patterns
	retryConfig := resilience.DefaultRetryConfig()
	retryConfig.BaseDelay = cfg.RetryBackoffMin
	retryConfig.MaxDelay = cfg.RetryBackoffMax
	
	circuitBreaker := resilience.NewCircuitBreaker(5, 30*time.Second)
	
	// Create a health checker for the sink
	sinkHealthChecker := &SinkHealthChecker{sink: dataSink}
	componentHealth := resilience.NewComponentHealth("sink", sinkHealthChecker, 3)
	
	// Phase 5: Initialize validation and reorg detection
	tickValidator := validation.NewTickValidator()
	reorgDetector := validation.NewReorgDetector(128) // Keep 128 ticks for reorg detection
	
	return &Streamer{
		config:          cfg,
		parser:          dataParser,
		sink:            dataSink,
		batcher:         dataBatcher,
		checkpoint:      checkpointStore,
		retryConfig:     retryConfig,
		circuitBreaker:  circuitBreaker,
		componentHealth: componentHealth,
		tickValidator:   tickValidator,
		reorgDetector:   reorgDetector,
		startTime:       time.Now(),
	}, nil
}

// Start begins the streaming process
// Phase 4: Now loads checkpoint and resumes from last processed tick
func (s *Streamer) Start(ctx context.Context) error {
	log.Printf("Starting streamer with checkpointing, connecting to %s", s.config.SequencerAddr)

	// Load checkpoint to determine starting position
	startTick, err := s.checkpoint.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}
	s.lastProcessedTick = startTick

	// Start the concurrent batcher
	if err := s.batcher.Start(ctx); err != nil {
		return fmt.Errorf("failed to start batcher: %w", err)
	}

	// Connect to gRPC server with retry logic
	err = resilience.RetryWithBackoff(ctx, s.retryConfig, func(ctx context.Context, attempt int) error {
		return s.connect(ctx)
	}, "gRPC connection")
	if err != nil {
		return fmt.Errorf("failed to connect to sequencer after retries: %w", err)
	}
	defer s.conn.Close()

	// Start streaming loop from checkpoint with resilience
	return s.streamLoopWithResilience(ctx, startTick)
}

// Stop gracefully shuts down the streamer
func (s *Streamer) Stop() {
	log.Println("Stopping streamer...")
	
	// Stop batcher first to ensure all pending data is flushed
	if s.batcher != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		if err := s.batcher.Stop(ctx); err != nil {
			log.Printf("Error stopping batcher: %v", err)
		}
	}
	
	// Wait for all checkpoint goroutines to complete
	log.Println("Waiting for checkpoint saves to complete...")
	checkpointDone := make(chan struct{})
	go func() {
		s.checkpointWg.Wait()
		close(checkpointDone)
	}()
	
	select {
	case <-checkpointDone:
		log.Println("All checkpoint saves completed")
	case <-time.After(10 * time.Second):
		log.Printf("⚠️ Timeout waiting for checkpoint saves (%d still running)", atomic.LoadInt32(&s.checkpointWorkers))
	}
	
	// Save final checkpoint before shutdown
	if s.checkpoint != nil && s.lastProcessedTick > 0 {
		log.Printf("Saving final checkpoint at tick %d...", s.lastProcessedTick)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		if err := s.checkpoint.Save(ctx, s.lastProcessedTick); err != nil {
			log.Printf("Error saving final checkpoint: %v", err)
		}
	}
	
	// Close checkpoint store
	if s.checkpoint != nil {
		if err := s.checkpoint.Close(); err != nil {
			log.Printf("Error closing checkpoint store: %v", err)
		}
	}
	
	// Close sink connection
	if s.sink != nil {
		if err := s.sink.Close(); err != nil {
			log.Printf("Error closing sink: %v", err)
		}
	}
	
	// Close gRPC connection
	if s.conn != nil {
		s.conn.Close()
	}
	
	log.Printf("Streamer stopped (final checkpoint: tick %d)", s.lastProcessedTick)
}

// connect establishes the gRPC connection
func (s *Streamer) connect(ctx context.Context) error {
	var opts []grpc.DialOption

	// Configure TLS based on configuration
	if s.config.SequencerTLS {
		tlsConfig, err := s.buildTLSConfig()
		if err != nil {
			return fmt.Errorf("failed to build TLS configuration: %w", err)
		}
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
		log.Println("TLS enabled for gRPC connection")
	} else {
		// Use insecure credentials only when TLS is explicitly disabled
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		log.Println("Warning: Using insecure gRPC connection. Enable TLS in production!")
	}

	// Create connection
	conn, err := grpc.NewClient(s.config.SequencerAddr, opts...)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client: %w", err)
	}

	s.conn = conn
	s.client = pb.NewSequencerServiceClient(conn)

	log.Println("Successfully connected to sequencer")
	return nil
}

// buildTLSConfig creates TLS configuration based on config settings
func (s *Streamer) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Set server name for verification if provided
	if s.config.SequencerServerName != "" {
		tlsConfig.ServerName = s.config.SequencerServerName
	}

	// Load custom CA certificate if provided
	if s.config.SequencerCACert != "" {
		caCert, err := os.ReadFile(s.config.SequencerCACert)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		// Create or get system certificate pool
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			// If system pool is not available, create a new one
			caCertPool = x509.NewCertPool()
		}

		// Append our CA certificate
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
		
		tlsConfig.RootCAs = caCertPool
	}

	// Configure mutual TLS if enabled
	if s.config.SequencerMTLS {
		cert, err := tls.LoadX509KeyPair(s.config.SequencerClientCert, s.config.SequencerClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// streamLoopWithResilience handles the main streaming logic with resilience patterns
func (s *Streamer) streamLoopWithResilience(ctx context.Context, startTick uint64) error {
	currentTick := startTick
	
	for {
		// Check if we should exit
		select {
		case <-ctx.Done():
			log.Println("Stream loop cancelled by context")
			return ctx.Err()
		default:
		}
		
		// Attempt to stream with retry logic
		err := s.attemptStreaming(ctx, currentTick)
		if err == nil {
			// Streaming completed normally (unlikely for infinite stream)
			return nil
		}
		
		// Handle streaming error
		log.Printf("⚠️ Streaming interrupted: %v", err)
		s.reconnectCount++
		s.lastReconnectTime = time.Now()
		
		// Check component health and decide whether to enter degraded mode
		s.checkComponentHealth(ctx)
		
		// If in degraded mode, handle differently
		if s.degradedMode {
			if err := s.handleDegradedMode(ctx); err != nil {
				return fmt.Errorf("degraded mode failed: %w", err)
			}
			continue
		}
		
		// Attempt reconnection with exponential backoff
		log.Printf("🔄 Attempting reconnection #%d...", s.reconnectCount)
		
		reconnectErr := resilience.RetryWithBackoff(ctx, s.retryConfig, func(ctx context.Context, attempt int) error {
			// Close old connection if exists
			if s.conn != nil {
				s.conn.Close()
			}
			
			// Establish new connection
			if err := s.connect(ctx); err != nil {
				return fmt.Errorf("reconnection failed: %w", err)
			}
			
			return nil
		}, fmt.Sprintf("reconnection attempt %d", s.reconnectCount))
		
		if reconnectErr != nil {
			log.Printf("❌ Failed to reconnect after retries: %v", reconnectErr)
			// Enter degraded mode or fail
			s.degradedMode = true
			continue
		}
		
		log.Printf("✅ Reconnected successfully (attempt #%d)", s.reconnectCount)
		// Continue streaming from the last processed tick
		currentTick = s.lastProcessedTick
	}
}

// attemptStreaming tries to stream data and returns an error if streaming fails
func (s *Streamer) attemptStreaming(ctx context.Context, startTick uint64) error {
	req := &pb.StreamTicksRequest{
		StartTick: startTick,
	}

	stream, err := s.client.StreamTicks(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to start streaming: %w", err)
	}

	if startTick > 0 {
		log.Printf("🔄 Resuming streaming from checkpoint tick %d", startTick)
	} else {
		log.Printf("🚀 Starting fresh streaming from tick %d", startTick)
	}

	// Main receive loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Receive next tick with timeout
			tick, err := stream.Recv()
			if err == io.EOF {
				log.Println("Stream ended by server")
				return fmt.Errorf("stream ended unexpectedly")
			}
			if err != nil {
				return fmt.Errorf("stream receive error: %w", err)
			}

			// Process the tick with circuit breaker protection
			err = s.circuitBreaker.Execute(ctx, func() error {
				return s.processTick(ctx, tick)
			}, "tick processing")
			
			if err != nil {
				// Enhanced error logging with circuit breaker context
				if strings.Contains(err.Error(), "circuit breaker open") {
					stats := s.circuitBreaker.GetStats()
					resetTime := s.circuitBreaker.GetNextResetTime()
					timeUntilReset := time.Until(resetTime)
					
					// Provide comprehensive circuit breaker diagnostics
					log.Printf("⚠️ Error processing tick %d: %v", tick.TickNumber, err)
					log.Printf("🔴 Circuit Breaker Status: OPEN - consecutive failures: %d/%d, last failure: %v ago", 
						stats.CurrentFailures, stats.FailureThreshold, time.Since(stats.LastFailureTime).Truncate(time.Second))
					if !resetTime.IsZero() {
						log.Printf("🕐 Circuit will attempt reset in: %v (at %s)", 
							timeUntilReset.Truncate(time.Second), resetTime.Format("15:04:05"))
					}
					log.Printf("📊 Overall stats: %d total requests, %d successes, %d failures", 
						stats.TotalRequests, stats.SuccessCount, stats.FailureCount)
				} else {
					log.Printf("⚠️ Error processing tick %d: %v", tick.TickNumber, err)
				}
				// For critical errors, we might want to fail the stream
				// For non-critical errors, we continue processing
				if s.isCriticalError(err) {
					return fmt.Errorf("critical error processing tick %d: %w", tick.TickNumber, err)
				}
				// Continue processing for non-critical errors
			}
		}
	}
}

// checkComponentHealth checks the health of all components
func (s *Streamer) checkComponentHealth(ctx context.Context) {
	sinkHealthy := s.componentHealth.CheckHealth(ctx)
	
	if !sinkHealthy && !s.degradedMode {
		log.Printf("⚠️ Entering degraded mode due to unhealthy sink")
		s.degradedMode = true
	} else if sinkHealthy && s.degradedMode {
		log.Printf("✅ Exiting degraded mode - sink is healthy again")
		s.degradedMode = false
	}
}

// handleDegradedMode implements fallback behavior when components are unhealthy
func (s *Streamer) handleDegradedMode(ctx context.Context) error {
	log.Printf("🔶 Operating in degraded mode - limited functionality")
	
	// In degraded mode, we might:
	// 1. Continue streaming but drop data
	// 2. Store data in a temporary buffer
	// 3. Reduce processing rate
	// 4. Wait for components to recover
	
	// For now, we'll wait and periodically check if components recover
	degradedModeTimeout := 30 * time.Second
	
	select {
	case <-time.After(degradedModeTimeout):
		// Check if we can recover
		s.checkComponentHealth(ctx)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isCriticalError determines if an error should cause stream failure
func (s *Streamer) isCriticalError(err error) bool {
	if err == nil {
		return false
	}
	
	errorStr := err.Error()
	
	// Critical errors that should stop streaming
	criticalPatterns := []string{
		"context canceled",
		"context deadline exceeded", 
		"checkpoint save failed",
		"parser validation failed",
	}
	
	for _, pattern := range criticalPatterns {
		if containsIgnoreCase(errorStr, pattern) {
			return true
		}
	}
	
	return false
}

// containsIgnoreCase helper function (simple implementation)
func containsIgnoreCase(s, substr string) bool {
	sLower := strings.ToLower(s)
	substrLower := strings.ToLower(substr)
	return len(sLower) >= len(substrLower) && 
		   (sLower == substrLower || 
		    (len(sLower) > len(substrLower) && 
		     (strings.HasPrefix(sLower, substrLower) || 
		      strings.Contains(sLower, substrLower))))
}

// processTick handles a single tick using the concurrent processing pipeline
// Phase 5: Now includes validation, reorg detection, and resilience patterns
func (s *Streamer) processTick(ctx context.Context, tick *pb.Tick) error {
	s.ticksReceived++
	s.lastTickTime = time.Now()
	
	// Phase 5: Validate the tick data for corruption
	validationResult := s.tickValidator.ValidateTick(ctx, tick)
	if !validationResult.IsValid {
		// Log validation errors but decide whether to reject or continue
		for _, validationError := range validationResult.Errors {
			log.Printf("❌ Validation error in tick %d: %s", tick.TickNumber, validationError.Error())
		}
		
		// For critical validation failures, reject the tick
		if s.hasCriticalValidationErrors(validationResult.Errors) {
			return fmt.Errorf("critical validation failure for tick %d", tick.TickNumber)
		}
		
		// For non-critical errors, log and continue
		log.Printf("⚠️ Non-critical validation issues in tick %d, continuing processing", tick.TickNumber)
	}
	
	// Phase 5: Check for blockchain reorganizations
	reorgEvent, err := s.reorgDetector.CheckForReorg(tick)
	if err != nil {
		log.Printf("⚠️ Error checking for reorg in tick %d: %v", tick.TickNumber, err)
	}
	if reorgEvent != nil {
		if err := s.handleReorganization(ctx, reorgEvent); err != nil {
			log.Printf("❌ Failed to handle reorganization for tick %d: %v", tick.TickNumber, err)
			// Don't fail processing for reorg handling errors, just log them
		}
	}
	
	// Use parser plugin to transform protobuf tick to sink-compatible data
	parsedData, err := s.parser.ParseTick(ctx, tick)
	if err != nil {
		return fmt.Errorf("failed to parse tick %d: %w", tick.TickNumber, err)
	}
	
	// Send parsed data to concurrent batcher
	for _, data := range parsedData {
		if data == nil {
			continue
		}
		
		// Add to batcher - batcher now handles sophisticated backpressure and retries
		if err := s.batcher.Add(ctx, data); err != nil {
			// Batcher's internal retry mechanisms have been exhausted
			return fmt.Errorf("failed to add data to batcher for tick %d: %w", tick.TickNumber, err)
		}
	}
	
	// Update last processed tick for checkpoint system
	s.lastProcessedTick = tick.TickNumber
	
	// Save checkpoint periodically and log progress every 100 ticks
	if s.ticksReceived%100 == 0 {
		// Save checkpoint asynchronously to avoid blocking stream processing
		go func(tickNum uint64) {
			checkpointCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			
			if err := s.checkpoint.Save(checkpointCtx, tickNum); err != nil {
				log.Printf("⚠️ Failed to save checkpoint for tick %d: %v", tickNum, err)
			}
		}(s.lastProcessedTick)
		
		elapsed := s.lastTickTime.Sub(s.startTime)
		ticksPerSecond := float64(s.ticksReceived) / elapsed.Seconds()
		
		batchStats := s.batcher.GetStats()
		
		log.Printf("📊 STREAMER: %d ticks in %v (%.1f ticks/sec), checkpoint: %d", 
			s.ticksReceived, elapsed.Truncate(time.Second), ticksPerSecond, s.lastProcessedTick)
		log.Printf("📦 BATCHER: %d batches, %d items, queue_depth=%d, avg_flush=%.1fms", 
			batchStats.BatchesProcessed, batchStats.ItemsProcessed, 
			batchStats.QueueDepth, float64(batchStats.AverageFlushTime.Nanoseconds())/1e6)
	}

	return nil
}


// Health returns the current health status of the streamer
// Phase 4: Now includes checkpoint store health check
func (s *Streamer) Health() bool {
	// Check gRPC connection
	if s.conn == nil {
		return false
	}
	
	// Check sink health
	if s.sink == nil {
		return false
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	sinkHealthy := s.sink.Health(ctx)
	if !sinkHealthy {
		return false
	}
	
	// Check checkpoint store health
	if s.checkpoint == nil {
		return false
	}
	
	checkpointHealthy := s.checkpoint.Health(ctx) == nil
	return checkpointHealthy
}

// SinkHealthChecker implements HealthChecker for sink components
type SinkHealthChecker struct {
	sink sink.Sink
}

// IsHealthy checks if the sink is healthy
func (s *SinkHealthChecker) IsHealthy(ctx context.Context) bool {
	return s.sink.Health(ctx)
}

// GetHealthStatus returns a health status string
func (s *SinkHealthChecker) GetHealthStatus() string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	if s.IsHealthy(ctx) {
		return "sink: healthy"
	}
	return "sink: unhealthy"
}

// hasCriticalValidationErrors determines if validation errors are critical
func (s *Streamer) hasCriticalValidationErrors(errors []validation.ValidationError) bool {
	for _, err := range errors {
		// Critical validation rules that should stop processing
		switch err.Rule {
		case "not_null", "sequence", "critical_hash_mismatch":
			return true
		}
		
		// Critical field failures
		if strings.Contains(err.Field, "tick_number") && err.Rule == "sequence" {
			return true
		}
		if strings.Contains(err.Field, "vdf_proof") && err.Rule == "not_null" {
			return true
		}
	}
	
	return false
}

// handleReorganization processes a detected blockchain reorganization
func (s *Streamer) handleReorganization(ctx context.Context, reorgEvent *validation.ReorgEvent) error {
	log.Printf("🚨 HANDLING REORG: Tick %d - %s", reorgEvent.TickNumber, reorgEvent.ConflictReason)
	
	// Step 1: Invalidate the conflicting tick in the sink
	if err := s.sink.InvalidateTick(ctx, reorgEvent.TickNumber); err != nil {
		return fmt.Errorf("failed to invalidate tick %d during reorg: %w", reorgEvent.TickNumber, err)
	}
	
	// Step 2: The new tick will be processed normally by the regular flow
	// This ensures the replacement data gets persisted
	
	// Step 3: Update checkpoint if necessary
	// We don't need to roll back the checkpoint since the new tick will advance it properly
	
	log.Printf("✅ Reorg handled successfully for tick %d", reorgEvent.TickNumber)
	return nil
}

