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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zerooo111/tick-streamer/internal/config"
	"github.com/zerooo111/tick-streamer/internal/parser"
	"github.com/zerooo111/tick-streamer/internal/resilience"
	"github.com/zerooo111/tick-streamer/internal/sink"
	"github.com/zerooo111/tick-streamer/internal/validation"
	pb "github.com/zerooo111/tick-streamer/proto"
)

// Streamer handles the gRPC streaming connection with async worker pool processing
// ARCHITECTURE CHANGE: Async pipeline with parallel workers
// 
// DESIGN:
// - Stream data at natural gRPC rate and send to buffered channel
// - Multiple workers process ticks in parallel from the channel
// - Each worker handles: parse → sink independently
// - Decouples stream processing from database writes for maximum throughput
type Streamer struct {
	config *config.Config
	client pb.SequencerServiceClient
	conn   *grpc.ClientConn
	
	// Async pipeline: stream → channel → workers → sink
	parser     parser.Parser
	sink       sink.Sink
	tickChan   chan *pb.Tick  // Buffered channel for async processing
	workerWg   sync.WaitGroup // Track worker goroutines
	workerCtx  context.Context
	workerCancel context.CancelFunc
	
	// Monitoring metrics
	ticksDropped     uint64    // Counter for dropped ticks due to backpressure
	lastDropTime     time.Time // When we last dropped a tick
	
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
}

// New creates a new Streamer instance with the given configuration
// This is a common Go pattern - a constructor function that returns a pointer to a struct
func New(cfg *config.Config) (*Streamer, error) {
	// Create parser plugin with performance optimizations
	parserConfig := parser.ParserConfig{
		Type: "tick", // Default to tick parser
		Settings: map[string]interface{}{
			"detailed_logging": !cfg.LowLatencyMode, // Disable detailed logging in low latency mode
			"max_tx_to_log":   3,
		},
	}
	
	dataParser, err := parser.NewParser(parserConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}
	
	// Create sink based on configuration
	var sinkConfig sink.Config
	log.Printf("🔍 Configuration Debug:")
	log.Printf("  - cfg.DebugMode: %v", cfg.DebugMode)
	log.Printf("  - cfg.SinkKind: '%s'", cfg.SinkKind)
	
	if cfg.DebugMode {
		log.Printf("📋 Using DEBUG_MODE=true -> Debug Sink")
		sinkConfig = sink.Config{
			Kind:         "debug",
			MaxBatchSize: cfg.BatchSize,
		}
	} else {
		log.Printf("📋 Using SINK_KIND='%s' -> %s Sink", cfg.SinkKind, strings.ToUpper(cfg.SinkKind))
		sinkConfig = sink.Config{
			Kind:         cfg.SinkKind,
			MaxBatchSize: cfg.BatchSize,
			BatchTimeout: int(cfg.FlushInterval.Milliseconds()),
		}
	}
	
	log.Printf("🏗️  Creating sink with Kind='%s'", sinkConfig.Kind)
	dataSink, err := sink.NewSink(sinkConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink: %w", err)
	}
	log.Printf("✅ Sink created successfully")
	
	// Initialize async processing channel with buffer
	tickChan := make(chan *pb.Tick, cfg.ChannelBuffer)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	
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
		tickChan:        tickChan,
		workerCtx:       workerCtx,
		workerCancel:    workerCancel,
		retryConfig:     retryConfig,
		circuitBreaker:  circuitBreaker,
		componentHealth: componentHealth,
		tickValidator:   tickValidator,
		reorgDetector:   reorgDetector,
		startTime:       time.Now(),
	}, nil
}

// Start begins the streaming process with async worker pool
// Always starts from latest tick (0) - no checkpoint system
func (s *Streamer) Start(ctx context.Context) error {
	log.Printf("Starting streamer from latest tick, connecting to %s", s.config.SequencerAddr)

	// Always start from latest tick (0)
	startTick := uint64(0)
	s.lastProcessedTick = startTick

	log.Printf("🚀 Async streaming mode - %d workers processing in parallel", s.config.SinkWorkers)

	// Start worker pool before beginning stream
	s.startWorkerPool()
	defer s.stopWorkerPool()

	// Connect to gRPC server with retry logic
	if err := resilience.RetryWithBackoff(ctx, s.retryConfig, func(ctx context.Context, attempt int) error {
		return s.connect(ctx)
	}, "gRPC connection"); err != nil {
		return fmt.Errorf("failed to connect to sequencer after retries: %w", err)
	}
	defer s.conn.Close()

	// Start streaming loop from latest tick with resilience
	return s.streamLoopWithResilience(ctx, startTick)
}

// Stop gracefully shuts down the streamer
func (s *Streamer) Stop() {
	log.Println("Stopping streamer...")
	
	// First, stop accepting new ticks by shutting down worker pool
	s.stopWorkerPool()
	
	// Then flush sink to ensure all pending data is persisted
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := s.sink.Flush(ctx); err != nil {
		log.Printf("Error flushing sink: %v", err)
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
	
	// Print final statistics
	if s.ticksDropped > 0 {
		log.Printf("⚠️ Total ticks dropped due to backpressure: %d", s.ticksDropped)
	}
	log.Printf("Streamer stopped (last processed tick: %d)", s.lastProcessedTick)
}

// startWorkerPool launches the configured number of worker goroutines
func (s *Streamer) startWorkerPool() {
	log.Printf("🚀 Starting %d sink workers for async processing", s.config.SinkWorkers)
	log.Printf("📊 Channel buffer: %d ticks", s.config.ChannelBuffer)
	log.Printf("📦 Batch size: %d rows", s.config.BatchSize)
	log.Printf("⏱️  Flush interval: %v", s.config.FlushInterval)
	
	for i := 0; i < s.config.SinkWorkers; i++ {
		s.workerWg.Add(1)
		go s.sinkWorker(i)
	}
}

// stopWorkerPool gracefully shuts down all worker goroutines
func (s *Streamer) stopWorkerPool() {
	log.Println("Stopping worker pool...")
	
	// Cancel worker context to signal shutdown
	s.workerCancel()
	
	// Close channel to signal no more ticks
	close(s.tickChan)
	
	// Wait for all workers to complete
	s.workerWg.Wait()
	log.Println("All workers stopped")
}

// sinkWorker processes ticks from the channel
func (s *Streamer) sinkWorker(workerID int) {
	defer s.workerWg.Done()
	
	log.Printf("Worker %d started", workerID)
	
	for {
		select {
		case tick, ok := <-s.tickChan:
			if !ok {
				// Channel closed, worker should exit
				log.Printf("Worker %d shutting down - channel closed", workerID)
				return
			}
			
			// Process the tick
			if err := s.processTick(s.workerCtx, tick, workerID); err != nil {
				if !s.config.LowLatencyMode {
					log.Printf("Worker %d: Error processing tick %d: %v", workerID, tick.TickNumber, err)
				}
				// Continue processing other ticks even if one fails
			}
			
		case <-s.workerCtx.Done():
			// Context cancelled, worker should exit
			log.Printf("Worker %d shutting down - context cancelled", workerID)
			return
		}
	}
}

// processTick handles the full processing pipeline for a single tick
func (s *Streamer) processTick(ctx context.Context, tick *pb.Tick, workerID int) error {
	processStartTime := time.Now()
	
	// Phase 5: Optional validation (configurable for performance)
	if !s.config.SkipValidation {
		validationResult := s.tickValidator.ValidateTick(ctx, tick)
		if !validationResult.IsValid {
			if s.hasCriticalValidationErrors(validationResult.Errors) {
				return fmt.Errorf("critical validation errors in tick %d: %v", tick.TickNumber, validationResult.Errors)
			}
			if !s.config.LowLatencyMode {
				log.Printf("⚠️ Validation warnings for tick %d: %v", tick.TickNumber, validationResult.Errors)
			}
		}

		// Phase 5: Check for blockchain reorganizations (only when not skipping)
		reorgEvent, err := s.reorgDetector.CheckForReorg(tick)
		if err != nil && !s.config.LowLatencyMode {
			log.Printf("⚠️ Worker %d: Error checking for reorg in tick %d: %v", workerID, tick.TickNumber, err)
		}
		if reorgEvent != nil {
			if err := s.handleReorganization(ctx, reorgEvent); err != nil && !s.config.LowLatencyMode {
				log.Printf("❌ Worker %d: Failed to handle reorganization for tick %d: %v", workerID, tick.TickNumber, err)
				// Don't fail processing for reorg handling errors, just log them
			}
		}
	}
	
	// Use parser plugin to transform protobuf tick to sink-compatible data
	parseStartTime := time.Now()
	parsedData, err := s.parser.ParseTick(ctx, tick)
	if err != nil {
		return fmt.Errorf("failed to parse tick %d: %w", tick.TickNumber, err)
	}
	parseLatency := time.Since(parseStartTime)
	
	// Send parsed data directly to sink - let sink handle batching logic
	if len(parsedData) > 0 {
		sinkStartTime := time.Now()
		if err := s.sink.PersistData(ctx, parsedData); err != nil {
			return fmt.Errorf("failed to persist data for tick %d: %w", tick.TickNumber, err)
		}
		sinkLatency := time.Since(sinkStartTime)
		
		// Skip all logging in low latency mode for maximum performance
		if !s.config.LowLatencyMode {
			totalLatency := time.Since(processStartTime)
			log.Printf("⏱️  Worker %d: Tick #%d latency: parse=%v, sink=%v, total=%v", 
				workerID,
				tick.TickNumber, 
				parseLatency.Truncate(time.Microsecond),
				sinkLatency.Truncate(time.Microsecond), 
				totalLatency.Truncate(time.Microsecond))
		}
	}
	
	// Update last processed tick (thread-safe with atomic operations would be better)
	s.lastProcessedTick = tick.TickNumber
	
	return nil
}

// GetAsyncStats returns async processing statistics
func (s *Streamer) GetAsyncStats() map[string]interface{} {
	return map[string]interface{}{
		"channel_depth":    len(s.tickChan),
		"channel_capacity": cap(s.tickChan),
		"channel_usage":    float64(len(s.tickChan)) / float64(cap(s.tickChan)) * 100,
		"worker_count":     s.config.SinkWorkers,
		"ticks_dropped":    s.ticksDropped,
		"last_drop_time":   s.lastDropTime,
	}
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

			// Async processing: Send tick to worker channel with intelligent backpressure
			channelUsage := float64(len(s.tickChan)) / float64(cap(s.tickChan)) * 100
			
			// If channel is getting full, try harder to avoid dropping
			if channelUsage > 90 {
				// Use blocking send for high-priority scenarios to avoid data loss
				select {
				case s.tickChan <- tick:
					// Successfully queued even though channel was nearly full
					if !s.config.LowLatencyMode {
						log.Printf("⚠️ HIGH PRESSURE: Tick %d queued (channel: %.1f%% full)", 
							tick.TickNumber, channelUsage)
					}
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
					// Only drop after trying to send for 10ms
					s.ticksDropped++
					s.lastDropTime = time.Now()
					
					if s.ticksDropped%100 == 0 { // Log every 100 drops to reduce spam
						log.Printf("🔴 CRITICAL: Channel blocked for >10ms, dropping tick %d - total dropped: %d", 
							tick.TickNumber, s.ticksDropped)
					}
				}
			} else {
				// Normal operation - non-blocking send
				select {
				case s.tickChan <- tick:
					// Tick sent successfully to worker pool
					if !s.config.LowLatencyMode && tick.TickNumber%200 == 0 {
						log.Printf("📤 Tick %d queued (channel: %.1f%% full)", 
							tick.TickNumber, channelUsage)
					}
				case <-ctx.Done():
					return ctx.Err()
				default:
					// Channel is full - this should be rare with better sizing
					s.ticksDropped++
					s.lastDropTime = time.Now()
					
					if s.ticksDropped%50 == 0 { // Log every 50 drops
						log.Printf("⚠️ Channel full, dropping tick %d - total dropped: %d", 
							tick.TickNumber, s.ticksDropped)
					}
				}
			}
			
			// Old synchronous processing removed - now handled by worker pool
			continue
			
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


// Health returns the current health status of the streamer
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
	
	return s.sink.Health(ctx)
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

