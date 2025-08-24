package batcher

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/zerooo111/tick-streamer/internal/parser"
	"github.com/zerooo111/tick-streamer/internal/sink"
)

// BatchItem wraps parsed data with retry metadata
type BatchItem struct {
	Data         *parser.ParsedData
	RetryCount   int
	FirstAttempt time.Time
	LastAttempt  time.Time
}

// Batcher handles concurrent batching of parsed data
// This demonstrates Go's concurrency patterns with channels and goroutines
type Batcher struct {
	sink sink.Sink
	
	// Batching configuration
	maxBatchSize int
	maxWaitTime  time.Duration
	
	// Channels for concurrent processing
	dataCh     chan *parser.ParsedData // Input channel from streamer
	flushCh    chan chan error         // Channel for manual flush requests
	stopCh     chan struct{}           // Channel for shutdown signal
	
	// Current batch state
	currentBatch []*BatchItem
	batchTimer   *time.Timer
	
	// Concurrency control
	wg     sync.WaitGroup
	mu     sync.RWMutex
	
	// Circuit breaker for resilience
	consecutiveFailures int
	lastFailureTime     time.Time
	circuitOpen         bool
	circuitResetTime    time.Duration
	
	// Retry configuration
	maxRetryAttempts    int
	maxRetryAge         time.Duration
	
	// Statistics
	stats BatchStats
}

// BatchStats tracks batching performance metrics
type BatchStats struct {
	BatchesProcessed   uint64
	ItemsProcessed     uint64
	TotalFlushDuration time.Duration
	AverageFlushTime   time.Duration
	LastFlushTime      time.Time
	QueueDepth         int
	mu                 sync.RWMutex
}

// New creates a new Batcher with the specified configuration
func New(s sink.Sink, maxBatchSize int, maxWaitTime time.Duration) *Batcher {
	return &Batcher{
		sink:             s,
		maxBatchSize:     maxBatchSize,
		maxWaitTime:      maxWaitTime,
		dataCh:           make(chan *parser.ParsedData, maxBatchSize*2), // Buffer 2x batch size
		flushCh:          make(chan chan error),
		stopCh:           make(chan struct{}),
		currentBatch:     make([]*BatchItem, 0, maxBatchSize),
		circuitResetTime: 30 * time.Second, // Circuit breaker reset time
		maxRetryAttempts: 3,                 // Maximum retry attempts per batch item
		maxRetryAge:      5 * time.Minute,   // Drop items older than 5 minutes
	}
}

// Start begins the batching goroutine
// This demonstrates the goroutine and select pattern for concurrent processing
func (b *Batcher) Start(ctx context.Context) error {
	log.Printf("Starting batcher with max_batch_size=%d, max_wait_time=%v", 
		b.maxBatchSize, b.maxWaitTime)
	
	b.wg.Add(1)
	go b.batchLoop(ctx)
	
	return nil
}

// Stop gracefully shuts down the batcher
func (b *Batcher) Stop(ctx context.Context) error {
	log.Println("Stopping batcher...")
	
	close(b.stopCh)
	
	// Wait for batch loop to finish with timeout
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		log.Println("Batcher stopped gracefully")
	case <-ctx.Done():
		log.Println("Batcher stop timeout")
		return ctx.Err()
	}
	
	return nil
}

// Add sends data to the batcher for processing
// Enhanced backpressure handling with retry logic
func (b *Batcher) Add(ctx context.Context, data *parser.ParsedData) error {
	if data == nil {
		return fmt.Errorf("cannot add nil data to batch")
	}
	
	return b.addWithBackpressure(ctx, data, 3) // Allow up to 3 retries
}

// addWithBackpressure implements sophisticated backpressure handling
func (b *Batcher) addWithBackpressure(ctx context.Context, data *parser.ParsedData, maxRetries int) error {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		select {
		case b.dataCh <- data:
			// Success - update queue depth for monitoring
			b.stats.mu.Lock()
			b.stats.QueueDepth = len(b.dataCh)
			b.stats.mu.Unlock()
			return nil
			
		case <-ctx.Done():
			return ctx.Err()
			
		default:
			// Channel is full - implement backpressure handling
			if attempt == maxRetries {
				// Final attempt failed - return backpressure error
				return fmt.Errorf("batcher queue is full after %d attempts (backpressure)", maxRetries+1)
			}
			
			// Wait briefly before retry, with exponential backoff
			backoffMs := (1 << attempt) * 10 // 10ms, 20ms, 40ms
			select {
			case <-time.After(time.Duration(backoffMs) * time.Millisecond):
				// Continue to retry
				log.Printf("Batcher backpressure, retry %d/%d after %dms", 
					attempt+1, maxRetries, backoffMs)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	
	return fmt.Errorf("unexpected end of backpressure retry loop")
}

// Flush forces immediate processing of current batch
func (b *Batcher) Flush(ctx context.Context) error {
	responseCh := make(chan error, 1)
	
	select {
	case b.flushCh <- responseCh:
		// Wait for flush to complete
		select {
		case err := <-responseCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// filterRetryableItems filters items that can still be retried
func (b *Batcher) filterRetryableItems(items []*BatchItem) []*BatchItem {
	now := time.Now()
	retryable := make([]*BatchItem, 0, len(items))
	
	for _, item := range items {
		// Skip items that have exceeded retry limits
		if item.RetryCount >= b.maxRetryAttempts {
			log.Printf("⚠️ Dropping %s item after %d retry attempts", 
				item.Data.Type, item.RetryCount)
			continue
		}
		
		// Skip items that are too old
		if now.Sub(item.FirstAttempt) > b.maxRetryAge {
			log.Printf("⚠️ Dropping %s item due to age (%.1f minutes old)",
				item.Data.Type, now.Sub(item.FirstAttempt).Minutes())
			continue
		}
		
		retryable = append(retryable, item)
	}
	
	return retryable
}

// prepareRetryBatch updates retry metadata for items being retried
func (b *Batcher) prepareRetryBatch(items []*BatchItem) []*BatchItem {
	now := time.Now()
	retryBatch := make([]*BatchItem, 0, len(items))
	
	for _, item := range items {
		// Skip items that shouldn't be retried
		if item.RetryCount >= b.maxRetryAttempts {
			log.Printf("⚠️ Dropping %s item after %d retry attempts",
				item.Data.Type, item.RetryCount)
			continue
		}
		
		if now.Sub(item.FirstAttempt) > b.maxRetryAge {
			log.Printf("⚠️ Dropping %s item due to age (%.1f minutes old)",
				item.Data.Type, now.Sub(item.FirstAttempt).Minutes())
			continue
		}
		
		// Update retry metadata
		item.RetryCount++
		item.LastAttempt = now
		retryBatch = append(retryBatch, item)
	}
	
	if len(items) > len(retryBatch) {
		log.Printf("📊 Retry batch: %d items dropped, %d items queued for retry",
			len(items)-len(retryBatch), len(retryBatch))
	}
	
	return retryBatch
}

// GetStats returns current batching statistics
func (b *Batcher) GetStats() BatchStats {
	b.stats.mu.RLock()
	defer b.stats.mu.RUnlock()
	
	stats := b.stats
	stats.QueueDepth = len(b.dataCh)
	return stats
}

// batchLoop is the main goroutine that handles batching logic
// This demonstrates Go's select statement for multiplexing channels
func (b *Batcher) batchLoop(ctx context.Context) {
	defer b.wg.Done()
	defer b.stopBatchTimer()
	
	log.Println("Batcher loop started")
	
	for {
		select {
		case <-ctx.Done():
			log.Println("Batcher loop cancelled by context")
			b.flushCurrentBatch(ctx, "context_cancelled")
			return
			
		case <-b.stopCh:
			log.Println("Batcher loop stopped")
			b.flushCurrentBatch(ctx, "shutdown")
			return
			
		case data := <-b.dataCh:
			// Add data to current batch
			b.addToBatch(data)
			
			// Check if batch is full
			if len(b.currentBatch) >= b.maxBatchSize {
				b.flushCurrentBatch(ctx, "size_limit")
			} else if len(b.currentBatch) == 1 {
				// First item in batch - start timer
				b.startBatchTimer()
			}
			
		case <-b.getBatchTimerCh():
			// Batch timeout reached
			if len(b.currentBatch) > 0 {
				b.flushCurrentBatch(ctx, "time_limit")
			}
			
		case responseCh := <-b.flushCh:
			// Manual flush requested
			err := b.flushCurrentBatch(ctx, "manual")
			responseCh <- err
		}
	}
}

// addToBatch adds data to the current batch
func (b *Batcher) addToBatch(data *parser.ParsedData) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	now := time.Now()
	item := &BatchItem{
		Data:         data,
		RetryCount:   0,
		FirstAttempt: now,
		LastAttempt:  now,
	}
	b.currentBatch = append(b.currentBatch, item)
	
	// Update stats
	b.stats.mu.Lock()
	b.stats.ItemsProcessed++
	b.stats.mu.Unlock()
}

// flushCurrentBatch sends the current batch to the sink with circuit breaker protection
func (b *Batcher) flushCurrentBatch(ctx context.Context, trigger string) error {
	b.mu.Lock()
	batchSize := len(b.currentBatch)
	batch := make([]*BatchItem, batchSize)
	copy(batch, b.currentBatch)
	b.currentBatch = b.currentBatch[:0] // Reset slice but keep capacity
	
	// Check circuit breaker
	if b.circuitOpen {
		if time.Since(b.lastFailureTime) < b.circuitResetTime {
			b.mu.Unlock()
			// Circuit still open - filter and retry eligible items
			b.mu.Lock()
			b.currentBatch = b.filterRetryableItems(batch)
			b.mu.Unlock()
			return fmt.Errorf("circuit breaker open - rejecting batch")
		} else {
			// Try to reset circuit breaker
			log.Println("🔄 Attempting to reset circuit breaker")
			b.circuitOpen = false
			b.consecutiveFailures = 0
		}
	}
	b.mu.Unlock()
	
	// Stop the timer since we're flushing
	b.stopBatchTimer()
	
	if batchSize == 0 {
		return nil
	}
	
	start := time.Now()
	
	// Extract the actual data for persistence
	dataToFlush := make([]*parser.ParsedData, 0, len(batch))
	for _, item := range batch {
		dataToFlush = append(dataToFlush, item.Data)
	}
	
	// Send batch to sink
	err := b.sink.PersistData(ctx, dataToFlush)
	if err != nil {
		log.Printf("❌ Batch flush failed: %v (batch_size=%d, trigger=%s)", 
			err, batchSize, trigger)
		
		// Handle circuit breaker logic
		b.handleFlushFailure()
		
		// Update retry counts and filter items that should be retried
		retryBatch := b.prepareRetryBatch(batch)
		
		// Put eligible items back for retry
		b.mu.Lock()
		b.currentBatch = append(retryBatch, b.currentBatch...)
		b.mu.Unlock()
		
		return err
	}
	
	// Call sink flush to ensure persistence
	if err := b.sink.Flush(ctx); err != nil {
		log.Printf("❌ Sink flush failed: %v", err)
		b.handleFlushFailure()
		return err
	}
	
	// Success - reset circuit breaker counters
	b.handleFlushSuccess()
	
	duration := time.Since(start)
	
	// Update statistics
	b.updateFlushStats(batchSize, duration)
	
	// Log successful flush
	tickCount := 0
	txCount := 0
	for _, item := range batch {
		switch item.Data.Type {
		case "tick":
			tickCount++
		case "transaction":
			txCount++
		}
	}
	
	log.Printf("✅ Flushed batch: %d items (ticks=%d, tx=%d) in %v (trigger=%s)", 
		batchSize, tickCount, txCount, duration.Truncate(time.Millisecond), trigger)
	
	return nil
}

// handleFlushFailure manages circuit breaker state on failures
func (b *Batcher) handleFlushFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	b.consecutiveFailures++
	b.lastFailureTime = time.Now()
	
	// Open circuit after 5 consecutive failures
	if b.consecutiveFailures >= 5 && !b.circuitOpen {
		b.circuitOpen = true
		log.Printf("🚨 Circuit breaker OPENED after %d consecutive failures", b.consecutiveFailures)
	}
}

// handleFlushSuccess resets circuit breaker state on success
func (b *Batcher) handleFlushSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if b.consecutiveFailures > 0 || b.circuitOpen {
		log.Printf("✅ Circuit breaker reset after successful flush")
		b.consecutiveFailures = 0
		b.circuitOpen = false
	}
}

// updateFlushStats updates batching performance statistics
func (b *Batcher) updateFlushStats(batchSize int, duration time.Duration) {
	b.stats.mu.Lock()
	defer b.stats.mu.Unlock()
	
	b.stats.BatchesProcessed++
	b.stats.TotalFlushDuration += duration
	b.stats.LastFlushTime = time.Now()
	
	// Calculate rolling average
	if b.stats.BatchesProcessed == 1 {
		b.stats.AverageFlushTime = duration
	} else {
		// Exponential moving average
		alpha := 0.1
		b.stats.AverageFlushTime = time.Duration(
			alpha*float64(duration) + (1-alpha)*float64(b.stats.AverageFlushTime),
		)
	}
}

// Batch timer management methods
func (b *Batcher) startBatchTimer() {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if b.batchTimer == nil {
		b.batchTimer = time.NewTimer(b.maxWaitTime)
	} else {
		b.batchTimer.Reset(b.maxWaitTime)
	}
}

func (b *Batcher) stopBatchTimer() {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if b.batchTimer != nil {
		if !b.batchTimer.Stop() {
			// Drain the channel if timer fired but we haven't read from it
			select {
			case <-b.batchTimer.C:
			default:
			}
		}
	}
}

func (b *Batcher) getBatchTimerCh() <-chan time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	if b.batchTimer != nil {
		return b.batchTimer.C
	}
	
	// Return a nil channel that will never be ready
	return nil
}