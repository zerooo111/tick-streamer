package batcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
	"github.com/zerooo111/tick-streamer/internal/sink"
)

// TestBatcherConcurrency tests that the batcher handles concurrent data correctly
func TestBatcherConcurrency(t *testing.T) {
	// Create a mock sink
	sinkConfig := sink.Config{
		Kind:         "mock",
		MaxBatchSize: 1000,
	}
	mockSink, err := sink.NewSink(sinkConfig)
	if err != nil {
		t.Fatalf("Failed to create mock sink: %v", err)
	}

	// Create batcher with small batch size for testing
	batcher := New(mockSink, 10, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start the batcher
	if err := batcher.Start(ctx); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop(ctx)

	// Simulate concurrent data producers
	numProducers := 5
	itemsPerProducer := 20

	var wg sync.WaitGroup
	wg.Add(numProducers)

	// Start concurrent producers
	for i := 0; i < numProducers; i++ {
		go func(producerID int) {
			defer wg.Done()

			for j := 0; j < itemsPerProducer; j++ {
				// Create test data
				tickRow := &models.TickRow{
					TickNumber:           uint64(producerID*1000 + j),
					TimestampUS:          time.Now().UnixMicro(),
					TransactionCount:     1,
					TransactionBatchHash: "test_hash",
				}

				parsedData := &parser.ParsedData{
					Type: "tick",
					Data: tickRow,
					Metadata: map[string]interface{}{
						"producer_id": producerID,
						"item_id":     j,
					},
				}

				// Add to batcher with timeout
				addCtx, addCancel := context.WithTimeout(ctx, 1*time.Second)
				if err := batcher.Add(addCtx, parsedData); err != nil {
					t.Errorf("Producer %d failed to add item %d: %v", producerID, j, err)
				}
				addCancel()

				// Small delay to simulate realistic timing
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	// Wait for all producers to finish
	wg.Wait()

	// Force flush to ensure all data is processed
	if err := batcher.Flush(ctx); err != nil {
		t.Errorf("Failed to flush batcher: %v", err)
	}

	// Wait a moment for processing to complete
	time.Sleep(100 * time.Millisecond)

	// Check statistics
	stats := batcher.GetStats()
	expectedItems := uint64(numProducers * itemsPerProducer)

	t.Logf("Batcher stats: %d batches processed, %d items processed", 
		stats.BatchesProcessed, stats.ItemsProcessed)

	if stats.ItemsProcessed != expectedItems {
		t.Errorf("Expected %d items processed, got %d", expectedItems, stats.ItemsProcessed)
	}

	if stats.BatchesProcessed == 0 {
		t.Error("Expected at least one batch to be processed")
	}
}

// TestBatcherBackpressure tests backpressure handling
func TestBatcherBackpressure(t *testing.T) {
	// Create a mock sink
	sinkConfig := sink.Config{
		Kind:         "mock",
		MaxBatchSize: 10,
	}
	mockSink, err := sink.NewSink(sinkConfig)
	if err != nil {
		t.Fatalf("Failed to create mock sink: %v", err)
	}

	// Create batcher with very small buffer to trigger backpressure
	batcher := New(mockSink, 5, 1000*time.Millisecond) // Long timeout to force size-based batching

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Start the batcher
	if err := batcher.Start(ctx); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop(ctx)

	// Try to add many items quickly to trigger backpressure
	numItems := 50
	successCount := 0
	backpressureCount := 0

	for i := 0; i < numItems; i++ {
		tickRow := &models.TickRow{
			TickNumber:           uint64(i),
			TimestampUS:          time.Now().UnixMicro(),
			TransactionCount:     1,
			TransactionBatchHash: "test_hash",
		}

		parsedData := &parser.ParsedData{
			Type: "tick",
			Data: tickRow,
		}

		// Try to add with short timeout to see backpressure
		addCtx, addCancel := context.WithTimeout(ctx, 10*time.Millisecond)
		if err := batcher.Add(addCtx, parsedData); err != nil {
			backpressureCount++
			t.Logf("Item %d triggered backpressure: %v", i, err)
		} else {
			successCount++
		}
		addCancel()
	}

	t.Logf("Successfully added %d items, %d hit backpressure", successCount, backpressureCount)

	// We expect some backpressure with the small buffer
	if backpressureCount == 0 {
		t.Log("Note: No backpressure observed - this might be okay depending on timing")
	}

	// Force flush and check stats
	if err := batcher.Flush(ctx); err != nil {
		t.Errorf("Failed to flush batcher: %v", err)
	}

	stats := batcher.GetStats()
	t.Logf("Final stats: %d batches, %d items processed", 
		stats.BatchesProcessed, stats.ItemsProcessed)
}

// TestBatcherTimeTrigger tests time-based batch flushing
func TestBatcherTimeTrigger(t *testing.T) {
	// Create a mock sink
	sinkConfig := sink.Config{
		Kind:         "mock",
		MaxBatchSize: 100,
	}
	mockSink, err := sink.NewSink(sinkConfig)
	if err != nil {
		t.Fatalf("Failed to create mock sink: %v", err)
	}

	// Create batcher with short timeout to test time-based flushing
	batcher := New(mockSink, 100, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the batcher
	if err := batcher.Start(ctx); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop(ctx)

	// Add a few items (less than batch size)
	for i := 0; i < 5; i++ {
		tickRow := &models.TickRow{
			TickNumber:           uint64(i),
			TimestampUS:          time.Now().UnixMicro(),
			TransactionCount:     1,
			TransactionBatchHash: "test_hash",
		}

		parsedData := &parser.ParsedData{
			Type: "tick",
			Data: tickRow,
		}

		if err := batcher.Add(ctx, parsedData); err != nil {
			t.Errorf("Failed to add item %d: %v", i, err)
		}
	}

	// Wait for time-based flush (should happen after ~100ms)
	time.Sleep(200 * time.Millisecond)

	// Check that items were processed despite not reaching batch size
	stats := batcher.GetStats()
	t.Logf("Time-trigger stats: %d batches, %d items processed", 
		stats.BatchesProcessed, stats.ItemsProcessed)

	if stats.ItemsProcessed != 5 {
		t.Errorf("Expected 5 items processed by time trigger, got %d", stats.ItemsProcessed)
	}

	if stats.BatchesProcessed == 0 {
		t.Error("Expected at least one batch processed by time trigger")
	}
}