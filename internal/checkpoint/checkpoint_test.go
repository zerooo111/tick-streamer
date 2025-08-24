package checkpoint

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestSQLiteStoreBasicOperations tests basic checkpoint operations
func TestSQLiteStoreBasicOperations(t *testing.T) {
	// Create temporary directory for test database
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_checkpoint.db")

	config := Config{
		DSN:       "file:" + dbPath,
		TableName: "test_checkpoints",
	}

	// Create store
	store, err := NewSQLiteStore(config)
	if err != nil {
		t.Fatalf("Failed to create SQLite store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test initial load (should return 0 for empty store)
	tick, err := store.Load(ctx)
	if err != nil {
		t.Errorf("Failed to load initial checkpoint: %v", err)
	}
	if tick != 0 {
		t.Errorf("Expected initial tick to be 0, got %d", tick)
	}

	// Test saving checkpoints
	testTicks := []uint64{100, 200, 150, 300} // Note: 150 < 200, should be ignored
	expectedTick := uint64(0)

	for _, tickToSave := range testTicks {
		err := store.Save(ctx, tickToSave)
		if err != nil {
			t.Errorf("Failed to save checkpoint %d: %v", tickToSave, err)
		}

		// Only update expected if tick advanced
		if tickToSave > expectedTick {
			expectedTick = tickToSave
		}

		// Verify the tick was saved correctly
		loadedTick, err := store.Load(ctx)
		if err != nil {
			t.Errorf("Failed to load checkpoint after saving %d: %v", tickToSave, err)
		}

		if loadedTick != expectedTick {
			t.Errorf("After saving %d, expected to load %d, got %d", tickToSave, expectedTick, loadedTick)
		}
	}

	// Test health check
	err = store.Health(ctx)
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

// TestSQLiteStoreConcurrentOperations tests concurrent access
func TestSQLiteStoreConcurrentOperations(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "concurrent_test.db")

	config := Config{
		DSN:       "file:" + dbPath,
		TableName: "concurrent_checkpoints",
	}

	store, err := NewSQLiteStore(config)
	if err != nil {
		t.Fatalf("Failed to create SQLite store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start multiple goroutines saving checkpoints
	numWorkers := 5
	ticksPerWorker := 20
	done := make(chan error, numWorkers)

	for worker := 0; worker < numWorkers; worker++ {
		go func(workerID int) {
			for i := 0; i < ticksPerWorker; i++ {
				tick := uint64(workerID*1000 + i)
				if err := store.Save(ctx, tick); err != nil {
					done <- err
					return
				}
				time.Sleep(1 * time.Millisecond) // Small delay to simulate real timing
			}
			done <- nil
		}(worker)
	}

	// Wait for all workers to complete
	for i := 0; i < numWorkers; i++ {
		if err := <-done; err != nil {
			t.Errorf("Worker failed: %v", err)
		}
	}

	// Verify final checkpoint
	finalTick, err := store.Load(ctx)
	if err != nil {
		t.Errorf("Failed to load final checkpoint: %v", err)
	}

	t.Logf("Final checkpoint after concurrent operations: %d", finalTick)

	// Should be the highest tick saved
	expectedMax := uint64((numWorkers-1)*1000 + ticksPerWorker - 1)
	if finalTick != expectedMax {
		t.Errorf("Expected final tick to be %d, got %d", expectedMax, finalTick)
	}
}

// TestMemoryStore tests the in-memory checkpoint store
func TestMemoryStore(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Test initial state
	tick, err := store.Load(ctx)
	if err != nil {
		t.Errorf("Failed to load from memory store: %v", err)
	}
	if tick != 0 {
		t.Errorf("Expected initial tick to be 0, got %d", tick)
	}

	// Test saving and loading
	testTick := uint64(12345)
	err = store.Save(ctx, testTick)
	if err != nil {
		t.Errorf("Failed to save to memory store: %v", err)
	}

	loadedTick, err := store.Load(ctx)
	if err != nil {
		t.Errorf("Failed to load from memory store: %v", err)
	}
	if loadedTick != testTick {
		t.Errorf("Expected to load %d, got %d", testTick, loadedTick)
	}

	// Test health
	err = store.Health(ctx)
	if err != nil {
		t.Errorf("Memory store health check failed: %v", err)
	}
}

// TestStoreFactory tests the factory function
func TestStoreFactory(t *testing.T) {
	// Test memory store creation
	memStore, err := NewStore("")
	if err != nil {
		t.Errorf("Failed to create memory store via factory: %v", err)
	}
	defer memStore.Close()

	ctx := context.Background()
	if err := memStore.Health(ctx); err != nil {
		t.Errorf("Memory store health check failed: %v", err)
	}

	// Test memory store creation with explicit "memory"
	memStore2, err := NewStore("memory")
	if err != nil {
		t.Errorf("Failed to create memory store with 'memory' DSN: %v", err)
	}
	defer memStore2.Close()

	// Test SQLite store creation
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "factory_test.db")
	
	sqliteStore, err := NewStore("file:" + dbPath)
	if err != nil {
		t.Errorf("Failed to create SQLite store via factory: %v", err)
	}
	defer sqliteStore.Close()

	if err := sqliteStore.Health(ctx); err != nil {
		t.Errorf("SQLite store health check failed: %v", err)
	}
}

// TestCheckpointRecoveryScenario simulates a crash and recovery scenario
func TestCheckpointRecoveryScenario(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "recovery_test.db")
	dsn := "file:" + dbPath

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Simulate first run - process some ticks and save checkpoints
	{
		store1, err := NewStore(dsn)
		if err != nil {
			t.Fatalf("Failed to create first store: %v", err)
		}

		// Simulate processing ticks 1-100
		for tick := uint64(1); tick <= 100; tick += 10 {
			if err := store1.Save(ctx, tick); err != nil {
				t.Errorf("Failed to save checkpoint %d: %v", tick, err)
			}
		}

		// Verify final checkpoint
		lastTick, err := store1.Load(ctx)
		if err != nil {
			t.Errorf("Failed to load checkpoint from first store: %v", err)
		}
		if lastTick != 91 { // Last tick in the loop: 91
			t.Errorf("Expected last tick to be 91, got %d", lastTick)
		}

		// Close store (simulating shutdown)
		store1.Close()
	}

	// Simulate crash and recovery - create new store instance
	{
		store2, err := NewStore(dsn)
		if err != nil {
			t.Fatalf("Failed to create recovery store: %v", err)
		}
		defer store2.Close()

		// Load checkpoint - should resume from where we left off
		resumeTick, err := store2.Load(ctx)
		if err != nil {
			t.Errorf("Failed to load checkpoint during recovery: %v", err)
		}

		if resumeTick != 91 {
			t.Errorf("Expected to resume from tick 91, got %d", resumeTick)
		}

		t.Logf("Successfully recovered from checkpoint: resuming at tick %d", resumeTick)

		// Continue processing from resume point
		for tick := resumeTick + 1; tick <= resumeTick+50; tick += 5 {
			if err := store2.Save(ctx, tick); err != nil {
				t.Errorf("Failed to save post-recovery checkpoint %d: %v", tick, err)
			}
		}

		// Verify final state
		finalTick, err := store2.Load(ctx)
		if err != nil {
			t.Errorf("Failed to load final checkpoint: %v", err)
		}

		expectedFinal := uint64(137) // 91 + 46 (last increment: 91+1+45)
		if finalTick != expectedFinal {
			t.Errorf("Expected final tick to be %d, got %d", expectedFinal, finalTick)
		}
	}
}

// TestCheckpointStats tests the statistics functionality
func TestCheckpointStats(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "stats_test.db")

	config := Config{
		DSN:       "file:" + dbPath,
		TableName: "stats_checkpoints",
	}

	store, err := NewSQLiteStore(config)
	if err != nil {
		t.Fatalf("Failed to create SQLite store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Save several checkpoints
	testTicks := []uint64{10, 20, 30, 40, 50}
	for _, tick := range testTicks {
		if err := store.Save(ctx, tick); err != nil {
			t.Errorf("Failed to save checkpoint %d: %v", tick, err)
		}
		time.Sleep(10 * time.Millisecond) // Small delay for different timestamps
	}

	// Get statistics
	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Errorf("Failed to get checkpoint stats: %v", err)
	}

	// Verify stats
	if stats.LastTickNumber != 50 {
		t.Errorf("Expected last tick to be 50, got %d", stats.LastTickNumber)
	}

	if stats.TotalCheckpoints != int64(len(testTicks)) {
		t.Errorf("Expected %d checkpoints, got %d", len(testTicks), stats.TotalCheckpoints)
	}

	if stats.OldestTick != 10 {
		t.Errorf("Expected oldest tick to be 10, got %d", stats.OldestTick)
	}

	if stats.NewestTick != 50 {
		t.Errorf("Expected newest tick to be 50, got %d", stats.NewestTick)
	}

	if stats.DatabasePath != dbPath {
		t.Errorf("Expected database path %s, got %s", dbPath, stats.DatabasePath)
	}

	t.Logf("Checkpoint stats: %+v", stats)
}

// Benchmark checkpoint operations
func BenchmarkCheckpointSave(b *testing.B) {
	tempDir := b.TempDir()
	dbPath := filepath.Join(tempDir, "benchmark.db")

	store, err := NewSQLiteStore(Config{
		DSN:       "file:" + dbPath,
		TableName: "benchmark_checkpoints",
	})
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Save(ctx, uint64(i)); err != nil {
			b.Errorf("Failed to save checkpoint: %v", err)
		}
	}
}

func BenchmarkCheckpointLoad(b *testing.B) {
	tempDir := b.TempDir()
	dbPath := filepath.Join(tempDir, "benchmark_load.db")

	store, err := NewSQLiteStore(Config{
		DSN:       "file:" + dbPath,
		TableName: "benchmark_load_checkpoints",
	})
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save one checkpoint to load
	if err := store.Save(ctx, 12345); err != nil {
		b.Fatalf("Failed to save initial checkpoint: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Load(ctx); err != nil {
			b.Errorf("Failed to load checkpoint: %v", err)
		}
	}
}