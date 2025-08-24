package sink

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
)

func TestSQLiteSink_BasicOperations(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite_sink_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create sink
	cfg := Config{
		Kind: "sqlite",
		DSN:  dbPath,
	}

	sink, err := NewSQLiteSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create SQLite sink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()

	// Test health check
	if !sink.Health(ctx) {
		t.Error("Expected sink to be healthy")
	}

	// Test initial last tick
	lastTick, err := sink.GetLastTick(ctx)
	if err != nil {
		t.Fatalf("Failed to get last tick: %v", err)
	}
	if lastTick != 0 {
		t.Errorf("Expected last tick to be 0, got %d", lastTick)
	}

	// Create test data
	tickRow := &models.TickRow{
		TickNumber:           100,
		TimestampUS:          time.Now().UnixMicro(),
		VdfInput:            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
		VdfOutput:           "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
		VdfIterations:       1000,
		VdfProof:            "proof_data",
		PreviousOutput:      "9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",
		TransactionBatchHash: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
		TransactionCount:    2,
		ProcessedAt:         time.Now(),
		IngestionTS:         time.Now().UnixMicro(),
		Version:            1,
	}

	txRow := &models.TxRow{
		TickNumber:         100,
		SequenceNumber:     0,
		TxHash:            "1122334455667788aabbccddeeff00111122334455667788aabbccddeeff0011",
		TxID:              "tx_0",
		Nonce:             0,
		Payload:           []byte("test_payload"),
		Timestamp:         uint64(time.Now().UnixMicro()),
		PublicKey:         "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
		Signature:         "deadbeef12345678deadbeef12345678deadbeef12345678deadbeef12345678",
		IngestionTimestamp: uint64(time.Now().UnixMicro()),
		ProcessedAt:       time.Now(),
		PayloadSize:       12,
		PayloadType:       "small",
		Version:          1,
	}

	// Test persist data
	data := []*parser.ParsedData{
		{Type: "tick", Data: tickRow},
		{Type: "transaction", Data: txRow},
	}

	err = sink.PersistData(ctx, data)
	if err != nil {
		t.Fatalf("Failed to persist data: %v", err)
	}

	// Check last tick updated
	lastTick, err = sink.GetLastTick(ctx)
	if err != nil {
		t.Fatalf("Failed to get last tick: %v", err)
	}
	if lastTick != 100 {
		t.Errorf("Expected last tick to be 100, got %d", lastTick)
	}

	// Verify data was inserted by querying the database directly
	var count int
	err = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ticks WHERE version > 0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query tick count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 tick in database, got %d", count)
	}

	err = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transactions WHERE version > 0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query transaction count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 transaction in database, got %d", count)
	}

	// Test flush
	err = sink.Flush(ctx)
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
}

func TestSQLiteSink_InvalidateTick(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite_sink_invalidate_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create sink
	cfg := Config{
		Kind: "sqlite",
		DSN:  dbPath,
	}

	sink, err := NewSQLiteSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create SQLite sink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()

	// Add test data first
	tickRow := &models.TickRow{
		TickNumber:  100,
		TimestampUS: time.Now().UnixMicro(),
		VdfInput:   "input",
		VdfOutput:  "output",
		Version:    1,
	}

	txRow := &models.TxRow{
		TickNumber:     100,
		SequenceNumber: 0,
		TxHash:        "hash",
		TxID:          "tx_0",
		Version:       1,
	}

	data := []*parser.ParsedData{
		{Type: "tick", Data: tickRow},
		{Type: "transaction", Data: txRow},
	}

	err = sink.PersistData(ctx, data)
	if err != nil {
		t.Fatalf("Failed to persist initial data: %v", err)
	}

	// Verify data exists
	var count int
	err = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ticks WHERE tick_number = 100 AND version > 0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query tick count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 valid tick before invalidation, got %d", count)
	}

	// Invalidate the tick
	err = sink.InvalidateTick(ctx, 100)
	if err != nil {
		t.Fatalf("Failed to invalidate tick: %v", err)
	}

	// Verify data was invalidated (version set to -1)
	err = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ticks WHERE tick_number = 100 AND version > 0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query valid tick count after invalidation: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 valid ticks after invalidation, got %d", count)
	}

	// Verify invalidated records exist
	err = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ticks WHERE tick_number = 100 AND version = -1").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query invalidated tick count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 invalidated tick, got %d", count)
	}

	// Same for transactions
	err = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transactions WHERE tick_number = 100 AND version = -1").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query invalidated transaction count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 invalidated transaction, got %d", count)
	}
}

func TestSQLiteSink_Concurrency(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite_sink_concurrency_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create sink
	cfg := Config{
		Kind: "sqlite",
		DSN:  dbPath,
	}

	sink, err := NewSQLiteSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create SQLite sink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()

	// Test concurrent operations
	done := make(chan bool, 10)
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		go func(tickNum int) {
			tickRow := &models.TickRow{
				TickNumber:  uint64(tickNum),
				TimestampUS: time.Now().UnixMicro(),
				VdfInput:   "input",
				VdfOutput:  "output",
				Version:    1,
			}

			data := []*parser.ParsedData{
				{Type: "tick", Data: tickRow},
			}

			if err := sink.PersistData(ctx, data); err != nil {
				errors <- err
				return
			}

			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		select {
		case <-done:
			// Success
		case err := <-errors:
			t.Fatalf("Concurrent operation failed: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent operations")
		}
	}

	// Verify all data was inserted
	var count int
	err = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ticks WHERE version > 0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query final tick count: %v", err)
	}
	if count != 10 {
		t.Errorf("Expected 10 ticks after concurrent operations, got %d", count)
	}
}

func TestSQLiteSink_DatabaseStats(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite_sink_stats_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create sink
	cfg := Config{
		Kind: "sqlite",
		DSN:  dbPath,
	}

	sink, err := NewSQLiteSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create SQLite sink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()

	// Add some test data
	for i := 1; i <= 5; i++ {
		tickRow := &models.TickRow{
			TickNumber:  uint64(i),
			TimestampUS: time.Now().UnixMicro(),
			VdfInput:   "input",
			VdfOutput:  "output",
			Version:    1,
		}

		data := []*parser.ParsedData{
			{Type: "tick", Data: tickRow},
		}

		err = sink.PersistData(ctx, data)
		if err != nil {
			t.Fatalf("Failed to persist data: %v", err)
		}
	}

	// Get database statistics
	stats, err := sink.GetDatabaseStats(ctx)
	if err != nil {
		t.Fatalf("Failed to get database stats: %v", err)
	}

	// Verify stats
	if tickCount, ok := stats["tick_count"].(int64); !ok || tickCount != 5 {
		t.Errorf("Expected tick_count to be 5, got %v", stats["tick_count"])
	}

	if txCount, ok := stats["transaction_count"].(int64); !ok || txCount != 0 {
		t.Errorf("Expected transaction_count to be 0, got %v", stats["transaction_count"])
	}

	// Database size should be > 0
	if dbSize, ok := stats["database_size_bytes"].(int64); !ok || dbSize <= 0 {
		t.Errorf("Expected database_size_bytes to be > 0, got %v", stats["database_size_bytes"])
	}
}

func TestSQLiteSink_Close(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite_sink_close_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create sink
	cfg := Config{
		Kind: "sqlite",
		DSN:  dbPath,
	}

	sink, err := NewSQLiteSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create SQLite sink: %v", err)
	}

	ctx := context.Background()

	// Test operations work before close
	if !sink.Health(ctx) {
		t.Error("Expected sink to be healthy before close")
	}

	// Close sink
	err = sink.Close()
	if err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Test operations fail after close
	if sink.Health(ctx) {
		t.Error("Expected sink to be unhealthy after close")
	}

	_, err = sink.GetLastTick(ctx)
	if err != ErrSinkClosed {
		t.Errorf("Expected ErrSinkClosed, got %v", err)
	}

	err = sink.PersistData(ctx, nil)
	if err != ErrSinkClosed {
		t.Errorf("Expected ErrSinkClosed, got %v", err)
	}

	err = sink.Flush(ctx)
	if err != ErrSinkClosed {
		t.Errorf("Expected ErrSinkClosed, got %v", err)
	}

	err = sink.InvalidateTick(ctx, 100)
	if err != ErrSinkClosed {
		t.Errorf("Expected ErrSinkClosed, got %v", err)
	}

	// Multiple closes should not error
	err = sink.Close()
	if err != nil {
		t.Errorf("Multiple closes should not error, got %v", err)
	}
}

func TestSQLiteSink_Recovery(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite_sink_recovery_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create first sink and add data
	cfg := Config{
		Kind: "sqlite",
		DSN:  dbPath,
	}

	sink1, err := NewSQLiteSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create first SQLite sink: %v", err)
	}

	ctx := context.Background()

	// Add test data
	tickRow := &models.TickRow{
		TickNumber:  500,
		TimestampUS: time.Now().UnixMicro(),
		VdfInput:   "input",
		VdfOutput:  "output",
		Version:    1,
	}

	data := []*parser.ParsedData{
		{Type: "tick", Data: tickRow},
	}

	err = sink1.PersistData(ctx, data)
	if err != nil {
		t.Fatalf("Failed to persist data in first sink: %v", err)
	}

	err = sink1.Close()
	if err != nil {
		t.Fatalf("Failed to close first sink: %v", err)
	}

	// Create second sink using the same database
	sink2, err := NewSQLiteSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create second SQLite sink: %v", err)
	}
	defer sink2.Close()

	// Check that last tick was recovered
	lastTick, err := sink2.GetLastTick(ctx)
	if err != nil {
		t.Fatalf("Failed to get last tick from recovered sink: %v", err)
	}

	if lastTick != 500 {
		t.Errorf("Expected recovered last tick to be 500, got %d", lastTick)
	}
}