package sink

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
)

func TestLogFileSink_BasicOperations(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "logfile_sink_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create sink
	cfg := Config{
		Kind: "logfile",
		DSN:  tmpDir,
	}

	sink, err := NewLogFileSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create log file sink: %v", err)
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

	// Test flush
	err = sink.Flush(ctx)
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Check last tick updated
	lastTick, err = sink.GetLastTick(ctx)
	if err != nil {
		t.Fatalf("Failed to get last tick: %v", err)
	}
	if lastTick != 100 {
		t.Errorf("Expected last tick to be 100, got %d", lastTick)
	}

	// Verify files were created and contain data
	tickFile := filepath.Join(tmpDir, "ticks.jsonl")
	txFile := filepath.Join(tmpDir, "transactions.jsonl")

	tickData, err := os.ReadFile(tickFile)
	if err != nil {
		t.Fatalf("Failed to read tick file: %v", err)
	}
	
	if len(tickData) == 0 {
		t.Error("Tick file is empty")
	}

	txData, err := os.ReadFile(txFile)
	if err != nil {
		t.Fatalf("Failed to read transaction file: %v", err)
	}
	
	if len(txData) == 0 {
		t.Error("Transaction file is empty")
	}

	// Parse and verify JSON content
	var tickEntry map[string]interface{}
	lines := string(tickData)
	firstLine := lines[:len(lines)-1] // Remove trailing newline
	err = json.Unmarshal([]byte(firstLine), &tickEntry)
	if err != nil {
		t.Fatalf("Failed to parse tick JSON: %v", err)
	}

	if tickEntry["type"] != "tick" {
		t.Errorf("Expected type 'tick', got %v", tickEntry["type"])
	}
}

func TestLogFileSink_InvalidateTick(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "logfile_sink_invalidate_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create sink
	cfg := Config{
		Kind: "logfile",
		DSN:  tmpDir,
	}

	sink, err := NewLogFileSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create log file sink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()

	// Invalidate a tick
	err = sink.InvalidateTick(ctx, 100)
	if err != nil {
		t.Fatalf("Failed to invalidate tick: %v", err)
	}

	// Verify invalidation entries were written
	tickFile := filepath.Join(tmpDir, "ticks.jsonl")
	txFile := filepath.Join(tmpDir, "transactions.jsonl")

	tickData, err := os.ReadFile(tickFile)
	if err != nil {
		t.Fatalf("Failed to read tick file: %v", err)
	}

	txData, err := os.ReadFile(txFile)
	if err != nil {
		t.Fatalf("Failed to read transaction file: %v", err)
	}

	// Check that invalidation entries contain the correct type
	var tickEntry map[string]interface{}
	err = json.Unmarshal(tickData[:len(tickData)-1], &tickEntry) // Remove newline
	if err != nil {
		t.Fatalf("Failed to parse tick invalidation JSON: %v", err)
	}

	if tickEntry["type"] != "invalidation" {
		t.Errorf("Expected type 'invalidation', got %v", tickEntry["type"])
	}

	if tickEntry["tick_number"] != float64(100) { // JSON unmarshals numbers as float64
		t.Errorf("Expected tick_number 100, got %v", tickEntry["tick_number"])
	}

	// Same check for transaction file
	var txEntry map[string]interface{}
	err = json.Unmarshal(txData[:len(txData)-1], &txEntry) // Remove newline
	if err != nil {
		t.Fatalf("Failed to parse transaction invalidation JSON: %v", err)
	}

	if txEntry["type"] != "invalidation" {
		t.Errorf("Expected type 'invalidation', got %v", txEntry["type"])
	}
}

func TestLogFileSink_Stats(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "logfile_sink_stats_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create sink
	cfg := Config{
		Kind: "logfile",
		DSN:  tmpDir,
	}

	sink, err := NewLogFileSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create log file sink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()

	// Initial stats
	stats := sink.GetStats()
	if stats.TicksInserted != 0 {
		t.Errorf("Expected 0 ticks inserted, got %d", stats.TicksInserted)
	}
	if stats.TransactionsInserted != 0 {
		t.Errorf("Expected 0 transactions inserted, got %d", stats.TransactionsInserted)
	}
	if !stats.Connected {
		t.Error("Expected sink to be connected")
	}

	// Add some data
	tickRow := &models.TickRow{
		TickNumber:  200,
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

	// Check updated stats
	stats = sink.GetStats()
	if stats.TicksInserted != 1 {
		t.Errorf("Expected 1 tick inserted, got %d", stats.TicksInserted)
	}
	if stats.LastTickNumber != 200 {
		t.Errorf("Expected last tick number 200, got %d", stats.LastTickNumber)
	}

	// Test reset stats
	sink.ResetStats()
	stats = sink.GetStats()
	if stats.TicksInserted != 0 {
		t.Errorf("Expected 0 ticks after reset, got %d", stats.TicksInserted)
	}
	// Last tick number should not be reset
	if stats.LastTickNumber != 200 {
		t.Errorf("Expected last tick number to remain 200, got %d", stats.LastTickNumber)
	}
}

func TestLogFileSink_Close(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "logfile_sink_close_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create sink
	cfg := Config{
		Kind: "logfile",
		DSN:  tmpDir,
	}

	sink, err := NewLogFileSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create log file sink: %v", err)
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