package sink

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
)

func TestClickHouseSinkIntegration(t *testing.T) {
	// Skip this test if CLICKHOUSE_PASSWORD is not set
	if os.Getenv("CLICKHOUSE_PASSWORD") == "" {
		t.Skip("CLICKHOUSE_PASSWORD not set, skipping integration test")
	}

	// Set up test configuration
	os.Setenv("CLICKHOUSE_HOST", "z9jq89387u.ap-south-1.aws.clickhouse.cloud")
	os.Setenv("CLICKHOUSE_PORT", "9440")
	os.Setenv("CLICKHOUSE_DATABASE", "default")
	os.Setenv("CLICKHOUSE_USERNAME", "default")
	
	// Create sink configuration
	cfg := Config{
		Kind:         "clickhouse",
		MaxBatchSize: 10,
		BatchTimeout: 1000,
	}

	// Create ClickHouse sink
	sink, err := NewClickHouseSink(cfg)
	if err != nil {
		t.Fatalf("Failed to create ClickHouse sink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()

	// Test health check
	if !sink.Health(ctx) {
		t.Fatal("Sink health check failed")
	}

	// Create test data - a tick with transactions
	tickRow := &models.TickRow{
		TickNumber:           uint64(time.Now().Unix()), // Use timestamp as unique tick number
		TimestampUS:          time.Now().UnixMicro(),
		VdfInput:            "test_input",
		VdfOutput:           "test_output",
		VdfIterations:       1000,
		VdfProof:            "test_proof",
		PreviousOutput:      "previous_output",
		TransactionBatchHash: "batch_hash",
		TransactionCount:    2,
		ProcessedAt:         time.Now(),
		IngestionTS:         time.Now().UnixMicro(),
		Version:             1,
	}

	// Create test transactions
	txRow1 := &models.TxRow{
		TickNumber:         tickRow.TickNumber,
		SequenceNumber:     1,
		TxHash:            "tx_hash_1",
		TxID:              "tx_id_1",
		Nonce:             1,
		Payload:           []byte("test_payload_1"),
		Timestamp:         uint64(time.Now().UnixMicro()),
		PublicKey:         "public_key_1",
		Signature:         "signature_1",
		IngestionTimestamp: uint64(time.Now().UnixMicro()),
		ProcessedAt:       time.Now(),
		PayloadSize:       14,
		PayloadType:       "test",
		Version:          1,
	}

	txRow2 := &models.TxRow{
		TickNumber:         tickRow.TickNumber,
		SequenceNumber:     2,
		TxHash:            "tx_hash_2",
		TxID:              "tx_id_2",
		Nonce:             2,
		Payload:           []byte("test_payload_2"),
		Timestamp:         uint64(time.Now().UnixMicro()),
		PublicKey:         "public_key_2",
		Signature:         "signature_2",
		IngestionTimestamp: uint64(time.Now().UnixMicro()),
		ProcessedAt:       time.Now(),
		PayloadSize:       14,
		PayloadType:       "test",
		Version:          1,
	}

	// Create parsed data (tick with transactions - should be stored)
	parsedData := []*parser.ParsedData{
		{
			Type: "tick",
			Data: tickRow,
		},
		{
			Type: "transaction",
			Data: txRow1,
		},
		{
			Type: "transaction",
			Data: txRow2,
		},
	}

	// Test PersistData - this should store both tick and transactions
	err = sink.PersistData(ctx, parsedData)
	if err != nil {
		t.Fatalf("Failed to persist data: %v", err)
	}

	// Test flush
	err = sink.Flush(ctx)
	if err != nil {
		t.Fatalf("Failed to flush data: %v", err)
	}

	// Test GetLastTick
	lastTick, err := sink.GetLastTick(ctx)
	if err != nil {
		t.Fatalf("Failed to get last tick: %v", err)
	}

	if lastTick != tickRow.TickNumber {
		t.Errorf("Expected last tick %d, got %d", tickRow.TickNumber, lastTick)
	}

	// Create test data for tick without transactions (should be filtered out)
	emptyTickRow := &models.TickRow{
		TickNumber:           uint64(time.Now().Unix() + 1), // Different tick number
		TimestampUS:          time.Now().UnixMicro(),
		VdfInput:            "empty_tick_input",
		VdfOutput:           "empty_tick_output",
		VdfIterations:       1000,
		VdfProof:            "empty_tick_proof",
		PreviousOutput:      "previous_output",
		TransactionBatchHash: "batch_hash",
		TransactionCount:    0, // No transactions
		ProcessedAt:         time.Now(),
		IngestionTS:         time.Now().UnixMicro(),
		Version:             1,
	}

	// Create parsed data for empty tick (no transactions)
	emptyParsedData := []*parser.ParsedData{
		{
			Type: "tick",
			Data: emptyTickRow,
		},
	}

	// Test PersistData with empty tick - this should NOT store the tick
	err = sink.PersistData(ctx, emptyParsedData)
	if err != nil {
		t.Fatalf("Failed to persist empty tick data: %v", err)
	}

	err = sink.Flush(ctx)
	if err != nil {
		t.Fatalf("Failed to flush empty tick data: %v", err)
	}

	// The last tick should still be the previous one (empty tick was filtered out)
	lastTickAfterEmpty, err := sink.GetLastTick(ctx)
	if err != nil {
		t.Fatalf("Failed to get last tick after empty: %v", err)
	}

	if lastTickAfterEmpty != tickRow.TickNumber {
		t.Errorf("Expected last tick to remain %d after filtering empty tick, got %d", 
			tickRow.TickNumber, lastTickAfterEmpty)
	}

	t.Logf("Integration test passed! Last tick: %d", lastTick)
}