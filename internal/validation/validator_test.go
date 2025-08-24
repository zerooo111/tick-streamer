package validation

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	pb "github.com/zerooo111/tick-streamer/proto"
)

// TestTickValidatorBasicValidation tests basic tick validation
func TestTickValidatorBasicValidation(t *testing.T) {
	validator := NewTickValidator()
	ctx := context.Background()

	t.Run("ValidTick", func(t *testing.T) {
		tick := createValidTick(100)
		
		result := validator.ValidateTick(ctx, tick)
		if !result.IsValid {
			t.Errorf("Expected valid tick, got errors: %v", result.Errors)
		}
	})

	t.Run("NilTick", func(t *testing.T) {
		result := validator.ValidateTick(ctx, nil)
		
		if result.IsValid {
			t.Error("Expected validation failure for nil tick")
		}
		
		if len(result.Errors) == 0 {
			t.Error("Expected validation errors for nil tick")
		}
	})

	t.Run("InvalidTickNumber", func(t *testing.T) {
		// Process a tick first to set up sequence validation
		validTick := createValidTick(100)
		validator.ValidateTick(ctx, validTick)
		
		// Now try a tick with a lower number (sequence error)
		invalidTick := createValidTick(50)
		result := validator.ValidateTick(ctx, invalidTick)
		
		if result.IsValid {
			t.Error("Expected validation failure for backward tick number")
		}
		
		hasSequenceError := false
		for _, err := range result.Errors {
			if err.Rule == "sequence" && err.Field == "tick_number" {
				hasSequenceError = true
				break
			}
		}
		if !hasSequenceError {
			t.Error("Expected sequence validation error")
		}
	})

	t.Run("InvalidTimestamp", func(t *testing.T) {
		tick := createValidTick(200)
		tick.Timestamp = 0 // Invalid timestamp
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for zero timestamp")
		}
	})

	t.Run("InvalidVDFProof", func(t *testing.T) {
		tick := createValidTick(300)
		tick.VdfProof = nil // Missing VDF proof
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for nil VDF proof")
		}
	})

	t.Run("InvalidHash", func(t *testing.T) {
		tick := createValidTick(400)
		tick.TransactionBatchHash = "invalid_hash" // Not 64-char hex
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for invalid hash")
		}
	})
}

// TestTransactionValidation tests transaction-specific validation
func TestTransactionValidation(t *testing.T) {
	validator := NewTickValidator()
	ctx := context.Background()

	t.Run("ValidTransactions", func(t *testing.T) {
		tick := createTickWithTransactions(500, 3)
		
		result := validator.ValidateTick(ctx, tick)
		if !result.IsValid {
			t.Errorf("Expected valid tick with transactions, got errors: %v", result.Errors)
		}
	})

	t.Run("TooManyTransactions", func(t *testing.T) {
		tick := createTickWithTransactions(600, 15000) // Exceeds limit
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for too many transactions")
		}
	})

	t.Run("NilTransaction", func(t *testing.T) {
		tick := createValidTick(700)
		tick.Transactions = []*pb.OrderedTransaction{
			nil, // Nil transaction
		}
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for nil transaction")
		}
	})

	t.Run("DuplicateSequenceNumbers", func(t *testing.T) {
		tick := createValidTick(800)
		tick.Transactions = []*pb.OrderedTransaction{
			createValidOrderedTransaction(1),
			createValidOrderedTransaction(1), // Duplicate sequence number
		}
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for duplicate sequence numbers")
		}
	})

	t.Run("InvalidTransactionHash", func(t *testing.T) {
		tick := createValidTick(900)
		tx := createValidOrderedTransaction(1)
		tx.TxHash = "invalid_hash" // Not 64-char hex
		tick.Transactions = []*pb.OrderedTransaction{tx}
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for invalid transaction hash")
		}
	})
}

// TestReorgDetector tests blockchain reorganization detection
func TestReorgDetector(t *testing.T) {
	detector := NewReorgDetector(10)

	t.Run("NoConflict", func(t *testing.T) {
		tick1 := createValidTick(100)
		
		reorg, err := detector.CheckForReorg(tick1)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if reorg != nil {
			t.Error("Expected no reorg for first tick")
		}
	})

	t.Run("SameTickNoConflict", func(t *testing.T) {
		tick1 := createValidTick(100)
		tick2 := createValidTick(100) // Same tick, same content
		
		// Add first tick
		detector.CheckForReorg(tick1)
		
		// Add same tick again
		reorg, err := detector.CheckForReorg(tick2)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if reorg != nil {
			t.Error("Expected no reorg for identical tick")
		}
	})

	t.Run("ReorgDetected", func(t *testing.T) {
		tick1 := createValidTick(200)
		tick2 := createValidTick(200)
		
		// Make tick2 different (conflict)
		tick2.VdfProof.Output = "0000000000000000111111111111111122222222222222223333333333333333" // 64 chars, different from original
		
		// Add first tick
		detector.CheckForReorg(tick1)
		
		// Add conflicting tick
		reorg, err := detector.CheckForReorg(tick2)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if reorg == nil {
			t.Error("Expected reorg detection for conflicting tick")
		}
		
		if reorg.TickNumber != 200 {
			t.Errorf("Expected reorg at tick 200, got %d", reorg.TickNumber)
		}
		
		if reorg.ConflictReason == "" {
			t.Error("Expected conflict reason to be set")
		}
	})

	t.Run("WindowMaintenance", func(t *testing.T) {
		smallDetector := NewReorgDetector(3) // Small window for testing
		
		// Add more ticks than window size
		for i := uint64(1); i <= 5; i++ {
			tick := createValidTick(i)
			smallDetector.CheckForReorg(tick)
		}
		
		stats := smallDetector.GetStats()
		if stats.CurrentWindow > stats.WindowSize {
			t.Errorf("Window size exceeded: current=%d, max=%d", 
				stats.CurrentWindow, stats.WindowSize)
		}
	})
}

// TestValidationStats tests validation statistics
func TestValidationStats(t *testing.T) {
	validator := NewTickValidator()
	ctx := context.Background()

	// Process some ticks
	for i := uint64(1); i <= 5; i++ {
		tick := createValidTick(i)
		validator.ValidateTick(ctx, tick)
	}

	stats := validator.GetStats()
	
	if stats.LastValidatedTick != 5 {
		t.Errorf("Expected last validated tick to be 5, got %d", stats.LastValidatedTick)
	}
	
	if stats.LastValidatedTime.IsZero() {
		t.Error("Expected last validated time to be set")
	}
}

// Helper functions for creating test data

func createValidTick(tickNumber uint64) *pb.Tick {
	return &pb.Tick{
		TickNumber: tickNumber,
		Timestamp:  uint64(time.Now().UnixMicro()),
		VdfProof: &pb.VdfProof{
			Input:      "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", // 64 chars
			Output:     "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321", // 64 chars
			Proof:      "proof_data_here",
			Iterations: 1000,
		},
		TransactionBatchHash: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890", // 64 chars
		PreviousOutput:       "9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba", // 64 chars
		Transactions:         []*pb.OrderedTransaction{},
	}
}

func createTickWithTransactions(tickNumber uint64, txCount int) *pb.Tick {
	tick := createValidTick(tickNumber)
	
	for i := 0; i < txCount; i++ {
		tx := createValidOrderedTransaction(uint64(i))
		tick.Transactions = append(tick.Transactions, tx)
	}
	
	return tick
}

func createValidOrderedTransaction(seqNum uint64) *pb.OrderedTransaction {
	// Create valid hex strings for signature (128 chars) and public key (64 chars)
	signature := "deadbeef12345678deadbeef12345678deadbeef12345678deadbeef12345678deadbeef12345678deadbeef12345678deadbeef12345678deadbeef12345678"
	publicKey := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	
	// Convert hex strings to bytes as expected by protobuf
	sigBytes, _ := hex.DecodeString(signature)
	pubKeyBytes, _ := hex.DecodeString(publicKey)
	
	return &pb.OrderedTransaction{
		Transaction: &pb.Transaction{
			TxId:      "tx_" + string(rune(seqNum)),
			Payload:   []byte("test_payload_data"),
			Signature: sigBytes,
			PublicKey: pubKeyBytes,
			Nonce:     seqNum,
			Timestamp: uint64(time.Now().UnixMicro()),
		},
		SequenceNumber:       seqNum,
		TxHash:              "1122334455667788aabbccddeeff00111122334455667788aabbccddeeff0011", // 64 chars
		IngestionTimestamp:  uint64(time.Now().UnixMicro()),
	}
}

// Test validation edge cases
func TestValidationEdgeCases(t *testing.T) {
	validator := NewTickValidator()
	ctx := context.Background()

	t.Run("FutureTimestamp", func(t *testing.T) {
		tick := createValidTick(1000)
		tick.Timestamp = uint64(time.Now().Add(10 * time.Minute).UnixMicro()) // Too far in future
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for future timestamp")
		}
	})

	t.Run("PastTimestamp", func(t *testing.T) {
		tick := createValidTick(1001)
		tick.Timestamp = uint64(time.Now().Add(-25 * time.Hour).UnixMicro()) // Too far in past
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for past timestamp")
		}
	})

	t.Run("LargeTickJump", func(t *testing.T) {
		// Process a normal tick first
		tick1 := createValidTick(1002)
		validator.ValidateTick(ctx, tick1)
		
		// Then try a huge jump
		tick2 := createValidTick(1002 + 5000) // Large jump
		result := validator.ValidateTick(ctx, tick2)
		
		if result.IsValid {
			t.Error("Expected validation failure for large tick jump")
		}
	})

	t.Run("ZeroVDFIterations", func(t *testing.T) {
		tick := createValidTick(1003)
		tick.VdfProof.Iterations = 0
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for zero VDF iterations")
		}
	})

	t.Run("EmptyTransactionID", func(t *testing.T) {
		tick := createValidTick(1004)
		tx := createValidOrderedTransaction(1)
		tx.Transaction.TxId = ""
		tick.Transactions = []*pb.OrderedTransaction{tx}
		
		result := validator.ValidateTick(ctx, tick)
		
		if result.IsValid {
			t.Error("Expected validation failure for empty transaction ID")
		}
	})
}

// Test reorg detection edge cases
func TestReorgDetectionEdgeCases(t *testing.T) {
	detector := NewReorgDetector(5)

	t.Run("NilTickError", func(t *testing.T) {
		_, err := detector.CheckForReorg(nil)
		
		if err == nil {
			t.Error("Expected error for nil tick")
		}
	})

	t.Run("MultipleConflictTypes", func(t *testing.T) {
		tick1 := createValidTick(2000)
		tick2 := createValidTick(2000)
		
		// Make multiple conflicts
		tick2.VdfProof.Output = "different_output"
		tick2.TransactionBatchHash = "different_batch"
		tick2.PreviousOutput = "different_prev"
		tick2.Transactions = []*pb.OrderedTransaction{createValidOrderedTransaction(1)} // Different tx count
		
		detector.CheckForReorg(tick1)
		reorg, err := detector.CheckForReorg(tick2)
		
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if reorg == nil {
			t.Error("Expected reorg detection")
		}
		
		// Should contain multiple conflict reasons
		reasons := reorg.ConflictReason
		if len(reasons) == 0 {
			t.Error("Expected conflict reasons to be populated")
		}
	})
}

// Benchmark validation performance
func BenchmarkTickValidation(b *testing.B) {
	validator := NewTickValidator()
	ctx := context.Background()
	tick := createTickWithTransactions(1, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validator.ValidateTick(ctx, tick)
	}
}

func BenchmarkReorgDetection(b *testing.B) {
	detector := NewReorgDetector(100)
	tick := createValidTick(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		detector.CheckForReorg(tick)
	}
}