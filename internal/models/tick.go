package models

import (
	"time"
	
	pb "github.com/zerooo111/tick-streamer/proto"
)

// TickRow represents a tick record optimized for database storage
// This demonstrates Go struct tags for JSON serialization and database mapping
type TickRow struct {
	// Primary identifier
	TickNumber uint64 `json:"tick_number" db:"tick_number"`
	
	// Timestamps - stored as Unix microseconds for precision
	TimestampUS int64 `json:"timestamp_us" db:"timestamp_us"`
	
	// VDF (Verifiable Delay Function) proof data
	VdfInput      string `json:"vdf_input" db:"vdf_input"`
	VdfOutput     string `json:"vdf_output" db:"vdf_output"`
	VdfIterations uint64 `json:"vdf_iterations" db:"vdf_iterations"`
	VdfProof      string `json:"vdf_proof" db:"vdf_proof"`
	
	// Blockchain state
	PreviousOutput       string `json:"previous_output" db:"previous_output"`
	TransactionBatchHash string `json:"transaction_batch_hash" db:"transaction_batch_hash"`
	
	// Metrics
	TransactionCount int32 `json:"transaction_count" db:"transaction_count"`
	
	// Processing metadata
	ProcessedAt  time.Time `json:"processed_at" db:"processed_at"`
	IngestionTS  int64     `json:"ingestion_ts" db:"ingestion_ts"`
	
	// Versioning for reorg handling (Phase 5)
	Version int32 `json:"version" db:"version"`
}

// NewTickRow creates a TickRow from a protobuf Tick message
// This teaches Go type conversion and struct initialization patterns
func NewTickRow(pbTick *pb.Tick) *TickRow {
	row := &TickRow{
		TickNumber:           pbTick.TickNumber,
		TimestampUS:          int64(pbTick.Timestamp),
		PreviousOutput:       pbTick.PreviousOutput,
		TransactionBatchHash: pbTick.TransactionBatchHash,
		TransactionCount:     int32(len(pbTick.Transactions)),
		ProcessedAt:          time.Now(),
		IngestionTS:          time.Now().UnixMicro(),
		Version:              1, // Default version for new records
	}
	
	// Handle VDF proof if present
	if pbTick.VdfProof != nil {
		row.VdfInput = pbTick.VdfProof.Input
		row.VdfOutput = pbTick.VdfProof.Output
		row.VdfIterations = pbTick.VdfProof.Iterations
		row.VdfProof = pbTick.VdfProof.Proof
	}
	
	return row
}

// GetHumanTime returns the tick timestamp as a human-readable time
// This demonstrates Go method definition on structs
func (t *TickRow) GetHumanTime() time.Time {
	return time.UnixMicro(t.TimestampUS)
}

// IsValid performs basic validation on the tick data
// This teaches Go validation patterns and error handling
func (t *TickRow) IsValid() bool {
	return t.TickNumber > 0 && 
		   t.TimestampUS > 0 && 
		   t.TransactionBatchHash != ""
}

// TableName returns the database table name for this model
// This is a common pattern in Go ORMs
func (t *TickRow) TableName() string {
	return "ticks"
}