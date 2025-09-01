// Package models defines the core data structures for blockchain tick data.
// These models are optimized for TimescaleDB time-series storage and provide
// efficient representation of blockchain state transitions.
//
// TickRow represents a single blockchain tick (block) with all associated
// metadata needed for time-series analysis and blockchain state tracking.
package models

import (
	"time"
	
	pb "github.com/zerooo111/tick-streamer/proto"
)

// TickRow represents a tick record optimized for database storage
// This demonstrates Go struct tags for JSON serialization and database mapping
type TickRow struct {
	// Primary identifiers
	TickNumber uint64 `json:"tick_number" db:"tick_number"`
	Height     uint64 `json:"height" db:"height"`
	
	// Block identifiers
	BlockHash  string `json:"block_hash" db:"block_hash"`
	ParentHash string `json:"parent_hash" db:"parent_hash"`
	
	// Block metrics
	TxCount          uint32 `json:"tx_count" db:"tx_count"`
	PayloadSizeBytes uint64 `json:"payload_size_bytes" db:"payload_size_bytes"`
	SizeBytes        uint64 `json:"size_bytes" db:"size_bytes"`
	Timestamp        uint64 `json:"timestamp" db:"timestamp"`
	
	// Processing metadata
	ProcessedAt time.Time `json:"processed_at" db:"processed_at"`
	
	// Block proposer information
	ProposerID  string `json:"proposer_id" db:"proposer_id"`
	ProposerKey string `json:"proposer_key" db:"proposer_key"`
	
	// Network information
	ChainID string `json:"chain_id" db:"chain_id"`
	Network string `json:"network" db:"network"`
	
	// Versioning for reorg handling
	Version int32 `json:"version" db:"version"`
}

// NewTickRow creates a TickRow from a protobuf Tick message
// This teaches Go type conversion and struct initialization patterns
func NewTickRow(pbTick *pb.Tick) *TickRow {
	row := &TickRow{
		TickNumber:       pbTick.TickNumber,
		Height:           pbTick.TickNumber, // Use tick_number as height for now
		BlockHash:        pbTick.TransactionBatchHash, // Use transaction batch hash as block hash
		ParentHash:       pbTick.PreviousOutput,
		TxCount:          uint32(len(pbTick.Transactions)),
		PayloadSizeBytes: 0, // Calculate from transactions if needed
		SizeBytes:        0, // Calculate total size if needed
		Timestamp:        pbTick.Timestamp,
		ProcessedAt:      time.Now(),
		ProposerID:       "", // Set if available in protobuf
		ProposerKey:      "", // Set if available in protobuf
		ChainID:          "mainnet", // Default chain ID
		Network:          "qubic", // Default network
		Version:          1, // Default version for new records
	}
	
	return row
}

// GetHumanTime returns the tick timestamp as a human-readable time
// This demonstrates Go method definition on structs
func (t *TickRow) GetHumanTime() time.Time {
	return time.UnixMicro(int64(t.Timestamp))
}

// IsValid performs basic validation on the tick data
// This teaches Go validation patterns and error handling
func (t *TickRow) IsValid() bool {
	return t.TickNumber > 0 && 
		   t.Timestamp > 0 && 
		   t.BlockHash != ""
}

// TableName returns the database table name for this model
// This is a common pattern in Go ORMs
func (t *TickRow) TableName() string {
	return "ticks"
}