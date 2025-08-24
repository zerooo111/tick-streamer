package models

import (
	"fmt"
	"time"
	
	pb "github.com/zerooo111/tick-streamer/proto"
)

// TxRow represents a transaction record optimized for database storage
// This demonstrates Go's approach to denormalizing data for performance
type TxRow struct {
	// Composite key: tick_number + sequence_number uniquely identifies a transaction
	TickNumber     uint64 `json:"tick_number" db:"tick_number"`
	SequenceNumber uint64 `json:"sequence_number" db:"sequence_number"`
	
	// Transaction hash for quick lookups
	TxHash string `json:"tx_hash" db:"tx_hash"`
	
	// Core transaction data
	TxID      string `json:"tx_id" db:"tx_id"`
	Nonce     uint64 `json:"nonce" db:"nonce"`
	Payload   []byte `json:"payload" db:"payload"`
	Timestamp uint64 `json:"timestamp" db:"timestamp"`
	
	// Cryptographic data - stored as hex strings for database compatibility
	PublicKey string `json:"public_key" db:"public_key"`
	Signature string `json:"signature" db:"signature"`
	
	// Processing metadata
	IngestionTimestamp uint64    `json:"ingestion_timestamp" db:"ingestion_timestamp"`
	ProcessedAt        time.Time `json:"processed_at" db:"processed_at"`
	
	// Payload analysis (computed fields)
	PayloadSize   int32  `json:"payload_size" db:"payload_size"`
	PayloadType   string `json:"payload_type,omitempty" db:"payload_type"` // Future: detect payload type
	
	// Versioning for reorg handling (Phase 5)
	Version int32 `json:"version" db:"version"`
}

// NewTxRow creates a TxRow from a protobuf OrderedTransaction
// This teaches Go byte slice handling and hex encoding
func NewTxRow(tickNumber uint64, pbOrderedTx *pb.OrderedTransaction) *TxRow {
	if pbOrderedTx == nil || pbOrderedTx.Transaction == nil {
		return nil
	}
	
	tx := pbOrderedTx.Transaction
	
	row := &TxRow{
		TickNumber:         tickNumber,
		SequenceNumber:     pbOrderedTx.SequenceNumber,
		TxHash:            pbOrderedTx.TxHash,
		TxID:              tx.TxId,
		Nonce:             tx.Nonce,
		Payload:           tx.Payload,
		Timestamp:         tx.Timestamp,
		PublicKey:         fmt.Sprintf("%x", tx.PublicKey),  // Convert bytes to hex string
		Signature:         fmt.Sprintf("%x", tx.Signature),  // Convert bytes to hex string
		IngestionTimestamp: pbOrderedTx.IngestionTimestamp,
		ProcessedAt:       time.Now(),
		PayloadSize:       int32(len(tx.Payload)),
		Version:          1, // Default version for new records
	}
	
	// Analyze payload type (basic implementation)
	row.PayloadType = analyzePayloadType(tx.Payload)
	
	return row
}

// NewTxRowsFromTick creates multiple TxRows from a Tick's transactions
// This demonstrates Go slices and iteration patterns
func NewTxRowsFromTick(pbTick *pb.Tick) []*TxRow {
	if len(pbTick.Transactions) == 0 {
		return nil
	}
	
	rows := make([]*TxRow, 0, len(pbTick.Transactions))
	
	for _, orderedTx := range pbTick.Transactions {
		if row := NewTxRow(pbTick.TickNumber, orderedTx); row != nil {
			rows = append(rows, row)
		}
	}
	
	return rows
}

// GetHumanTime returns the transaction timestamp as a human-readable time
func (tx *TxRow) GetHumanTime() time.Time {
	return time.UnixMicro(int64(tx.Timestamp))
}

// GetIngestionTime returns the ingestion timestamp as a human-readable time
func (tx *TxRow) GetIngestionTime() time.Time {
	return time.UnixMicro(int64(tx.IngestionTimestamp))
}

// IsValid performs basic validation on the transaction data
func (tx *TxRow) IsValid() bool {
	return tx.TickNumber > 0 &&
		   tx.TxHash != "" &&
		   tx.TxID != "" &&
		   len(tx.PublicKey) > 0 &&
		   len(tx.Signature) > 0
}

// TableName returns the database table name for this model
func (tx *TxRow) TableName() string {
	return "transactions"
}

// analyzePayloadType provides basic payload type detection
// This is a placeholder for more sophisticated payload analysis in the future
func analyzePayloadType(payload []byte) string {
	if len(payload) == 0 {
		return "empty"
	}
	
	// Basic heuristics - in production, this would be much more sophisticated
	switch {
	case len(payload) < 32:
		return "small"
	case len(payload) < 256:
		return "medium"
	case len(payload) < 1024:
		return "large"
	default:
		return "xlarge"
	}
}