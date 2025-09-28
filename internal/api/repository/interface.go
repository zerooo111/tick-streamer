package repository

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/zerooo111/tick-streamer/internal/config"
)

// Data models
type TickData struct {
	TickNumber           uint64    `json:"tick_number"`
	Timestamp            uint64    `json:"timestamp"`
	VDFInput             string    `json:"vdf_input,omitempty"`
	VDFOutput            string    `json:"vdf_output,omitempty"`
	VDFProof             string    `json:"vdf_proof,omitempty"`
	VDFIterations        uint64    `json:"vdf_iterations,omitempty"`
	TransactionBatchHash string    `json:"transaction_batch_hash,omitempty"`
	PreviousOutput       string    `json:"previous_output,omitempty"`
	TxCount              uint32    `json:"tx_count"`
	ProcessedAt          time.Time `json:"processed_at"`
	Version              int32     `json:"version"`
	Transactions         []TransactionData `json:"transactions"`
}

type TransactionData struct {
	TickNumber          uint64      `json:"tick_number"`
	SequenceNumber      uint64      `json:"sequence_number"`
	TxHash              string      `json:"tx_hash"`
	TxID                string      `json:"tx_id"`
	Nonce               uint64      `json:"nonce"`
	Payload             string      `json:"payload"` // hex-encoded payload
	PayloadDecoded      interface{} `json:"payload_decoded,omitempty"` // human-readable decoded payload
	Timestamp           uint64      `json:"timestamp"`
	PublicKey           string      `json:"public_key"` // hex-encoded public key
	Signature           string      `json:"signature"` // hex-encoded signature
	IngestionTimestamp  uint64      `json:"ingestion_timestamp"`
	ProcessedAt         time.Time   `json:"processed_at"`
	PayloadSize         int32       `json:"payload_size"`
	PayloadType         string      `json:"payload_type,omitempty"`
	Version             int32       `json:"version"`
}

type RecentTransactionData struct {
	SequenceNumber      uint64      `json:"sequence_number"`
	TxHash              string      `json:"tx_hash"`
	TickNumber          uint64      `json:"tick_number"`
	TxID                string      `json:"tx_id"`
	Timestamp           uint64      `json:"timestamp"` // raw timestamp from database
}

type ChainStateData struct {
	ChainHeight      string                `json:"chain_height"`
	TotalTransactions string               `json:"total_transactions"`
	RecentTicks      []TickData            `json:"recent_ticks"`
	TxToTickSample   map[string]string     `json:"tx_to_tick_sample"`
}

type OHLCCandle struct {
	Timestamp time.Time `json:"t"` // timestamp
	Open      float64   `json:"o"` // open price
	High      float64   `json:"h"` // high price
	Low       float64   `json:"l"` // low price
	Close     float64   `json:"c"` // close price
}

// DecodePayload decodes a hex-encoded payload into human-readable format
func DecodePayload(hexPayload string) interface{} {
	if hexPayload == "" {
		return nil
	}
	
	// Decode hex to bytes
	payloadBytes, err := hex.DecodeString(hexPayload)
	if err != nil {
		return map[string]interface{}{
			"error": "invalid hex encoding",
			"raw":   hexPayload,
		}
	}
	
	payloadStr := string(payloadBytes)
	
	// Check if it's FRM protocol format
	if strings.HasPrefix(payloadStr, "FRM_v1.0:") {
		jsonPart := strings.TrimPrefix(payloadStr, "FRM_v1.0:")
		
		var parsedJSON interface{}
		if err := json.Unmarshal([]byte(jsonPart), &parsedJSON); err != nil {
			return map[string]interface{}{
				"protocol": "FRM_v1.0",
				"error":    "invalid JSON in payload",
				"raw_json": jsonPart,
			}
		}
		
		return map[string]interface{}{
			"protocol": "FRM_v1.0",
			"data":     parsedJSON,
		}
	}
	
	// Try to parse as JSON directly
	var parsedJSON interface{}
	if err := json.Unmarshal(payloadBytes, &parsedJSON); err == nil {
		return parsedJSON
	}
	
	// Return as plain text if not JSON
	return map[string]interface{}{
		"type": "text",
		"data": payloadStr,
	}
}

// Repository defines the common interface for data access
type Repository interface {
	GetTick(ctx context.Context, tickNumber uint64) (*TickData, error)
	GetRecentTicks(ctx context.Context, limit int) ([]TickData, error)
	GetRecentTransactions(ctx context.Context, limit int) ([]RecentTransactionData, error)
	GetChainState(ctx context.Context, tickLimit *int) (*ChainStateData, error)
	GetTransaction(ctx context.Context, txHash string) (*TransactionData, error)
	GetMarketCandles(ctx context.Context, marketID string, timeframe string, from, to time.Time) ([]OHLCCandle, error)
	Close() error
}

// NewRepository creates a repository instance based on the SINK_KIND configuration
func NewRepository(cfg *config.Config) (Repository, error) {
	sinkKind := cfg.SinkKind
	if sinkKind == "" {
		sinkKind = "timescaledb" // Default to TimescaleDB
	}
	
	switch sinkKind {
	case "timescaledb", "tsdb", "postgres":
		repo, err := NewTimescaleDBRepository(cfg)
		if err != nil {
			return nil, err
		}
		// Return as Repository interface
		return repo, nil
	default:
		return nil, fmt.Errorf("unsupported sink kind: %s", sinkKind)
	}
}