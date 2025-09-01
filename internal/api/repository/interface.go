package repository

import (
	"context"
	"fmt"
	"time"
	
	"github.com/zerooo111/tick-streamer/internal/config"
)

// Data models
type TickData struct {
	TickNumber       uint64    `json:"tick_number"`
	Height           uint64    `json:"height"`
	BlockHash        string    `json:"block_hash"`
	ParentHash       string    `json:"parent_hash"`
	TxCount          uint32    `json:"tx_count"`
	PayloadSizeBytes uint64    `json:"payload_size_bytes"`
	SizeBytes        uint64    `json:"size_bytes"`
	Timestamp        uint64    `json:"timestamp"`
	ProcessedAt      time.Time `json:"processed_at"`
	ProposerID       string    `json:"proposer_id"`
	ProposerKey      string    `json:"proposer_key"`
	ChainID          string    `json:"chain_id"`
	Network          string    `json:"network"`
	Version          int32     `json:"version"`
	Transactions     []TransactionData `json:"transactions"`
}

type TransactionData struct {
	TickNumber          uint64    `json:"tick_number"`
	SequenceNumber      uint64    `json:"sequence_number"`
	TxHash              string    `json:"tx_hash"`
	TxID                string    `json:"tx_id"`
	Nonce               uint64    `json:"nonce"`
	Payload             string    `json:"payload"`
	Timestamp           uint64    `json:"timestamp"`
	PublicKey           string    `json:"public_key"`
	Signature           string    `json:"signature"`
	IngestionTimestamp  uint64    `json:"ingestion_timestamp"`
	ProcessedAt         time.Time `json:"processed_at"`
	PayloadSize         int32     `json:"payload_size"`
	PayloadType         string    `json:"payload_type"`
	Version             int32     `json:"version"`
}

type ChainStateData struct {
	ChainHeight      string                `json:"chain_height"`
	TotalTransactions string               `json:"total_transactions"`
	RecentTicks      []TickData            `json:"recent_ticks"`
	TxToTickSample   map[string]string     `json:"tx_to_tick_sample"`
}

// Repository defines the common interface for data access
type Repository interface {
	GetTick(ctx context.Context, tickNumber uint64) (*TickData, error)
	GetRecentTicks(ctx context.Context, limit int) ([]TickData, error)
	GetRecentTransactions(ctx context.Context, limit int) ([]TransactionData, error)
	GetChainState(ctx context.Context, tickLimit *int) (*ChainStateData, error)
	GetTransaction(ctx context.Context, txHash string) (*TransactionData, error)
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