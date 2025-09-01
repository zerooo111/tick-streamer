// Package parser provides a pluggable data transformation system for the Continuum Streamer.
// It converts raw protobuf tick data into sink-compatible formats with support for
// different parsing strategies and performance optimizations.
//
// Key Features:
// - Pluggable architecture: Support for different parser implementations
// - Performance optimization: Optional raw protobuf passthrough
// - Error handling: Graceful handling of malformed data
// - Extensible: Easy to add new parser types
//
// Parser Types:
// - "tick": Standard tick parser with full data transformation
// - "raw": Minimal parser for ultra-low latency (future)
// - "custom": User-defined parsers (future)
package parser

import (
	"context"

	pb "github.com/zerooo111/tick-streamer/proto"
)

// ParsedData represents the result of parsing a protobuf message
// This is a generic container that can hold any type of parsed data
type ParsedData struct {
	// Type indicates what kind of data this contains
	Type string `json:"type"`
	
	// Data holds the actual parsed content (can be any type)
	Data interface{} `json:"data"`
	
	// Metadata holds additional parsing information
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// TickData represents a parsed tick with all fields from protobuf
type TickData struct {
	TickNumber           uint64    `json:"tick_number"`
	Timestamp            uint64    `json:"timestamp"`
	VDFInput             string    `json:"vdf_input"`
	VDFOutput            string    `json:"vdf_output"`
	VDFProof             string    `json:"vdf_proof"`
	VDFIterations        uint64    `json:"vdf_iterations"`
	TransactionBatchHash string    `json:"transaction_batch_hash"`
	PreviousOutput       string    `json:"previous_output"`
	TxCount              uint32    `json:"tx_count"`
}

// TransactionData represents a parsed transaction with all fields from protobuf
type TransactionData struct {
	TickNumber         uint64 `json:"tick_number"`
	SequenceNumber     uint64 `json:"sequence_number"`
	TxHash             string `json:"tx_hash"`
	TxID               string `json:"tx_id"`
	Nonce              uint64 `json:"nonce"`
	Payload            []byte `json:"payload"`
	Timestamp          uint64 `json:"timestamp"`
	PublicKey          []byte `json:"public_key"`
	Signature          []byte `json:"signature"`
	IngestionTimestamp uint64 `json:"ingestion_timestamp"`
	PayloadSize        int32  `json:"payload_size"`
}

// ParsedBundle represents a complete tick with parsed transactions
type ParsedBundle struct {
	Tick         TickData          `json:"tick"`
	Transactions []TransactionData `json:"transactions"`
}

// Parser defines the interface for parsing protobuf messages into sink-compatible format
// This enables a plugin architecture where different parsers can be plugged into the system
type Parser interface {
	// ParseTick transforms a protobuf Tick message into sink-compatible data
	// Returns a slice because a single tick might produce multiple data records
	ParseTick(ctx context.Context, tick *pb.Tick) ([]*ParsedData, error)
	
	// GetParserInfo returns information about this parser
	GetParserInfo() ParserInfo
	
	// Validate checks if the parser can handle the given tick format
	Validate(ctx context.Context, tick *pb.Tick) error
}

// ParserInfo provides metadata about a parser implementation
type ParserInfo struct {
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Description string   `json:"description"`
	DataTypes   []string `json:"data_types"` // Types of data this parser produces
}

// ParserConfig holds configuration for parser instances
type ParserConfig struct {
	// Parser type identifier
	Type string `json:"type" env:"PARSER_TYPE"`
	
	// Parser-specific settings
	Settings map[string]interface{} `json:"settings,omitempty"`
}

// NewParser creates a parser instance based on configuration
// This is the factory pattern for parser implementations
func NewParser(cfg ParserConfig) (Parser, error) {
	switch cfg.Type {
	case "tick":
		return NewTickParser(cfg)
	default:
		return nil, ErrInvalidParserType
	}
}