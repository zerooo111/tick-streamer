package parser

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	pb "github.com/zerooo111/tick-streamer/proto"
)

// TickParser handles parsing of protobuf Tick messages into TickRow and TxRow models
// This parser is responsible for all the data transformation logic previously in streamer
type TickParser struct {
	config ParserConfig
	
	// Parser settings
	enableDetailedLogging bool
	maxTxToLog           int
}

// NewTickParser creates a new tick parser with the given configuration
func NewTickParser(cfg ParserConfig) (*TickParser, error) {
	parser := &TickParser{
		config:                cfg,
		enableDetailedLogging: true, // Default to detailed logging
		maxTxToLog:           3,     // Default to logging first 3 transactions
	}
	
	// Apply configuration settings
	if settings := cfg.Settings; settings != nil {
		if val, ok := settings["detailed_logging"].(bool); ok {
			parser.enableDetailedLogging = val
		}
		if val, ok := settings["max_tx_to_log"].(int); ok {
			parser.maxTxToLog = val
		}
	}
	
	return parser, nil
}

// ParseTick transforms a protobuf Tick into ParsedData records with full parsing
func (p *TickParser) ParseTick(ctx context.Context, tick *pb.Tick) ([]*ParsedData, error) {
	if tick == nil {
		return nil, ErrInvalidTickData
	}
	
	// Parse tick data
	tickData := TickData{
		TickNumber:           tick.TickNumber,
		Timestamp:           tick.Timestamp,
		TransactionBatchHash: tick.TransactionBatchHash,
		PreviousOutput:      tick.PreviousOutput,
		TxCount:             uint32(len(tick.Transactions)),
	}
	
	// Parse VDF proof if present
	if tick.VdfProof != nil {
		tickData.VDFInput = tick.VdfProof.Input
		tickData.VDFOutput = tick.VdfProof.Output
		tickData.VDFProof = tick.VdfProof.Proof
		tickData.VDFIterations = tick.VdfProof.Iterations
	}
	
	// Parse transactions
	transactions := make([]TransactionData, 0, len(tick.Transactions))
	for _, orderedTx := range tick.Transactions {
		if orderedTx.Transaction == nil {
			continue // Skip nil transactions
		}
		
		tx := orderedTx.Transaction
		
		// Calculate transaction hash from OrderedTransaction data if provided, otherwise from Transaction
		txHash := orderedTx.TxHash
		if txHash == "" {
			// Generate hash from transaction data
			hashData := fmt.Sprintf("%s_%d_%d_%s", tx.TxId, tx.Nonce, tx.Timestamp, hex.EncodeToString(tx.Payload))
			hashBytes := sha256.Sum256([]byte(hashData))
			txHash = hex.EncodeToString(hashBytes[:])
		}
		
		txData := TransactionData{
			TickNumber:         tick.TickNumber,
			SequenceNumber:     orderedTx.SequenceNumber,
			TxHash:             txHash,
			TxID:               tx.TxId,
			Nonce:              tx.Nonce,
			Payload:            tx.Payload,
			Timestamp:          tx.Timestamp,
			PublicKey:          tx.PublicKey,
			Signature:          tx.Signature,
			IngestionTimestamp: orderedTx.IngestionTimestamp,
			PayloadSize:        int32(len(tx.Payload)),
		}
		
		transactions = append(transactions, txData)
	}
	
	// Create parsed bundle
	bundle := &ParsedBundle{
		Tick:         tickData,
		Transactions: transactions,
	}
	
	parsedData := &ParsedData{
		Type: "parsed_bundle",
		Data: bundle,
		Metadata: map[string]interface{}{
			"tick_number":       tick.TickNumber,
			"transaction_count": len(transactions),
			"timestamp":         tick.Timestamp,
			"parsed":            true,
		},
	}
	
	return []*ParsedData{parsedData}, nil
}

// GetParserInfo returns metadata about this parser
func (p *TickParser) GetParserInfo() ParserInfo {
	return ParserInfo{
		Name:        "TickParser",
		Version:     "4.0",
		Description: "Full-featured tick parser that extracts all fields for querying",
		DataTypes:   []string{"parsed_bundle"},
	}
}

// Validate checks if this parser can handle the given tick
func (p *TickParser) Validate(ctx context.Context, tick *pb.Tick) error {
	if tick == nil {
		return fmt.Errorf("tick cannot be nil")
	}
	
	if tick.TickNumber == 0 {
		return fmt.Errorf("tick number cannot be zero")
	}
	
	return nil
}