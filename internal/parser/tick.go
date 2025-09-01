package parser

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
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

// ParseTick transforms a protobuf Tick into ParsedData records
// This contains all the parsing logic previously in streamer.processTick
func (p *TickParser) ParseTick(ctx context.Context, tick *pb.Tick) ([]*ParsedData, error) {
	if tick == nil {
		return nil, ErrInvalidTickData
	}
	
	var results []*ParsedData
	
	// ALWAYS use ultra-fast raw storage - no config needed
	// Pre-serialize once and bundle everything for maximum performance
	tickBytes, err := proto.Marshal(tick)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tick %d: %w", tick.TickNumber, err)
	}
	
	// Pre-serialize all transactions - extract the inner Transaction from OrderedTransaction
	transactions := make([]RawTransactionData, len(tick.Transactions))
	for i, orderedTx := range tick.Transactions {
		// Extract the actual Transaction from OrderedTransaction
		if orderedTx.Transaction == nil {
			return nil, fmt.Errorf("ordered transaction %d in tick %d has nil transaction", i, tick.TickNumber)
		}
		
		txBytes, err := proto.Marshal(orderedTx.Transaction)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal transaction %d in tick %d: %w", i, tick.TickNumber, err)
		}
		
		transactions[i] = RawTransactionData{
			SequenceNumber: orderedTx.SequenceNumber,
			TxBytes:        txBytes,
		}
	}
	
	// Create single bundle with all pre-serialized data
	bundle := &RawTickBundle{
		TickNumber:       tick.TickNumber,
		TimestampUS:      int64(tick.Timestamp),
		TransactionCount: int32(len(tick.Transactions)),
		TickBytes:        tickBytes,
		Transactions:     transactions,
	}
	
	rawBundleData := &ParsedData{
		Type: "raw_bundle",
		Data: bundle,
		Metadata: map[string]interface{}{
			"tick_number":       tick.TickNumber,
			"transaction_count": len(tick.Transactions),
			"timestamp":        tick.Timestamp,
			"ultra_fast_path":  true,
		},
	}
	results = append(results, rawBundleData)
	
	return results, nil
}

// GetParserInfo returns metadata about this parser
func (p *TickParser) GetParserInfo() ParserInfo {
	return ParserInfo{
		Name:        "RawTickParser",
		Version:     "3.0",
		Description: "Ultra-fast raw protobuf storage parser for maximum performance",
		DataTypes:   []string{"raw_bundle"},
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