package parser

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode"

	"github.com/zerooo111/tick-streamer/internal/models"
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
	
	// Check if we should skip heavy parsing and store raw data
	if skipParsing, ok := p.config.Settings["skip_parsing"].(bool); ok && skipParsing {
		// Fast path: Store raw protobuf data with minimal processing
		rawData := &ParsedData{
			Type: "raw_tick",
			Data: tick, // Store the raw protobuf message
			Metadata: map[string]interface{}{
				"tick_number":       tick.TickNumber,
				"transaction_count": len(tick.Transactions),
				"timestamp":        tick.Timestamp,
				"fast_path":        true,
			},
		}
		results = append(results, rawData)
		
		// Minimal logging for performance
		if len(tick.Transactions) > 0 {
			log.Printf("🚀 Fast-parsed tick #%d: %d txs", tick.TickNumber, len(tick.Transactions))
		}
		
		return results, nil
	}
	
	// Traditional parsing path with full data transformation
	// Only process ticks that have transactions
	if len(tick.Transactions) > 0 {
		// Create tick record
		tickRow := models.NewTickRow(tick)
		if tickRow != nil {
			tickData := &ParsedData{
				Type: "tick",
				Data: tickRow,
				Metadata: map[string]interface{}{
					"tick_number":       tick.TickNumber,
					"transaction_count": len(tick.Transactions),
				},
			}
			results = append(results, tickData)
		}
		
		// Create transaction records
		txRows := models.NewTxRowsFromTick(tick)
		if len(txRows) > 0 {
			for _, txRow := range txRows {
				txData := &ParsedData{
					Type: "transaction",
					Data: txRow,
					Metadata: map[string]interface{}{
						"tick_number":     tick.TickNumber,
						"sequence_number": txRow.SequenceNumber,
					},
				}
				results = append(results, txData)
			}
		}
		
		// Note: Detailed logging removed from sink pipeline
		// Logs should be handled separately, not sent through data persistence
	}
	
	// Only log ticks with transactions (we don't process empty ticks)
	if len(tick.Transactions) > 0 {
		log.Printf("📊 Parsed tick #%d: %d transactions, VDF proof: %t", 
			tick.TickNumber, len(tick.Transactions), tick.VdfProof != nil)
	}
	
	return results, nil
}

// GetParserInfo returns metadata about this parser
func (p *TickParser) GetParserInfo() ParserInfo {
	return ParserInfo{
		Name:        "TickParser",
		Version:     "1.0.0",
		Description: "Parses Continuum Tick protobuf messages into database models",
		DataTypes:   []string{"tick", "transaction", "log", "stats"},
	}
}

// Validate checks if the tick data is valid for parsing
func (p *TickParser) Validate(ctx context.Context, tick *pb.Tick) error {
	if tick == nil {
		return ErrInvalidTickData
	}
	
	if tick.TickNumber == 0 {
		return fmt.Errorf("tick number cannot be zero: %w", ErrInvalidTickData)
	}
	
	if tick.Timestamp == 0 {
		return fmt.Errorf("tick timestamp cannot be zero: %w", ErrInvalidTickData)
	}
	
	return nil
}

// generateDetailedLog creates a detailed log entry for ticks with transactions
// This contains all the detailed logging logic previously in streamer.processTick
func (p *TickParser) generateDetailedLog(tick *pb.Tick, tickRow *models.TickRow, txRows []*models.TxRow) *ParsedData {
	var logBuilder strings.Builder
	
	// Header
	logBuilder.WriteString(fmt.Sprintf("--- TICK #%d (WITH %d TRANSACTIONS) ---\n", tick.TickNumber, len(tick.Transactions)))
	logBuilder.WriteString(fmt.Sprintf("  Timestamp: %d (Unix microseconds)\n", tick.Timestamp))
	logBuilder.WriteString(fmt.Sprintf("  Human Time: %s\n", time.UnixMicro(int64(tick.Timestamp)).Format("2006-01-02 15:04:05.000")))
	logBuilder.WriteString(fmt.Sprintf("  Batch Hash: %s\n", tick.TransactionBatchHash))
	logBuilder.WriteString(fmt.Sprintf("  Previous Output: %s\n", truncateString(tick.PreviousOutput, 64)))
	
	// VDF proof details
	if tick.VdfProof != nil {
		logBuilder.WriteString("  VDF Proof:\n")
		logBuilder.WriteString(fmt.Sprintf("    Input:      %s\n", truncateString(tick.VdfProof.Input, 32)))
		logBuilder.WriteString(fmt.Sprintf("    Output:     %s\n", truncateString(tick.VdfProof.Output, 32)))
		logBuilder.WriteString(fmt.Sprintf("    Iterations: %d\n", tick.VdfProof.Iterations))
		logBuilder.WriteString(fmt.Sprintf("    Proof:      %s\n", truncateString(tick.VdfProof.Proof, 32)))
	}
	
	// Transaction details
	logBuilder.WriteString("  Transaction Details:\n")
	maxTxToShow := p.maxTxToLog
	if len(tick.Transactions) < maxTxToShow {
		maxTxToShow = len(tick.Transactions)
	}
	
	for i := 0; i < maxTxToShow; i++ {
		tx := tick.Transactions[i]
		logBuilder.WriteString(fmt.Sprintf("    TX[%d]:\n", i))
		logBuilder.WriteString(fmt.Sprintf("      Hash:      %s\n", tx.TxHash))
		logBuilder.WriteString(fmt.Sprintf("      Sequence:  %d\n", tx.SequenceNumber))
		logBuilder.WriteString(fmt.Sprintf("      TX ID:     %s\n", tx.Transaction.TxId))
		logBuilder.WriteString(fmt.Sprintf("      Nonce:     %d\n", tx.Transaction.Nonce))
		logBuilder.WriteString(fmt.Sprintf("      Payload:   %d bytes\n", len(tx.Transaction.Payload)))
		logBuilder.WriteString(fmt.Sprintf("      Payload (hex): %s\n", truncateString(formatPayload(tx.Transaction.Payload), 64)))
		logBuilder.WriteString(fmt.Sprintf("      PubKey:    %s\n", truncateString(formatBytes(tx.Transaction.PublicKey), 32)))
		logBuilder.WriteString(fmt.Sprintf("      Signature: %s\n", truncateString(formatBytes(tx.Transaction.Signature), 32)))
		logBuilder.WriteString(fmt.Sprintf("      Timestamp: %d\n", tx.Transaction.Timestamp))
		logBuilder.WriteString(fmt.Sprintf("      Ingestion: %d\n", tx.IngestionTimestamp))
	}
	
	if len(tick.Transactions) > maxTxToShow {
		logBuilder.WriteString(fmt.Sprintf("    ... and %d more transactions\n", len(tick.Transactions)-maxTxToShow))
	}
	
	logBuilder.WriteString(fmt.Sprintf("--- END TICK #%d ---", tick.TickNumber))
	
	return &ParsedData{
		Type: "log",
		Data: map[string]interface{}{
			"level":   "info",
			"message": logBuilder.String(),
			"tick_number": tick.TickNumber,
		},
		Metadata: map[string]interface{}{
			"log_type": "detailed_tick",
			"generated_at": time.Now(),
		},
	}
}

// Helper functions moved from streamer

// truncateString truncates a string to maxLen characters, adding "..." if truncated
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// formatPayload converts binary payload to human-readable format
// This function tries to detect if the payload contains readable text or should be shown as hex
func formatPayload(payload []byte) string {
	if len(payload) == 0 {
		return "<empty>"
	}
	
	// Check if payload contains mostly printable characters
	if isReadableText(payload) {
		// Show as string with escaped non-printable characters
		return fmt.Sprintf(`"%s"`, sanitizeString(string(payload)))
	}
	
	// Show as hex for binary data
	return hex.EncodeToString(payload)
}

// formatBytes converts byte arrays (like public keys, signatures) to hex string
func formatBytes(data []byte) string {
	if len(data) == 0 {
		return "<empty>"
	}
	return hex.EncodeToString(data)
}

// isReadableText checks if byte slice contains mostly printable ASCII characters
func isReadableText(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	
	printableCount := 0
	for _, b := range data {
		if unicode.IsPrint(rune(b)) || b == '\n' || b == '\r' || b == '\t' {
			printableCount++
		}
	}
	
	// Consider it readable text if more than 80% of characters are printable
	return float64(printableCount)/float64(len(data)) > 0.8
}

// sanitizeString replaces non-printable characters with escape sequences
func sanitizeString(s string) string {
	var builder strings.Builder
	for _, r := range s {
		if unicode.IsPrint(r) {
			builder.WriteRune(r)
		} else {
			switch r {
			case '\n':
				builder.WriteString("\\n")
			case '\r':
				builder.WriteString("\\r")
			case '\t':
				builder.WriteString("\\t")
			default:
				builder.WriteString(fmt.Sprintf("\\x%02x", r))
			}
		}
	}
	return builder.String()
}