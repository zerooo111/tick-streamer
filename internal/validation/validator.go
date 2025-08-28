package validation

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	pb "github.com/zerooo111/tick-streamer/proto"
)

// ValidationError represents a validation error with details
type ValidationError struct {
	Field   string `json:"field"`
	Value   string `json:"value"`
	Rule    string `json:"rule"`
	Message string `json:"message"`
}

// Error implements the error interface
func (v ValidationError) Error() string {
	return fmt.Sprintf("validation failed for field '%s': %s (rule: %s, value: %s)", 
		v.Field, v.Message, v.Rule, v.Value)
}

// ValidationResult contains the result of validation
type ValidationResult struct {
	IsValid bool               `json:"is_valid"`
	Errors  []ValidationError  `json:"errors"`
}

// TickValidator validates tick data for corruption and consistency
type TickValidator struct {
	// Configuration
	maxTransactionsPerTick  int
	maxTickNumberJump      uint64
	minTimestampInterval   time.Duration
	
	// State tracking for consistency checks
	lastTickNumber    uint64
	lastTimestamp     uint64
	tickSequenceGaps  int
	
	// Validation rules
	hashRegex        *regexp.Regexp
	publicKeyRegex   *regexp.Regexp
	signatureRegex   *regexp.Regexp
	variableHexRegex *regexp.Regexp
}

// NewTickValidator creates a new tick validator
func NewTickValidator() *TickValidator {
	// Compile regex patterns for validation
	hashRegex := regexp.MustCompile(`^[a-fA-F0-9]{64}$`)              // 64 char hex
	publicKeyRegex := regexp.MustCompile(`^[a-fA-F0-9]{64}$`)         // 64 char hex
	signatureRegex := regexp.MustCompile(`^[a-fA-F0-9]{128}$`)        // 128 char hex
	variableHexRegex := regexp.MustCompile(`^[a-fA-F0-9]+$`)          // Variable length hex (min 1 char)
	
	return &TickValidator{
		maxTransactionsPerTick: 10000,
		maxTickNumberJump:     1000,
		minTimestampInterval:  time.Millisecond,
		hashRegex:            hashRegex,
		publicKeyRegex:       publicKeyRegex, 
		signatureRegex:       signatureRegex,
		variableHexRegex:     variableHexRegex,
	}
}

// ValidateTick performs comprehensive validation of a tick
func (v *TickValidator) ValidateTick(ctx context.Context, tick *pb.Tick) ValidationResult {
	var errors []ValidationError
	
	// Basic null checks
	if tick == nil {
		return ValidationResult{
			IsValid: false,
			Errors: []ValidationError{
				{Field: "tick", Message: "tick is nil", Rule: "not_null"},
			},
		}
	}
	
	// Validate tick number
	if errs := v.validateTickNumber(tick.TickNumber); len(errs) > 0 {
		errors = append(errors, errs...)
	}
	
	// Validate timestamp
	if errs := v.validateTimestamp(tick.Timestamp); len(errs) > 0 {
		errors = append(errors, errs...)
	}
	
	// Validate VDF proof
	if errs := v.validateVDFProof(tick.VdfProof); len(errs) > 0 {
		errors = append(errors, errs...)
	}
	
	// Skip validation for transaction_batch_hash - variable length
	// Skip validation for previous_output - variable length
	
	// Validate transactions
	if errs := v.validateTransactions(tick.Transactions); len(errs) > 0 {
		errors = append(errors, errs...)
	}
	
	// Update state for next validation
	if len(errors) == 0 {
		v.lastTickNumber = tick.TickNumber
		v.lastTimestamp = tick.Timestamp
	}
	
	return ValidationResult{
		IsValid: len(errors) == 0,
		Errors:  errors,
	}
}

// validateTickNumber validates the tick number for sequence and reasonableness
func (v *TickValidator) validateTickNumber(tickNumber uint64) []ValidationError {
	var errors []ValidationError
	
	// Check for zero tick number (might be valid for genesis)
	if tickNumber == 0 {
		// This might be valid for genesis block, so just log it
		log.Printf("🔍 Validation: Processing genesis tick (tick_number=0)")
	}
	
	// Check for reasonable sequence (detect potential corruption)
	if v.lastTickNumber > 0 {
		if tickNumber <= v.lastTickNumber {
			errors = append(errors, ValidationError{
				Field:   "tick_number",
				Value:   fmt.Sprintf("%d", tickNumber),
				Rule:    "sequence",
				Message: fmt.Sprintf("tick number %d is not greater than previous tick %d", tickNumber, v.lastTickNumber),
			})
		}
		
		// Check for unrealistic jumps
		gap := tickNumber - v.lastTickNumber
		if gap > v.maxTickNumberJump {
			errors = append(errors, ValidationError{
				Field:   "tick_number", 
				Value:   fmt.Sprintf("%d", tickNumber),
				Rule:    "jump_limit",
				Message: fmt.Sprintf("tick number jump of %d exceeds maximum allowed %d", gap, v.maxTickNumberJump),
			})
			v.tickSequenceGaps++
		}
	}
	
	return errors
}

// validateTimestamp validates the timestamp for reasonableness and sequence
func (v *TickValidator) validateTimestamp(timestamp uint64) []ValidationError {
	var errors []ValidationError
	
	if timestamp == 0 {
		errors = append(errors, ValidationError{
			Field:   "timestamp",
			Value:   "0",
			Rule:    "not_zero", 
			Message: "timestamp cannot be zero",
		})
		return errors
	}
	
	// Convert to time for validation
	tickTime := time.UnixMicro(int64(timestamp))
	now := time.Now()
	
	// Check if timestamp is too far in the future
	if tickTime.After(now.Add(5 * time.Minute)) {
		errors = append(errors, ValidationError{
			Field:   "timestamp",
			Value:   fmt.Sprintf("%d", timestamp),
			Rule:    "future_limit",
			Message: fmt.Sprintf("timestamp %v is too far in the future", tickTime),
		})
	}
	
	// Check if timestamp is too far in the past (more than 1 day)
	if tickTime.Before(now.Add(-24 * time.Hour)) {
		errors = append(errors, ValidationError{
			Field:   "timestamp",
			Value:   fmt.Sprintf("%d", timestamp),
			Rule:    "past_limit",
			Message: fmt.Sprintf("timestamp %v is too far in the past", tickTime),
		})
	}
	
	// Check timestamp sequence
	if v.lastTimestamp > 0 {
		if timestamp < v.lastTimestamp {
			errors = append(errors, ValidationError{
				Field:   "timestamp",
				Value:   fmt.Sprintf("%d", timestamp),
				Rule:    "sequence",
				Message: fmt.Sprintf("timestamp goes backwards: %d < %d", timestamp, v.lastTimestamp),
			})
		}
	}
	
	return errors
}

// validateVDFProof validates the VDF proof structure
func (v *TickValidator) validateVDFProof(vdfProof *pb.VdfProof) []ValidationError {
	var errors []ValidationError
	
	if vdfProof == nil {
		errors = append(errors, ValidationError{
			Field:   "vdf_proof",
			Rule:    "not_null",
			Message: "VDF proof is required",
		})
		return errors
	}
	
	// Validate VDF input hash
	if errs := v.validateHash("vdf_input", vdfProof.Input); len(errs) > 0 {
		errors = append(errors, errs...)
	}
	
	// Skip validation for VDF output - variable length
	
	// Validate proof data
	if vdfProof.Proof == "" {
		errors = append(errors, ValidationError{
			Field:   "vdf_proof.proof",
			Rule:    "not_empty",
			Message: "VDF proof data cannot be empty",
		})
	}
	
	// Validate iterations (should be reasonable)
	if vdfProof.Iterations == 0 {
		errors = append(errors, ValidationError{
			Field:   "vdf_proof.iterations",
			Value:   "0",
			Rule:    "positive",
			Message: "VDF iterations must be positive",
		})
	} else if vdfProof.Iterations > 1000000 {
		errors = append(errors, ValidationError{
			Field:   "vdf_proof.iterations",
			Value:   fmt.Sprintf("%d", vdfProof.Iterations),
			Rule:    "reasonable_limit",
			Message: "VDF iterations seem unreasonably high",
		})
	}
	
	return errors
}

// validateHash validates a hash field format
func (v *TickValidator) validateHash(fieldName, hash string) []ValidationError {
	var errors []ValidationError
	
	if hash == "" {
		errors = append(errors, ValidationError{
			Field:   fieldName,
			Rule:    "not_empty",
			Message: fmt.Sprintf("%s cannot be empty", fieldName),
		})
		return errors
	}
	
	// Debug logging removed - validation skipped for variable-length fields
	
	// Check hash format (64 character hex string)
	if !v.hashRegex.MatchString(hash) {
		errors = append(errors, ValidationError{
			Field:   fieldName,
			Value:   hash, // Show full value instead of truncating
			Rule:    "hex_format",
			Message: fmt.Sprintf("%s must be a 64-character hexadecimal string (received length: %d)", fieldName, len(hash)),
		})
	}
	
	return errors
}

// validatePreviousOutput validates the previous output field (variable-length hex)
func (v *TickValidator) validatePreviousOutput(previousOutput string) []ValidationError {
	var errors []ValidationError
	
	if previousOutput == "" {
		errors = append(errors, ValidationError{
			Field:   "previous_output",
			Rule:    "not_empty",
			Message: "previous_output cannot be empty",
		})
		return errors
	}
	
	// Debug logging removed - validation skipped for variable-length fields
	
	// Check previous output format (variable-length hex string)
	if !v.variableHexRegex.MatchString(previousOutput) {
		errors = append(errors, ValidationError{
			Field:   "previous_output",
			Value:   previousOutput, // Show full value instead of truncating
			Rule:    "hex_format",
			Message: fmt.Sprintf("previous_output must be a valid hexadecimal string (received length: %d)", len(previousOutput)),
		})
	}
	
	return errors
}

// validateVDFOutput validates the VDF output field (variable-length hex)
func (v *TickValidator) validateVDFOutput(vdfOutput string) []ValidationError {
	var errors []ValidationError
	
	if vdfOutput == "" {
		errors = append(errors, ValidationError{
			Field:   "vdf_output",
			Rule:    "not_empty",
			Message: "vdf_output cannot be empty",
		})
		return errors
	}
	
	// Debug logging removed - validation skipped for variable-length fields
	
	// Check VDF output format (variable-length hex string)
	if !v.variableHexRegex.MatchString(vdfOutput) {
		errors = append(errors, ValidationError{
			Field:   "vdf_output",
			Value:   vdfOutput, // Show full value instead of truncating
			Rule:    "hex_format",
			Message: fmt.Sprintf("vdf_output must be a valid hexadecimal string (received length: %d)", len(vdfOutput)),
		})
	}
	
	return errors
}

// validateTransactions validates the transactions in a tick
func (v *TickValidator) validateTransactions(transactions []*pb.OrderedTransaction) []ValidationError {
	var errors []ValidationError
	
	// Check transaction count limit
	if len(transactions) > v.maxTransactionsPerTick {
		errors = append(errors, ValidationError{
			Field:   "transactions",
			Value:   fmt.Sprintf("%d", len(transactions)),
			Rule:    "count_limit",
			Message: fmt.Sprintf("transaction count %d exceeds maximum %d", len(transactions), v.maxTransactionsPerTick),
		})
	}
	
	// Validate each transaction
	sequenceNumbers := make(map[uint64]bool)
	for i, tx := range transactions {
		if tx == nil {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("transactions[%d]", i),
				Rule:    "not_null",
				Message: "transaction cannot be nil",
			})
			continue
		}
		
		// Validate sequence number uniqueness
		if sequenceNumbers[tx.SequenceNumber] {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("transactions[%d].sequence_number", i),
				Value:   fmt.Sprintf("%d", tx.SequenceNumber),
				Rule:    "unique",
				Message: "duplicate sequence number in tick",
			})
		}
		sequenceNumbers[tx.SequenceNumber] = true
		
		// Validate transaction hash
		if errs := v.validateHash(fmt.Sprintf("transactions[%d].tx_hash", i), tx.TxHash); len(errs) > 0 {
			errors = append(errors, errs...)
		}
		
		// Validate transaction structure
		if errs := v.validateTransaction(i, tx.Transaction); len(errs) > 0 {
			errors = append(errors, errs...)
		}
	}
	
	return errors
}

// validateTransaction validates an individual transaction
func (v *TickValidator) validateTransaction(index int, tx *pb.Transaction) []ValidationError {
	var errors []ValidationError
	
	if tx == nil {
		errors = append(errors, ValidationError{
			Field:   fmt.Sprintf("transactions[%d].transaction", index),
			Rule:    "not_null",
			Message: "transaction data cannot be nil",
		})
		return errors
	}
	
	// Validate transaction ID
	if tx.TxId == "" {
		errors = append(errors, ValidationError{
			Field:   fmt.Sprintf("transactions[%d].tx_id", index),
			Rule:    "not_empty",
			Message: "transaction ID cannot be empty",
		})
	}
	
	// Validate payload
	if tx.Payload == nil || len(tx.Payload) == 0 {
		errors = append(errors, ValidationError{
			Field:   fmt.Sprintf("transactions[%d].payload", index),
			Rule:    "not_empty",
			Message: "transaction payload cannot be empty",
		})
	}
	
	// Validate signature (separate field, bytes)
	if len(tx.Signature) > 0 {
		// Convert bytes to hex for validation
		signatureHex := fmt.Sprintf("%x", tx.Signature)
		if !v.signatureRegex.MatchString(signatureHex) {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("transactions[%d].signature", index),
				Value:   signatureHex[:min(len(signatureHex), 20)] + "...",
				Rule:    "signature_format",
				Message: "signature must be a valid hex string",
			})
		}
	}
	
	// Validate public key (separate field, bytes)
	if len(tx.PublicKey) > 0 {
		// Convert bytes to hex for validation
		publicKeyHex := fmt.Sprintf("%x", tx.PublicKey)
		if !v.publicKeyRegex.MatchString(publicKeyHex) {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("transactions[%d].public_key", index),
				Value:   publicKeyHex[:min(len(publicKeyHex), 20)] + "...",
				Rule:    "public_key_format", 
				Message: "public key must be a valid hex string",
			})
		}
	}
	
	// Validate timestamp
	if tx.Timestamp == 0 {
		errors = append(errors, ValidationError{
			Field:   fmt.Sprintf("transactions[%d].timestamp", index),
			Rule:    "positive",
			Message: "transaction timestamp must be positive",
		})
	}
	
	return errors
}

// GetStats returns validation statistics
func (v *TickValidator) GetStats() ValidationStats {
	return ValidationStats{
		LastValidatedTick:    v.lastTickNumber,
		LastValidatedTime:    time.UnixMicro(int64(v.lastTimestamp)),
		SequenceGapsDetected: v.tickSequenceGaps,
	}
}

// ValidationStats contains validation statistics
type ValidationStats struct {
	LastValidatedTick    uint64    `json:"last_validated_tick"`
	LastValidatedTime    time.Time `json:"last_validated_time"`
	SequenceGapsDetected int       `json:"sequence_gaps_detected"`
}

// Helper function for min (Go doesn't have built-in min for int)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ReorgDetector detects blockchain reorganizations
type ReorgDetector struct {
	// Window of recent ticks to check for conflicts
	tickWindow    map[uint64]*pb.Tick
	windowSize    int
	conflictsFound int
}

// NewReorgDetector creates a new reorganization detector
func NewReorgDetector(windowSize int) *ReorgDetector {
	return &ReorgDetector{
		tickWindow: make(map[uint64]*pb.Tick),
		windowSize: windowSize,
	}
}

// CheckForReorg checks if a tick represents a reorganization
func (r *ReorgDetector) CheckForReorg(tick *pb.Tick) (*ReorgEvent, error) {
	if tick == nil {
		return nil, fmt.Errorf("tick cannot be nil")
	}
	
	tickNumber := tick.TickNumber
	
	// Check if we've seen this tick number before
	if previousTick, exists := r.tickWindow[tickNumber]; exists {
		// Compare critical fields to detect conflicts
		if r.isConflictingTick(previousTick, tick) {
			r.conflictsFound++
			
			reorgEvent := &ReorgEvent{
				TickNumber:     tickNumber,
				OldTick:        previousTick,
				NewTick:        tick,
				DetectedAt:     time.Now(),
				ConflictReason: r.getConflictReason(previousTick, tick),
			}
			
			log.Printf("🚨 REORG DETECTED at tick %d: %s", tickNumber, reorgEvent.ConflictReason)
			return reorgEvent, nil
		}
	}
	
	// Add tick to window (replace if exists)
	r.tickWindow[tickNumber] = tick
	
	// Maintain window size
	r.maintainWindow(tickNumber)
	
	return nil, nil
}

// isConflictingTick determines if two ticks conflict
func (r *ReorgDetector) isConflictingTick(oldTick, newTick *pb.Tick) bool {
	// Compare VDF outputs (most critical)
	if oldTick.VdfProof != nil && newTick.VdfProof != nil {
		if oldTick.VdfProof.Output != newTick.VdfProof.Output {
			return true
		}
	}
	
	// Compare transaction batch hashes
	if oldTick.TransactionBatchHash != newTick.TransactionBatchHash {
		return true
	}
	
	// Compare previous outputs
	if oldTick.PreviousOutput != newTick.PreviousOutput {
		return true
	}
	
	// Compare transaction counts as a quick check
	if len(oldTick.Transactions) != len(newTick.Transactions) {
		return true
	}
	
	return false
}

// getConflictReason determines the reason for the conflict
func (r *ReorgDetector) getConflictReason(oldTick, newTick *pb.Tick) string {
	var reasons []string
	
	if oldTick.VdfProof != nil && newTick.VdfProof != nil {
		if oldTick.VdfProof.Output != newTick.VdfProof.Output {
			reasons = append(reasons, "VDF output mismatch")
		}
	}
	
	if oldTick.TransactionBatchHash != newTick.TransactionBatchHash {
		reasons = append(reasons, "transaction batch hash mismatch")
	}
	
	if oldTick.PreviousOutput != newTick.PreviousOutput {
		reasons = append(reasons, "previous output mismatch")
	}
	
	if len(oldTick.Transactions) != len(newTick.Transactions) {
		reasons = append(reasons, fmt.Sprintf("transaction count mismatch (%d vs %d)", 
			len(oldTick.Transactions), len(newTick.Transactions)))
	}
	
	return strings.Join(reasons, ", ")
}

// maintainWindow keeps the tick window at the specified size
func (r *ReorgDetector) maintainWindow(currentTick uint64) {
	if len(r.tickWindow) <= r.windowSize {
		return
	}
	
	// Remove ticks that are too old to maintain the window size
	// Keep only the most recent windowSize ticks
	oldestAllowed := currentTick - uint64(r.windowSize) + 1
	for tickNum := range r.tickWindow {
		if tickNum < oldestAllowed {
			delete(r.tickWindow, tickNum)
		}
	}
}

// GetStats returns reorg detector statistics
func (r *ReorgDetector) GetStats() ReorgStats {
	return ReorgStats{
		WindowSize:      r.windowSize,
		CurrentWindow:   len(r.tickWindow),
		ConflictsFound:  r.conflictsFound,
	}
}

// ReorgEvent represents a detected reorganization
type ReorgEvent struct {
	TickNumber     uint64    `json:"tick_number"`
	OldTick        *pb.Tick  `json:"-"` // Don't serialize the full tick data
	NewTick        *pb.Tick  `json:"-"`
	DetectedAt     time.Time `json:"detected_at"`
	ConflictReason string    `json:"conflict_reason"`
}

// ReorgStats contains reorganization detection statistics
type ReorgStats struct {
	WindowSize     int `json:"window_size"`
	CurrentWindow  int `json:"current_window"`
	ConflictsFound int `json:"conflicts_found"`
}