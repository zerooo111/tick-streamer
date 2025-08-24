package sink

import "errors"

// Common sink errors
// This demonstrates Go's error handling patterns with predefined errors
var (
	// Configuration errors
	ErrInvalidSinkKind     = errors.New("invalid sink kind specified")
	ErrSinkNotImplemented  = errors.New("sink type not yet implemented")
	ErrInvalidDSN         = errors.New("invalid database connection string")
	
	// Connection errors
	ErrSinkDisconnected   = errors.New("sink is disconnected")
	ErrConnectionFailed   = errors.New("failed to connect to database")
	ErrConnectionTimeout  = errors.New("database connection timeout")
	
	// Operation errors
	ErrBatchTooLarge      = errors.New("batch size exceeds maximum allowed")
	ErrInvalidTickNumber  = errors.New("invalid tick number")
	ErrDuplicateData      = errors.New("duplicate data detected")
	ErrFlushFailed        = errors.New("failed to flush data to storage")
	ErrSinkClosed         = errors.New("sink is closed")
	
	// Data validation errors
	ErrInvalidTickData    = errors.New("invalid tick data")
	ErrInvalidTxData      = errors.New("invalid transaction data")
	ErrMissingRequiredField = errors.New("required field is missing")
)