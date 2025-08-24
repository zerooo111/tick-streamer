package parser

import "errors"

// Parser-specific errors
var (
	ErrInvalidParserType = errors.New("invalid parser type")
	ErrParsingFailed     = errors.New("parsing failed")
	ErrInvalidTickData   = errors.New("invalid tick data")
	ErrParserNotReady    = errors.New("parser not ready")
)