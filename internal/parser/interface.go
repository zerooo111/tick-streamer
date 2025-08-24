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