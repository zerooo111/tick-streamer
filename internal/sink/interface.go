package sink

import (
	"context"
	
	"github.com/zerooo111/tick-streamer/internal/parser"
)

// Sink defines the interface for database persistence operations
// Clean, simple interface that works with parsed data from any parser plugin
type Sink interface {
	// PersistData handles generic parsed data from any parser
	// This is the primary method that supports the plugin architecture
	PersistData(ctx context.Context, data []*parser.ParsedData) error
	
	// InvalidateTick marks all data for a specific tick as invalid (for reorgs)
	// This sets version to -1 or deletes records, depending on implementation
	InvalidateTick(ctx context.Context, tickNumber uint64) error
	
	// Flush ensures all pending writes are committed to storage
	// This is called before updating checkpoints to ensure durability
	Flush(ctx context.Context) error
	
	// Close gracefully shuts down the sink and releases resources
	Close() error
	
	// Health returns true if the sink is operational
	Health(ctx context.Context) bool
	
	// GetLastTick returns the highest tick number successfully persisted
	// Used for checkpoint recovery and monitoring
	GetLastTick(ctx context.Context) (uint64, error)
}

// SinkStats provides metrics about sink operations
// This will be used for monitoring and observability
type SinkStats struct {
	// Operation counts
	TicksInserted        uint64 `json:"ticks_inserted"`
	TransactionsInserted uint64 `json:"transactions_inserted"`
	FlushCount          uint64 `json:"flush_count"`
	ErrorCount          uint64 `json:"error_count"`
	
	// Performance metrics
	LastFlushDuration    int64   `json:"last_flush_duration_ms"`
	AverageFlushDuration float64 `json:"avg_flush_duration_ms"`
	
	// Current state
	LastTickNumber    uint64 `json:"last_tick_number"`
	PendingBatches    int    `json:"pending_batches"`
	Connected         bool   `json:"connected"`
}

// StatsProvider extends Sink with metrics capabilities
// This demonstrates Go's interface composition pattern
type StatsProvider interface {
	Sink
	
	// GetStats returns current operational statistics
	GetStats() SinkStats
	
	// ResetStats clears counters (useful for monitoring)
	ResetStats()
}

// Config holds sink configuration parameters
// This demonstrates Go's approach to configuration structs
type Config struct {
	// Connection settings - supports "timescaledb", "debug"
	Kind string `json:"kind" env:"SINK_KIND"`
	
	// Performance tuning
	MaxBatchSize     int `json:"max_batch_size" env:"SINK_MAX_BATCH_SIZE"`
	BatchTimeout     int `json:"batch_timeout_ms" env:"SINK_BATCH_TIMEOUT_MS"`
	ConnectionPool   int `json:"connection_pool" env:"SINK_CONNECTION_POOL"`
	
	// Retry behavior
	MaxRetries     int `json:"max_retries" env:"SINK_MAX_RETRIES"`
	RetryDelayMS   int `json:"retry_delay_ms" env:"SINK_RETRY_DELAY_MS"`
	
	// Storage settings
	EnableCompression bool   `json:"enable_compression" env:"SINK_ENABLE_COMPRESSION"`
	TablePrefix      string `json:"table_prefix" env:"SINK_TABLE_PREFIX"`
}

// NewSink creates a sink instance based on configuration
func NewSink(cfg Config) (Sink, error) {
	switch cfg.Kind {
	case "timescaledb", "tsdb", "postgres":
		return NewTimescaleDBSink(cfg)
	case "debug":
		return NewDebugSink(cfg)
	default:
		return nil, ErrInvalidSinkKind
	}
}

// NewStatsProviderSink creates a sink with stats capabilities
func NewStatsProviderSink(cfg Config) (StatsProvider, error) {
	baseSink, err := NewSink(cfg)
	if err != nil {
		return nil, err
	}
	
	// TimescaleDB sinks already implement StatsProvider
	if statsSink, ok := baseSink.(StatsProvider); ok {
		return statsSink, nil
	}
	
	// This shouldn't happen with TimescaleDB, but kept for safety
	return NewStatsWrapper(baseSink), nil
}