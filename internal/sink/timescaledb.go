// Package sink implements database persistence layers for the Continuum Streamer.
// The TimescaleDB sink provides high-performance time-series data storage
// optimized for blockchain tick and transaction data.
//
// TimescaleDB Features Used:
// - Hypertables for automatic time-based partitioning
// - Batch inserts for maximum throughput
// - UPSERT operations for handling reorgs
// - Connection pooling for concurrency
// - Compression policies for storage efficiency
//
// Performance Optimizations:
// - Prepared statements for repeated queries
// - Batch operations to reduce roundtrips
// - Asynchronous processing with worker pools
// - Smart batching based on size and time thresholds
package sink

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	
	"github.com/zerooo111/tick-streamer/internal/parser"
)

// TickRow represents tick data for database storage
type TickRow struct {
	TickNumber           uint64 `db:"tick_number"`
	Timestamp            uint64 `db:"timestamp_us"`
	VDFInput             string `db:"vdf_input"`
	VDFOutput            string `db:"vdf_output"`
	VDFProof             string `db:"vdf_proof"`
	VDFIterations        uint64 `db:"vdf_iterations"`
	TransactionBatchHash string `db:"transaction_batch_hash"`
	PreviousOutput       string `db:"previous_output"`
	TxCount              uint32 `db:"tx_count"`
}

// TransactionRow represents transaction data for database storage
type TransactionRow struct {
	TickNumber         uint64 `db:"tick_number"`
	SequenceNumber     uint64 `db:"sequence_number"`
	TxHash             string `db:"tx_hash"`
	TxID               string `db:"tx_id"`
	Nonce              uint64 `db:"nonce"`
	Payload            []byte `db:"payload"`
	Timestamp          uint64 `db:"timestamp_us"`
	PublicKey          []byte `db:"public_key"`
	Signature          []byte `db:"signature"`
	IngestionTimestamp uint64 `db:"ingestion_timestamp"`
	PayloadSize        int32  `db:"payload_size"`
}


// TimescaleDBSink implements the Sink interface using TimescaleDB (PostgreSQL extension)
// Now handles all batching logic internally - accumulates data and flushes based on size/time
type TimescaleDBSink struct {
	mu        sync.RWMutex
	config    Config
	db        *sql.DB
	lastTick  uint64
	stats     SinkStats
	closed    bool
	
	// Internal batching - accumulate data until flush conditions met
	tickBatch     []*TickRow
	txBatch       []*TransactionRow
	
	// Batch configuration - sink decides when to flush
	batchSize     int
	flushInterval time.Duration
	lastFlush     time.Time
	autoFlush     bool // Whether to auto-flush on size/time thresholds
}

// TimescaleDBConfig holds TimescaleDB specific configuration
type TimescaleDBConfig struct {
	Host           string        `json:"host" env:"TIMESCALEDB_HOST"`
	Port           int           `json:"port" env:"TIMESCALEDB_PORT"`
	Database       string        `json:"database" env:"TIMESCALEDB_DATABASE"`
	Username       string        `json:"username" env:"TIMESCALEDB_USERNAME"`
	Password       string        `json:"password" env:"TIMESCALEDB_PASSWORD"`
	MaxConnections int32         `json:"max_connections" env:"TIMESCALEDB_MAX_CONNECTIONS"`
	ConnectTimeout time.Duration `json:"connect_timeout" env:"TIMESCALEDB_CONNECT_TIMEOUT"`
	SSLMode        string        `json:"ssl_mode" env:"TIMESCALEDB_SSL_MODE"`
}

// NewTimescaleDBSink creates a new TimescaleDB sink
func NewTimescaleDBSink(cfg Config) (*TimescaleDBSink, error) {
	// Get TimescaleDB configuration from environment variables
	tsConfig := getTimescaleDBConfigFromEnv()
	
	// Required settings
	if tsConfig.Host == "" {
		return nil, fmt.Errorf("TIMESCALEDB_HOST is required")
	}
	if tsConfig.Password == "" {
		return nil, fmt.Errorf("TIMESCALEDB_PASSWORD is required")
	}
	
	// Set defaults for optional settings
	if tsConfig.Port == 0 {
		tsConfig.Port = 5432
	}
	if tsConfig.Database == "" {
		tsConfig.Database = "postgres"
	}
	if tsConfig.Username == "" {
		tsConfig.Username = "postgres"
	}
	// Check if direct write mode is enabled from environment
	directWrite := os.Getenv("DIRECT_WRITE") == "true"
	
	// Use unified batch settings from sink config
	batchSize := cfg.MaxBatchSize
	if batchSize == 0 {
		if directWrite {
			batchSize = 1 // Force immediate writes for direct mode
		} else {
			batchSize = 2000 // Default optimized batch size
		}
	}
	
	flushInterval := time.Duration(cfg.BatchTimeout) * time.Millisecond
	if flushInterval == 0 {
		if directWrite {
			flushInterval = 0 // No time-based batching for direct mode
		} else {
			flushInterval = 200 * time.Millisecond // Default optimized interval
		}
	}
	if tsConfig.MaxConnections == 0 {
		if directWrite {
			tsConfig.MaxConnections = 50  // Max allowed connections for direct writes
		} else {
			tsConfig.MaxConnections = 25
		}
	}
	if tsConfig.ConnectTimeout == 0 {
		tsConfig.ConnectTimeout = 30 * time.Second
	}
	if tsConfig.SSLMode == "" {
		tsConfig.SSLMode = "prefer"
	}

	// Check for full connection string first
	connStr := os.Getenv("TIMESCALEDB_CONNECTION_STRING")
	lowLatencyMode := os.Getenv("LOW_LATENCY_MODE") == "true"
	
	if connStr == "" {
		// Build from individual parameters
		if !lowLatencyMode {
			fmt.Printf("🔍 Connecting to TimescaleDB at %s:%d...\n", tsConfig.Host, tsConfig.Port)
		}
		connStr = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			tsConfig.Host, tsConfig.Port, tsConfig.Username, tsConfig.Password,
			tsConfig.Database, tsConfig.SSLMode)
	} else {
		if !lowLatencyMode {
			fmt.Printf("🔍 Connecting to TimescaleDB using connection string...\n")
		}
	}
	
	// Create database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open TimescaleDB connection: %w", err)
	}
	
	// Configure connection pool for ultra-high performance
	db.SetMaxOpenConns(int(tsConfig.MaxConnections))
	db.SetMaxIdleConns(int(tsConfig.MaxConnections))  // Keep all connections idle
	
	if directWrite {
		// Ultra-low latency settings
		db.SetConnMaxLifetime(30 * time.Minute)  // Longer lifetime to avoid reconnects
		db.SetConnMaxIdleTime(10 * time.Minute) // Keep connections warm longer
	} else {
		// Traditional settings
		db.SetConnMaxLifetime(time.Hour)
		db.SetConnMaxIdleTime(time.Minute * 30)
	}
	
	// Test connection
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer testCancel()
	
	if err := db.PingContext(testCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping TimescaleDB: %w", err)
	}
	
	if !lowLatencyMode {
		fmt.Printf("✅ Connected to TimescaleDB successfully\n")
	}
	
	// Override batch settings for direct write mode
	if directWrite {
		batchSize = 1                      // Force immediate writes
		flushInterval = 0                  // No time-based batching
	}
	
	sink := &TimescaleDBSink{
		config:        cfg,
		db:            db,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
		autoFlush:     true, // Enable automatic batching (or immediate writes in direct mode)
		stats: SinkStats{
			Connected: true,
		},
	}
	
	return sink, nil
}

// PersistData handles generic parsed data from any parser
// Now accumulates data in batches and flushes based on size/time thresholds
func (s *TimescaleDBSink) PersistData(ctx context.Context, data []*parser.ParsedData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return ErrSinkClosed
	}
	
	// Process parsed data - handle parsed_bundle type
	for _, parsed := range data {
		// Handle parsed_bundle
		if bundle, ok := parsed.Data.(*parser.ParsedBundle); ok {
			// Skip saving tick if it has 0 transactions
			if len(bundle.Transactions) == 0 {
				continue
			}
			
			// Store tick data
			tickRow := &TickRow{
				TickNumber:           bundle.Tick.TickNumber,
				Timestamp:            bundle.Tick.Timestamp,
				VDFInput:             bundle.Tick.VDFInput,
				VDFOutput:            bundle.Tick.VDFOutput,
				VDFProof:             bundle.Tick.VDFProof,
				VDFIterations:        bundle.Tick.VDFIterations,
				TransactionBatchHash: bundle.Tick.TransactionBatchHash,
				PreviousOutput:       bundle.Tick.PreviousOutput,
				TxCount:              bundle.Tick.TxCount,
			}
			s.tickBatch = append(s.tickBatch, tickRow)
			
			// Store all transactions
			for _, tx := range bundle.Transactions {
				txRow := &TransactionRow{
					TickNumber:         tx.TickNumber,
					SequenceNumber:     tx.SequenceNumber,
					TxHash:             tx.TxHash,
					TxID:               tx.TxID,
					Nonce:              tx.Nonce,
					Payload:            tx.Payload,
					Timestamp:          tx.Timestamp,
					PublicKey:          tx.PublicKey,
					Signature:          tx.Signature,
					IngestionTimestamp: tx.IngestionTimestamp,
					PayloadSize:        tx.PayloadSize,
				}
				s.txBatch = append(s.txBatch, txRow)
			}
			
			if bundle.Tick.TickNumber > s.lastTick {
				s.lastTick = bundle.Tick.TickNumber
			}
		}
	}
	
	// Check if we should auto-flush based on batch size or time
	if s.autoFlush && s.shouldFlush() {
		return s.flushBatches(ctx)
	}
	
	return nil
}


// InvalidateTick marks all data for a specific tick as invalid (for reorgs)
func (s *TimescaleDBSink) InvalidateTick(ctx context.Context, tickNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return ErrSinkClosed
	}
	
	// Update version to -1 for invalidated records
	_, err := s.db.ExecContext(ctx, "UPDATE ticks SET version = -1 WHERE tick_number = $1", tickNumber)
	if err != nil {
		return fmt.Errorf("failed to invalidate tick %d: %w", tickNumber, err)
	}
	
	_, err = s.db.ExecContext(ctx, "UPDATE transactions SET version = -1 WHERE tick_number = $1", tickNumber)
	if err != nil {
		return fmt.Errorf("failed to invalidate transactions for tick %d: %w", tickNumber, err)
	}
	
	fmt.Printf("🔄 Invalidated tick %d in TimescaleDB\n", tickNumber)
	return nil
}

// Flush ensures all pending writes are committed to storage
// Now actually flushes accumulated batches to database
func (s *TimescaleDBSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return ErrSinkClosed
	}
	
	return s.flushBatches(ctx)
}

// Health returns true if the sink is operational
func (s *TimescaleDBSink) Health(ctx context.Context) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed || s.db == nil {
		return false
	}
	
	// Quick health check with timeout
	healthCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	
	return s.db.PingContext(healthCtx) == nil
}

// GetLastTick returns the highest tick number successfully persisted
func (s *TimescaleDBSink) GetLastTick(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed {
		return 0, ErrSinkClosed
	}
	
	var lastTick uint64
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(tick_number), 0) FROM ticks WHERE version > 0").Scan(&lastTick)
	if err != nil {
		return s.lastTick, nil // Return cached value if query fails
	}
	
	s.mu.Lock()
	s.lastTick = lastTick
	s.mu.Unlock()
	
	return lastTick, nil
}

// Close gracefully shuts down the sink and releases resources
func (s *TimescaleDBSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return nil
	}
	
	s.closed = true
	s.stats.Connected = false
	
	if s.db != nil {
		s.db.Close()
	}
	
	fmt.Printf("🔒 TimescaleDB sink closed\n")
	return nil
}

// GetStats returns current operational statistics (implements StatsProvider)
func (s *TimescaleDBSink) GetStats() SinkStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Update current state
	s.stats.LastTickNumber = s.lastTick
	s.stats.Connected = !s.closed && s.db != nil
	
	return s.stats
}

// ResetStats clears counters (implements StatsProvider)
func (s *TimescaleDBSink) ResetStats() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.stats = SinkStats{
		Connected:      s.stats.Connected,
		LastTickNumber: s.stats.LastTickNumber,
	}
}

// Helper functions

func getTimescaleDBConfigFromEnv() TimescaleDBConfig {
	return TimescaleDBConfig{
		Host:           os.Getenv("TIMESCALEDB_HOST"),
		Port:           getEnvAsInt("TIMESCALEDB_PORT", 0),
		Database:       os.Getenv("TIMESCALEDB_DATABASE"),
		Username:       os.Getenv("TIMESCALEDB_USERNAME"),
		Password:       os.Getenv("TIMESCALEDB_PASSWORD"),
		MaxConnections: int32(getEnvAsInt("TIMESCALEDB_MAX_CONNECTIONS", 0)),
		ConnectTimeout: time.Duration(getEnvAsInt("TIMESCALEDB_CONNECT_TIMEOUT_SECONDS", 0)) * time.Second,
		SSLMode:        os.Getenv("TIMESCALEDB_SSL_MODE"),
	}
}

func getEnvAsInt(name string, defaultVal int) int {
	valueStr := os.Getenv(name)
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultVal
}

// shouldFlush determines if batches should be flushed based on size or time
func (s *TimescaleDBSink) shouldFlush() bool {
	totalRows := len(s.tickBatch) + len(s.txBatch)
	
	// Direct write mode: Flush immediately if any data exists
	if s.batchSize <= 1 && totalRows > 0 {
		return true
	}
	
	// Traditional batching: Flush if we have enough data
	if totalRows >= s.batchSize {
		return true
	}
	
	// Traditional batching: Flush if enough time has passed and we have any data
	if totalRows > 0 && s.flushInterval > 0 && time.Since(s.lastFlush) >= s.flushInterval {
		return true
	}
	
	return false
}

// flushBatches actually writes accumulated data to database
func (s *TimescaleDBSink) flushBatches(ctx context.Context) error {
	start := time.Now()
	
	// Flush ticks if any
	if len(s.tickBatch) > 0 {
		if err := s.insertTicks(ctx, s.tickBatch); err != nil {
			s.stats.ErrorCount++
			return fmt.Errorf("failed to insert ticks: %w", err)
		}
		s.stats.TicksInserted += uint64(len(s.tickBatch))
		s.tickBatch = s.tickBatch[:0] // Clear batch but keep capacity
	}
	
	// Flush transactions if any
	if len(s.txBatch) > 0 {
		if err := s.insertTransactions(ctx, s.txBatch); err != nil {
			s.stats.ErrorCount++
			return fmt.Errorf("failed to insert transactions: %w", err)
		}
		s.stats.TransactionsInserted += uint64(len(s.txBatch))
		s.txBatch = s.txBatch[:0] // Clear batch but keep capacity
	}
	
	// Update flush stats
	duration := time.Since(start)
	s.stats.FlushCount++
	s.stats.LastFlushDuration = duration.Nanoseconds() / 1e6 // Convert to milliseconds
	
	// Update average
	if s.stats.FlushCount == 1 {
		s.stats.AverageFlushDuration = float64(s.stats.LastFlushDuration)
	} else {
		s.stats.AverageFlushDuration = (s.stats.AverageFlushDuration*float64(s.stats.FlushCount-1) + 
			float64(s.stats.LastFlushDuration)) / float64(s.stats.FlushCount)
	}
	
	s.lastFlush = time.Now()
	return nil
}

// insertTicks bulk inserts tick data using batch INSERT for maximum performance
func (s *TimescaleDBSink) insertTicks(ctx context.Context, ticks []*TickRow) error {
	if len(ticks) == 0 {
		return nil
	}
	
	// Build batch INSERT statement - 11 fields
	valueStrings := make([]string, 0, len(ticks))
	valueArgs := make([]interface{}, 0, len(ticks)*11)
	
	processedAt := time.Now()
	
	for i, tick := range ticks {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))
		
		valueArgs = append(valueArgs,
			tick.TickNumber,
			tick.Timestamp,
			nullString(tick.VDFInput),
			nullString(tick.VDFOutput),
			nullString(tick.VDFProof),
			tick.VDFIterations,
			nullString(tick.TransactionBatchHash),
			nullString(tick.PreviousOutput),
			tick.TxCount,
			processedAt,
			1, // version
		)
	}
	
	stmt := fmt.Sprintf(`INSERT INTO ticks (
		tick_number, timestamp_us, vdf_input, vdf_output, vdf_proof, vdf_iterations,
		transaction_batch_hash, previous_output, tx_count, processed_at, version
	) VALUES %s ON CONFLICT (processed_at, tick_number) DO UPDATE SET 
		timestamp_us = EXCLUDED.timestamp_us,
		vdf_input = EXCLUDED.vdf_input,
		vdf_output = EXCLUDED.vdf_output,
		vdf_proof = EXCLUDED.vdf_proof,
		vdf_iterations = EXCLUDED.vdf_iterations,
		transaction_batch_hash = EXCLUDED.transaction_batch_hash,
		previous_output = EXCLUDED.previous_output,
		tx_count = EXCLUDED.tx_count,
		version = EXCLUDED.version`,
		strings.Join(valueStrings, ","))
	
	_, err := s.db.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}
	
	fmt.Printf("📝 Inserted %d ticks to TimescaleDB\n", len(ticks))
	return nil
}

// insertTransactions bulk inserts transaction data using batch INSERT for maximum performance
func (s *TimescaleDBSink) insertTransactions(ctx context.Context, txs []*TransactionRow) error {
	if len(txs) == 0 {
		return nil
	}
	
	// Build batch INSERT statement - 13 fields
	valueStrings := make([]string, 0, len(txs))
	valueArgs := make([]interface{}, 0, len(txs)*14) // 13 fields + version
	
	processedAt := time.Now()
	
	for i, tx := range txs {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			i*14+1, i*14+2, i*14+3, i*14+4, i*14+5, i*14+6, i*14+7, i*14+8, i*14+9, i*14+10, i*14+11, i*14+12, i*14+13, i*14+14))
		
		valueArgs = append(valueArgs,
			tx.TickNumber,
			tx.SequenceNumber,
			tx.TxHash,
			tx.TxID,
			tx.Nonce,
			tx.Payload,
			tx.Timestamp,
			tx.PublicKey,
			tx.Signature,
			tx.IngestionTimestamp,
			processedAt,
			tx.PayloadSize,
			"", // payload_type (empty)
			1,  // version
		)
	}
	
	stmt := fmt.Sprintf(`INSERT INTO transactions (
		tick_number, sequence_number, tx_hash, tx_id, nonce, payload, timestamp_us,
		public_key, signature, ingestion_timestamp, processed_at, payload_size, payload_type, version
	) VALUES %s ON CONFLICT (processed_at, sequence_number) DO UPDATE SET 
		tick_number = EXCLUDED.tick_number,
		tx_hash = EXCLUDED.tx_hash,
		tx_id = EXCLUDED.tx_id,
		nonce = EXCLUDED.nonce,
		payload = EXCLUDED.payload,
		timestamp_us = EXCLUDED.timestamp_us,
		public_key = EXCLUDED.public_key,
		signature = EXCLUDED.signature,
		ingestion_timestamp = EXCLUDED.ingestion_timestamp,
		payload_size = EXCLUDED.payload_size,
		payload_type = EXCLUDED.payload_type,
		version = EXCLUDED.version`,
		strings.Join(valueStrings, ","))
	
	_, err := s.db.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}
	
	fmt.Printf("📝 Inserted %d transactions to TimescaleDB\n", len(txs))
	return nil
}

func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}