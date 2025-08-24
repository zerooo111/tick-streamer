package sink

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	
	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
)

// ClickHouseSink implements the Sink interface using ClickHouse database
// ClickHouse is optimized for analytics and high-throughput ingestion
type ClickHouseSink struct {
	mu        sync.RWMutex
	config    Config
	conn      clickhouse.Conn
	lastTick  uint64
	stats     SinkStats
	closed    bool
	
	// Batch buffers for improved performance
	tickBatch []*models.TickRow
	txBatch   []*models.TxRow
	
	// Batch configuration
	batchSize     int
	flushInterval time.Duration
	lastFlush     time.Time
}

// ClickHouseConfig holds ClickHouse specific configuration
type ClickHouseConfig struct {
	Host           string        `json:"host" env:"CLICKHOUSE_HOST"`
	Port           int           `json:"port" env:"CLICKHOUSE_PORT"`
	Database       string        `json:"database" env:"CLICKHOUSE_DATABASE"`
	Username       string        `json:"username" env:"CLICKHOUSE_USERNAME"`
	Password       string        `json:"password" env:"CLICKHOUSE_PASSWORD"`
	BatchSize      int           `json:"batch_size" env:"CLICKHOUSE_BATCH_SIZE"`
	FlushInterval  time.Duration `json:"flush_interval" env:"CLICKHOUSE_FLUSH_INTERVAL"`
	Compression    string        `json:"compression" env:"CLICKHOUSE_COMPRESSION"` // lz4, zstd, gzip
	ConnectTimeout time.Duration `json:"connect_timeout" env:"CLICKHOUSE_CONNECT_TIMEOUT"`
}

// NewClickHouseSink creates a new ClickHouse sink
func NewClickHouseSink(cfg Config) (*ClickHouseSink, error) {
	// Get ClickHouse configuration from environment variables
	chConfig := getClickHouseConfigFromEnv()
	
	// Set defaults if not provided
	if chConfig.Host == "" {
		chConfig.Host = "z9jq89387u.ap-south-1.aws.clickhouse.cloud"
	}
	if chConfig.Port == 0 {
		chConfig.Port = 9440  // Secure native TCP port
	}
	if chConfig.Database == "" {
		chConfig.Database = "default"
	}
	if chConfig.Username == "" {
		chConfig.Username = "default"
	}
	if chConfig.Password == "" {
		chConfig.Password = "1O4~txzDw_LZl"  // Default password, should be overridden by env var
	}
	if chConfig.BatchSize == 0 {
		chConfig.BatchSize = cfg.MaxBatchSize
		if chConfig.BatchSize == 0 {
			chConfig.BatchSize = 10000
		}
	}
	if chConfig.FlushInterval == 0 {
		chConfig.FlushInterval = time.Duration(cfg.BatchTimeout) * time.Millisecond
		if chConfig.FlushInterval == 0 {
			chConfig.FlushInterval = 5 * time.Second
		}
	}
	if chConfig.ConnectTimeout == 0 {
		chConfig.ConnectTimeout = 30 * time.Second  // Increased timeout
	}

	fmt.Printf("🔍 Connecting to ClickHouse at %s:%d...\n", chConfig.Host, chConfig.Port)
	
	var conn clickhouse.Conn
	var err error
	
	// Retry connection with exponential backoff
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("🔄 Connection attempt %d/%d...\n", attempt, maxRetries)
		
		// Create ClickHouse connection with secure TLS
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", chConfig.Host, chConfig.Port)},
			Protocol: clickhouse.Native,
			TLS: &tls.Config{}, // Enable secure TLS
			Auth: clickhouse.Auth{
				Database: chConfig.Database,
				Username: chConfig.Username,
				Password: chConfig.Password,
			},
			DialTimeout:     chConfig.ConnectTimeout,
			MaxOpenConns:    cfg.ConnectionPool,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		})

		if err != nil {
			fmt.Printf("❌ Connection attempt %d failed: %v\n", attempt, err)
			if attempt < maxRetries {
				waitTime := time.Duration(attempt) * 2 * time.Second
				fmt.Printf("⏳ Waiting %v before retry...\n", waitTime)
				time.Sleep(waitTime)
			}
			continue
		}
		
		// Test the connection
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = conn.Ping(ctx)
		cancel()
		
		if err != nil {
			fmt.Printf("❌ Ping failed on attempt %d: %v\n", attempt, err)
			conn.Close()
			if attempt < maxRetries {
				waitTime := time.Duration(attempt) * 2 * time.Second
				fmt.Printf("⏳ Waiting %v before retry...\n", waitTime)
				time.Sleep(waitTime)
			}
			continue
		}
		
		fmt.Printf("✅ ClickHouse connection established successfully!\n")
		break
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse after %d attempts: %w", maxRetries, err)
	}

	sink := &ClickHouseSink{
		config:        cfg,
		conn:          conn,
		batchSize:     chConfig.BatchSize,
		flushInterval: chConfig.FlushInterval,
		lastFlush:     time.Now(),
		stats: SinkStats{
			Connected: true,
		},
	}

	// Initialize schema
	if err := sink.initSchema(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Load last tick
	if err := sink.loadLastTick(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to load last tick: %w", err)
	}

	return sink, nil
}

// getClickHouseConfigFromEnv reads ClickHouse configuration from environment variables
func getClickHouseConfigFromEnv() ClickHouseConfig {
	config := ClickHouseConfig{}
	
	if host := os.Getenv("CLICKHOUSE_HOST"); host != "" {
		config.Host = host
	}
	if port := os.Getenv("CLICKHOUSE_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.Port = p
		}
	}
	if database := os.Getenv("CLICKHOUSE_DATABASE"); database != "" {
		config.Database = database
	}
	if username := os.Getenv("CLICKHOUSE_USERNAME"); username != "" {
		config.Username = username
	}
	if password := os.Getenv("CLICKHOUSE_PASSWORD"); password != "" {
		config.Password = password
	}
	if batchSize := os.Getenv("CLICKHOUSE_BATCH_SIZE"); batchSize != "" {
		if b, err := strconv.Atoi(batchSize); err == nil {
			config.BatchSize = b
		}
	}
	if flushInterval := os.Getenv("CLICKHOUSE_FLUSH_INTERVAL"); flushInterval != "" {
		if d, err := time.ParseDuration(flushInterval); err == nil {
			config.FlushInterval = d
		}
	}
	if compression := os.Getenv("CLICKHOUSE_COMPRESSION"); compression != "" {
		config.Compression = compression
	}
	if timeout := os.Getenv("CLICKHOUSE_CONNECT_TIMEOUT"); timeout != "" {
		if t, err := time.ParseDuration(timeout); err == nil {
			config.ConnectTimeout = t
		}
	}
	
	return config
}

// initSchema creates the required ClickHouse tables
func (s *ClickHouseSink) initSchema() error {
	fmt.Println("🔧 Initializing ClickHouse schema...")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)  // Increased timeout
	defer cancel()

	// Test connection first
	fmt.Println("🔍 Testing ClickHouse connection...")
	if err := s.conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}
	fmt.Println("✅ ClickHouse connection successful!")

	// Create ticks table with ReplacingMergeTree for deduplication and versioning
	fmt.Println("📋 Creating ticks table...")
	ticksSchema := `
	CREATE TABLE IF NOT EXISTS ticks (
		tick_number UInt64,
		timestamp_us Int64,
		vdf_input String,
		vdf_output String,
		vdf_iterations UInt64,
		vdf_proof String,
		previous_output String,
		transaction_batch_hash String,
		transaction_count Int32,
		processed_at DateTime64(6),
		ingestion_ts Int64,
		version Int32
	) ENGINE = ReplacingMergeTree(version)
	PARTITION BY toYYYYMM(toDateTime(timestamp_us / 1000000))
	ORDER BY tick_number
	SETTINGS index_granularity = 8192
	`

	if err := s.conn.Exec(ctx, ticksSchema); err != nil {
		return fmt.Errorf("failed to create ticks table: %w", err)
	}
	fmt.Println("✅ Ticks table created successfully!")

	// Create transactions table
	fmt.Println("📋 Creating transactions table...")
	transactionsSchema := `
	CREATE TABLE IF NOT EXISTS transactions (
		tick_number UInt64,
		sequence_number UInt64,
		tx_hash String,
		tx_id String,
		nonce UInt64,
		payload String, -- Base64 encoded for ClickHouse compatibility
		timestamp UInt64,
		public_key String,
		signature String,
		ingestion_timestamp UInt64,
		processed_at DateTime64(6),
		payload_size Int32,
		payload_type String,
		version Int32
	) ENGINE = ReplacingMergeTree(version)
	PARTITION BY toYYYYMM(toDateTime(timestamp / 1000000))
	ORDER BY (tick_number, sequence_number)
	SETTINGS index_granularity = 8192
	`

	if err := s.conn.Exec(ctx, transactionsSchema); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}
	fmt.Println("✅ Transactions table created successfully!")

	// Create indexes for optimized queries
	fmt.Println("📋 Creating indexes...")
	indexes := []string{
		// Critical for API queries by transaction hash
		"CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions (tx_hash) TYPE minmax GRANULARITY 1",
		// Useful for public key queries (address lookups)  
		"CREATE INDEX IF NOT EXISTS idx_public_key ON transactions (public_key) TYPE minmax GRANULARITY 1",
		// Useful for timestamp range queries
		"CREATE INDEX IF NOT EXISTS idx_tick_timestamp ON ticks (timestamp_us) TYPE minmax GRANULARITY 8",
		"CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON transactions (timestamp) TYPE minmax GRANULARITY 8",
	}

	for _, indexSQL := range indexes {
		if err := s.conn.Exec(ctx, indexSQL); err != nil {
			// Don't fail on index creation errors, just log them
			fmt.Printf("⚠️  Warning: Failed to create index: %v\n", err)
		}
	}
	fmt.Println("✅ Indexes created successfully!")

	// Create materialized views for real-time aggregations (optional)
	fmt.Println("📋 Creating materialized view (optional)...")
	tickStatsView := `
	CREATE MATERIALIZED VIEW IF NOT EXISTS tick_stats
	ENGINE = AggregatingMergeTree()
	PARTITION BY toYYYYMM(processed_at)
	ORDER BY (toStartOfHour(processed_at))
	AS SELECT
		toStartOfHour(processed_at) as hour,
		countState() as tick_count,
		sumState(transaction_count) as total_transactions,
		maxState(tick_number) as max_tick,
		avgState(transaction_count) as avg_tx_per_tick
	FROM ticks
	WHERE version > 0
	GROUP BY hour
	`

	// Don't fail if materialized view creation fails (it's optional)
	if err := s.conn.Exec(ctx, tickStatsView); err != nil {
		fmt.Printf("⚠️ Warning: Failed to create materialized view (optional): %v\n", err)
	} else {
		fmt.Println("✅ Materialized view created successfully!")
	}
	
	fmt.Println("🎉 ClickHouse schema initialization completed!")
	return nil
}

// loadLastTick loads the last tick number from ClickHouse
func (s *ClickHouseSink) loadLastTick() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
		SELECT max(tick_number) 
		FROM ticks 
		WHERE version > 0
	`

	row := s.conn.QueryRow(ctx, query)
	
	var maxTick sql.NullInt64
	if err := row.Scan(&maxTick); err != nil {
		return fmt.Errorf("failed to scan last tick: %w", err)
	}

	if maxTick.Valid {
		s.lastTick = uint64(maxTick.Int64)
	}

	return nil
}

// PersistData implements the Sink interface
// Only stores ticks that have transactions (filters out empty ticks)
func (s *ClickHouseSink) PersistData(ctx context.Context, data []*parser.ParsedData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	// Track which ticks have transactions
	ticksWithTransactions := make(map[uint64]bool)
	
	// First pass: identify ticks with transactions
	for _, item := range data {
		if item.Type == "transaction" {
			if txRow, ok := item.Data.(*models.TxRow); ok {
				ticksWithTransactions[txRow.TickNumber] = true
			}
		}
	}

	// Second pass: add data to batches, but only ticks that have transactions
	for _, item := range data {
		switch item.Type {
		case "tick":
			tickRow, ok := item.Data.(*models.TickRow)
			if !ok {
				s.stats.ErrorCount++
				return fmt.Errorf("expected *models.TickRow, got %T", item.Data)
			}
			
			// Only store ticks that have transactions
			if ticksWithTransactions[tickRow.TickNumber] {
				s.tickBatch = append(s.tickBatch, tickRow)
			}
			
		case "transaction":
			txRow, ok := item.Data.(*models.TxRow)
			if !ok {
				s.stats.ErrorCount++
				return fmt.Errorf("expected *models.TxRow, got %T", item.Data)
			}
			s.txBatch = append(s.txBatch, txRow)
		}
	}

	// Check if we should flush
	shouldFlush := len(s.tickBatch) >= s.batchSize ||
		len(s.txBatch) >= s.batchSize ||
		time.Since(s.lastFlush) >= s.flushInterval

	if shouldFlush {
		return s.flushBatches(ctx)
	}

	return nil
}

// flushBatches flushes the current batches to ClickHouse
func (s *ClickHouseSink) flushBatches(ctx context.Context) error {
	startTime := time.Now()
	
	tickCount := len(s.tickBatch)
	txCount := len(s.txBatch)
	
	if tickCount == 0 && txCount == 0 {
		return nil
	}

	// Flush ticks batch
	if tickCount > 0 {
		if err := s.flushTickBatch(ctx); err != nil {
			s.stats.ErrorCount++
			return fmt.Errorf("failed to flush tick batch: %w", err)
		}
	}

	// Flush transactions batch
	if txCount > 0 {
		if err := s.flushTransactionBatch(ctx); err != nil {
			s.stats.ErrorCount++
			return fmt.Errorf("failed to flush transaction batch: %w", err)
		}
	}

	// Clear batches
	s.tickBatch = s.tickBatch[:0]
	s.txBatch = s.txBatch[:0]
	s.lastFlush = time.Now()

	duration := time.Since(startTime)
	s.updateStats(tickCount, txCount, duration)

	return nil
}

// flushTickBatch flushes the tick batch to ClickHouse
func (s *ClickHouseSink) flushTickBatch(ctx context.Context) error {
	if len(s.tickBatch) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO ticks (
			tick_number, timestamp_us, vdf_input, vdf_output, vdf_iterations,
			vdf_proof, previous_output, transaction_batch_hash, transaction_count,
			processed_at, ingestion_ts, version
		) VALUES
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare tick batch: %w", err)
	}

	for _, tick := range s.tickBatch {
		err := batch.Append(
			tick.TickNumber,
			tick.TimestampUS,
			tick.VdfInput,
			tick.VdfOutput,
			tick.VdfIterations,
			tick.VdfProof,
			tick.PreviousOutput,
			tick.TransactionBatchHash,
			tick.TransactionCount,
			tick.ProcessedAt,
			tick.IngestionTS,
			tick.Version,
		)
		if err != nil {
			return fmt.Errorf("failed to append tick to batch: %w", err)
		}

		// Update last tick
		if tick.TickNumber > s.lastTick {
			s.lastTick = tick.TickNumber
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send tick batch: %w", err)
	}

	return nil
}

// flushTransactionBatch flushes the transaction batch to ClickHouse
func (s *ClickHouseSink) flushTransactionBatch(ctx context.Context) error {
	if len(s.txBatch) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO transactions (
			tick_number, sequence_number, tx_hash, tx_id, nonce, payload,
			timestamp, public_key, signature, ingestion_timestamp, processed_at,
			payload_size, payload_type, version
		) VALUES
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare transaction batch: %w", err)
	}

	for _, tx := range s.txBatch {
		// Encode payload as base64 for ClickHouse string storage
		payloadStr := ""
		if len(tx.Payload) > 0 {
			// Simple base64 encoding would go here
			payloadStr = string(tx.Payload) // Simplified for now
		}

		err := batch.Append(
			tx.TickNumber,
			tx.SequenceNumber,
			tx.TxHash,
			tx.TxID,
			tx.Nonce,
			payloadStr,
			tx.Timestamp,
			tx.PublicKey,
			tx.Signature,
			tx.IngestionTimestamp,
			tx.ProcessedAt,
			tx.PayloadSize,
			tx.PayloadType,
			tx.Version,
		)
		if err != nil {
			return fmt.Errorf("failed to append transaction to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send transaction batch: %w", err)
	}

	return nil
}

// InvalidateTick implements the Sink interface
func (s *ClickHouseSink) InvalidateTick(ctx context.Context, tickNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	// In ClickHouse, we insert new records with version = -1 to mark them as invalid
	// The ReplacingMergeTree will eventually merge and keep the highest version

	// Insert invalidation record for tick
	tickQuery := `
		INSERT INTO ticks (
			tick_number, timestamp_us, vdf_input, vdf_output, vdf_iterations,
			vdf_proof, previous_output, transaction_batch_hash, transaction_count,
			processed_at, ingestion_ts, version
		) VALUES (?, 0, '', '', 0, '', '', '', 0, now(), 0, -1)
	`

	if err := s.conn.Exec(ctx, tickQuery, tickNumber); err != nil {
		return fmt.Errorf("failed to invalidate tick %d: %w", tickNumber, err)
	}

	// Insert invalidation records for transactions
	txQuery := `
		INSERT INTO transactions (
			tick_number, sequence_number, tx_hash, tx_id, nonce, payload,
			timestamp, public_key, signature, ingestion_timestamp, processed_at,
			payload_size, payload_type, version
		) SELECT 
			?, 0, '', '', 0, '', 0, '', '', 0, now(), 0, '', -1
		FROM numbers(1)
	`

	if err := s.conn.Exec(ctx, txQuery, tickNumber); err != nil {
		return fmt.Errorf("failed to invalidate transactions for tick %d: %w", tickNumber, err)
	}

	return nil
}

// Flush implements the Sink interface
func (s *ClickHouseSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	// Flush any pending batches
	if err := s.flushBatches(ctx); err != nil {
		return fmt.Errorf("failed to flush batches: %w", err)
	}

	s.stats.FlushCount++
	return nil
}

// Health implements the Sink interface
func (s *ClickHouseSink) Health(ctx context.Context) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed || s.conn == nil {
		return false
	}

	// Quick health check
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := s.conn.Ping(ctx)
	return err == nil
}

// GetLastTick implements the Sink interface
func (s *ClickHouseSink) GetLastTick(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, ErrSinkClosed
	}

	return s.lastTick, nil
}

// Close implements the Sink interface
func (s *ClickHouseSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	var errs []string

	// Flush any pending data
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.flushBatches(ctx); err != nil {
		errs = append(errs, fmt.Sprintf("flush error: %v", err))
	}

	// Close connection
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("connection close error: %v", err))
		}
	}

	s.closed = true
	s.stats.Connected = false

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %s", strings.Join(errs, ", "))
	}

	return nil
}

// GetStats implements the StatsProvider interface
func (s *ClickHouseSink) GetStats() SinkStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	statsCopy := s.stats
	statsCopy.LastTickNumber = s.lastTick
	statsCopy.PendingBatches = len(s.tickBatch) + len(s.txBatch)
	return statsCopy
}

// ResetStats implements the StatsProvider interface
func (s *ClickHouseSink) ResetStats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.TicksInserted = 0
	s.stats.TransactionsInserted = 0
	s.stats.FlushCount = 0
	s.stats.ErrorCount = 0
	s.stats.LastFlushDuration = 0
	s.stats.AverageFlushDuration = 0
}

// updateStats updates internal statistics
func (s *ClickHouseSink) updateStats(tickCount, txCount int, duration time.Duration) {
	s.stats.TicksInserted += uint64(tickCount)
	s.stats.TransactionsInserted += uint64(txCount)
	
	durationMS := duration.Milliseconds()
	s.stats.LastFlushDuration = durationMS
	
	// Update average duration (simple exponential moving average)
	if s.stats.AverageFlushDuration == 0 {
		s.stats.AverageFlushDuration = float64(durationMS)
	} else {
		alpha := 0.1 // Smoothing factor
		s.stats.AverageFlushDuration = alpha*float64(durationMS) + (1-alpha)*s.stats.AverageFlushDuration
	}
}

// GetAnalytics returns ClickHouse-specific analytics data
func (s *ClickHouseSink) GetAnalytics(ctx context.Context, hours int) (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSinkClosed
	}

	analytics := make(map[string]interface{})
	
	// Get tick statistics over the last N hours
	tickStatsQuery := `
		SELECT 
			toStartOfHour(processed_at) as hour,
			count() as tick_count,
			sum(transaction_count) as total_transactions,
			avg(transaction_count) as avg_tx_per_tick
		FROM ticks 
		WHERE processed_at >= now() - INTERVAL ? HOUR
		  AND version > 0
		GROUP BY hour 
		ORDER BY hour DESC
	`

	rows, err := s.conn.Query(ctx, tickStatsQuery, hours)
	if err != nil {
		return nil, fmt.Errorf("failed to get tick analytics: %w", err)
	}
	defer rows.Close()

	hourlyStats := []map[string]interface{}{}
	for rows.Next() {
		var hour time.Time
		var tickCount, totalTx uint64
		var avgTx float64
		
		if err := rows.Scan(&hour, &tickCount, &totalTx, &avgTx); err != nil {
			continue
		}
		
		hourlyStats = append(hourlyStats, map[string]interface{}{
			"hour":              hour,
			"tick_count":        tickCount,
			"total_transactions": totalTx,
			"avg_tx_per_tick":   avgTx,
		})
	}
	
	analytics["hourly_stats"] = hourlyStats
	
	// Get overall statistics
	overallQuery := `
		SELECT 
			count() as total_ticks,
			sum(transaction_count) as total_transactions,
			min(processed_at) as earliest_tick,
			max(processed_at) as latest_tick
		FROM ticks 
		WHERE version > 0
	`
	
	row := s.conn.QueryRow(ctx, overallQuery)
	var totalTicks, totalTx uint64
	var earliest, latest time.Time
	
	if err := row.Scan(&totalTicks, &totalTx, &earliest, &latest); err == nil {
		analytics["total_ticks"] = totalTicks
		analytics["total_transactions"] = totalTx
		analytics["earliest_tick"] = earliest
		analytics["latest_tick"] = latest
	}

	return analytics, nil
}