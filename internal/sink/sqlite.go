package sink

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
	
	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
)

// SQLiteSink implements the Sink interface using SQLite database
// SQLite is perfect for development, testing, and single-node deployments
type SQLiteSink struct {
	mu       sync.RWMutex
	config   Config
	db       *sql.DB
	lastTick uint64
	stats    SinkStats
	closed   bool
	
	// Prepared statements for performance
	insertTickStmt *sql.Stmt
	insertTxStmt   *sql.Stmt
	updateTickStmt *sql.Stmt
	updateTxStmt   *sql.Stmt
}

// NewSQLiteSink creates a new SQLite sink
func NewSQLiteSink(cfg Config) (*SQLiteSink, error) {
	dsn := cfg.DSN
	if dsn == "" {
		dsn = "./tick_streamer.db" // Default database file
	}

	// Open database connection with optimized settings
	db, err := sql.Open("sqlite3", dsn+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=cache_size(10000)")
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(1) // SQLite doesn't benefit from multiple connections
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // Connections never expire

	sink := &SQLiteSink{
		config: cfg,
		db:     db,
		stats: SinkStats{
			Connected: true,
		},
	}

	// Initialize database schema
	if err := sink.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Prepare statements
	if err := sink.prepareStatements(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	// Load last tick
	if err := sink.loadLastTick(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load last tick: %w", err)
	}

	return sink, nil
}

// initSchema creates the required tables and indexes
func (s *SQLiteSink) initSchema() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create ticks table
	ticksSchema := `
	CREATE TABLE IF NOT EXISTS ticks (
		tick_number INTEGER PRIMARY KEY,
		timestamp_us INTEGER NOT NULL,
		vdf_input TEXT NOT NULL,
		vdf_output TEXT NOT NULL,
		vdf_iterations INTEGER NOT NULL,
		vdf_proof TEXT NOT NULL,
		previous_output TEXT NOT NULL,
		transaction_batch_hash TEXT NOT NULL,
		transaction_count INTEGER NOT NULL,
		processed_at TEXT NOT NULL,
		ingestion_ts INTEGER NOT NULL,
		version INTEGER NOT NULL DEFAULT 1,
		
		UNIQUE(tick_number, version)
	)`

	if _, err := s.db.ExecContext(ctx, ticksSchema); err != nil {
		return fmt.Errorf("failed to create ticks table: %w", err)
	}

	// Create transactions table
	transactionsSchema := `
	CREATE TABLE IF NOT EXISTS transactions (
		tick_number INTEGER NOT NULL,
		sequence_number INTEGER NOT NULL,
		tx_hash TEXT NOT NULL,
		tx_id TEXT NOT NULL,
		nonce INTEGER NOT NULL,
		payload BLOB,
		timestamp INTEGER NOT NULL,
		public_key TEXT NOT NULL,
		signature TEXT NOT NULL,
		ingestion_timestamp INTEGER NOT NULL,
		processed_at TEXT NOT NULL,
		payload_size INTEGER NOT NULL,
		payload_type TEXT,
		version INTEGER NOT NULL DEFAULT 1,
		
		PRIMARY KEY (tick_number, sequence_number, version),
		FOREIGN KEY (tick_number) REFERENCES ticks(tick_number)
	)`

	if _, err := s.db.ExecContext(ctx, transactionsSchema); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}

	// Create indexes for performance
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_ticks_timestamp ON ticks(timestamp_us)",
		"CREATE INDEX IF NOT EXISTS idx_ticks_version ON ticks(version) WHERE version > 0",
		"CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions(tx_hash)",
		"CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON transactions(timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_tx_version ON transactions(version) WHERE version > 0",
	}

	for _, indexSQL := range indexes {
		if _, err := s.db.ExecContext(ctx, indexSQL); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// prepareStatements prepares SQL statements for better performance
func (s *SQLiteSink) prepareStatements() error {
	var err error

	// Insert tick statement
	s.insertTickStmt, err = s.db.Prepare(`
		INSERT OR REPLACE INTO ticks (
			tick_number, timestamp_us, vdf_input, vdf_output, vdf_iterations, 
			vdf_proof, previous_output, transaction_batch_hash, transaction_count,
			processed_at, ingestion_ts, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert tick statement: %w", err)
	}

	// Insert transaction statement
	s.insertTxStmt, err = s.db.Prepare(`
		INSERT OR REPLACE INTO transactions (
			tick_number, sequence_number, tx_hash, tx_id, nonce, payload, 
			timestamp, public_key, signature, ingestion_timestamp, processed_at,
			payload_size, payload_type, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert transaction statement: %w", err)
	}

	// Update tick version (for reorgs)
	s.updateTickStmt, err = s.db.Prepare(`
		UPDATE ticks SET version = -1 WHERE tick_number = ? AND version > 0
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare update tick statement: %w", err)
	}

	// Update transaction version (for reorgs)
	s.updateTxStmt, err = s.db.Prepare(`
		UPDATE transactions SET version = -1 WHERE tick_number = ? AND version > 0
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare update transaction statement: %w", err)
	}

	return nil
}

// loadLastTick loads the last tick number from the database
func (s *SQLiteSink) loadLastTick() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(tick_number), 0) 
		FROM ticks 
		WHERE version > 0
	`).Scan(&s.lastTick)
	
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to load last tick: %w", err)
	}

	return nil
}

// PersistData implements the Sink interface
func (s *SQLiteSink) PersistData(ctx context.Context, data []*parser.ParsedData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	startTime := time.Now()

	// Begin transaction for atomicity
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	tickCount := 0
	txCount := 0

	for _, item := range data {
		switch item.Type {
		case "tick":
			if err := s.persistTickInTx(ctx, tx, item); err != nil {
				s.stats.ErrorCount++
				return fmt.Errorf("failed to persist tick: %w", err)
			}
			tickCount++

		case "transaction":
			if err := s.persistTransactionInTx(ctx, tx, item); err != nil {
				s.stats.ErrorCount++
				return fmt.Errorf("failed to persist transaction: %w", err)
			}
			txCount++
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		s.stats.ErrorCount++
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	duration := time.Since(startTime)
	s.updateStats(tickCount, txCount, duration)

	return nil
}

// persistTickInTx persists a tick within a database transaction
func (s *SQLiteSink) persistTickInTx(ctx context.Context, tx *sql.Tx, item *parser.ParsedData) error {
	tickRow, ok := item.Data.(*models.TickRow)
	if !ok {
		return fmt.Errorf("expected *models.TickRow, got %T", item.Data)
	}

	stmt := tx.StmtContext(ctx, s.insertTickStmt)
	
	_, err := stmt.ExecContext(ctx,
		tickRow.TickNumber,
		tickRow.TimestampUS,
		tickRow.VdfInput,
		tickRow.VdfOutput,
		tickRow.VdfIterations,
		tickRow.VdfProof,
		tickRow.PreviousOutput,
		tickRow.TransactionBatchHash,
		tickRow.TransactionCount,
		tickRow.ProcessedAt.Format(time.RFC3339Nano),
		tickRow.IngestionTS,
		tickRow.Version,
	)

	if err != nil {
		return fmt.Errorf("failed to insert tick: %w", err)
	}

	// Update last tick
	if tickRow.TickNumber > s.lastTick {
		s.lastTick = tickRow.TickNumber
	}

	return nil
}

// persistTransactionInTx persists a transaction within a database transaction
func (s *SQLiteSink) persistTransactionInTx(ctx context.Context, tx *sql.Tx, item *parser.ParsedData) error {
	txRow, ok := item.Data.(*models.TxRow)
	if !ok {
		return fmt.Errorf("expected *models.TxRow, got %T", item.Data)
	}

	stmt := tx.StmtContext(ctx, s.insertTxStmt)
	
	_, err := stmt.ExecContext(ctx,
		txRow.TickNumber,
		txRow.SequenceNumber,
		txRow.TxHash,
		txRow.TxID,
		txRow.Nonce,
		txRow.Payload,
		txRow.Timestamp,
		txRow.PublicKey,
		txRow.Signature,
		txRow.IngestionTimestamp,
		txRow.ProcessedAt.Format(time.RFC3339Nano),
		txRow.PayloadSize,
		txRow.PayloadType,
		txRow.Version,
	)

	if err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	return nil
}

// InvalidateTick implements the Sink interface
func (s *SQLiteSink) InvalidateTick(ctx context.Context, tickNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	// Begin transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Invalidate tick
	tickStmt := tx.StmtContext(ctx, s.updateTickStmt)
	if _, err := tickStmt.ExecContext(ctx, tickNumber); err != nil {
		return fmt.Errorf("failed to invalidate tick %d: %w", tickNumber, err)
	}

	// Invalidate transactions
	txStmt := tx.StmtContext(ctx, s.updateTxStmt)
	if _, err := txStmt.ExecContext(ctx, tickNumber); err != nil {
		return fmt.Errorf("failed to invalidate transactions for tick %d: %w", tickNumber, err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit invalidation: %w", err)
	}

	return nil
}

// Flush implements the Sink interface
func (s *SQLiteSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	startTime := time.Now()

	// SQLite with WAL mode auto-flushes, but we can force a checkpoint
	if _, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		// Don't fail on checkpoint error, just log it
		// In production, you'd use a proper logger here
	}

	s.stats.FlushCount++
	s.stats.LastFlushDuration = time.Since(startTime).Milliseconds()

	return nil
}

// Health implements the Sink interface
func (s *SQLiteSink) Health(ctx context.Context) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed || s.db == nil {
		return false
	}

	// Quick health check
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	err := s.db.PingContext(ctx)
	return err == nil
}

// GetLastTick implements the Sink interface
func (s *SQLiteSink) GetLastTick(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, ErrSinkClosed
	}

	return s.lastTick, nil
}

// Close implements the Sink interface
func (s *SQLiteSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	var errs []string

	// Close prepared statements
	statements := []*sql.Stmt{
		s.insertTickStmt,
		s.insertTxStmt,
		s.updateTickStmt,
		s.updateTxStmt,
	}

	for _, stmt := range statements {
		if stmt != nil {
			if err := stmt.Close(); err != nil {
				errs = append(errs, fmt.Sprintf("statement close error: %v", err))
			}
		}
	}

	// Close database
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("database close error: %v", err))
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
func (s *SQLiteSink) GetStats() SinkStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	statsCopy := s.stats
	statsCopy.LastTickNumber = s.lastTick
	return statsCopy
}

// ResetStats implements the StatsProvider interface
func (s *SQLiteSink) ResetStats() {
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
func (s *SQLiteSink) updateStats(tickCount, txCount int, duration time.Duration) {
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

// GetDatabaseStats returns SQLite-specific database statistics
func (s *SQLiteSink) GetDatabaseStats(ctx context.Context) (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSinkClosed
	}

	stats := make(map[string]interface{})

	// Get table row counts
	var tickCount, txCount int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ticks WHERE version > 0").Scan(&tickCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get tick count: %w", err)
	}
	stats["tick_count"] = tickCount

	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transactions WHERE version > 0").Scan(&txCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction count: %w", err)
	}
	stats["transaction_count"] = txCount

	// Get database size
	var pageCount, pageSize int64
	err = s.db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pageCount)
	if err == nil {
		stats["page_count"] = pageCount
		if err := s.db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err == nil {
			dbSize := pageCount * pageSize
			stats["database_size_bytes"] = dbSize
		}
	}

	// Get WAL size if available
	var walPages int64
	err = s.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint").Scan(&walPages)
	if err == nil {
		stats["wal_pages"] = walPages
	}

	return stats, nil
}