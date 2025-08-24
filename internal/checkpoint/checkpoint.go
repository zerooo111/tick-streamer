package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// Store defines the interface for checkpoint persistence
// This demonstrates Go's interface design for pluggable storage backends
type Store interface {
	Load(ctx context.Context) (uint64, error)
	Save(ctx context.Context, tickNumber uint64) error
	Close() error
	Health(ctx context.Context) error
}

// SQLiteStore implements checkpoint persistence using SQLite
// This teaches Go database integration and transaction management
type SQLiteStore struct {
	db        *sql.DB
	dbPath    string
	tableName string
	mu        sync.RWMutex
	lastTick  uint64
	
	// Prepared statements for performance
	loadStmt *sql.Stmt
	saveStmt *sql.Stmt
}

// CheckpointRecord represents a checkpoint entry in the database
type CheckpointRecord struct {
	ID          int       `db:"id"`
	TickNumber  uint64    `db:"tick_number"`
	UpdatedAt   time.Time `db:"updated_at"`
	Version     int       `db:"version"`
	Description string    `db:"description"`
}

// Config holds configuration for checkpoint store
type Config struct {
	DSN         string // Data Source Name, e.g., "file:./checkpoint.db"
	TableName   string // Table name for checkpoints
	MaxRetries  int    // Maximum retry attempts for database operations
}

// NewSQLiteStore creates a new SQLite-based checkpoint store
// This demonstrates Go constructor patterns and database initialization
func NewSQLiteStore(config Config) (*SQLiteStore, error) {
	// Set defaults
	if config.TableName == "" {
		config.TableName = "checkpoints"
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}

	// Parse DSN to get file path
	dbPath := config.DSN
	if len(dbPath) > 5 && dbPath[:5] == "file:" {
		dbPath = dbPath[5:]
	}

	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if dir != "." {
		// In a real implementation, we would create the directory
		// For now, we'll assume it exists or use current directory
	}

	// Open database connection
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=FULL&_cache_size=1000")
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(1)  // SQLite works best with single connection
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	store := &SQLiteStore{
		db:        db,
		dbPath:    dbPath,
		tableName: config.TableName,
	}

	// Initialize database schema
	if err := store.initSchema(config.TableName); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Prepare statements for better performance
	if err := store.prepareStatements(config.TableName); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	log.Printf("SQLite checkpoint store initialized at %s", dbPath)
	return store, nil
}

// initSchema creates the checkpoint table if it doesn't exist
func (s *SQLiteStore) initSchema(tableName string) error {
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY,
			tick_number INTEGER NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			version INTEGER DEFAULT 1,
			description TEXT DEFAULT 'checkpoint'
		);
		
		CREATE INDEX IF NOT EXISTS idx_%s_tick_number ON %s(tick_number);
		CREATE INDEX IF NOT EXISTS idx_%s_updated_at ON %s(updated_at);
	`, tableName, tableName, tableName, tableName, tableName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := s.db.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	return nil
}

// prepareStatements prepares SQL statements for better performance
func (s *SQLiteStore) prepareStatements(tableName string) error {
	var err error

	// Prepare load statement - get the latest checkpoint
	loadSQL := fmt.Sprintf("SELECT tick_number FROM %s ORDER BY id DESC LIMIT 1", tableName)
	s.loadStmt, err = s.db.Prepare(loadSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare load statement: %w", err)
	}

	// Prepare save statement - insert new checkpoint
	saveSQL := fmt.Sprintf(`
		INSERT INTO %s (tick_number, updated_at, description) 
		VALUES (?, ?, ?)
	`, tableName)
	s.saveStmt, err = s.db.Prepare(saveSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare save statement: %w", err)
	}

	return nil
}

// Load retrieves the last saved checkpoint tick number
// This demonstrates Go database query patterns and error handling
func (s *SQLiteStore) Load(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var tickNumber uint64

	err := s.loadStmt.QueryRowContext(ctx).Scan(&tickNumber)
	if err == sql.ErrNoRows {
		// No checkpoint found - start from beginning
		log.Println("No checkpoint found, starting from tick 0")
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	s.lastTick = tickNumber
	log.Printf("Loaded checkpoint: resuming from tick %d", tickNumber)
	return tickNumber, nil
}

// Save persists a new checkpoint with the given tick number
// This demonstrates transaction management and atomic operations
func (s *SQLiteStore) Save(ctx context.Context, tickNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Don't save if tick number hasn't advanced
	if tickNumber <= s.lastTick {
		return nil
	}

	// Begin transaction for atomic checkpoint save
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return fmt.Errorf("failed to begin checkpoint transaction: %w", err)
	}

	// Ensure transaction is rolled back if we don't commit
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("Failed to rollback checkpoint transaction: %v", rollbackErr)
			}
		}
	}()

	// Execute save within transaction
	now := time.Now()
	description := fmt.Sprintf("checkpoint_tick_%d", tickNumber)
	
	_, err = tx.StmtContext(ctx, s.saveStmt).ExecContext(ctx, tickNumber, now, description)
	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit checkpoint transaction: %w", err)
	}

	// Update in-memory state
	s.lastTick = tickNumber
	
	log.Printf("💾 Checkpoint saved: tick %d at %s", tickNumber, now.Format(time.RFC3339))
	return nil
}

// GetLastTick returns the last saved tick number without querying the database
func (s *SQLiteStore) GetLastTick() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastTick
}

// Close closes the database connection and cleans up resources
func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close prepared statements
	if s.loadStmt != nil {
		s.loadStmt.Close()
	}
	if s.saveStmt != nil {
		s.saveStmt.Close()
	}

	// Close database connection
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return fmt.Errorf("failed to close checkpoint database: %w", err)
		}
	}

	log.Printf("Checkpoint store closed (last tick: %d)", s.lastTick)
	return nil
}

// Health checks if the checkpoint store is accessible
func (s *SQLiteStore) Health(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	// Test database connectivity with a simple query
	var result int
	err := s.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("checkpoint database health check failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("checkpoint database returned unexpected result: %d", result)
	}

	return nil
}

// GetStats returns checkpoint store statistics
func (s *SQLiteStore) GetStats(ctx context.Context) (CheckpointStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := CheckpointStats{
		LastTickNumber: s.lastTick,
		DatabasePath:   s.dbPath,
	}

	// Query total checkpoint count
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.tableName)
	err := s.db.QueryRowContext(ctx, countSQL).Scan(&stats.TotalCheckpoints)
	if err != nil {
		return stats, fmt.Errorf("failed to get checkpoint count: %w", err)
	}

	// Query oldest checkpoint
	oldestSQL := fmt.Sprintf("SELECT tick_number, updated_at FROM %s ORDER BY id ASC LIMIT 1", s.tableName)
	err = s.db.QueryRowContext(ctx, oldestSQL).Scan(&stats.OldestTick, &stats.OldestCheckpointTime)
	if err != nil && err != sql.ErrNoRows {
		return stats, fmt.Errorf("failed to get oldest checkpoint: %w", err)
	}

	// Query newest checkpoint
	newestSQL := fmt.Sprintf("SELECT tick_number, updated_at FROM %s ORDER BY id DESC LIMIT 1", s.tableName)
	err = s.db.QueryRowContext(ctx, newestSQL).Scan(&stats.NewestTick, &stats.NewestCheckpointTime)
	if err != nil && err != sql.ErrNoRows {
		return stats, fmt.Errorf("failed to get newest checkpoint: %w", err)
	}

	return stats, nil
}

// CheckpointStats contains statistics about the checkpoint store
type CheckpointStats struct {
	LastTickNumber        uint64    `json:"last_tick_number"`
	DatabasePath          string    `json:"database_path"`
	TotalCheckpoints      int64     `json:"total_checkpoints"`
	OldestTick            uint64    `json:"oldest_tick"`
	NewestTick            uint64    `json:"newest_tick"`
	OldestCheckpointTime  time.Time `json:"oldest_checkpoint_time"`
	NewestCheckpointTime  time.Time `json:"newest_checkpoint_time"`
}

// NewStore creates a checkpoint store based on the provided DSN
// This demonstrates the factory pattern for different storage backends
func NewStore(dsn string) (Store, error) {
	config := Config{
		DSN:        dsn,
		TableName:  "checkpoints",
		MaxRetries: 3,
	}

	// For now, we only support SQLite, but this could be extended
	// to support other databases like PostgreSQL
	if dsn == "" || dsn == "memory" {
		// In-memory store for testing
		return NewMemoryStore(), nil
	}

	// Default to SQLite
	return NewSQLiteStore(config)
}

// MemoryStore is a simple in-memory checkpoint store for testing
type MemoryStore struct {
	mu       sync.RWMutex
	lastTick uint64
}

// NewMemoryStore creates an in-memory checkpoint store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// Load returns the last saved tick from memory
func (m *MemoryStore) Load(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastTick, nil
}

// Save stores the tick number in memory
func (m *MemoryStore) Save(ctx context.Context, tickNumber uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastTick = tickNumber
	return nil
}

// Close is a no-op for memory store
func (m *MemoryStore) Close() error {
	return nil
}

// Health is always healthy for memory store
func (m *MemoryStore) Health(ctx context.Context) error {
	return nil
}