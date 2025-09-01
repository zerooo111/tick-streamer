package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	
	"github.com/zerooo111/tick-streamer/internal/config"
)

type TimescaleDBRepository struct {
	db *sql.DB
}

func NewTimescaleDBRepository(cfg *config.Config) (*TimescaleDBRepository, error) {
	// Get TimescaleDB configuration from environment variables
	host := getTSEnvOrDefault("TIMESCALEDB_HOST", "")
	port := getTSEnvOrDefaultInt("TIMESCALEDB_PORT", 5432)
	database := getTSEnvOrDefault("TIMESCALEDB_DATABASE", "tick_streamer")
	username := getTSEnvOrDefault("TIMESCALEDB_USERNAME", "postgres")
	password := getTSEnvOrDefault("TIMESCALEDB_PASSWORD", "")
	sslMode := getTSEnvOrDefault("TIMESCALEDB_SSL_MODE", "prefer")
	
	// Check if TimescaleDB is configured
	if host == "" || password == "" {
		fmt.Println("⚠️ TimescaleDB not configured (missing TIMESCALEDB_HOST or TIMESCALEDB_PASSWORD), falling back to REST API only")
		return &TimescaleDBRepository{db: nil}, nil // Return with nil connection
	}
	
	// Check for full connection string first
	connStr := getTSEnvOrDefault("TIMESCALEDB_CONNECTION_STRING", "")
	if connStr == "" {
		// Build from individual parameters
		fmt.Printf("🔗 Connecting to TimescaleDB at %s:%d as %s\n", host, port, username)
		connStr = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			host, port, username, password, database, sslMode)
	} else {
		fmt.Printf("🔗 Connecting to TimescaleDB using connection string...\n")
	}
	
	// Create database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("⚠️ Warning: Failed to open TimescaleDB connection: %v (falling back to REST API)\n", err)
		return &TimescaleDBRepository{db: nil}, nil // Return with nil connection for fallback
	}
	
	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(time.Minute * 30)
	
	// Test connection
	testCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := db.PingContext(testCtx); err != nil {
		fmt.Printf("⚠️ Warning: Failed to ping TimescaleDB: %v (falling back to REST API)\n", err)
		db.Close()
		return &TimescaleDBRepository{db: nil}, nil // Return with nil connection for fallback
	}
	
	fmt.Println("✅ Connected to TimescaleDB successfully")
	
	return &TimescaleDBRepository{
		db: db,
	}, nil
}

// GetTick retrieves a specific tick by number
func (r *TimescaleDBRepository) GetTick(ctx context.Context, tickNumber uint64) (*TickData, error) {
	if r.db == nil {
		return nil, fmt.Errorf("TimescaleDB connection not available")
	}
	
	// Query main tick data
	tickQuery := `
		SELECT tick_number, timestamp_us, vdf_input, vdf_output, vdf_proof, vdf_iterations,
			   transaction_batch_hash, previous_output, tx_count, processed_at, version
		FROM ticks 
		WHERE tick_number = $1 AND version > 0
		ORDER BY processed_at DESC
		LIMIT 1`
	
	var tick TickData
	var vdfInput, vdfOutput, vdfProof, transactionBatchHash, previousOutput sql.NullString
	var vdfIterations sql.NullInt64
	
	err := r.db.QueryRowContext(ctx, tickQuery, tickNumber).Scan(
		&tick.TickNumber, &tick.Timestamp, &vdfInput, &vdfOutput, &vdfProof, &vdfIterations,
		&transactionBatchHash, &previousOutput, &tick.TxCount, &tick.ProcessedAt, &tick.Version,
	)
	
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("tick %d not found", tickNumber)
		}
		return nil, fmt.Errorf("failed to query tick: %w", err)
	}
	
	// Handle nullable fields
	if vdfInput.Valid {
		tick.VDFInput = vdfInput.String
	}
	if vdfOutput.Valid {
		tick.VDFOutput = vdfOutput.String
	}
	if vdfProof.Valid {
		tick.VDFProof = vdfProof.String
	}
	if vdfIterations.Valid {
		tick.VDFIterations = uint64(vdfIterations.Int64)
	}
	if transactionBatchHash.Valid {
		tick.TransactionBatchHash = transactionBatchHash.String
	}
	if previousOutput.Valid {
		tick.PreviousOutput = previousOutput.String
	}
	
	// Query associated transactions
	txQuery := `
		SELECT tick_number, sequence_number, tx_hash, tx_id, nonce,
			   encode(payload, 'hex') as payload, timestamp_us, 
			   encode(public_key, 'hex') as public_key, encode(signature, 'hex') as signature,
			   ingestion_timestamp, processed_at, payload_size, payload_type, version
		FROM transactions 
		WHERE tick_number = $1 AND version > 0
		ORDER BY sequence_number`
	
	rows, err := r.db.QueryContext(ctx, txQuery, tickNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()
	
	var transactions []TransactionData
	for rows.Next() {
		var tx TransactionData
		var payloadType sql.NullString
		
		err := rows.Scan(
			&tx.TickNumber, &tx.SequenceNumber, &tx.TxHash, &tx.TxID,
			&tx.Nonce, &tx.Payload, &tx.Timestamp, &tx.PublicKey, &tx.Signature,
			&tx.IngestionTimestamp, &tx.ProcessedAt, &tx.PayloadSize,
			&payloadType, &tx.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}
		
		if payloadType.Valid {
			tx.PayloadType = payloadType.String
		}
		
		// Decode payload to human-readable format
		tx.PayloadDecoded = DecodePayload(tx.Payload)
		
		transactions = append(transactions, tx)
	}
	
	tick.Transactions = transactions
	return &tick, nil
}

// GetRecentTicks retrieves the most recent ticks
func (r *TimescaleDBRepository) GetRecentTicks(ctx context.Context, limit int) ([]TickData, error) {
	if r.db == nil {
		return nil, fmt.Errorf("TimescaleDB connection not available")
	}
	
	query := `
		SELECT tick_number, timestamp_us, vdf_input, vdf_output, vdf_proof, vdf_iterations,
			   transaction_batch_hash, previous_output, tx_count, processed_at, version
		FROM ticks 
		WHERE version > 0
		ORDER BY processed_at DESC, tick_number DESC
		LIMIT $1`
	
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent ticks: %w", err)
	}
	defer rows.Close()
	
	var ticks []TickData
	for rows.Next() {
		var tick TickData
		var vdfInput, vdfOutput, vdfProof, transactionBatchHash, previousOutput sql.NullString
		var vdfIterations sql.NullInt64
		
		err := rows.Scan(
			&tick.TickNumber, &tick.Timestamp, &vdfInput, &vdfOutput, &vdfProof, &vdfIterations,
			&transactionBatchHash, &previousOutput, &tick.TxCount, &tick.ProcessedAt, &tick.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan tick: %w", err)
		}
		
		// Handle nullable fields
		if vdfInput.Valid {
			tick.VDFInput = vdfInput.String
		}
		if vdfOutput.Valid {
			tick.VDFOutput = vdfOutput.String
		}
		if vdfProof.Valid {
			tick.VDFProof = vdfProof.String
		}
		if vdfIterations.Valid {
			tick.VDFIterations = uint64(vdfIterations.Int64)
		}
		if transactionBatchHash.Valid {
			tick.TransactionBatchHash = transactionBatchHash.String
		}
		if previousOutput.Valid {
			tick.PreviousOutput = previousOutput.String
		}
		
		ticks = append(ticks, tick)
	}
	
	return ticks, nil
}

// GetRecentTransactions retrieves the most recent transactions
func (r *TimescaleDBRepository) GetRecentTransactions(ctx context.Context, limit int) ([]RecentTransactionData, error) {
	if r.db == nil {
		return nil, fmt.Errorf("TimescaleDB connection not available")
	}
	
	// Query from transactions table - only essential fields for recent transactions
	query := `
		SELECT sequence_number, tx_hash, tick_number, tx_id, timestamp_us
		FROM transactions 
		WHERE version > 0
		ORDER BY processed_at DESC
		LIMIT $1`
	
	// Add reasonable query timeout 
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	
	rows, err := r.db.QueryContext(queryCtx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent transactions: %w", err)
	}
	defer rows.Close()
	
	var transactions []RecentTransactionData
	for rows.Next() {
		var tx RecentTransactionData
		
		err := rows.Scan(
			&tx.SequenceNumber, &tx.TxHash, &tx.TickNumber, &tx.TxID, &tx.Timestamp,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}
		
		transactions = append(transactions, tx)
	}
	
	return transactions, nil
}

// GetChainState retrieves overall chain statistics
func (r *TimescaleDBRepository) GetChainState(ctx context.Context, tickLimit *int) (*ChainStateData, error) {
	if r.db == nil {
		return nil, fmt.Errorf("TimescaleDB connection not available")
	}
	
	// Get chain height from ticks and total transactions from transactions table
	statsQuery := `
		SELECT 
			COALESCE((SELECT MAX(tick_number) FROM ticks WHERE version > 0), 0) as chain_height,
			COALESCE((SELECT COUNT(*) FROM transactions WHERE version > 0), 0) as total_transactions`
	
	var chainHeight, totalTransactions uint64
	err := r.db.QueryRowContext(ctx, statsQuery).Scan(&chainHeight, &totalTransactions)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain stats: %w", err)
	}
	
	// Get recent ticks with limit
	limit := 10 // Default limit
	if tickLimit != nil {
		limit = *tickLimit
	}
	recentTicks, err := r.GetRecentTicks(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent ticks: %w", err)
	}
	
	// Get sample of transaction to tick mapping
	sampleQuery := `
		SELECT tx_hash, tick_number
		FROM transactions 
		WHERE version > 0
		ORDER BY processed_at DESC
		LIMIT 5`
	
	rows, err := r.db.QueryContext(ctx, sampleQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query tx sample: %w", err)
	}
	defer rows.Close()
	
	txToTickSample := make(map[string]string)
	for rows.Next() {
		var txHash string
		var tickNumber uint64
		err := rows.Scan(&txHash, &tickNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to scan tx sample: %w", err)
		}
		txToTickSample[txHash] = fmt.Sprintf("%d", tickNumber)
	}
	
	return &ChainStateData{
		ChainHeight:       fmt.Sprintf("%d", chainHeight),
		TotalTransactions: fmt.Sprintf("%d", totalTransactions),
		RecentTicks:       recentTicks,
		TxToTickSample:    txToTickSample,
	}, nil
}

// GetTransaction retrieves a specific transaction by hash
func (r *TimescaleDBRepository) GetTransaction(ctx context.Context, txHash string) (*TransactionData, error) {
	if r.db == nil {
		return nil, fmt.Errorf("TimescaleDB connection not available")
	}
	
	// Query from transactions table by tx_hash (supports partial hash matching)
	query := `
		SELECT tick_number, sequence_number, tx_hash, tx_id, nonce,
			   encode(payload, 'hex') as payload, timestamp_us,
			   encode(public_key, 'hex') as public_key, encode(signature, 'hex') as signature,
			   ingestion_timestamp, processed_at, payload_size, payload_type, version
		FROM transactions 
		WHERE tx_hash LIKE $1 AND version > 0
		ORDER BY processed_at DESC
		LIMIT 1`
	
	var tx TransactionData
	var payloadType sql.NullString
	
	// Support partial hash matching - if hash is short, add wildcard
	searchPattern := txHash
	if len(txHash) < 64 { // Full hash is 64 characters
		searchPattern = txHash + "%"
	}
	
	err := r.db.QueryRowContext(ctx, query, searchPattern).Scan(
		&tx.TickNumber, &tx.SequenceNumber, &tx.TxHash, &tx.TxID,
		&tx.Nonce, &tx.Payload, &tx.Timestamp, &tx.PublicKey, &tx.Signature,
		&tx.IngestionTimestamp, &tx.ProcessedAt, &tx.PayloadSize,
		&payloadType, &tx.Version,
	)
	
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("transaction %s not found", txHash)
		}
		return nil, fmt.Errorf("failed to query transaction: %w", err)
	}
	
	if payloadType.Valid {
		tx.PayloadType = payloadType.String
	}
	
	// Decode payload to human-readable format
	tx.PayloadDecoded = DecodePayload(tx.Payload)
	
	return &tx, nil
}

// Close closes the database connection
func (r *TimescaleDBRepository) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// Helper functions for TimescaleDB
func getTSEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getTSEnvOrDefaultInt(key string, defaultValue int) int {
	if valueStr := os.Getenv(key); valueStr != "" {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return defaultValue
}