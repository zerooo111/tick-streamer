package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"google.golang.org/protobuf/proto"
	
	"github.com/zerooo111/tick-streamer/internal/config"
	pb "github.com/zerooo111/tick-streamer/proto"
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
		SELECT tick_number, height, block_hash, parent_hash, tx_count, 
			   payload_size_bytes, size_bytes, timestamp_us, processed_at,
			   proposer_id, proposer_key, chain_id, network, version
		FROM ticks 
		WHERE tick_number = $1 AND version > 0
		ORDER BY processed_at DESC
		LIMIT 1`
	
	var tick TickData
	var timestampUs int64
	
	err := r.db.QueryRowContext(ctx, tickQuery, tickNumber).Scan(
		&tick.TickNumber, &tick.Height, &tick.BlockHash, &tick.ParentHash,
		&tick.TxCount, &tick.PayloadSizeBytes, &tick.SizeBytes,
		&timestampUs, &tick.ProcessedAt, &tick.ProposerID, &tick.ProposerKey,
		&tick.ChainID, &tick.Network, &tick.Version,
	)
	
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("tick %d not found", tickNumber)
		}
		return nil, fmt.Errorf("failed to query tick: %w", err)
	}
	
	tick.Timestamp = uint64(timestampUs)
	
	// Query associated transactions
	txQuery := `
		SELECT tick_number, sequence_number, tx_hash, tx_id, nonce,
			   encode(payload, 'hex') as payload, timestamp_us, public_key, signature,
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
		var txTimestampUs int64
		var ingestionTimestampUs int64
		
		err := rows.Scan(
			&tx.TickNumber, &tx.SequenceNumber, &tx.TxHash, &tx.TxID,
			&tx.Nonce, &tx.Payload, &txTimestampUs, &tx.PublicKey, &tx.Signature,
			&ingestionTimestampUs, &tx.ProcessedAt, &tx.PayloadSize,
			&tx.PayloadType, &tx.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}
		
		tx.Timestamp = uint64(txTimestampUs)
		tx.IngestionTimestamp = uint64(ingestionTimestampUs)
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
		SELECT tick_number, timestamp_us, processed_at, transaction_count, raw_data
		FROM raw_ticks 
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
		var tickNumber uint64
		var timestampUs int64
		var processedAt time.Time
		var transactionCount int32
		var rawData []byte
		
		err := rows.Scan(&tickNumber, &timestampUs, &processedAt, &transactionCount, &rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to scan raw tick: %w", err)
		}
		
		// Deserialize protobuf data
		var pbTick pb.Tick
		if err := proto.Unmarshal(rawData, &pbTick); err != nil {
			return nil, fmt.Errorf("failed to unmarshal tick protobuf: %w", err)
		}
		
		// Convert protobuf to API format
		tick := TickData{
			TickNumber:       tickNumber,
			Timestamp:        pbTick.Timestamp,
			TxCount:          uint32(len(pbTick.Transactions)),
			ProcessedAt:      processedAt,
			// Set reasonable defaults for fields not in raw data
			Height:           0,
			BlockHash:        "",
			ParentHash:       "",
			PayloadSizeBytes: 0,
			SizeBytes:        uint64(len(rawData)),
			ProposerID:       "",
			ProposerKey:      "",
			ChainID:          "mainnet",
			Network:          "qubic",
			Version:          1,
		}
		ticks = append(ticks, tick)
	}
	
	return ticks, nil
}

// GetRecentTransactions retrieves the most recent transactions
func (r *TimescaleDBRepository) GetRecentTransactions(ctx context.Context, limit int) ([]TransactionData, error) {
	if r.db == nil {
		return nil, fmt.Errorf("TimescaleDB connection not available")
	}
	
	// Ultra-fast single column ordering - processed_at is indexed for time-series
	query := `
		SELECT tick_number, sequence_number, timestamp_us, processed_at, raw_data
		FROM raw_transactions 
		WHERE version > 0
		ORDER BY processed_at DESC
		LIMIT $1`
	
	// Add query timeout for faster failure detection
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	
	rows, err := r.db.QueryContext(queryCtx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent transactions: %w", err)
	}
	defer rows.Close()
	
	var transactions []TransactionData
	for rows.Next() {
		var tickNumber uint64
		var sequenceNumber uint32
		var timestampUs int64
		var processedAt time.Time
		var rawData []byte
		
		err := rows.Scan(&tickNumber, &sequenceNumber, &timestampUs, &processedAt, &rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to scan raw transaction: %w", err)
		}
		
		// Deserialize protobuf data - now correctly as Transaction (fixed in parser)
		var pbTx pb.Transaction
		if err := proto.Unmarshal(rawData, &pbTx); err != nil {
			// Log error but continue with other transactions
			fmt.Printf("⚠️ Skipping corrupted transaction at tick %d, seq %d: %v\n", 
				tickNumber, sequenceNumber, err)
			continue
		}
		
		// Generate hash from transaction data (since it's not in protobuf)
		hashBytes := sha256.Sum256(rawData)
		txHash := hex.EncodeToString(hashBytes[:])
		
		// Convert protobuf to API format
		tx := TransactionData{
			TickNumber:         tickNumber,
			SequenceNumber:     uint64(sequenceNumber),
			Timestamp:          pbTx.Timestamp,
			TxHash:            txHash,
			TxID:              pbTx.TxId,
			Nonce:             pbTx.Nonce,
			Payload:           string(pbTx.Payload),
			PublicKey:         hex.EncodeToString(pbTx.PublicKey),
			Signature:         hex.EncodeToString(pbTx.Signature),
			IngestionTimestamp: uint64(timestampUs),
			ProcessedAt:       processedAt,
			PayloadSize:       int32(len(pbTx.Payload)),
			PayloadType:       "", // Set default
			Version:           1,
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
	
	// Get chain height and total transactions
	statsQuery := `
		SELECT 
			COALESCE(MAX(tick_number), 0) as chain_height,
			COALESCE(SUM(tx_count), 0) as total_transactions
		FROM ticks 
		WHERE version > 0`
	
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
	
	query := `
		SELECT tick_number, sequence_number, tx_hash, tx_id, nonce,
			   encode(payload, 'hex') as payload, timestamp_us, public_key, signature,
			   ingestion_timestamp, processed_at, payload_size, payload_type, version
		FROM transactions 
		WHERE tx_hash = $1 AND version > 0
		LIMIT 1`
	
	var tx TransactionData
	var timestampUs int64
	var ingestionTimestampUs int64
	
	err := r.db.QueryRowContext(ctx, query, txHash).Scan(
		&tx.TickNumber, &tx.SequenceNumber, &tx.TxHash, &tx.TxID,
		&tx.Nonce, &tx.Payload, &timestampUs, &tx.PublicKey, &tx.Signature,
		&ingestionTimestampUs, &tx.ProcessedAt, &tx.PayloadSize,
		&tx.PayloadType, &tx.Version,
	)
	
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("transaction %s not found", txHash)
		}
		return nil, fmt.Errorf("failed to query transaction: %w", err)
	}
	
	tx.Timestamp = uint64(timestampUs)
	tx.IngestionTimestamp = uint64(ingestionTimestampUs)
	
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