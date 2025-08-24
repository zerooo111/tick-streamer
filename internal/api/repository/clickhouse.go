package repository

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/zerooo111/tick-streamer/internal/config"
)

type TickData struct {
	TickNumber            uint64    `json:"tick_number" ch:"tick_number"`
	TimestampUS           int64     `json:"timestamp_us" ch:"timestamp_us"`
	VDFInput             string    `json:"vdf_input" ch:"vdf_input"`
	VDFOutput            string    `json:"vdf_output" ch:"vdf_output"`
	VDFIterations        uint64    `json:"vdf_iterations" ch:"vdf_iterations"`
	VDFProof             string    `json:"vdf_proof" ch:"vdf_proof"`
	PreviousOutput       string    `json:"previous_output" ch:"previous_output"`
	TransactionBatchHash string    `json:"transaction_batch_hash" ch:"transaction_batch_hash"`
	TransactionCount     int32     `json:"transaction_count" ch:"transaction_count"`
	ProcessedAt          time.Time `json:"processed_at" ch:"processed_at"`
	IngestionTS          int64     `json:"ingestion_ts" ch:"ingestion_ts"`
	Version              int32     `json:"version" ch:"version"`
	Transactions         []TransactionData `json:"transactions"`
}

type TransactionData struct {
	TickNumber          uint64    `json:"tick_number" ch:"tick_number"`
	SequenceNumber      uint64    `json:"sequence_number" ch:"sequence_number"`
	TxHash              string    `json:"tx_hash" ch:"tx_hash"`
	TxID                string    `json:"tx_id" ch:"tx_id"`
	Nonce               uint64    `json:"nonce" ch:"nonce"`
	Payload             string    `json:"payload" ch:"payload"`
	Timestamp           uint64    `json:"timestamp" ch:"timestamp"`
	PublicKey           string    `json:"public_key" ch:"public_key"`
	Signature           string    `json:"signature" ch:"signature"`
	IngestionTimestamp  uint64    `json:"ingestion_timestamp" ch:"ingestion_timestamp"`
	ProcessedAt         time.Time `json:"processed_at" ch:"processed_at"`
	PayloadSize         int32     `json:"payload_size" ch:"payload_size"`
	PayloadType         string    `json:"payload_type" ch:"payload_type"`
	Version             int32     `json:"version" ch:"version"`
}

type ChainStateData struct {
	ChainHeight      string                `json:"chain_height"`
	TotalTransactions string               `json:"total_transactions"`
	RecentTicks      []TickData            `json:"recent_ticks"`
	TxToTickSample   map[string]string     `json:"tx_to_tick_sample"`
}

type ClickHouseRepository struct {
	conn clickhouse.Conn
}

func NewClickHouseRepository(cfg *config.Config) (*ClickHouseRepository, error) {
	// Get ClickHouse configuration from environment
	host := getEnvOrDefault("CLICKHOUSE_HOST", "")
	port := getEnvOrDefaultInt("CLICKHOUSE_PORT", 9440)
	database := getEnvOrDefault("CLICKHOUSE_DATABASE", "default")
	username := getEnvOrDefault("CLICKHOUSE_USERNAME", "default")
	password := getEnvOrDefault("CLICKHOUSE_PASSWORD", "")
	
	// Check if ClickHouse is configured
	if host == "" || host == "your-clickhouse-host" {
		return &ClickHouseRepository{conn: nil}, nil // Return with nil connection
	}
	
	// Create ClickHouse connection
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		TLS: &tls.Config{
			InsecureSkipVerify: false,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:      30 * time.Second,
		MaxOpenConns:     5,
		MaxIdleConns:     2,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})
	
	if err != nil {
		fmt.Printf("⚠️ Warning: Failed to connect to ClickHouse: %v (falling back to REST API)\n", err)
		return &ClickHouseRepository{conn: nil}, nil
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := conn.Ping(ctx); err != nil {
		fmt.Printf("⚠️ Warning: Failed to ping ClickHouse: %v (falling back to REST API)\n", err)
		return &ClickHouseRepository{conn: nil}, nil
	}

	fmt.Println("✅ Connected to ClickHouse successfully!")
	return &ClickHouseRepository{conn: conn}, nil
}

func (r *ClickHouseRepository) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *ClickHouseRepository) GetTick(ctx context.Context, tickNumber uint64) (*TickData, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("ClickHouse not available")
	}
	
	query := `
		SELECT
			tick_number,
			timestamp_us,
			vdf_input,
			vdf_output,
			vdf_iterations,
			vdf_proof,
			previous_output,
			transaction_batch_hash,
			transaction_count,
			processed_at,
			ingestion_ts,
			version
		FROM ticks FINAL
		WHERE tick_number = $1 AND version > 0
		ORDER BY version DESC
		LIMIT 1
	`
	
	row := r.conn.QueryRow(ctx, query, tickNumber)
	
	var tick TickData
	err := row.Scan(
		&tick.TickNumber,
		&tick.TimestampUS,
		&tick.VDFInput,
		&tick.VDFOutput,
		&tick.VDFIterations,
		&tick.VDFProof,
		&tick.PreviousOutput,
		&tick.TransactionBatchHash,
		&tick.TransactionCount,
		&tick.ProcessedAt,
		&tick.IngestionTS,
		&tick.Version,
	)
	
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, fmt.Errorf("tick not found")
		}
		return nil, fmt.Errorf("failed to scan tick: %w", err)
	}

	// Get transactions for this tick
	transactions, err := r.getTransactionsForTick(ctx, tickNumber)
	if err != nil {
		// Log warning but don't fail - tick data is still valid
		fmt.Printf("Warning: failed to get transactions for tick %d: %v\n", tickNumber, err)
	}
	tick.Transactions = transactions

	return &tick, nil
}

func (r *ClickHouseRepository) GetTransaction(ctx context.Context, txHash string) (*TransactionData, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("ClickHouse not available")
	}
	query := `
		SELECT
			tick_number,
			sequence_number,
			tx_hash,
			tx_id,
			nonce,
			payload,
			timestamp,
			public_key,
			signature,
			ingestion_timestamp,
			processed_at,
			payload_size,
			payload_type,
			version
		FROM transactions FINAL
		WHERE tx_hash = $1 AND version > 0
		ORDER BY version DESC
		LIMIT 1
	`
	
	row := r.conn.QueryRow(ctx, query, txHash)
	
	var tx TransactionData
	err := row.Scan(
		&tx.TickNumber,
		&tx.SequenceNumber,
		&tx.TxHash,
		&tx.TxID,
		&tx.Nonce,
		&tx.Payload,
		&tx.Timestamp,
		&tx.PublicKey,
		&tx.Signature,
		&tx.IngestionTimestamp,
		&tx.ProcessedAt,
		&tx.PayloadSize,
		&tx.PayloadType,
		&tx.Version,
	)
	
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, fmt.Errorf("transaction not found")
		}
		return nil, fmt.Errorf("failed to scan transaction: %w", err)
	}

	return &tx, nil
}

func (r *ClickHouseRepository) GetRecentTicks(ctx context.Context, limit int) ([]TickData, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("ClickHouse not available")
	}
	
	if limit <= 0 || limit > 1000 {
		limit = 50
	}
	
	query := `
		SELECT
			tick_number,
			timestamp_us,
			vdf_input,
			vdf_output,
			vdf_iterations,
			vdf_proof,
			previous_output,
			transaction_batch_hash,
			transaction_count,
			processed_at,
			ingestion_ts,
			version
		FROM ticks FINAL
		WHERE version > 0
		ORDER BY tick_number DESC
		LIMIT $1
	`
	
	rows, err := r.conn.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent ticks: %w", err)
	}
	defer rows.Close()

	var ticks []TickData
	for rows.Next() {
		var tick TickData
		err := rows.Scan(
			&tick.TickNumber,
			&tick.TimestampUS,
			&tick.VDFInput,
			&tick.VDFOutput,
			&tick.VDFIterations,
			&tick.VDFProof,
			&tick.PreviousOutput,
			&tick.TransactionBatchHash,
			&tick.TransactionCount,
			&tick.ProcessedAt,
			&tick.IngestionTS,
			&tick.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan tick row: %w", err)
		}
		ticks = append(ticks, tick)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return ticks, nil
}

func (r *ClickHouseRepository) GetChainState(ctx context.Context, tickLimit *int) (*ChainStateData, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("ClickHouse not available")
	}
	// Get chain height (latest tick number)
	var chainHeight uint64
	heightQuery := `SELECT MAX(tick_number) FROM ticks FINAL WHERE version > 0`
	row := r.conn.QueryRow(ctx, heightQuery)
	if err := row.Scan(&chainHeight); err != nil {
		return nil, fmt.Errorf("failed to get chain height: %w", err)
	}

	// Get total transactions count
	var totalTx uint64
	txCountQuery := `SELECT COUNT(*) FROM transactions FINAL WHERE version > 0`
	row = r.conn.QueryRow(ctx, txCountQuery)
	if err := row.Scan(&totalTx); err != nil {
		return nil, fmt.Errorf("failed to get transaction count: %w", err)
	}

	// Get recent ticks
	limit := 10
	if tickLimit != nil && *tickLimit > 0 && *tickLimit <= 100 {
		limit = *tickLimit
	}
	
	recentTicks, err := r.GetRecentTicks(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent ticks: %w", err)
	}

	// Create a sample of tx to tick mappings
	sampleQuery := `
		SELECT tx_hash, tick_number 
		FROM transactions FINAL 
		WHERE version > 0 
		ORDER BY tick_number DESC 
		LIMIT 10
	`
	
	rows, err := r.conn.Query(ctx, sampleQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query tx sample: %w", err)
	}
	defer rows.Close()

	txToTickSample := make(map[string]string)
	for rows.Next() {
		var txHash string
		var tickNum uint64
		if err := rows.Scan(&txHash, &tickNum); err != nil {
			continue // Skip errors in sample data
		}
		txToTickSample[txHash] = fmt.Sprintf("%d", tickNum)
	}

	return &ChainStateData{
		ChainHeight:       fmt.Sprintf("%d", chainHeight),
		TotalTransactions: fmt.Sprintf("%d", totalTx),
		RecentTicks:       recentTicks,
		TxToTickSample:    txToTickSample,
	}, nil
}

func (r *ClickHouseRepository) getTransactionsForTick(ctx context.Context, tickNumber uint64) ([]TransactionData, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("ClickHouse not available")
	}
	query := `
		SELECT
			tick_number,
			sequence_number,
			tx_hash,
			tx_id,
			nonce,
			payload,
			timestamp,
			public_key,
			signature,
			ingestion_timestamp,
			processed_at,
			payload_size,
			payload_type,
			version
		FROM transactions FINAL
		WHERE tick_number = $1 AND version > 0
		ORDER BY sequence_number ASC
	`
	
	rows, err := r.conn.Query(ctx, query, tickNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []TransactionData
	for rows.Next() {
		var tx TransactionData
		err := rows.Scan(
			&tx.TickNumber,
			&tx.SequenceNumber,
			&tx.TxHash,
			&tx.TxID,
			&tx.Nonce,
			&tx.Payload,
			&tx.Timestamp,
			&tx.PublicKey,
			&tx.Signature,
			&tx.IngestionTimestamp,
			&tx.ProcessedAt,
			&tx.PayloadSize,
			&tx.PayloadType,
			&tx.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// Helper functions for environment variable handling
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}