-- TimescaleDB schema for transactions table  
-- Optimized for time-series data with minimal parsing overhead

-- Create the main transactions table
CREATE TABLE transactions (
    tick_number          BIGINT NOT NULL,
    sequence_number      BIGINT NOT NULL,
    tx_hash              TEXT NOT NULL,
    tx_id                TEXT NOT NULL,
    nonce                BIGINT NOT NULL,
    payload              BYTEA, -- Store raw payload bytes
    timestamp_us         BIGINT NOT NULL,
    public_key           TEXT NOT NULL,
    signature            TEXT NOT NULL,
    ingestion_timestamp  BIGINT NOT NULL,
    processed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_size         INTEGER NOT NULL DEFAULT 0,
    payload_type         TEXT,
    version              INTEGER NOT NULL DEFAULT 1,
    
    PRIMARY KEY (processed_at, tick_number, sequence_number)
);

-- Convert to hypertable partitioned by time
SELECT create_hypertable('transactions', 'processed_at',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_transactions_tick_number ON transactions (tick_number);
CREATE INDEX IF NOT EXISTS idx_transactions_tx_hash ON transactions USING HASH (tx_hash);
CREATE INDEX IF NOT EXISTS idx_transactions_tx_id ON transactions USING HASH (tx_id);
CREATE INDEX IF NOT EXISTS idx_transactions_public_key ON transactions USING HASH (public_key);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions (timestamp_us);
CREATE INDEX IF NOT EXISTS idx_transactions_payload_type ON transactions (payload_type);

-- Add aggressive compression policy (compress chunks older than 2 hours due to high volume)
SELECT add_compression_policy('transactions', INTERVAL '2 hours', if_not_exists => TRUE);

-- Create continuous aggregates for transaction analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', processed_at) AS hour,
    payload_type,
    COUNT(*) as tx_count,
    AVG(payload_size) as avg_payload_size,
    SUM(payload_size) as total_payload_bytes,
    COUNT(DISTINCT public_key) as unique_signers,
    MIN(tick_number) as min_tick_number,
    MAX(tick_number) as max_tick_number
FROM transactions
GROUP BY hour, payload_type
WITH NO DATA;

-- Create continuous aggregate for tick-level transaction summaries
CREATE MATERIALIZED VIEW IF NOT EXISTS tick_tx_summary
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('5 minutes', processed_at) AS bucket,
    tick_number,
    COUNT(*) as tx_count,
    COUNT(DISTINCT public_key) as unique_signers,
    AVG(payload_size) as avg_payload_size,
    SUM(payload_size) as total_payload_bytes
FROM transactions
GROUP BY bucket, tick_number
WITH NO DATA;

-- Refresh policies for continuous aggregates
SELECT add_continuous_aggregate_policy('transactions_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour', 
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('tick_tx_summary',
    start_offset => INTERVAL '30 minutes',
    end_offset => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '15 minutes', 
    if_not_exists => TRUE
);