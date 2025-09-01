-- TimescaleDB schema for ticks table
-- Optimized for time-series data with minimal parsing overhead

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create the main ticks table
CREATE TABLE ticks (
    tick_number         BIGINT NOT NULL,
    height              BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    parent_hash         TEXT,
    tx_count            INTEGER NOT NULL DEFAULT 0,
    payload_size_bytes  BIGINT NOT NULL DEFAULT 0,
    size_bytes          BIGINT NOT NULL DEFAULT 0,
    timestamp_us        BIGINT NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    proposer_id         TEXT,
    proposer_key        TEXT,
    chain_id            TEXT NOT NULL DEFAULT 'mainnet',
    network             TEXT NOT NULL DEFAULT 'qubic',
    version             INTEGER NOT NULL DEFAULT 1,
    
    PRIMARY KEY (processed_at, tick_number)
);

-- Convert to hypertable partitioned by time
SELECT create_hypertable('ticks', 'processed_at', 
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_ticks_tick_number ON ticks (tick_number);
CREATE INDEX IF NOT EXISTS idx_ticks_block_hash ON ticks USING HASH (block_hash);
CREATE INDEX IF NOT EXISTS idx_ticks_timestamp ON ticks (timestamp_us);
CREATE INDEX IF NOT EXISTS idx_ticks_network_chain ON ticks (network, chain_id);

-- Add compression policy (compress chunks older than 6 hours for better storage efficiency)
SELECT add_compression_policy('ticks', INTERVAL '6 hours', if_not_exists => TRUE);

-- Create continuous aggregates for common analytics queries
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', processed_at) AS hour,
    network,
    chain_id,
    COUNT(*) as tick_count,
    AVG(tx_count) as avg_tx_per_tick,
    SUM(payload_size_bytes) as total_payload_bytes,
    MIN(tick_number) as min_tick_number,
    MAX(tick_number) as max_tick_number
FROM ticks
GROUP BY hour, network, chain_id
WITH NO DATA;

-- Refresh continuous aggregate policy
SELECT add_continuous_aggregate_policy('ticks_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);