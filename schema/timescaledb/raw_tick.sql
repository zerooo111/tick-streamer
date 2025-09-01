-- TimescaleDB schema for raw_ticks table
-- Optimized for storing raw protobuf data with minimal parsing overhead

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create the raw ticks table for storing protobuf data
CREATE TABLE raw_ticks (
    tick_number         BIGINT NOT NULL,
    timestamp_us        BIGINT NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    transaction_count   INTEGER NOT NULL DEFAULT 0,
    raw_data            BYTEA NOT NULL,  -- Raw protobuf bytes
    chain_id            TEXT NOT NULL DEFAULT 'mainnet',
    network             TEXT NOT NULL DEFAULT 'qubic',
    version             INTEGER NOT NULL DEFAULT 1,
    
    PRIMARY KEY (processed_at, tick_number)
);

-- Convert to hypertable partitioned by time
SELECT create_hypertable('raw_ticks', 'processed_at', 
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_raw_ticks_tick_number ON raw_ticks (tick_number);
CREATE INDEX IF NOT EXISTS idx_raw_ticks_timestamp ON raw_ticks (timestamp_us);
CREATE INDEX IF NOT EXISTS idx_raw_ticks_network_chain ON raw_ticks (network, chain_id);

-- Add compression policy (compress chunks older than 1 hour for better storage efficiency)
-- Raw data compresses very well
SELECT add_compression_policy('raw_ticks', INTERVAL '1 hour', if_not_exists => TRUE);