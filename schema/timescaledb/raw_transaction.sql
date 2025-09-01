-- TimescaleDB schema for raw_transactions table
-- Optimized for storing raw protobuf transaction data with minimal parsing overhead

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create the raw transactions table for storing protobuf data
CREATE TABLE raw_transactions (
    tick_number         BIGINT NOT NULL,
    sequence_number     INTEGER NOT NULL,
    timestamp_us        BIGINT NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_data            BYTEA NOT NULL,  -- Raw protobuf transaction bytes
    chain_id            TEXT NOT NULL DEFAULT 'mainnet',
    network             TEXT NOT NULL DEFAULT 'qubic',
    version             INTEGER NOT NULL DEFAULT 1,
    
    PRIMARY KEY (processed_at, tick_number, sequence_number)
);

-- Convert to hypertable partitioned by time
SELECT create_hypertable('raw_transactions', 'processed_at', 
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_raw_transactions_tick_number ON raw_transactions (tick_number);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_timestamp ON raw_transactions (timestamp_us);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_network_chain ON raw_transactions (network, chain_id);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_tick_seq ON raw_transactions (tick_number, sequence_number);

-- Add compression policy (compress chunks older than 1 hour for better storage efficiency)
-- Raw transaction data compresses very well
SELECT add_compression_policy('raw_transactions', INTERVAL '1 hour', if_not_exists => TRUE);