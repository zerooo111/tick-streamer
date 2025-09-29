-- TimescaleDB schema for ticks table
-- Optimized for time-series data with minimal parsing overhead

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create the main ticks table
CREATE TABLE ticks (
    tick_number              BIGINT NOT NULL,
    timestamp_us             BIGINT NOT NULL,
    vdf_input                TEXT,
    vdf_output               TEXT,
    vdf_proof                TEXT,
    vdf_iterations           BIGINT,
    transaction_batch_hash   TEXT,
    previous_output          TEXT,
    tx_count                 INTEGER NOT NULL DEFAULT 0,
    processed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version                  INTEGER NOT NULL DEFAULT 1,
    
    PRIMARY KEY (processed_at,tick_number)
);

-- Convert to hypertable partitioned by time
SELECT create_hypertable('ticks', 'processed_at', 
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ESSENTIAL INDEXES ONLY - based on actual query patterns
-- 1. For GetTick() - queries by tick_number
CREATE INDEX IF NOT EXISTS idx_ticks_tick_number ON ticks (tick_number);

-- REMOVED UNNECESSARY INDEXES:
-- - idx_ticks_timestamp (not used in queries)
-- - idx_ticks_processed_at (covered by primary key)
-- - idx_ticks_vdf_output (not used in queries)
-- - idx_ticks_tx_count (not used in queries)

-- Add compression policy (compress chunks older than 6 hours for better storage efficiency)
SELECT add_compression_policy('ticks', INTERVAL '6 hours', if_not_exists => TRUE);

-- Create continuous aggregates for common analytics queries
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', processed_at) AS hour,
    COUNT(*) as tick_count,
    AVG(tx_count) as avg_tx_per_tick,
    MIN(tick_number) as min_tick_number,
    MAX(tick_number) as max_tick_number,
    COUNT(CASE WHEN vdf_iterations > 0 THEN 1 END) as ticks_with_vdf
FROM ticks
GROUP BY hour
WITH NO DATA;

-- Refresh continuous aggregate policy
SELECT add_continuous_aggregate_policy('ticks_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);