CREATE TABLE default.ticks
(
    `tick_number` UInt64,
    `height` UInt64,
    `block_hash` String CODEC(ZSTD(6)),
    `parent_hash` String CODEC(ZSTD(6)),
    `tx_count` UInt32,
    `payload_size_bytes` UInt64,
    `size_bytes` UInt64,
    `timestamp` UInt64,
    `processed_at` DateTime64(6),
    `proposer_id` String CODEC(ZSTD(6)),
    `proposer_key` String CODEC(ZSTD(6)),
    `chain_id` LowCardinality(String),
    `network` LowCardinality(String),
    `version` Int32,
    INDEX idx_block_hash block_hash TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_parent_hash parent_hash TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_chain chain_id TYPE set(100) GRANULARITY 1,
    INDEX idx_network network TYPE set(100) GRANULARITY 1,
    PROJECTION p_by_processed_full
    (
        SELECT
            processed_at,
            tick_number,
            height,
            block_hash,
            parent_hash,
            tx_count,
            payload_size_bytes,
            size_bytes,
            timestamp,
            proposer_id,
            proposer_key,
            chain_id,
            network,
            version
        ORDER BY
            processed_at,
            tick_number
    ),
    PROJECTION p_recent_slim
    (
        SELECT
            processed_at,
            tick_number,
            height,
            block_hash,
            tx_count,
            network,
            chain_id
        ORDER BY
            processed_at,
            tick_number
    ),
    PROJECTION p_by_block_hash
    (
        SELECT
            block_hash,
            processed_at,
            tick_number,
            height,
            parent_hash,
            tx_count,
            network,
            chain_id,
            version
        ORDER BY block_hash
    )
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
PARTITION BY toYYYYMMDD(processed_at)
PRIMARY KEY (processed_at, tick_number)
ORDER BY (processed_at, tick_number)
TTL processed_at + toIntervalDay(7) RECOMPRESS CODEC(ZSTD(15))
SETTINGS index_granularity = 2048, allow_nullable_key = 0, deduplicate_merge_projection_mode = 'rebuild'

-- Projections

-- Full projection for time-range and tail queries
ALTER TABLE default.ticks_v2
  ADD PROJECTION p_by_processed_full
  (
    SELECT
      processed_at, tick_number, height,
      block_hash, parent_hash,
      tx_count, payload_size_bytes, size_bytes,
      timestamp, proposer_id, proposer_key,
      chain_id, network, version
    ORDER BY processed_at, tick_number
  );

-- Slim projection for "recent list" views
ALTER TABLE default.ticks_v2
  ADD PROJECTION p_recent_slim
  (
    SELECT
      processed_at, tick_number, height,
      block_hash, tx_count, network, chain_id
    ORDER BY processed_at, tick_number
  );

-- Projection for fast point lookups by block_hash
ALTER TABLE default.ticks_v2
  ADD PROJECTION p_by_block_hash
  (
    SELECT
      block_hash, processed_at, tick_number, height,
      parent_hash, tx_count, network, chain_id, version
    ORDER BY block_hash
  );

-- Materialize them now
ALTER TABLE default.ticks_v2 MATERIALIZE PROJECTION p_by_processed_full;
ALTER TABLE default.ticks_v2 MATERIALIZE PROJECTION p_recent_slim;
ALTER TABLE default.ticks_v2 MATERIALIZE PROJECTION p_by_block_hash;
