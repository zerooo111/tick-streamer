-- These are clickhouse related queries to create the schema for the transaction table

CREATE TABLE default.transactions
(
    `tick_number` UInt64,
    `sequence_number` UInt64,
    `tx_hash` String CODEC(ZSTD(6)),
    `tx_id` String CODEC(ZSTD(6)),
    `nonce` UInt64,
    `payload` String CODEC(ZSTD(6)),
    `timestamp` UInt64,
    `public_key` String CODEC(ZSTD(6)),
    `signature` String CODEC(ZSTD(6)),
    `ingestion_timestamp` UInt64,
    `processed_at` DateTime64(6),
    `payload_size` Int32,
    `payload_type` LowCardinality(String),
    `version` Int32,
    INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_public_key public_key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_payload_type payload_type TYPE set(100) GRANULARITY 1,
    INDEX idx_tx_timestamp timestamp TYPE minmax GRANULARITY 8,
    PROJECTION p_by_processed_full
    (
        SELECT
            processed_at,
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
            payload_size,
            payload_type,
            version
        ORDER BY
            processed_at,
            tick_number,
            sequence_number
    ),
    PROJECTION p_recent_slim
    (
        SELECT
            processed_at,
            tx_hash,
            public_key,
            payload_type,
            payload_size
        ORDER BY
            processed_at,
            tick_number,
            sequence_number
    )
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
PARTITION BY toYYYYMMDD(processed_at)
PRIMARY KEY (processed_at, tick_number)
ORDER BY (processed_at, tick_number, sequence_number)
TTL processed_at + toIntervalDay(7) RECOMPRESS CODEC(ZSTD(15))
SETTINGS index_granularity = 2048, allow_nullable_key = 0, deduplicate_merge_projection_mode = 'rebuild'


-- Add projections
ALTER TABLE default.transactions_v2
  ADD PROJECTION p_by_processed_full
  (
    SELECT
      processed_at,
      tick_number, sequence_number,
      tx_hash, tx_id, nonce, payload, timestamp,
      public_key, signature, ingestion_timestamp,
      payload_size, payload_type, version
    ORDER BY processed_at, tick_number, sequence_number
  );

-- Slim projection for "recent list" queries
ALTER TABLE default.transactions_v2
  ADD PROJECTION p_recent_slim
  (
    SELECT
      processed_at,
      tx_hash, public_key, payload_type, payload_size
    ORDER BY processed_at, tick_number, sequence_number
  );

-- Materialize them immediately
ALTER TABLE default.transactions_v2 MATERIALIZE PROJECTION p_by_processed_full;
ALTER TABLE default.transactions_v2 MATERIALIZE PROJECTION p_recent_slim;
