-- TimescaleDB schema for market_prices table
-- Based on the market-price-ohlc-spec.md specification

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit; -- optional if using toolkit hyperfunctions

CREATE TABLE IF NOT EXISTS market_prices (
  market_id UUID NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  price NUMERIC(18,8) NOT NULL,
  PRIMARY KEY (market_id, ts)
);

SELECT create_hypertable('market_prices', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_market_prices_mkt_ts ON market_prices (market_id, ts DESC);

-- Optional retention & compression policies
-- SELECT add_retention_policy('market_prices', INTERVAL '90 days', if_not_exists => TRUE);
-- ALTER TABLE market_prices SET (timescaledb.compress, timescaledb.compress_segmentby = 'market_id');
-- SELECT add_compression_policy('market_prices', INTERVAL '7 days', if_not_exists => TRUE);

-- Example insertion query (for reference):
-- INSERT INTO market_prices (market_id, ts, price)
-- VALUES ($1::uuid, NOW(), $2::numeric)
-- ON CONFLICT DO NOTHING; -- avoid duplicates on same ts

-- Gap-filled OHLC query (for reference):
-- WITH buckets AS (
--   SELECT time_bucket_gapfill($1::interval, ts, start => $3, finish => $4) AS bucket,
--          ts,
--          price
--   FROM market_prices
--   WHERE market_id = $2 AND ts >= $3 - $1::interval AND ts < $4
-- )
-- SELECT
--   bucket AS t,
--   first(price, ts) AS o,
--   max(price)       AS h,
--   min(price)       AS l,
--   last(price, ts)  AS c
-- FROM buckets
-- GROUP BY bucket
-- ORDER BY bucket ASC;

