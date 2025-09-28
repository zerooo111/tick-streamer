## Market Price Storage, OHLC Queries, Ingestion, and API/WS Plan

This document outlines the SQL schema and queries for storing market prices in TimescaleDB, retrieving gap-filled OHLC candles, a Go ingestor service specification, and the plan to add API and WebSocket endpoints.

Reference market stats endpoint: `http://44.194.22.128:8083/markets/87487a2a-319e-4aab-ab71-42edffc9aeee/stats`.

### 1) TimescaleDB SQL

- Single hypertable `market_prices` used for all timeframes; OHLC computed via `time_bucket` with gapfilling. No separate per-timeframe tables.

#### a) Create the price table

```sql
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
```

#### b) Insertion

- Insert on significant price changes (thresholded), and optionally a heartbeat row every 5–10s when idle.

```sql
INSERT INTO market_prices (market_id, ts, price)
VALUES ($1::uuid, NOW(), $2::numeric)
ON CONFLICT DO NOTHING; -- avoid duplicates on same ts
```

#### c) Gap-filled OHLC candles

- Ad-hoc OHLC using `time_bucket_gapfill` + `first/last/min/max`.
- This produces continuous buckets even if no writes occurred; prices can be carried forward using LOCF if needed.

```sql
-- Parameters:
-- $1 interval text (e.g., '1 minute', '5 minutes', '1 hour')
-- $2 uuid (market_id)
-- $3 timestamptz start
-- $4 timestamptz end

WITH buckets AS (
  SELECT time_bucket_gapfill($1::interval, ts, start => $3, finish => $4) AS bucket,
         ts,
         price
  FROM market_prices
  WHERE market_id = $2 AND ts >= $3 - $1::interval AND ts < $4
)
SELECT
  bucket AS t,
  first(price, ts) AS o,
  max(price)       AS h,
  min(price)       AS l,
  last(price, ts)  AS c
FROM buckets
GROUP BY bucket
ORDER BY bucket ASC;
```

- Optional: use Toolkit candlestick aggregate (simpler accessor functions):

```sql
SELECT
  bucket,
  open(cs)  AS o,
  high(cs)  AS h,
  low(cs)   AS l,
  close(cs) AS c
FROM (
  SELECT time_bucket($1::interval, ts) AS bucket,
         toolkit_experimental.candlestick_agg(ts, price, NULL) AS cs
  FROM market_prices
  WHERE market_id = $2 AND ts >= $3 AND ts < $4
  GROUP BY bucket
) s
ORDER BY bucket;
```

Notes:

- If using `time_bucket_gapfill` and you want strict continuity with no missing buckets, keep a small write heartbeat or use LOCF semantics in queries as appropriate for your UI.

### 2) Go Ingestor Service Specification

Goal: poll the trading engine stats endpoint and insert price points efficiently with write-shedding.

- Source URL format: `http://44.194.22.128:8083/markets/{marketId}/stats`.
- Poll cadence: 1s baseline; cap to ≤10 Hz if upstream supports push.
- Change threshold: insert only when `abs(newPrice - lastPrice) >= max(min_tick, epsilon * lastPrice)`; recommended `epsilon = 0.0001` (0.01%).
- Heartbeat: if no insert has occurred for 30s, insert the last seen price once (optional if queries do gapfill/LOCF).
- Resilience: retries with exponential backoff; metrics for success/error rates.
- DB: single prepared statement; connection pool sized ~ CPU cores; context deadlines (3s HTTP, 500ms DB).

Pseudo API types and flow:

```go
type StatsResponse struct {
  Code int `json:"code"`
  Data struct {
    MarketID  string  `json:"market_id"`
    MarkPrice float64 `json:"mark_price"`
  } `json:"data"`
}

type PriceIngestor struct {
  Client     *http.Client
  DB         *sql.DB
  BaseURL    string
  MarketID   string
  Interval   time.Duration // e.g., 1 * time.Second
  Epsilon    float64       // e.g., 0.0001
  MinTick    float64       // e.g., 0.0001
  lastPrice  atomic.Value  // float64
  lastInsert atomic.Value  // time.Time
}

func (p *PriceIngestor) Start(ctx context.Context) { /*
  - ticker := time.NewTicker(p.Interval)
  - on tick: GET BaseURL + "/markets/" + MarketID + "/stats"
  - if status 200 and Code==200: parse MarkPrice
  - compare to lastPrice with threshold; if pass OR heartbeat overdue -> INSERT
  - broadcast to WS channel (if integrated) regardless of DB decision
*/ }

// DB insert (prepared once):
// INSERT INTO market_prices (market_id, ts, price) VALUES ($1, NOW(), $2) ON CONFLICT DO NOTHING
```

### 3) API Route and WebSocket Plan

#### Candles REST endpoint

- Path: `GET /api/v1/me/markets/:marketId/candles?tf=1m&from=ISO&to=ISO`
- `tf` maps to an interval string; allowed: `1m,5m,15m,1h,4h,1d`.
- Use the gap-filled query above; default window = last 24h if no `from/to`.

Handler outline:

```go
// internal/api/handlers/candles.go
func (h *Handler) GetMarketCandles(c *gin.Context) {
  // parse marketId, tf -> interval, from/to -> time
  // query DB using the gap-filled OHLC SQL
  // return [{t, o, h, l, c}] with Cache-Control: public, max-age=5
}
```

Server route edit in `internal/api/server.go`:

```go
me.GET("/markets/:marketId/candles", s.handler.GetMarketCandles)
```

#### WebSocket live market stats endpoint

- Path: `GET /ws/markets/:marketId/price`
- Message format:

```json
{ "type": "price", "market_id": "<uuid>", "ts": "<iso>", "price": 199.0 }
```

Implementation notes:

- Reuse `internal/api/websocket` hub pattern; add a channel/topic keyed by `marketId`.
- On each poll success, broadcast the fresh `mark_price` immediately to subscribers.
- Backpressure: bounded send channels; drop slow clients; periodic pings already present in hub.

Frontend flow:

- On page load, fetch candles for timeframe and render chart.
- Open WS to `/ws/markets/:marketId/price`; on message, patch the last candle (update H/L/C) or append a new candle when the bucket changes.
