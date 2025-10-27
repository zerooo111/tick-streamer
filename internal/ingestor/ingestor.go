package ingestor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// StatsResponse mirrors the trading engine stats payload minimally
type StatsResponse struct {
	Code int `json:"code"`
	Data struct {
		MarketID  string  `json:"market_id"`
		MarkPrice float64 `json:"mark_price"`
	} `json:"data"`
}

// PriceIngestor pulls mark_price for a market and writes into TimescaleDB
type PriceIngestor struct {
	baseURL           string
	marketID          string
	client            *http.Client
	db                *sql.DB
	insertStmt        *sql.Stmt
	interval          time.Duration
	heartbeatInterval time.Duration
    stateMu           sync.Mutex
    state             map[string]*marketState // per-market state

    // Error tracking
    consecutiveErrors int
    lastErrorTime     time.Time
    totalErrors       int64
    totalSuccess      int64
}

type IngestorOptions struct {
	Interval          time.Duration
	HeartbeatInterval time.Duration
	HTTPTimeout       time.Duration
}

type marketState struct {
    lastPrice  float64
    lastInsert time.Time
}

// NewPriceIngestor constructs an ingestor. Caller must Close the prepared statement via Stop if added.
func NewPriceIngestor(db *sql.DB, baseURL, marketID string, opts IngestorOptions) (*PriceIngestor, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
    if baseURL == "" {
        return nil, fmt.Errorf("baseURL is required")
	}

	interval := opts.Interval
	if interval <= 0 {
		interval = time.Second
	}
	heartbeat := opts.HeartbeatInterval
	if heartbeat <= 0 {
		heartbeat = 30 * time.Second
	}

	client := &http.Client{Timeout: opts.HTTPTimeout}
	if client.Timeout == 0 {
		client.Timeout = 3 * time.Second
	}

    stmt, err := db.Prepare("INSERT INTO market_prices (market_id, ts, price) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
	if err != nil {
		return nil, fmt.Errorf("prepare insert failed: %w", err)
	}

	ing := &PriceIngestor{
		baseURL:           baseURL,
		marketID:          marketID,
		client:            client,
		db:                db,
		insertStmt:        stmt,
		interval:          interval,
		heartbeatInterval: heartbeat,
        state:             make(map[string]*marketState),
	}
	return ing, nil
}

// Stop cleans up prepared statements
func (p *PriceIngestor) Stop() {
	if p.insertStmt != nil {
		_ = p.insertStmt.Close()
	}
}

// Start begins the polling loop until ctx is canceled
func (p *PriceIngestor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	heartbeatTicker := time.NewTicker(p.heartbeatInterval)
	defer heartbeatTicker.Stop()

	log.Println("📊 Price ingestor started polling...")

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Context canceled, stopping ingestor")
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		case <-heartbeatTicker.C:
			p.logHealthStatus()
		}
	}
}

// logHealthStatus logs the current health and statistics
func (p *PriceIngestor) logHealthStatus() {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	marketCount := len(p.state)
	successRate := float64(0)
	if p.totalSuccess+p.totalErrors > 0 {
		successRate = float64(p.totalSuccess) / float64(p.totalSuccess+p.totalErrors) * 100
	}

	log.Printf("💓 HEARTBEAT: Markets=%d | Success=%d | Errors=%d | SuccessRate=%.2f%% | ConsecutiveErrors=%d",
		marketCount, p.totalSuccess, p.totalErrors, successRate, p.consecutiveErrors)

	if p.consecutiveErrors > 10 {
		log.Printf("⚠️  WARNING: High consecutive error count: %d", p.consecutiveErrors)
	}
}

func (p *PriceIngestor) pollOnce(ctx context.Context) {
    // Fetch all markets and ingest for all perp markets with a mark price
    url := fmt.Sprintf("%s/markets", p.baseURL)

    fetchStart := time.Now()
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		p.recordError(fmt.Errorf("HTTP_REQUEST_CREATE_ERROR | URL=%s | Error: %w", url, err))
		return
	}

	resp, err := p.client.Do(req)
	fetchDuration := time.Since(fetchStart)
	if err != nil {
		p.recordError(fmt.Errorf("HTTP_REQUEST_FAILED after %v | URL=%s | Timeout=%v | Error: %w",
			fetchDuration, url, p.client.Timeout, err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.recordError(fmt.Errorf("HTTP_INVALID_STATUS | Status=%d | URL=%s | Duration=%v",
			resp.StatusCode, url, fetchDuration))
		return
	}

	// Log slow HTTP requests
	if fetchDuration > 1*time.Second {
		log.Printf("⚠️  Slow HTTP request: %v | URL=%s", fetchDuration, url)
	}

    var markets []struct {
        UUID     string `json:"uuid"`
        Kind     string `json:"kind"`
        PerpState *struct {
            MarkPrice            *float64 `json:"mark_price"`
            MarkPriceTimestamp   *int64   `json:"mark_price_timestamp"`
        } `json:"perp_state"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&markets); err != nil {
        p.recordError(fmt.Errorf("JSON_DECODE_ERROR | URL=%s | ContentType=%s | Error: %w",
            url, resp.Header.Get("Content-Type"), err))
        return
    }

    log.Printf("📡 Fetched %d total markets from API in %v", len(markets), fetchDuration)

    now := time.Now().UTC()
    insertedCount := 0
    dbErrors := 0
    var perpMarkets []string

    for _, m := range markets {
        if m.Kind != "perp" || m.PerpState == nil || m.PerpState.MarkPrice == nil {
            continue
        }
        perpMarkets = append(perpMarkets, m.UUID)
        marketID := m.UUID
        price := *m.PerpState.MarkPrice
        ts := now
        if m.PerpState.MarkPriceTimestamp != nil {
            // Engine timestamp is in seconds since epoch
            ts = time.Unix(*m.PerpState.MarkPriceTimestamp, 0).UTC()
        }

        p.stateMu.Lock()
        st, ok := p.state[marketID]
        if !ok {
            st = &marketState{}
            p.state[marketID] = st
        }
        prev := st.lastPrice
        lastIns := st.lastInsert
        write := p.shouldInsert(prev, price) || lastIns.IsZero() || now.Sub(lastIns) >= p.heartbeatInterval
        if write {
            dbTimeout := 500 * time.Millisecond
            ctxDB, cancel := context.WithTimeout(ctx, dbTimeout)
            insertStart := time.Now()
            _, err := p.insertStmt.ExecContext(ctxDB, marketID, ts, price)
            insertDuration := time.Since(insertStart)
            cancel()
            if err != nil {
                dbErrors++
                log.Printf("❌ Database insert failed after %v (timeout=%v) | Market=%s | Price=%.8f | TS=%s | Error: %v",
                    insertDuration, dbTimeout, marketID, price, ts.Format(time.RFC3339), err)
                p.stateMu.Unlock()
                p.tryReconnectDB(ctx)
                continue
            }
            st.lastInsert = now
            insertedCount++

            // Log slow inserts as warnings
            if insertDuration > 200*time.Millisecond {
                log.Printf("⚠️  Slow database insert: %v | Market=%s | Price=%.8f", insertDuration, marketID, price)
            }
        }
        st.lastPrice = price
        p.stateMu.Unlock()
    }

    // Log which perp markets were found
    if len(perpMarkets) > 0 {
        log.Printf("📊 Found %d perp markets with mark prices: %v", len(perpMarkets), perpMarkets)
    } else {
        log.Printf("⚠️  No perp markets with mark prices found")
    }

    // Record success if we got here
    if dbErrors == 0 {
        p.recordSuccess()
        if insertedCount > 0 {
            skippedCount := len(perpMarkets) - insertedCount
            if skippedCount > 0 {
                log.Printf("✅ Successfully inserted %d market prices (%d skipped due to no price change)", insertedCount, skippedCount)
            } else {
                log.Printf("✅ Successfully inserted %d market prices", insertedCount)
            }
        }
    } else {
        successCount := len(perpMarkets) - dbErrors
        p.recordError(fmt.Errorf("%d database insert errors occurred (succeeded=%d, failed=%d)", dbErrors, successCount, dbErrors))
    }
}

// recordError tracks errors and logs them with categorization
func (p *PriceIngestor) recordError(err error) {
    p.stateMu.Lock()
    defer p.stateMu.Unlock()

    p.consecutiveErrors++
    p.totalErrors++
    now := time.Now()
    timeSinceLastError := now.Sub(p.lastErrorTime)
    p.lastErrorTime = now

    // Categorize error type for better diagnostics
    errStr := err.Error()
    var category string
    switch {
    case containsAny(errStr, "HTTP_REQUEST_FAILED", "HTTP_REQUEST_CREATE_ERROR", "HTTP_INVALID_STATUS"):
        category = "NETWORK"
    case containsAny(errStr, "Database insert failed", "database insert errors"):
        category = "DATABASE"
    case containsAny(errStr, "JSON_DECODE_ERROR"):
        category = "PARSING"
    default:
        category = "UNKNOWN"
    }

    log.Printf("❌ [%s] ERROR #%d (consecutive=%d, gap=%v): %v",
        category, p.totalErrors, p.consecutiveErrors, timeSinceLastError.Round(time.Millisecond), err)

    // Progressive alerting based on consecutive errors
    if p.consecutiveErrors == 3 {
        log.Printf("⚠️  WARNING: 3 consecutive %s errors - investigating connectivity", category)
    } else if p.consecutiveErrors == 5 {
        log.Printf("⚠️  WARNING: 5 consecutive %s errors - system may be unhealthy", category)
    } else if p.consecutiveErrors == 10 {
        log.Printf("🚨 ALERT: 10 consecutive %s errors - manual intervention may be required", category)
    } else if p.consecutiveErrors == 20 {
        log.Printf("🚨 CRITICAL: 20 consecutive %s errors - check network/database connectivity immediately!", category)
    }
}

func containsAny(s string, substrs ...string) bool {
    for _, substr := range substrs {
        if len(s) >= len(substr) && findSubstring(s, substr) {
            return true
        }
    }
    return false
}

func findSubstring(s, substr string) bool {
    for i := 0; i <= len(s)-len(substr); i++ {
        match := true
        for j := 0; j < len(substr); j++ {
            if s[i+j] != substr[j] {
                match = false
                break
            }
        }
        if match {
            return true
        }
    }
    return false
}

// recordSuccess resets consecutive error counter
func (p *PriceIngestor) recordSuccess() {
    p.stateMu.Lock()
    defer p.stateMu.Unlock()

    if p.consecutiveErrors > 0 {
        timeSinceError := time.Since(p.lastErrorTime)
        log.Printf("✅ Recovered from %d consecutive errors after %v", p.consecutiveErrors, timeSinceError.Round(time.Millisecond))
        p.consecutiveErrors = 0
    }
    p.totalSuccess++
}

// tryReconnectDB attempts to reconnect to the database if connection is lost
func (p *PriceIngestor) tryReconnectDB(ctx context.Context) {
    p.stateMu.Lock()
    defer p.stateMu.Unlock()

    // Only try reconnect if we have consecutive errors
    if p.consecutiveErrors < 3 {
        return
    }

    log.Printf("🔄 Attempting database reconnection (consecutive_errors=%d)...", p.consecutiveErrors)
    reconnectStart := time.Now()

    // Test database connection
    pingTimeout := 2 * time.Second
    ctxPing, cancel := context.WithTimeout(ctx, pingTimeout)
    defer cancel()

    pingStart := time.Now()
    if err := p.db.PingContext(ctxPing); err != nil {
        pingDuration := time.Since(pingStart)
        log.Printf("❌ Database ping failed after %v (timeout=%v) | Error: %v", pingDuration, pingTimeout, err)
        return
    }
    pingDuration := time.Since(pingStart)
    log.Printf("✅ Database ping successful in %v", pingDuration)

    // Try to re-prepare the statement
    prepareStart := time.Now()
    stmt, err := p.db.Prepare("INSERT INTO market_prices (market_id, ts, price) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
    if err != nil {
        prepareDuration := time.Since(prepareStart)
        log.Printf("❌ Failed to re-prepare statement after %v | Error: %v", prepareDuration, err)
        return
    }
    prepareDuration := time.Since(prepareStart)

    // Close old statement and replace
    if p.insertStmt != nil {
        _ = p.insertStmt.Close()
    }
    p.insertStmt = stmt

    totalReconnectDuration := time.Since(reconnectStart)
    log.Printf("✅ Database connection re-established successfully in %v (ping=%v, prepare=%v) | Cleared %d consecutive errors",
        totalReconnectDuration, pingDuration, prepareDuration, p.consecutiveErrors)
    p.consecutiveErrors = 0
}

func (p *PriceIngestor) shouldInsert(prev, next float64) bool {
    if prev == 0 {
        return true
    }
    return next != prev
}


