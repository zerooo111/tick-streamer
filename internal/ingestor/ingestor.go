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
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		p.recordError(fmt.Errorf("failed to create HTTP request: %w", err))
		return
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.recordError(fmt.Errorf("HTTP request failed to %s: %w", url, err))
		p.tryReconnectDB(ctx)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.recordError(fmt.Errorf("HTTP request returned status %d from %s", resp.StatusCode, url))
		return
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
        p.recordError(fmt.Errorf("failed to decode markets JSON: %w", err))
        return
    }

    now := time.Now().UTC()
    insertedCount := 0
    dbErrors := 0

    for _, m := range markets {
        if m.Kind != "perp" || m.PerpState == nil || m.PerpState.MarkPrice == nil {
            continue
        }
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
            ctxDB, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
            _, err := p.insertStmt.ExecContext(ctxDB, marketID, ts, price)
            cancel()
            if err != nil {
                dbErrors++
                log.Printf("❌ Database insert failed for market %s: %v", marketID, err)
                p.stateMu.Unlock()
                p.tryReconnectDB(ctx)
                continue
            }
            st.lastInsert = now
            insertedCount++
        }
        st.lastPrice = price
        p.stateMu.Unlock()
    }

    // Record success if we got here
    if dbErrors == 0 {
        p.recordSuccess()
        if insertedCount > 0 {
            log.Printf("✅ Successfully inserted %d market prices", insertedCount)
        }
    } else {
        p.recordError(fmt.Errorf("%d database insert errors occurred", dbErrors))
    }
}

// recordError tracks errors and logs them
func (p *PriceIngestor) recordError(err error) {
    p.stateMu.Lock()
    defer p.stateMu.Unlock()

    p.consecutiveErrors++
    p.totalErrors++
    p.lastErrorTime = time.Now()

    log.Printf("❌ ERROR (consecutive=%d, total=%d): %v", p.consecutiveErrors, p.totalErrors, err)

    if p.consecutiveErrors == 5 {
        log.Printf("⚠️  WARNING: 5 consecutive errors detected - system may be unhealthy")
    } else if p.consecutiveErrors == 20 {
        log.Printf("🚨 CRITICAL: 20 consecutive errors - check network/database connectivity!")
    }
}

// recordSuccess resets consecutive error counter
func (p *PriceIngestor) recordSuccess() {
    p.stateMu.Lock()
    defer p.stateMu.Unlock()

    if p.consecutiveErrors > 0 {
        log.Printf("✅ Recovered from %d consecutive errors", p.consecutiveErrors)
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

    log.Printf("🔄 Attempting to reconnect to database...")

    // Test database connection
    ctxPing, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()

    if err := p.db.PingContext(ctxPing); err != nil {
        log.Printf("❌ Database ping failed: %v", err)
        return
    }

    // Try to re-prepare the statement
    stmt, err := p.db.Prepare("INSERT INTO market_prices (market_id, ts, price) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
    if err != nil {
        log.Printf("❌ Failed to re-prepare statement: %v", err)
        return
    }

    // Close old statement and replace
    if p.insertStmt != nil {
        _ = p.insertStmt.Close()
    }
    p.insertStmt = stmt

    log.Printf("✅ Database connection re-established successfully")
    p.consecutiveErrors = 0
}

func (p *PriceIngestor) shouldInsert(prev, next float64) bool {
    if prev == 0 {
        return true
    }
    return next != prev
}


