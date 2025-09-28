package ingestor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

func (p *PriceIngestor) pollOnce(ctx context.Context) {
    // Fetch all markets and ingest for all perp markets with a mark price
    url := fmt.Sprintf("%s/markets", p.baseURL)
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
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
        return
    }

    now := time.Now().UTC()

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
            _, _ = p.insertStmt.ExecContext(ctxDB, marketID, ts, price)
            cancel()
            st.lastInsert = now
        }
        st.lastPrice = price
        p.stateMu.Unlock()
    }
}

func (p *PriceIngestor) shouldInsert(prev, next float64) bool {
    if prev == 0 {
        return true
    }
    return next != prev
}


