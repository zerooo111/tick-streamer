package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/zerooo111/tick-streamer/internal/ingestor"
)

func main() {
  baseURL := mustGetenv("REST_BASE_URL")
	marketID := mustGetenv("MARKET_ID")

	// Build TimescaleDB connection string from existing env vars (or use full connection string if provided)
	connStr := os.Getenv("TIMESCALEDB_CONNECTION_STRING")
	if connStr == "" {
		host := getenv("TIMESCALEDB_HOST", "localhost")
		port := getenv("TIMESCALEDB_PORT", "5432")
		database := getenv("TIMESCALEDB_DATABASE", "tick_streamer")
		username := getenv("TIMESCALEDB_USERNAME", "postgres")
		password := getenv("TIMESCALEDB_PASSWORD", "")
		sslMode := getenv("TIMESCALEDB_SSL_MODE", "prefer")
		connStr = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", host, port, username, password, database, sslMode)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	if err := pingWithTimeout(db, 10*time.Second); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

    opts := ingestor.IngestorOptions{
        Interval:          getDuration("INGEST_INTERVAL", time.Second),
        HeartbeatInterval: getDuration("HEARTBEAT_INTERVAL", 30*time.Second),
        HTTPTimeout:       getDuration("HTTP_TIMEOUT", 3*time.Second),
    }

	ing, err := ingestor.NewPriceIngestor(db, baseURL, marketID, opts)
	if err != nil {
		log.Fatalf("failed to init ingestor: %v", err)
	}
	defer ing.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	go ing.Start(ctx)

	waitForSignal()
	cancel()
	// Allow a brief shutdown period
	time.Sleep(300 * time.Millisecond)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustGetenv(key string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	log.Fatalf("missing required env: %s", key)
	return ""
}

func getDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		var f float64
		_, err := fmt.Sscan(v, &f)
		if err == nil {
			return f
		}
	}
	return def
}

func pingWithTimeout(db *sql.DB, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return db.PingContext(ctx)
}

func waitForSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}


