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

	"github.com/zerooo111/tick-streamer/internal/config"
	"github.com/zerooo111/tick-streamer/internal/ingestor"
)

func main() {
	log.Println("🚀 Starting market price ingestor...")
	
	// Load configuration from .env file
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("❌ Failed to load configuration: %v", err)
	}
	
	log.Printf("📡 Rollup REST Endpoint: %s", cfg.RollupRestEndpoint)

	// Build TimescaleDB connection string from config
	connStr := os.Getenv("TIMESCALEDB_CONNECTION_STRING")
	if connStr == "" {
		connStr = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", 
			cfg.TimescaleDBHost, cfg.TimescaleDBPort, cfg.TimescaleDBUsername, 
			cfg.TimescaleDBPassword, cfg.TimescaleDBDatabase, cfg.TimescaleDBSSLMode)
		log.Printf("🔗 Built connection string from config")
	} else {
		log.Printf("🔗 Using provided connection string")
	}

	log.Printf("🗄️ Connecting to TimescaleDB...")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("❌ Failed to open database connection: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("⚠️ Error closing database: %v", err)
		} else {
			log.Println("✅ Database connection closed")
		}
	}()

	// Configure connection pool for better resilience
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)
	log.Println("⚙️  Database connection pool configured (max_open=10, max_idle=5)")

	log.Printf("🏓 Testing database connection...")
	if err := pingWithTimeout(db, 10*time.Second); err != nil {
		log.Fatalf("❌ Failed to ping database: %v", err)
	}
	log.Println("✅ Database connection successful")

	interval := getDuration("INGEST_INTERVAL", time.Second)
	heartbeat := getDuration("HEARTBEAT_INTERVAL", 30*time.Second)
	httpTimeout := getDuration("HTTP_TIMEOUT", 3*time.Second)
	
	log.Printf("⚙️ Configuration:")
	log.Printf("   - Ingest interval: %v", interval)
	log.Printf("   - Heartbeat interval: %v", heartbeat)
	log.Printf("   - HTTP timeout: %v", httpTimeout)

	opts := ingestor.IngestorOptions{
		Interval:          interval,
		HeartbeatInterval: heartbeat,
		HTTPTimeout:       httpTimeout,
	}

	log.Printf("🔧 Initializing price ingestor...")
	ing, err := ingestor.NewPriceIngestor(db, cfg.RollupRestEndpoint, "", opts)
	if err != nil {
		log.Fatalf("❌ Failed to initialize ingestor: %v", err)
	}
	defer func() {
		log.Println("🛑 Stopping ingestor...")
		ing.Stop()
		log.Println("✅ Ingestor stopped")
	}()

	log.Println("🎯 Starting price ingestion...")
	ctx, cancel := context.WithCancel(context.Background())

	// Run ingestor with panic recovery
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("🚨 PANIC in ingestor: %v", r)
				log.Println("⚠️  Attempting to restart ingestor...")
				// Wait a bit before restarting
				time.Sleep(5 * time.Second)
				// Restart the ingestor
				go ing.Start(ctx)
			}
		}()
		ing.Start(ctx)
	}()

	log.Println("✅ Ingestor started successfully. Press Ctrl+C to stop.")

	// Periodic health check
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Test database connection periodically
				if err := pingWithTimeout(db, 5*time.Second); err != nil {
					log.Printf("⚠️  Health check: Database ping failed: %v", err)
				} else {
					log.Println("💚 Health check: Database connection OK")
				}
			}
		}
	}()

	waitForSignal()

	log.Println("🛑 Shutdown signal received...")
	cancel()
	// Allow graceful shutdown period
	time.Sleep(2 * time.Second)
	log.Println("🏁 Shutdown complete")
}


func getDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
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


