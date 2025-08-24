package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the Continuum Streamer
// This struct uses Go's struct tags for documentation and validation
type Config struct {
	// Sequencer connection settings
	SequencerAddr string `env:"SEQUENCER_ADDR"`
	SequencerTLS  bool   `env:"SEQUENCER_TLS"`
	SequencerMTLS bool   `env:"SEQUENCER_MTLS"`

	// Sink (database) settings
	SinkKind string `env:"SINK_KIND"`
	SinkDSN  string `env:"SINK_DSN"`

	// Checkpoint persistence
	CheckpointDSN string `env:"CHECKPOINT_DSN"`

	// Batching configuration
	BatchRowsTx      int           `env:"BATCH_ROWS_TX"`
	BatchRowsTick    int           `env:"BATCH_ROWS_TICK"`
	BatchMaxWaitTime time.Duration `env:"BATCH_MAX_WAIT_MS"`

	// Retry settings
	RetryBackoffMin time.Duration `env:"RETRY_BACKOFF_MIN_MS"`
	RetryBackoffMax time.Duration `env:"RETRY_BACKOFF_MAX_MS"`

	// HTTP server settings
	HTTPBind string `env:"HTTP_BIND"`

	// API server specific settings
	APIPort              string   `env:"API_PORT"`
	RestBaseURL          string   `env:"REST_BASE_URL"`
	MatchEngineURL       string   `env:"MATCH_ENGINE_URL"`
	CORSAllowedOrigins   []string `env:"CORS_ALLOWED_ORIGINS"`
	CORSAllowCredentials bool     `env:"CORS_ALLOW_CREDENTIALS"`
	Debug                bool     `env:"DEBUG"`

	// Logging
	LogLevel string `env:"LOG_LEVEL"`
}

// Load reads configuration from .env file and environment variables
// Returns an error if .env file doesn't exist or required variables are missing
func Load() (*Config, error) {
	// Load .env file - fail if it doesn't exist
	if err := godotenv.Load(); err != nil {
		return nil, fmt.Errorf("failed to load .env file: %w", err)
	}

	cfg := &Config{
		// Load all values from environment variables (no defaults)
		SequencerAddr:        getRequiredEnvString("SEQUENCER_ADDR"),
		SequencerTLS:         getEnvBool("SEQUENCER_TLS", false),
		SequencerMTLS:        getEnvBool("SEQUENCER_MTLS", false),
		SinkKind:             getRequiredEnvString("SINK_KIND"),
		SinkDSN:              getEnvString("SINK_DSN", ""),
		CheckpointDSN:        getRequiredEnvString("CHECKPOINT_DSN"),
		BatchRowsTx:          getEnvInt("BATCH_ROWS_TX", 20000),
		BatchRowsTick:        getEnvInt("BATCH_ROWS_TICK", 1000),
		BatchMaxWaitTime:     getEnvDuration("BATCH_MAX_WAIT_MS", 100*time.Millisecond),
		RetryBackoffMin:      getEnvDuration("RETRY_BACKOFF_MIN_MS", 200*time.Millisecond),
		RetryBackoffMax:      getEnvDuration("RETRY_BACKOFF_MAX_MS", 20000*time.Millisecond),
		HTTPBind:             getRequiredEnvString("HTTP_BIND"),
		LogLevel:             getEnvString("LOG_LEVEL", "info"),
		
		// API server configuration - all required
		APIPort:              getRequiredEnvString("API_PORT"),
		RestBaseURL:          getRequiredEnvString("REST_BASE_URL"),
		MatchEngineURL:       getRequiredEnvString("MATCH_ENGINE_URL"),
		CORSAllowedOrigins:   getRequiredEnvStringSlice("CORS_ALLOWED_ORIGINS"),
		CORSAllowCredentials: getEnvBool("CORS_ALLOW_CREDENTIALS", false),
		Debug:                getEnvBool("DEBUG", false),
	}

	// Validate required configuration
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// validate ensures the configuration is valid
func (c *Config) validate() error {
	if c.SequencerAddr == "" {
		return fmt.Errorf("SEQUENCER_ADDR is required")
	}

	if c.SinkKind == "" {
		return fmt.Errorf("SINK_KIND is required")
	}

	// Validate sink kind is supported
	switch c.SinkKind {
	case "mock", "logfile", "log", "sqlite", "sqlite3", "clickhouse", "ch", "postgres":
		// Valid sink types
	default:
		return fmt.Errorf("unsupported SINK_KIND: %s (must be one of: mock, logfile, log, sqlite, sqlite3, clickhouse, ch, postgres)", c.SinkKind)
	}

	if c.BatchRowsTx <= 0 {
		return fmt.Errorf("BATCH_ROWS_TX must be positive, got: %d", c.BatchRowsTx)
	}

	if c.BatchRowsTick <= 0 {
		return fmt.Errorf("BATCH_ROWS_TICK must be positive, got: %d", c.BatchRowsTick)
	}

	return nil
}

// Helper functions for reading environment variables with defaults
// These demonstrate Go's approach to optional parameters using default values

func getRequiredEnvString(key string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	panic(fmt.Sprintf("required environment variable %s is not set", key))
}

func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		// Parse boolean - common values are "true", "false", "on", "off"
		switch value {
		case "true", "on", "1", "yes":
			return true
		case "false", "off", "0", "no":
			return false
		}
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		// Try to parse as milliseconds first (integer)
		if msValue, err := strconv.Atoi(value); err == nil {
			return time.Duration(msValue) * time.Millisecond
		}
		// Try to parse as Go duration string (e.g., "100ms", "5s")
		if durValue, err := time.ParseDuration(value); err == nil {
			return durValue
		}
	}
	return defaultValue
}

func getRequiredEnvStringSlice(key string) []string {
	if value := os.Getenv(key); value != "" {
		// Split by comma and trim whitespace
		var result []string
		for _, item := range strings.Split(value, ",") {
			result = append(result, strings.TrimSpace(item))
		}
		return result
	}
	panic(fmt.Sprintf("required environment variable %s is not set", key))
}

func getEnvStringSlice(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		// Split by comma and trim whitespace
		var result []string
		for _, item := range strings.Split(value, ",") {
			result = append(result, strings.TrimSpace(item))
		}
		return result
	}
	return defaultValue
}