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
	SequencerAddr       string `env:"SEQUENCER_ADDR"`
	SequencerTLS        bool   `env:"SEQUENCER_TLS"`
	SequencerMTLS       bool   `env:"SEQUENCER_MTLS"`
	SequencerServerName string `env:"SEQUENCER_SERVER_NAME"` // Server name for TLS verification
	SequencerCACert     string `env:"SEQUENCER_CA_CERT"`     // Path to CA certificate
	SequencerClientCert string `env:"SEQUENCER_CLIENT_CERT"` // Path to client certificate (for mTLS)
	SequencerClientKey  string `env:"SEQUENCER_CLIENT_KEY"`  // Path to client key (for mTLS)

	// ClickHouse connection settings
	ClickHouseHost     string `env:"CLICKHOUSE_HOST"`
	ClickHousePort     int    `env:"CLICKHOUSE_PORT"`
	ClickHouseDatabase string `env:"CLICKHOUSE_DATABASE"`
	ClickHouseUsername string `env:"CLICKHOUSE_USERNAME"`
	ClickHousePassword string `env:"CLICKHOUSE_PASSWORD"`

	// Checkpoint persistence
	CheckpointDSN string `env:"CHECKPOINT_DSN"`

	// Batching configuration
	BatchRowsTx      int           `env:"BATCH_ROWS_TX"`
	BatchRowsTick    int           `env:"BATCH_ROWS_TICK"`
	BatchMaxWaitTime time.Duration `env:"BATCH_MAX_WAIT_MS"`

	// Retry settings
	RetryBackoffMin time.Duration `env:"RETRY_BACKOFF_MIN_MS"`
	RetryBackoffMax time.Duration `env:"RETRY_BACKOFF_MAX_MS"`

	// API server specific settings
	APIPort              string   `env:"API_PORT"`
	RestBaseURL          string   `env:"REST_BASE_URL"`
	MatchEngineURL       string   `env:"MATCH_ENGINE_URL"`
	CORSAllowedOrigins   []string `env:"CORS_ALLOWED_ORIGINS"`
	CORSAllowCredentials bool     `env:"CORS_ALLOW_CREDENTIALS"`
	Debug                bool     `env:"DEBUG"`

	// Logging
	LogLevel      string `env:"LOG_LEVEL"`
	LogDir        string `env:"LOG_DIR"`
	LogMaxSize    int    `env:"LOG_MAX_SIZE"`    // megabytes
	LogMaxBackups int    `env:"LOG_MAX_BACKUPS"`
	LogMaxAge     int    `env:"LOG_MAX_AGE"`     // days
	LogCompress   bool   `env:"LOG_COMPRESS"`
	LogConsole    bool   `env:"LOG_CONSOLE"`
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
		SequencerServerName:  getEnvString("SEQUENCER_SERVER_NAME", ""),
		SequencerCACert:      getEnvString("SEQUENCER_CA_CERT", ""),
		SequencerClientCert:  getEnvString("SEQUENCER_CLIENT_CERT", ""),
		SequencerClientKey:   getEnvString("SEQUENCER_CLIENT_KEY", ""),
		ClickHouseHost:       getRequiredEnvString("CLICKHOUSE_HOST"),
		ClickHousePort:       getEnvInt("CLICKHOUSE_PORT", 9440),
		ClickHouseDatabase:   getEnvString("CLICKHOUSE_DATABASE", "default"),
		ClickHouseUsername:   getEnvString("CLICKHOUSE_USERNAME", "default"),
		ClickHousePassword:   getRequiredEnvString("CLICKHOUSE_PASSWORD"),
		CheckpointDSN:        getRequiredEnvString("CHECKPOINT_DSN"),
		BatchRowsTx:          getEnvInt("BATCH_ROWS_TX", 20000),
		BatchRowsTick:        getEnvInt("BATCH_ROWS_TICK", 1000),
		BatchMaxWaitTime:     getEnvDuration("BATCH_MAX_WAIT_MS", 100*time.Millisecond),
		RetryBackoffMin:      getEnvDuration("RETRY_BACKOFF_MIN_MS", 200*time.Millisecond),
		RetryBackoffMax:      getEnvDuration("RETRY_BACKOFF_MAX_MS", 20000*time.Millisecond),
		LogLevel:             getEnvString("LOG_LEVEL", "info"),
		LogDir:               getEnvString("LOG_DIR", "./logs"),
		LogMaxSize:           getEnvInt("LOG_MAX_SIZE", 100),      // 100MB default
		LogMaxBackups:        getEnvInt("LOG_MAX_BACKUPS", 5),     // Keep 5 backups
		LogMaxAge:            getEnvInt("LOG_MAX_AGE", 30),        // Keep logs for 30 days
		LogCompress:          getEnvBool("LOG_COMPRESS", true),    // Compress old logs by default
		LogConsole:           getEnvBool("LOG_CONSOLE", true),     // Also log to console by default
		
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

	// Validate TLS configuration
	if err := cfg.validateTLS(); err != nil {
		return nil, fmt.Errorf("TLS configuration validation failed: %w", err)
	}

	return cfg, nil
}

// validate ensures the configuration is valid
func (c *Config) validate() error {
	if c.SequencerAddr == "" {
		return fmt.Errorf("SEQUENCER_ADDR is required")
	}

	if c.ClickHouseHost == "" {
		return fmt.Errorf("CLICKHOUSE_HOST is required")
	}

	if c.ClickHousePassword == "" {
		return fmt.Errorf("CLICKHOUSE_PASSWORD is required")
	}

	if c.ClickHousePort <= 0 {
		return fmt.Errorf("CLICKHOUSE_PORT must be positive, got: %d", c.ClickHousePort)
	}

	if c.BatchRowsTx <= 0 {
		return fmt.Errorf("BATCH_ROWS_TX must be positive, got: %d", c.BatchRowsTx)
	}

	if c.BatchRowsTick <= 0 {
		return fmt.Errorf("BATCH_ROWS_TICK must be positive, got: %d", c.BatchRowsTick)
	}

	return nil
}

// validateTLS ensures TLS configuration is consistent and valid
func (c *Config) validateTLS() error {
	if c.SequencerTLS {
		// When TLS is enabled, CA certificate is recommended for verification
		if c.SequencerCACert == "" {
			// Warning: using system CA bundle
			fmt.Println("Warning: SEQUENCER_TLS is enabled but SEQUENCER_CA_CERT is not set. Using system CA bundle.")
		} else {
			// Validate CA cert file exists
			if _, err := os.Stat(c.SequencerCACert); os.IsNotExist(err) {
				return fmt.Errorf("CA certificate file not found: %s", c.SequencerCACert)
			}
		}
	}

	if c.SequencerMTLS {
		// mTLS requires TLS to be enabled
		if !c.SequencerTLS {
			return fmt.Errorf("SEQUENCER_MTLS requires SEQUENCER_TLS to be enabled")
		}

		// mTLS requires client certificate and key
		if c.SequencerClientCert == "" || c.SequencerClientKey == "" {
			return fmt.Errorf("SEQUENCER_MTLS requires both SEQUENCER_CLIENT_CERT and SEQUENCER_CLIENT_KEY")
		}

		// Validate client cert and key files exist
		if _, err := os.Stat(c.SequencerClientCert); os.IsNotExist(err) {
			return fmt.Errorf("client certificate file not found: %s", c.SequencerClientCert)
		}
		if _, err := os.Stat(c.SequencerClientKey); os.IsNotExist(err) {
			return fmt.Errorf("client key file not found: %s", c.SequencerClientKey)
		}
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