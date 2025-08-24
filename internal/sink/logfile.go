package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
)

// LogFileSink implements the Sink interface using append-only log files
// This is the simplest and most reliable persistence method for high-throughput scenarios
type LogFileSink struct {
	mu           sync.RWMutex
	config       Config
	tickFile     *os.File
	txFile       *os.File
	lastTick     uint64
	stats        SinkStats
	closed       bool
	baseDir      string
}

// LogFileConfig holds log file specific configuration
type LogFileConfig struct {
	BaseDir       string `json:"base_dir" env:"LOG_SINK_BASE_DIR"`
	RotateSize    int64  `json:"rotate_size_mb" env:"LOG_SINK_ROTATE_SIZE_MB"`     // Max file size before rotation (MB)
	RetentionDays int    `json:"retention_days" env:"LOG_SINK_RETENTION_DAYS"`    // Days to keep old files
	Compress      bool   `json:"compress" env:"LOG_SINK_COMPRESS"`               // Enable gzip compression for rotated files
	SyncWrites    bool   `json:"sync_writes" env:"LOG_SINK_SYNC_WRITES"`         // Force sync after each write
}

// NewLogFileSink creates a new log file sink
func NewLogFileSink(cfg Config) (*LogFileSink, error) {
	baseDir := "./logs"
	if cfg.DSN != "" {
		baseDir = cfg.DSN
	}

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", baseDir, err)
	}

	sink := &LogFileSink{
		config:  cfg,
		baseDir: baseDir,
		stats: SinkStats{
			Connected: true,
		},
	}

	// Initialize log files
	if err := sink.initLogFiles(); err != nil {
		return nil, fmt.Errorf("failed to initialize log files: %w", err)
	}

	return sink, nil
}

// initLogFiles opens or creates the log files
func (s *LogFileSink) initLogFiles() error {
	tickPath := filepath.Join(s.baseDir, "ticks.jsonl")
	txPath := filepath.Join(s.baseDir, "transactions.jsonl")

	var err error
	
	// Open tick log file (create if doesn't exist)
	s.tickFile, err = os.OpenFile(tickPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open tick log file: %w", err)
	}

	// Open transaction log file (create if doesn't exist)
	s.txFile, err = os.OpenFile(txPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		s.tickFile.Close()
		return fmt.Errorf("failed to open transaction log file: %w", err)
	}

	// Load last tick number from existing files
	s.lastTick = s.loadLastTickFromFiles()

	return nil
}

// PersistData implements the Sink interface
func (s *LogFileSink) PersistData(ctx context.Context, data []*parser.ParsedData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	startTime := time.Now()
	
	tickCount := 0
	txCount := 0

	for _, item := range data {
		switch item.Type {
		case "tick":
			if err := s.writeTick(item); err != nil {
				s.stats.ErrorCount++
				return fmt.Errorf("failed to write tick: %w", err)
			}
			tickCount++
			
		case "transaction":
			if err := s.writeTransaction(item); err != nil {
				s.stats.ErrorCount++
				return fmt.Errorf("failed to write transaction: %w", err)
			}
			txCount++
		}
	}

	duration := time.Since(startTime)
	s.updateStats(tickCount, txCount, duration)

	return nil
}

// writeTick writes a tick record to the tick log file
func (s *LogFileSink) writeTick(item *parser.ParsedData) error {
	tickRow, ok := item.Data.(*models.TickRow)
	if !ok {
		return fmt.Errorf("expected *models.TickRow, got %T", item.Data)
	}

	// Create log entry with metadata
	logEntry := map[string]interface{}{
		"timestamp": time.Now().UnixMicro(),
		"type":      "tick",
		"data":      tickRow,
		"metadata":  item.Metadata,
	}

	jsonData, err := json.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal tick JSON: %w", err)
	}

	// Write JSON line
	if _, err := s.tickFile.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("failed to write tick to file: %w", err)
	}

	// Force sync if configured
	if s.config.EnableCompression { // Reusing this flag as sync flag
		if err := s.tickFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync tick file: %w", err)
		}
	}

	// Update last tick
	if tickRow.TickNumber > s.lastTick {
		s.lastTick = tickRow.TickNumber
	}

	return nil
}

// writeTransaction writes a transaction record to the transaction log file
func (s *LogFileSink) writeTransaction(item *parser.ParsedData) error {
	txRow, ok := item.Data.(*models.TxRow)
	if !ok {
		return fmt.Errorf("expected *models.TxRow, got %T", item.Data)
	}

	// Create log entry with metadata
	logEntry := map[string]interface{}{
		"timestamp": time.Now().UnixMicro(),
		"type":      "transaction",
		"data":      txRow,
		"metadata":  item.Metadata,
	}

	jsonData, err := json.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction JSON: %w", err)
	}

	// Write JSON line
	if _, err := s.txFile.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("failed to write transaction to file: %w", err)
	}

	// Force sync if configured
	if s.config.EnableCompression { // Reusing this flag as sync flag
		if err := s.txFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync transaction file: %w", err)
		}
	}

	return nil
}

// InvalidateTick implements the Sink interface
func (s *LogFileSink) InvalidateTick(ctx context.Context, tickNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	// Write invalidation record to both files
	invalidationEntry := map[string]interface{}{
		"timestamp":   time.Now().UnixMicro(),
		"type":        "invalidation",
		"tick_number": tickNumber,
		"reason":      "reorg_detected",
	}

	jsonData, err := json.Marshal(invalidationEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal invalidation JSON: %w", err)
	}

	jsonLine := append(jsonData, '\n')

	// Write to both files
	if _, err := s.tickFile.Write(jsonLine); err != nil {
		return fmt.Errorf("failed to write tick invalidation: %w", err)
	}

	if _, err := s.txFile.Write(jsonLine); err != nil {
		return fmt.Errorf("failed to write transaction invalidation: %w", err)
	}

	return nil
}

// Flush implements the Sink interface
func (s *LogFileSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}

	startTime := time.Now()

	// Sync both files
	if err := s.tickFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync tick file: %w", err)
	}

	if err := s.txFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync transaction file: %w", err)
	}

	s.stats.FlushCount++
	s.stats.LastFlushDuration = time.Since(startTime).Milliseconds()

	return nil
}

// Health implements the Sink interface
func (s *LogFileSink) Health(ctx context.Context) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return !s.closed && s.tickFile != nil && s.txFile != nil
}

// GetLastTick implements the Sink interface
func (s *LogFileSink) GetLastTick(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, ErrSinkClosed
	}

	return s.lastTick, nil
}

// Close implements the Sink interface
func (s *LogFileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	var errs []error

	if s.tickFile != nil {
		if err := s.tickFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close tick file: %w", err))
		}
	}

	if s.txFile != nil {
		if err := s.txFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close transaction file: %w", err))
		}
	}

	s.closed = true
	s.stats.Connected = false

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}

	return nil
}

// GetStats implements the StatsProvider interface
func (s *LogFileSink) GetStats() SinkStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	statsCopy := s.stats
	statsCopy.LastTickNumber = s.lastTick
	return statsCopy
}

// ResetStats implements the StatsProvider interface
func (s *LogFileSink) ResetStats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.TicksInserted = 0
	s.stats.TransactionsInserted = 0
	s.stats.FlushCount = 0
	s.stats.ErrorCount = 0
	s.stats.LastFlushDuration = 0
	s.stats.AverageFlushDuration = 0
}

// updateStats updates internal statistics
func (s *LogFileSink) updateStats(tickCount, txCount int, duration time.Duration) {
	s.stats.TicksInserted += uint64(tickCount)
	s.stats.TransactionsInserted += uint64(txCount)
	
	durationMS := duration.Milliseconds()
	s.stats.LastFlushDuration = durationMS
	
	// Update average duration (simple exponential moving average)
	if s.stats.AverageFlushDuration == 0 {
		s.stats.AverageFlushDuration = float64(durationMS)
	} else {
		alpha := 0.1 // Smoothing factor
		s.stats.AverageFlushDuration = alpha*float64(durationMS) + (1-alpha)*s.stats.AverageFlushDuration
	}
}

// loadLastTickFromFiles scans the log files to find the last tick number
// This is used for recovery when restarting the sink
func (s *LogFileSink) loadLastTickFromFiles() uint64 {
	// In a production system, we might maintain an index file or use tail scanning
	// For simplicity, we'll start from 0 and let the checkpoint system handle recovery
	return 0
}