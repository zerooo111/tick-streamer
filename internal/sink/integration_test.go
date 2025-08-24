package sink

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
)

// TestSinkFactory tests the sink factory with all supported adapters
func TestSinkFactory(t *testing.T) {
	// Create temp directory for file-based sinks
	tmpDir, err := os.MkdirTemp("", "sink_factory_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name     string
		config   Config
		wantErr  bool
		skipTest bool
	}{
		{
			name: "MockSink",
			config: Config{
				Kind: "mock",
			},
			wantErr: false,
		},
		{
			name: "LogFileSink",
			config: Config{
				Kind: "logfile",
				DSN:  filepath.Join(tmpDir, "logs"),
			},
			wantErr: false,
		},
		{
			name: "LogSink_Alias",
			config: Config{
				Kind: "log",
				DSN:  filepath.Join(tmpDir, "logs2"),
			},
			wantErr: false,
		},
		{
			name: "SQLiteSink",
			config: Config{
				Kind: "sqlite",
				DSN:  filepath.Join(tmpDir, "test.db"),
			},
			wantErr: false,
		},
		{
			name: "SQLite3Sink_Alias",
			config: Config{
				Kind: "sqlite3",
				DSN:  filepath.Join(tmpDir, "test2.db"),
			},
			wantErr: false,
		},
		{
			name: "ClickHouseSink_Local",
			config: Config{
				Kind: "clickhouse",
				DSN:  "localhost:9000",
			},
			wantErr:  false,
			skipTest: true, // Skip unless ClickHouse is available
		},
		{
			name: "ClickHouseSink_Alias",
			config: Config{
				Kind: "ch",
				DSN:  "localhost:9000",
			},
			wantErr:  false,
			skipTest: true, // Skip unless ClickHouse is available
		},
		{
			name: "PostgresSink_NotImplemented",
			config: Config{
				Kind: "postgres",
				DSN:  "postgres://localhost/test",
			},
			wantErr: true,
		},
		{
			name: "InvalidSink",
			config: Config{
				Kind: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipTest {
				t.Skipf("Skipping %s test - requires external dependency", tc.name)
				return
			}

			sink, err := NewSink(tc.config)

			if tc.wantErr {
				if err == nil {
					t.Errorf("Expected error for config %+v, got nil", tc.config)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error for config %+v: %v", tc.config, err)
				return
			}

			if sink == nil {
				t.Errorf("Expected non-nil sink for config %+v", tc.config)
				return
			}

			// Test basic operations
			ctx := context.Background()

			// Health check
			if !sink.Health(ctx) {
				t.Errorf("Expected sink to be healthy for config %+v", tc.config)
			}

			// Get last tick
			lastTick, err := sink.GetLastTick(ctx)
			if err != nil {
				t.Errorf("Failed to get last tick for config %+v: %v", tc.config, err)
			}

			if lastTick != 0 {
				t.Errorf("Expected initial last tick to be 0, got %d for config %+v", lastTick, tc.config)
			}

			// Clean up
			sink.Close()
		})
	}
}

// TestStatsProviderFactory tests the stats provider factory
func TestStatsProviderFactory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "stats_factory_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name   string
		config Config
	}{
		{
			name: "MockStatsProvider",
			config: Config{
				Kind: "mock",
			},
		},
		{
			name: "LogFileStatsProvider",
			config: Config{
				Kind: "logfile",
				DSN:  filepath.Join(tmpDir, "stats_logs"),
			},
		},
		{
			name: "SQLiteStatsProvider",
			config: Config{
				Kind: "sqlite",
				DSN:  filepath.Join(tmpDir, "stats.db"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			statsProvider, err := NewStatsProviderSink(tc.config)
			if err != nil {
				t.Fatalf("Failed to create stats provider for %s: %v", tc.name, err)
			}
			defer statsProvider.Close()

			// Test stats functionality
			initialStats := statsProvider.GetStats()
			if initialStats.TicksInserted != 0 {
				t.Errorf("Expected 0 initial ticks, got %d", initialStats.TicksInserted)
			}

			// Add some data
			ctx := context.Background()
			tickRow := &models.TickRow{
				TickNumber:           100,
				TimestampUS:          time.Now().UnixMicro(),
				VdfInput:            "input",
				VdfOutput:           "output",
				TransactionBatchHash: "hash",
				Version:             1,
			}

			data := []*parser.ParsedData{
				{Type: "tick", Data: tickRow},
			}

			err = statsProvider.PersistData(ctx, data)
			if err != nil {
				t.Fatalf("Failed to persist data: %v", err)
			}

			// Check updated stats
			updatedStats := statsProvider.GetStats()
			if updatedStats.TicksInserted != 1 {
				t.Errorf("Expected 1 tick after insert, got %d", updatedStats.TicksInserted)
			}

			// Test reset
			statsProvider.ResetStats()
			resetStats := statsProvider.GetStats()
			if resetStats.TicksInserted != 0 {
				t.Errorf("Expected 0 ticks after reset, got %d", resetStats.TicksInserted)
			}
		})
	}
}

// TestSinkPerformanceComparison compares performance across different sink types
func TestSinkPerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "sink_perf_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test configurations
	configs := []struct {
		name   string
		config Config
	}{
		{
			name: "Mock",
			config: Config{
				Kind: "mock",
			},
		},
		{
			name: "LogFile",
			config: Config{
				Kind:              "logfile",
				DSN:               filepath.Join(tmpDir, "perf_logs"),
				EnableCompression: true, // Use as sync flag
			},
		},
		{
			name: "SQLite",
			config: Config{
				Kind: "sqlite",
				DSN:  filepath.Join(tmpDir, "perf.db"),
			},
		},
	}

	const numTicks = 1000
	const numTxPerTick = 10

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			sink, err := NewSink(cfg.config)
			if err != nil {
				t.Fatalf("Failed to create sink: %v", err)
			}
			defer sink.Close()

			ctx := context.Background()
			start := time.Now()

			// Insert test data
			for i := 0; i < numTicks; i++ {
				tickRow := &models.TickRow{
					TickNumber:           uint64(i + 1),
					TimestampUS:          time.Now().UnixMicro(),
					VdfInput:            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					VdfOutput:           "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
					TransactionBatchHash: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
					Version:             1,
				}

				data := []*parser.ParsedData{
					{Type: "tick", Data: tickRow},
				}

				// Add transactions
				for j := 0; j < numTxPerTick; j++ {
					txRow := &models.TxRow{
						TickNumber:     uint64(i + 1),
						SequenceNumber: uint64(j),
						TxHash:        "1122334455667788aabbccddeeff00111122334455667788aabbccddeeff0011",
						TxID:          "tx",
						PublicKey:     "pubkey",
						Signature:     "signature",
						Version:       1,
					}
					data = append(data, &parser.ParsedData{Type: "transaction", Data: txRow})
				}

				err = sink.PersistData(ctx, data)
				if err != nil {
					t.Fatalf("Failed to persist data at tick %d: %v", i+1, err)
				}
			}

			// Final flush
			err = sink.Flush(ctx)
			if err != nil {
				t.Fatalf("Failed to flush: %v", err)
			}

			duration := time.Since(start)
			totalRecords := numTicks + (numTicks * numTxPerTick)
			rps := float64(totalRecords) / duration.Seconds()

			t.Logf("%s Performance: %d records in %v (%.2f records/sec)", 
				cfg.name, totalRecords, duration, rps)

			// Verify final state
			lastTick, err := sink.GetLastTick(ctx)
			if err != nil {
				t.Fatalf("Failed to get last tick: %v", err)
			}

			if lastTick != uint64(numTicks) {
				t.Errorf("Expected last tick %d, got %d", numTicks, lastTick)
			}

			// Get stats if available
			if statsProvider, ok := sink.(StatsProvider); ok {
				stats := statsProvider.GetStats()
				t.Logf("%s Stats: %d ticks, %d transactions, %d flushes", 
					cfg.name, stats.TicksInserted, stats.TransactionsInserted, stats.FlushCount)
			}
		})
	}
}

// TestSinkResilience tests error handling and recovery scenarios
func TestSinkResilience(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sink_resilience_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("InvalidData", func(t *testing.T) {
		cfg := Config{
			Kind: "sqlite",
			DSN:  filepath.Join(tmpDir, "invalid_data.db"),
		}

		sink, err := NewSQLiteSink(cfg)
		if err != nil {
			t.Fatalf("Failed to create sink: %v", err)
		}
		defer sink.Close()

		ctx := context.Background()

		// Try to persist invalid data types
		invalidData := []*parser.ParsedData{
			{Type: "tick", Data: "invalid_data"}, // Should be *models.TickRow
		}

		err = sink.PersistData(ctx, invalidData)
		if err == nil {
			t.Error("Expected error for invalid data type, got nil")
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		cfg := Config{
			Kind: "sqlite",
			DSN:  filepath.Join(tmpDir, "context_cancel.db"),
		}

		sink, err := NewSQLiteSink(cfg)
		if err != nil {
			t.Fatalf("Failed to create sink: %v", err)
		}
		defer sink.Close()

		// Create context that gets cancelled
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		tickRow := &models.TickRow{
			TickNumber: 100,
			Version:   1,
		}

		data := []*parser.ParsedData{
			{Type: "tick", Data: tickRow},
		}

		err = sink.PersistData(ctx, data)
		if err == nil {
			t.Error("Expected error for cancelled context, got nil")
		}
	})

	t.Run("InvalidTickResilience", func(t *testing.T) {
		cfg := Config{
			Kind: "sqlite",
			DSN:  filepath.Join(tmpDir, "invalid_tick.db"),
		}

		sink, err := NewSQLiteSink(cfg)
		if err != nil {
			t.Fatalf("Failed to create sink: %v", err)
		}
		defer sink.Close()

		ctx := context.Background()

		// Add valid tick first
		validTick := &models.TickRow{
			TickNumber:  200,
			TimestampUS: time.Now().UnixMicro(),
			VdfInput:   "input",
			VdfOutput:  "output",
			Version:    1,
		}

		data := []*parser.ParsedData{
			{Type: "tick", Data: validTick},
		}

		err = sink.PersistData(ctx, data)
		if err != nil {
			t.Fatalf("Failed to persist valid tick: %v", err)
		}

		// Invalidate the tick
		err = sink.InvalidateTick(ctx, 200)
		if err != nil {
			t.Fatalf("Failed to invalidate tick: %v", err)
		}

		// Verify sink is still functional
		anotherTick := &models.TickRow{
			TickNumber: 300,
			TimestampUS: time.Now().UnixMicro(),
			VdfInput:   "input2",
			VdfOutput:  "output2",
			Version:    1,
		}

		data = []*parser.ParsedData{
			{Type: "tick", Data: anotherTick},
		}

		err = sink.PersistData(ctx, data)
		if err != nil {
			t.Fatalf("Failed to persist tick after invalidation: %v", err)
		}

		lastTick, err := sink.GetLastTick(ctx)
		if err != nil {
			t.Fatalf("Failed to get last tick: %v", err)
		}

		if lastTick != 300 {
			t.Errorf("Expected last tick 300 after invalidation, got %d", lastTick)
		}
	})
}