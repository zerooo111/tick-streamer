package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zerooo111/tick-streamer/internal/parser"
)

// DebugSink implements the Sink interface but only logs data without persisting
// This is used for debugging purposes to see the raw parsed data
type DebugSink struct {
	stats     SinkStats
	startTime time.Time
}

// NewDebugSink creates a new debug-only sink
func NewDebugSink(cfg Config) (*DebugSink, error) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("🐛 DEBUG MODE ENABLED - Data will only be logged, not persisted to database")
	fmt.Println(strings.Repeat("=", 80) + "\n")
	
	return &DebugSink{
		stats: SinkStats{
			Connected: true,
		},
		startTime: time.Now(),
	}, nil
}

// debugPrint writes clean output without timestamps or prefixes
func debugPrint(format string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, format, args...)
}

// PersistData logs the parsed data instead of persisting it
func (d *DebugSink) PersistData(ctx context.Context, data []*parser.ParsedData) error {
	if len(data) == 0 {
		return nil
	}

	debugPrint("\n%s\n", strings.Repeat("-", 80))
	debugPrint("📦 PROCESSING %d PARSED DATA ITEMS\n", len(data))
	debugPrint("%s\n\n", strings.Repeat("-", 80))
	
	for _, item := range data {
		if item == nil {
			debugPrint("<nil>\n\n")
			continue
		}

		// Only TICK and TRANSACTION types should reach the sink now

		// Just show type and separator
		debugPrint("%s\n", strings.ToUpper(item.Type))
		debugPrint("%s\n", strings.Repeat("-", 40))
		
		// Pretty print the data as JSON
		if item.Data != nil {
			dataJSON, err := json.MarshalIndent(item.Data, "", "  ")
			if err != nil {
				debugPrint("<failed to serialize: %v>\n", err)
			} else {
				debugPrint("%s\n", string(dataJSON))
			}
		} else {
			debugPrint("<nil>\n")
		}
		
		// Log metadata if present
		if len(item.Metadata) > 0 {
			metadataJSON, err := json.MarshalIndent(item.Metadata, "", "  ")
			if err != nil {
				debugPrint("METADATA: <failed to serialize: %v>\n", err)
			} else {
				debugPrint("\nMETADATA:\n%s\n", string(metadataJSON))
			}
		}
		
		debugPrint("\n")
	}
	
	// Update stats as if we persisted the data
	for _, item := range data {
		if item != nil {
			switch item.Type {
			case "tick":
				d.stats.TicksInserted++
			case "transaction":
				d.stats.TransactionsInserted++
			}
		}
	}
	
	return nil
}

// InvalidateTick logs the invalidation request but doesn't actually do anything
func (d *DebugSink) InvalidateTick(ctx context.Context, tickNumber uint64) error {
	debugPrint("\n🗑️  INVALIDATE TICK %d (not actually invalidating)\n\n", tickNumber)
	return nil
}

// Flush is a no-op for debug sink but logs the call
func (d *DebugSink) Flush(ctx context.Context) error {
	d.stats.FlushCount++
	d.stats.LastFlushDuration = 1 // Mock 1ms flush time
	debugPrint("\n💾 FLUSH (no-op)\n\n")
	return nil
}

// Close is a no-op for debug sink
func (d *DebugSink) Close() error {
	debugPrint("\n%s\n", strings.Repeat("=", 80))
	debugPrint("🔚 DEBUG SINK CLOSED\n")
	debugPrint("%s\n\n", strings.Repeat("=", 80))
	return nil
}

// Health always returns true for debug sink
func (d *DebugSink) Health(ctx context.Context) bool {
	return true
}

// GetLastTick returns the mock last tick number
func (d *DebugSink) GetLastTick(ctx context.Context) (uint64, error) {
	return d.stats.LastTickNumber, nil
}

// GetStats returns the mock statistics
func (d *DebugSink) GetStats() SinkStats {
	d.stats.Connected = true
	
	// Calculate average flush duration
	if d.stats.FlushCount > 0 {
		d.stats.AverageFlushDuration = float64(d.stats.FlushCount) // Mock average
	}
	
	return d.stats
}

// ResetStats clears the counters
func (d *DebugSink) ResetStats() {
	d.stats = SinkStats{
		Connected: true,
	}
	debugPrint("\n📊 STATS RESET\n\n")
}

// Ensure DebugSink implements both Sink and StatsProvider interfaces
var _ Sink = (*DebugSink)(nil)
var _ StatsProvider = (*DebugSink)(nil)