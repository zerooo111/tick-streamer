package sink

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
	
	"github.com/zerooo111/tick-streamer/internal/models"
	"github.com/zerooo111/tick-streamer/internal/parser"
)

// MockSink is a testing implementation that simulates database operations
// This demonstrates Go's struct-based interface implementation
type MockSink struct {
	config Config
	
	// State management
	connected     bool
	lastTickNumber uint64
	
	// Statistics tracking
	stats SinkStats
	mutex sync.RWMutex // Protects stats from concurrent access
	
	// Simulation parameters
	latencyMS    int
	failureRate  float64 // 0.0 = never fail, 1.0 = always fail
	
	// In-memory storage for testing
	ticks        map[uint64]*models.TickRow
	transactions map[string]*models.TxRow // key: tick_number:sequence_number
}

// NewMockSink creates a new mock sink with the given configuration
// This teaches Go constructor patterns and default values
func NewMockSink(cfg Config) (*MockSink, error) {
	// Set reasonable defaults
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 10000
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 100
	}
	
	sink := &MockSink{
		config:       cfg,
		connected:    true,
		ticks:        make(map[uint64]*models.TickRow),
		transactions: make(map[string]*models.TxRow),
		latencyMS:    10, // Simulate 10ms database latency
		failureRate:  0.0, // Never fail by default
	}
	
	log.Printf("MockSink initialized with batch_size=%d, timeout=%dms", 
		cfg.MaxBatchSize, cfg.BatchTimeout)
	
	return sink, nil
}

// PersistData handles generic parsed data from any parser
// This is the new primary method that supports the plugin architecture
func (m *MockSink) PersistData(ctx context.Context, data []*parser.ParsedData) error {
	if !m.connected {
		return ErrSinkDisconnected
	}
	
	// Simulate processing time
	if err := m.simulateLatency(ctx); err != nil {
		return err
	}
	
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Process each parsed data item based on its type
	for _, item := range data {
		if item == nil {
			m.stats.ErrorCount++
			continue
		}
		
		switch item.Type {
		case "tick":
			if tickRow, ok := item.Data.(*models.TickRow); ok {
				if !tickRow.IsValid() {
					m.stats.ErrorCount++
					continue
				}
				m.ticks[tickRow.TickNumber] = tickRow
				if tickRow.TickNumber > m.lastTickNumber {
					m.lastTickNumber = tickRow.TickNumber
					m.stats.LastTickNumber = tickRow.TickNumber
				}
				m.stats.TicksInserted++
			}
			
		case "transaction":
			if txRow, ok := item.Data.(*models.TxRow); ok {
				if !txRow.IsValid() {
					m.stats.ErrorCount++
					continue
				}
				key := fmt.Sprintf("%d:%d", txRow.TickNumber, txRow.SequenceNumber)
				m.transactions[key] = txRow
				m.stats.TransactionsInserted++
			}
			
		case "log":
			// Handle log data - for now just log it
			if logData, ok := item.Data.(map[string]interface{}); ok {
				if message, exists := logData["message"]; exists {
					if level, exists := logData["level"]; exists {
						log.Printf("Parser Log [%v]: %v", level, message)
					}
				}
			}
			
		case "stats":
			// Handle stats data - could be used for monitoring
			if statsData, ok := item.Data.(map[string]interface{}); ok {
				log.Printf("Parser Stats: %+v", statsData)
			}
			
		default:
			log.Printf("MockSink: Unknown data type: %s", item.Type)
			m.stats.ErrorCount++
		}
	}
	
	log.Printf("MockSink: Persisted %d parsed data items", len(data))
	return nil
}


// InvalidateTick simulates marking tick data as invalid (for reorgs)
func (m *MockSink) InvalidateTick(ctx context.Context, tickNumber uint64) error {
	if !m.connected {
		return ErrSinkDisconnected
	}
	
	if err := m.simulateLatency(ctx); err != nil {
		return err
	}
	
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Remove tick from memory
	delete(m.ticks, tickNumber)
	
	// Remove associated transactions
	for key, tx := range m.transactions {
		if tx.TickNumber == tickNumber {
			delete(m.transactions, key)
		}
	}
	
	log.Printf("MockSink: Invalidated tick %d", tickNumber)
	return nil
}

// Flush simulates committing pending data
func (m *MockSink) Flush(ctx context.Context) error {
	if !m.connected {
		return ErrSinkDisconnected
	}
	
	start := time.Now()
	
	// Simulate flush latency (typically longer than normal operations)
	flushLatency := time.Duration(m.latencyMS*2) * time.Millisecond
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(flushLatency):
		// Flush completed
	}
	
	duration := time.Since(start)
	
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.stats.FlushCount++
	m.stats.LastFlushDuration = duration.Milliseconds()
	
	// Update rolling average
	if m.stats.FlushCount == 1 {
		m.stats.AverageFlushDuration = float64(duration.Milliseconds())
	} else {
		// Simple exponential moving average
		m.stats.AverageFlushDuration = 0.9*m.stats.AverageFlushDuration + 0.1*float64(duration.Milliseconds())
	}
	
	log.Printf("MockSink: Flushed in %v (avg: %.1fms, count: %d)", 
		duration, m.stats.AverageFlushDuration, m.stats.FlushCount)
	
	return nil
}

// Close simulates closing the database connection
func (m *MockSink) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.connected = false
	m.stats.Connected = false
	
	log.Printf("MockSink: Closed (final stats: %d ticks, %d transactions)", 
		m.stats.TicksInserted, m.stats.TransactionsInserted)
	
	return nil
}

// Health returns the connection status
func (m *MockSink) Health(ctx context.Context) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	return m.connected
}

// GetLastTick returns the highest persisted tick number
func (m *MockSink) GetLastTick(ctx context.Context) (uint64, error) {
	if !m.connected {
		return 0, ErrSinkDisconnected
	}
	
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	return m.lastTickNumber, nil
}

// GetStats returns current operational statistics (StatsProvider interface)
func (m *MockSink) GetStats() SinkStats {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	stats := m.stats
	stats.Connected = m.connected
	stats.PendingBatches = 0 // Mock sink doesn't batch
	
	return stats
}

// ResetStats clears all counters (StatsProvider interface)
func (m *MockSink) ResetStats() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.stats = SinkStats{
		LastTickNumber: m.stats.LastTickNumber, // Keep tick number
		Connected:      m.connected,
	}
	
	log.Println("MockSink: Stats reset")
}

// simulateLatency adds realistic database operation delay
func (m *MockSink) simulateLatency(ctx context.Context) error {
	if m.latencyMS <= 0 {
		return nil
	}
	
	latency := time.Duration(m.latencyMS) * time.Millisecond
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(latency):
		return nil
	}
}

// SetLatency allows adjusting simulated latency for testing
func (m *MockSink) SetLatency(ms int) {
	m.latencyMS = ms
}

// SetFailureRate allows simulating database failures for testing
func (m *MockSink) SetFailureRate(rate float64) {
	m.failureRate = rate
}

// GetStoredTickCount returns the number of ticks in memory (for testing)
func (m *MockSink) GetStoredTickCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	return len(m.ticks)
}

// GetStoredTransactionCount returns the number of transactions in memory (for testing)
func (m *MockSink) GetStoredTransactionCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	return len(m.transactions)
}