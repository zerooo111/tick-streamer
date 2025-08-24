package sink

import (
	"context"
	"sync"
	"time"
	
	"github.com/zerooo111/tick-streamer/internal/parser"
)

// StatsWrapper adds statistics tracking to any Sink implementation
// This demonstrates Go's composition pattern for extending functionality
type StatsWrapper struct {
	sink  Sink
	stats SinkStats
	mutex sync.RWMutex
}

// NewStatsWrapper creates a stats-enabled wrapper around any Sink
func NewStatsWrapper(baseSink Sink) *StatsWrapper {
	return &StatsWrapper{
		sink: baseSink,
		stats: SinkStats{
			Connected: true,
		},
	}
}

// PersistData wraps the base sink's PersistData with stats tracking
func (s *StatsWrapper) PersistData(ctx context.Context, data []*parser.ParsedData) error {
	err := s.sink.PersistData(ctx, data)
	
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	if err != nil {
		s.stats.ErrorCount++
	} else {
		// Track statistics based on data types
		for _, item := range data {
			if item == nil {
				continue
			}
			
			switch item.Type {
			case "tick":
				s.stats.TicksInserted++
				if tickNum, ok := item.Metadata["tick_number"].(uint64); ok && tickNum > s.stats.LastTickNumber {
					s.stats.LastTickNumber = tickNum
				}
			case "transaction":
				s.stats.TransactionsInserted++
			}
		}
	}
	
	return err
}


// InvalidateTick wraps the base sink's InvalidateTick
func (s *StatsWrapper) InvalidateTick(ctx context.Context, tickNumber uint64) error {
	err := s.sink.InvalidateTick(ctx, tickNumber)
	
	if err != nil {
		s.mutex.Lock()
		s.stats.ErrorCount++
		s.mutex.Unlock()
	}
	
	return err
}

// Flush wraps the base sink's Flush with timing statistics
func (s *StatsWrapper) Flush(ctx context.Context) error {
	start := time.Now()
	err := s.sink.Flush(ctx)
	duration := time.Since(start)
	
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	if err != nil {
		s.stats.ErrorCount++
	} else {
		s.stats.FlushCount++
		s.stats.LastFlushDuration = duration.Milliseconds()
		
		// Update rolling average
		if s.stats.FlushCount == 1 {
			s.stats.AverageFlushDuration = float64(duration.Milliseconds())
		} else {
			s.stats.AverageFlushDuration = 0.9*s.stats.AverageFlushDuration + 0.1*float64(duration.Milliseconds())
		}
	}
	
	return err
}

// Close wraps the base sink's Close
func (s *StatsWrapper) Close() error {
	err := s.sink.Close()
	
	s.mutex.Lock()
	s.stats.Connected = false
	s.mutex.Unlock()
	
	return err
}

// Health wraps the base sink's Health
func (s *StatsWrapper) Health(ctx context.Context) bool {
	healthy := s.sink.Health(ctx)
	
	s.mutex.Lock()
	s.stats.Connected = healthy
	s.mutex.Unlock()
	
	return healthy
}

// GetLastTick wraps the base sink's GetLastTick
func (s *StatsWrapper) GetLastTick(ctx context.Context) (uint64, error) {
	return s.sink.GetLastTick(ctx)
}

// GetStats returns current statistics (StatsProvider interface)
func (s *StatsWrapper) GetStats() SinkStats {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	return s.stats
}

// ResetStats clears all counters (StatsProvider interface)
func (s *StatsWrapper) ResetStats() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	lastTick := s.stats.LastTickNumber
	connected := s.stats.Connected
	
	s.stats = SinkStats{
		LastTickNumber: lastTick,
		Connected:      connected,
	}
}