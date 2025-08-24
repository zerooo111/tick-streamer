# Continuum Streamer - Project Memory

## Project Overview
**Continuum Streamer** is a high-performance, DB-agnostic streaming data ingestor that connects to a blockchain sequencer service and persists transaction/tick data to various database backends.

### Key Specifications
- **Performance Target**: 10,000 transactions/second sustained throughput
- **Latency Requirement**: <3s p95 ingest-to-query freshness  
- **Memory Footprint**: <512MB under steady-state
- **Architecture**: Single binary with pluggable database adapters

## Current Project State

### Existing Components
- ✅ **Protocol Buffers**: `proto/continuum.proto` defines gRPC service with StreamTicks method
- ✅ **Generated Code**: `proto/continuum.pb.go` and `proto/continuum_grpc.pb.go` 
- ✅ **Basic Client**: `main.go` with simple streaming client (using modern grpc.NewClient)
- ✅ **Dependencies**: `go.mod` configured with gRPC and protobuf libraries

### Data Flow Architecture
```
Sequencer gRPC Stream → Transformer → Batcher → Sink Interface → Database
                                         ↓
                                   Checkpoint System
```

## Core Components to Build

### 1. Data Models (`internal/models/`)
**Go Learning**: Structs, tags, and type definitions
```go
type TickRow struct {
    TickNumber           uint64    `json:"tick_number" db:"tick_number"`
    TimestampUS          int64     `json:"timestamp_us" db:"timestamp_us"`
    VdfInput            string    `json:"vdf_input" db:"vdf_input"`
    // ... more fields
}
```

### 2. Sink Interface (`internal/sink/`)
**Go Learning**: Interfaces, dependency injection, error handling
```go
type Sink interface {
    UpsertTicks(ctx context.Context, ticks []TickRow) error
    UpsertTransactions(ctx context.Context, txs []TxRow) error
    InvalidateTick(ctx context.Context, tickNumber uint64) error
    Flush(ctx context.Context) error
    Close() error
}
```

### 3. Batching System (`internal/streamer/`)
**Go Learning**: Channels, goroutines, timers, select statements
- Size-based batching (20k tx rows, 1k tick rows)
- Time-based batching (100ms max wait)
- Concurrent processing patterns

### 4. Checkpoint System (`internal/checkpoint/`)
**Go Learning**: File I/O, SQLite integration, durability patterns
- SQLite-based persistence of last processed tick
- Atomic checkpoint updates after successful sink flush

### 5. Configuration (`internal/config/`)
**Go Learning**: Environment variables, flag parsing, validation
```go
type Config struct {
    SequencerAddr    string `env:"SEQUENCER_ADDR" default:"localhost:9090"`
    SinkKind         string `env:"SINK_KIND" default:"mock"`
    BatchRowsTx      int    `env:"BATCH_ROWS_TX" default:"20000"`
    // ...
}
```

## Database Adapters

### Mock Adapter (for testing)
**Go Learning**: Interface implementation, testing patterns
- Counts rows, simulates latency
- Perfect for development and load testing

### ClickHouse Adapter (optional)
**Go Learning**: Database drivers, connection pooling
- ReplacingMergeTree with versioning
- Optimized for high-throughput analytics

### PostgreSQL Adapter (optional)
**Go Learning**: SQL generation, transaction management
- COPY for bulk inserts, ON CONFLICT for upserts

## Resilience Features

### Error Handling Categories
**Go Learning**: Error types, context cancellation, retry patterns
1. **gRPC Unavailable**: Exponential backoff with jitter
2. **Sink Write Failed**: Retry with degraded mode fallback  
3. **Checkpoint Save Failed**: Fail-fast to prevent data loss
4. **Payload Decode Error**: Log and continue (drop bad records)

### Reorg Handling
**Go Learning**: State management, conflict detection
- Detect conflicting ticks by comparing VDF outputs
- Invalidate old rows, insert new ones with incremented version

## Observability

### Health Checks (`/healthz`)
**Go Learning**: HTTP servers, middleware
- 200 OK when streaming + sink writable
- 500 when in degraded state

### Metrics (`/metrics`) 
**Go Learning**: Prometheus integration, instrumentation
- Counters: `stream_ticks_received_total`, `reconnects_total`
- Gauges: `last_committed_tick`, `batch_rows_tx`  
- Histograms: `sink_upsert_duration_seconds`

## Go Learning Path Integration

### Phase 1: Basic Go Concepts
- **Packages and imports**: Module structure, internal vs external packages
- **Types and structs**: Defining data models with proper tags
- **Interfaces**: Building the Sink abstraction
- **Error handling**: Go's explicit error handling patterns

### Phase 2: Concurrency Patterns  
- **Goroutines**: Background processing for batching
- **Channels**: Communication between streamer and batcher
- **Select statements**: Multiplexing timer and data channels
- **Context**: Cancellation and timeouts

### Phase 3: Production Patterns
- **Configuration**: Environment variables and validation
- **Logging**: Structured logging with levels  
- **Testing**: Unit tests, integration tests, benchmarks
- **Build and deployment**: Static binaries, containerization

## Development Environment Setup

### Required Tools
```bash
# Protocol Buffers compiler
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Development dependencies will be added to go.mod as needed
```

### Project Structure (Target)
```
tick-streamer/
├── cmd/streamer/           # Main application entry point
├── internal/
│   ├── config/            # Configuration management
│   ├── models/            # Data structure definitions  
│   ├── streamer/          # Core streaming logic
│   ├── sink/              # Database abstraction layer
│   └── checkpoint/        # Checkpoint persistence
├── proto/                 # gRPC definitions (existing)
├── .env.example          # Environment variable template
├── RUNBOOK.md            # Operations guide
└── REORG.md              # Reorg handling guide
```

## Implementation Phases

The spec defines 8 phases from skeleton to performance testing. We'll implement these incrementally, teaching Go concepts at each step while building a production-ready system.

## Key Design Decisions

### Why Go?
- **Performance**: Native compilation, efficient memory management
- **Concurrency**: Built-in goroutines and channels for streaming workloads  
- **Ecosystem**: Excellent gRPC support, database drivers
- **Operations**: Single binary deployment, cross-platform builds

### Why Pluggable Sinks?
- **Database Agnostic**: Works with ClickHouse, PostgreSQL, or custom stores
- **Testing**: Mock implementation enables comprehensive testing
- **Evolution**: Easy to add new backends without changing core logic

### Why Batching?
- **Performance**: Bulk operations are orders of magnitude faster than individual inserts
- **Backpressure**: Natural flow control when downstream systems slow down
- **Transactions**: Atomic batch commits ensure consistency

This project will teach you production Go development while building a real-world, high-performance system!