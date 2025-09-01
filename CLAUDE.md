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
Sequencer gRPC Stream → Parser → [Batcher OR Direct Write] → Sink Interface → Database
                                         ↓
                                 Performance Optimizations
```

**Two Processing Modes:**
- **Traditional Mode**: Stream → Parser → Batcher → Sink (high throughput)
- **Streaming Mode**: Stream → Parser → Direct Sink Write (ultra-low latency)

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

### 4. Performance Optimizations
**Go Learning**: Configuration patterns, conditional processing, latency optimization
- Streaming mode for direct writes bypassing batcher
- Configurable batch sizes and timeouts
- Raw protobuf storage option for minimal parsing overhead

### 5. Configuration (`internal/config/`)
**Go Learning**: Environment variables, flag parsing, validation
```go
type Config struct {
    SequencerAddr    string `env:"SEQUENCER_ADDR" default:"localhost:9090"`
    SinkKind         string `env:"SINK_KIND" default:"clickhouse"`
    BatchRowsTx      int    `env:"BATCH_ROWS_TX" default:"100"`        // Optimized for low latency
    StreamingMode    bool   `env:"STREAMING_MODE" default:"true"`      // Direct write mode
    LowLatencyMode   bool   `env:"LOW_LATENCY_MODE" default:"true"`    // Performance optimizations
    SkipParsing      bool   `env:"SKIP_PARSING" default:"false"`       // Raw protobuf storage
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
3. **Parser Error**: Log and continue (drop malformed records)
4. **Payload Decode Error**: Log and continue (drop bad records)

### Reorg Handling
**Go Learning**: State management, conflict detection
- Detect conflicting ticks by comparing VDF outputs
- Invalidate old rows, insert new ones with incremented version

## Performance & Observability

### Latency Optimizations
**IMPLEMENTED**: Ultra-low latency streaming pipeline
- **Streaming Mode**: Direct write bypassing batcher (10-20s → sub-second)
- **Small Batches**: BATCH_ROWS_TX=100, BATCH_MAX_WAIT_MS=10ms
- **No Checkpoints**: Always start from latest tick for real-time processing
- **Raw Storage**: Optional SKIP_PARSING for minimal overhead
- **Component Timing**: Per-tick latency breakdown (parse, sink, total)

### Health Checks (`/healthz`)
**Go Learning**: HTTP servers, middleware
- 200 OK when streaming + sink writable
- 500 when in degraded state

### Metrics & Monitoring
**Go Learning**: Performance measurement, instrumentation
- **Latency Metrics**: Parse time, sink time, total processing time per tick
- **Throughput**: Ticks per second, transactions processed
- **Mode Detection**: Traditional vs streaming mode indicators
- **Error Rates**: Failed writes, parser errors, connection issues

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

### Project Structure (Current)
```
tick-streamer/
├── cmd/streamer/           # Main application entry point
├── cmd/api-server/         # REST API server
├── internal/
│   ├── config/            # Configuration management
│   ├── models/            # Data structure definitions  
│   ├── streamer/          # Core streaming logic with latency optimizations
│   ├── parser/            # Pluggable data transformation
│   ├── batcher/           # Concurrent batching system
│   ├── sink/              # Database abstraction layer (ClickHouse focus)
│   ├── resilience/        # Circuit breakers, retries, health checks
│   └── validation/        # Tick validation and reorg detection
├── proto/                 # gRPC definitions (existing)
├── .env                  # Production-ready environment config
├── CLAUDE.md             # Project memory (this file)
└── README.md             # Updated documentation
```

## Current Implementation Status

### ✅ Completed (Production Ready)
- **Ultra-low latency streaming**: 10-20s → sub-second processing
- **Dual processing modes**: Traditional batching + Direct streaming
- **ClickHouse integration**: Optimized for high-throughput analytics
- **Resilience patterns**: Circuit breakers, retries, health monitoring
- **Performance monitoring**: Component-level latency tracking
- **Configuration system**: Environment-based with validation
- **Error handling**: Graceful degradation and recovery

### 🚀 Recent Optimizations (2024)
- **Checkpoint system removed**: Always start from latest tick (0)
- **Streaming mode**: Direct writes bypass batcher entirely  
- **Batch size optimization**: 20k→100 rows, 100ms→10ms timeouts
- **Parser optimization**: Optional raw protobuf storage
- **Resource optimization**: Batcher only runs when needed

### 🔮 Future Enhancements
- **Kafka integration**: Replace in-memory streaming with durable message queue
- **Horizontal scaling**: Multi-instance deployment with load balancing
- **Advanced monitoring**: Prometheus metrics, distributed tracing
- **Schema evolution**: Dynamic protobuf handling

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