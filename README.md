# Continuum Streamer

A **ultra-low latency**, high-performance streaming data ingestor for blockchain sequencer data. Features dual processing modes: traditional batching for high throughput and direct streaming for sub-second latency.

## 🚀 Quick Start

### Prerequisites

- **Go 1.21+** installed
- **TimescaleDB** database access (or use debug mode)

### Running the Streamer

```bash
# Clone and run
git clone <repo>
cd tick-streamer
go run ./cmd/streamer
```

The streamer will connect to the sequencer and start streaming real-time tick data with **sub-second latency**.

## ⚡ Performance Optimizations

### Ultra-Low Latency Mode (ENABLED)

```bash
STREAMING_MODE=true      # Direct write bypassing batcher
LOW_LATENCY_MODE=true    # All optimizations enabled
BATCH_ROWS_TX=100        # Small batches: 20k→100
BATCH_MAX_WAIT_MS=10     # Fast timeout: 100ms→10ms
```

**Results**: **10-20 seconds → sub-second** processing latency

### Latency Breakdown Monitoring

```
⏱️  Tick #337871801 latency breakdown: parse=127μs, sink=2.1ms, total=2.3ms
🚀 STREAMING: Direct write mode enabled, bypassing batcher
📊 STREAMER: 1000 ticks in 42s (23.8 ticks/sec), last_tick: 337871801
```

## 🏗️ Architecture

### Dual Processing Modes

**Traditional Mode** (High Throughput):
```
gRPC Stream → Parser → Batcher → TimescaleDB
```

**Streaming Mode** (Ultra-Low Latency):
```
gRPC Stream → Parser → Direct Write → TimescaleDB
```

### Key Components

- **Streamer**: gRPC connection management with resilience patterns
- **Parser**: Pluggable data transformation (supports raw protobuf passthrough)
- **Batcher**: Concurrent batching system (optional in streaming mode)
- **Sink**: TimescaleDB-optimized database layer
- **No Checkpoints**: Always starts from latest tick (0) for real-time streaming

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STREAMING_MODE` | `true` | Enable direct write mode |
| `LOW_LATENCY_MODE` | `true` | Enable all latency optimizations |
| `BATCH_ROWS_TX` | `100` | Transaction batch size (was 20000) |
| `BATCH_MAX_WAIT_MS` | `10` | Batch timeout (was 100ms) |
| `SKIP_PARSING` | `false` | Store raw protobuf for minimal overhead |
| `SEQUENCER_ADDR` | `54.242.85.197:9090` | gRPC sequencer address |

### Database Configuration

#### TimescaleDB Mode
```bash
SINK_KIND=timescaledb
TIMESCALEDB_HOST=localhost
TIMESCALEDB_PORT=5432
TIMESCALEDB_DATABASE=continuum
TIMESCALEDB_USERNAME=postgres
TIMESCALEDB_PASSWORD=your-password
```

#### Debug Mode (No Database)
```bash
SINK_KIND=debug
# No database connection required
# Logs all operations for development/testing
```

## 📊 What You'll See

### Streaming Mode Output

```bash
🚀 Streaming mode enabled - batcher bypassed for direct writes
⏱️  Tick #337871801 latency breakdown: parse=127μs, sink=2.1ms, total=2.3ms
📊 STREAMER: 1000 ticks in 42s (23.8 ticks/sec), last_tick: 337871801
🚀 STREAMING: Direct write mode enabled, bypassing batcher
```

### Traditional Mode Output

```bash
🔄 Batcher started for traditional batched processing
📦 BATCHER: 42 batches, 4200 items, queue_depth=0, avg_flush=15.2ms
✅ Flushed batch: 100 items (ticks=10, tx=90) in 12ms (trigger=size_limit)
```

## 🔧 Commands

### Development

```bash
# Run with optimizations
go run ./cmd/streamer

# Build optimized binary
go build -o streamer ./cmd/streamer

# Debug mode (no database required)
DEBUG_MODE=true go run ./cmd/streamer
```

### Performance Testing

```bash
# Enable all optimizations
STREAMING_MODE=true LOW_LATENCY_MODE=true go run ./cmd/streamer

# Traditional batching mode
STREAMING_MODE=false LOW_LATENCY_MODE=false go run ./cmd/streamer

# Raw protobuf storage (minimal parsing)
SKIP_PARSING=true go run ./cmd/streamer
```

## 📈 Performance Monitoring

### Health Check

```bash
curl http://localhost:8080/health
# Returns: "OK" with 200 status
```

### Key Metrics

1. **Per-tick latency**: Parse time + Sink time = Total time
2. **Throughput**: Ticks per second
3. **Mode detection**: Streaming vs batching indicators
4. **Queue depth**: Batcher backlog (streaming mode = 0)

### Log Analysis

```bash
# Monitor latency (streaming mode)
tail -f logs/streamer.log | grep "latency breakdown"

# Monitor throughput
tail -f logs/streamer.log | grep "ticks/sec"

# Check for streaming mode
tail -f logs/streamer.log | grep "STREAMING"
```

## 🚨 Troubleshooting

### High Latency Issues

1. **Check mode**: Ensure `STREAMING_MODE=true`
2. **Batch sizes**: Use small batches (100 rows, 10ms timeout)
3. **Network**: Verify TimescaleDB connectivity
4. **Parsing**: Consider `SKIP_PARSING=true` for raw storage

### Connection Issues

```bash
# Test sequencer connectivity
telnet 54.242.85.197 9090

# Test TimescaleDB connectivity
psql -h localhost -U postgres -d continuum -c "SELECT 1;"
```

## 📁 Project Structure

```
tick-streamer/
├── cmd/
│   ├── streamer/          # Ultra-low latency streamer
│   └── api-server/        # REST API server
├── internal/
│   ├── config/           # Environment-based configuration
│   ├── streamer/         # Core streaming logic + optimizations
│   ├── parser/           # Pluggable data transformation
│   ├── batcher/          # Concurrent batching (optional)
│   ├── sink/             # TimescaleDB-optimized database layer
│   ├── resilience/       # Circuit breakers, retries, health
│   └── validation/       # Tick validation and reorg detection
├── proto/                # gRPC definitions
├── .env                  # Production configuration
└── CLAUDE.md            # Detailed project memory
```

## 🎯 Use Cases

### Real-Time Analytics

```bash
# Ultra-low latency for real-time dashboards
STREAMING_MODE=true LOW_LATENCY_MODE=true ./streamer
```

### High-Throughput Ingestion

```bash
# Traditional batching for maximum throughput
STREAMING_MODE=false BATCH_ROWS_TX=10000 ./streamer
```

### Development/Testing

```bash
# Debug mode (no database required)
DEBUG_MODE=true ./streamer
```

## 🔮 Future Roadmap

- **Kafka Integration**: Replace in-memory streaming with durable message queue
- **Horizontal Scaling**: Multi-instance deployment with load balancing  
- **Prometheus Metrics**: Advanced monitoring and alerting
- **Dynamic Configuration**: Runtime configuration updates without restart

## 📜 Performance Results

**Before Optimization**: 10-20 second latency (large batches, checkpoint overhead)
**After Optimization**: Sub-second latency (direct writes, no checkpoints)

**Improvement**: **99%+ latency reduction** while maintaining data integrity and TimescaleDB's high throughput time-series capabilities.

---

*This project demonstrates production-grade Go development with focus on ultra-low latency streaming systems.*