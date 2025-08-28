# Continuum Streamer

A high-performance, database-agnostic streaming data ingestor with a clean plugin architecture. Features a gRPC streaming client, pluggable parsers, and configurable database backends for blockchain sequencer data.

## 🚀 Quick Start

### Prerequisites

- **Go 1.21+** installed
- **Protocol Buffers compiler** (for development)

### Running the Streamer

#### Option 1: Using Make Commands (Recommended)

```bash
# See all available commands
make help

# Run directly from source (development)
make run

# Build binary and run (production)
make build
./bin/streamer

# Install system-wide
make install
streamer

# Clean build artifacts
make clean
```

#### Option 2: Direct Go Commands

```bash
# Run directly from source
go run cmd/streamer/main.go

# Build the binary
go build -o bin/streamer cmd/streamer/main.go

# Run the binary
./bin/streamer
```

#### Option 3: Install and Run

```bash
# Install to $GOPATH/bin
go install ./cmd/streamer

# Run from anywhere (if $GOPATH/bin is in your PATH)
streamer
```

## 📊 What You'll See

When running, the streamer will:

1. **Connect** to the sequencer at `54.242.85.197:9090`
2. **Stream ticks** in real-time with detailed logging
3. **Provide metrics** showing throughput (ticks/second)
4. **Serve health endpoint** at `http://localhost:8080/health`

### Sample Output

```
2025/08/23 06:34:27 Continuum Streamer started successfully
2025/08/23 06:34:27 Connecting to sequencer at: 54.242.85.197:9090
2025/08/23 06:34:27 Health check available at: http://0.0.0.0:8080/health
2025/08/23 06:34:27 Successfully connected to sequencer
2025/08/23 06:34:27 Started streaming from tick 0

=== TICK #337871801 ===
  Timestamp: 1755911067955363 (Unix microseconds)
  Human Time: 2025-08-23 06:34:27.955
  Transactions: 0
  Batch Hash: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
  Previous Output: b9ef4a840aa01f7a1063d1d1ed5fdc971edce55aca1f816bcceee9aa12ba503e...
  Throughput: 2344.1 ticks/sec (total: 20732)
  VDF Proof:
    Input:      4afbd3900821f75e55818f43b21cd4db...
    Output:     c01792b5cad20ad33c80706aafb2563b...
    Iterations: 27
=== END TICK #337871801 ===
```

## ⚙️ Configuration

The streamer uses environment variables for configuration. See `.env.example` for all available options.

### Key Environment Variables

| Variable          | Default              | Description                                           |
| ----------------- | -------------------- | ----------------------------------------------------- |
| `SEQUENCER_ADDR`  | `54.242.85.197:9090` | gRPC address of the sequencer service                 |
| `SINK_KIND`       | `mock`               | Database sink type (`mock`, `postgres`, `clickhouse`) |
| `LOG_LEVEL`       | `info`               | Logging level (`debug`, `info`, `warn`, `error`)      |
| `BATCH_ROWS_TX`   | `20000`              | Transaction batch size for database writes            |
| `BATCH_ROWS_TICK` | `1000`               | Tick batch size for database writes                   |

### Using Environment Variables

#### Option 1: Using make with environment variables

```bash
# Set environment and run with make
SEQUENCER_ADDR="your-sequencer:9090" make run

# Multiple variables
SEQUENCER_ADDR="localhost:9090"  make run
```

#### Option 2: Use .env file

```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your settings
vim .env

# Environment variables will be loaded automatically
make run
```

#### Option 3: Set in shell

```bash
export SEQUENCER_ADDR="your-sequencer:9090"
make run
```

#### Option 4: Direct Go commands with environment

```bash
SEQUENCER_ADDR="localhost:9090"  go run cmd/streamer/main.go
```

## 🔧 Available Commands

### Make Commands (Recommended)

The project includes a `Makefile` with convenient commands:

```bash
# Get help with all available commands
make help
```

| Command            | Description                    |
| ------------------ | ------------------------------ |
| `make run`         | Run the streamer from source   |
| `make build`       | Build binary to `bin/streamer` |
| `make install`     | Install to system PATH         |
| `make clean`       | Remove build artifacts         |
| `make test`        | Run tests                      |
| `make race`        | Run with race detection        |
| `make health`      | Check health endpoint          |
| `make proto`       | Regenerate protobuf code       |
| `make proto-setup` | Install protobuf tools         |
| `make deps`        | Update and verify dependencies |
| `make fmt`         | Format code                    |
| `make lint`        | Run linter                     |

#### Make Examples

```bash
# Development workflow
make run                                    # Run from source
make build                                 # Build binary
make health                               # Check if healthy

# With environment variables
SEQUENCER_ADDR=localhost:9090 make run    # Custom sequencer

# Production workflow
make build                                # Build optimized binary
./bin/streamer                           # Run production binary

# Maintenance
make clean                               # Clean build artifacts
make deps                                # Update dependencies
```

### Direct Go Commands

If you prefer using Go commands directly instead of Make:

#### Run the application

```bash
go run cmd/streamer/main.go
```

#### Build binary

```bash
# Build for current platform
go build -o bin/streamer cmd/streamer/main.go

# Build for Linux (cross-compile)
GOOS=linux GOARCH=amd64 go build -o bin/streamer-linux cmd/streamer/main.go

# Build for Windows (cross-compile)
GOOS=windows GOARCH=amd64 go build -o bin/streamer.exe cmd/streamer/main.go
```

#### Install as system binary

```bash
go install ./cmd/streamer
```

#### Clean build artifacts

```bash
rm -rf bin/
go clean
```

### Protocol Buffer Commands (Development)

If you need to regenerate the gRPC code from `.proto` files:

```bash
# Install protobuf tools (one-time setup)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Regenerate Go code from proto files
protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative proto/continuum.proto
```

### Testing Commands

#### Run tests (when available)

```bash
go test ./...
```

#### Run with race detection

```bash
go run -race cmd/streamer/main.go
```

#### Check for dependencies

```bash
go mod tidy
go mod verify
```

## 🏥 Health Checks

The streamer provides a health endpoint for monitoring:

```bash
# Check if service is healthy (using make)
make health

# Or check manually with curl
curl http://localhost:8080/health

# Expected response: "OK"
```

## 🛑 Stopping the Application

The streamer supports graceful shutdown:

- **Ctrl+C** (SIGINT) - Triggers graceful shutdown
- **SIGTERM** - Also triggers graceful shutdown

During shutdown, the application will:

1. Stop accepting new ticks
2. Process any remaining ticks in the pipeline
3. Save checkpoint data
4. Close database connections
5. Shutdown HTTP server

## 🏗️ Architecture

**Clean Plugin-Based Design:**

```
gRPC Stream → Streamer → Parser Plugin → Sink → Database
```

- **Streamer**: Handles only gRPC streaming and connection management
- **Parser Plugin**: Transforms protobuf data into sink-compatible format
- **Sink**: Generic persistence layer supporting multiple databases
- **Models**: Clean data structures for database persistence

## 📁 Project Structure

```
tick-streamer/
├── cmd/streamer/           # Main application entry point
├── internal/
│   ├── config/            # Configuration management
│   ├── parser/            # Parser plugin interface & implementations
│   ├── models/            # Data structure definitions
│   ├── streamer/          # Core gRPC streaming logic
│   ├── sink/              # Database abstraction layer
│   └── checkpoint/        # Checkpoint persistence
├── proto/                 # gRPC definitions
├── bin/                   # Built binaries (created by build)
├── .env.example          # Environment variable template
└── README.md             # This file
```

## 🚨 Troubleshooting

### Common Issues

#### Connection refused to sequencer

```
Error: failed to connect to sequencer: connection refused
```

**Solution**: Verify the sequencer address and ensure it's accessible:

```bash
# Test connection
telnet 54.242.85.197 9090
```

#### Port already in use

```
Error: HTTP server error: listen tcp :8080: bind: address already in use
```

**Solution**: Change the HTTP port:

```bash
go run cmd/streamer/main.go
```

#### Permission denied on health endpoint

```
Error: curl: (7) Failed to connect to localhost port 8080
```

**Solution**: Check if the service is binding to the correct interface:

```bash
# Try different addresses
curl http://127.0.0.1:8080/health
curl http://0.0.0.0:8080/health
curl http://localhost:8080/health
```

### Performance Tuning

For high-throughput scenarios, consider adjusting:

```bash
# Increase batch sizes for better throughput
export BATCH_ROWS_TX=50000
export BATCH_ROWS_TICK=5000

# Reduce batch timeout for lower latency
export BATCH_MAX_WAIT_MS=50
```

## 📈 Monitoring

### Key Metrics to Watch

1. **Throughput**: Ticks/second in the logs
2. **Health Status**: HTTP endpoint returning 200 OK
3. **Memory Usage**: Should stay under 512MB
4. **Latency**: Time between tick timestamp and processing

### Log Analysis

```bash
# Monitor throughput (when LOG_FILE_DISABLE=false)
tail -f logs/streamer.log | grep "ticks/sec"

# Watch for errors (when LOG_FILE_DISABLE=false)  
tail -f logs/streamer.log | grep -i error

# Count processed ticks (when LOG_FILE_DISABLE=false)
grep "TICK #" logs/streamer.log | wc -l

# With file logging disabled (LOG_FILE_DISABLE=true), monitor via stdout:
# docker logs -f <container_name> | grep "ticks/sec"
# docker logs -f <container_name> | grep -i error
```

## 🤝 Contributing

This project follows Go best practices with a clean plugin architecture:

**Completed:**

- ✅ **Phase 1**: Project structure and gRPC streaming
- ✅ **Phase 2**: Parser plugin system and sink interface
- ✅ **Clean Architecture**: Separated concerns with plugin-based data processing

**Upcoming Phases:**

- 📋 **Phase 3**: Concurrency and batching
- 💾 **Phase 4**: Persistence and checkpoints
- 🔄 **Phase 5**: Error handling and resilience
- ⚙️ **Phase 6**: Advanced configuration
- 🗄️ **Phase 7**: Database adapters
- 📊 **Phase 8**: Observability and production features

**Key Design Principles:**

- **Single Responsibility**: Each component has one clear purpose
- **Plugin Architecture**: Parsers and sinks are easily extensible
- **Clean Interfaces**: Simple, focused method signatures
- **No Backward Compatibility**: Clean codebase without legacy cruft

## 📜 License

This project is part of a Go learning curriculum focused on building production-grade streaming systems.
