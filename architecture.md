# Tick Streamer Architecture

## Overview

The Continuum Tick Streamer is a high-performance, production-ready Go application designed for real-time blockchain data ingestion and serving. The system consists of two main services that work together to provide comprehensive tick and transaction data management.

## System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Sequencer     │    │  Match Engine   │    │   Frontend      │
│   (gRPC)        │    │   (REST API)    │    │   Client        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │ gRPC Stream           │ HTTP                  │ HTTP/WS
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    API SERVER (Port 3001)                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   REST API      │  │   WebSocket     │  │   Middleware    │ │
│  │   Handlers      │  │   Hub           │  │   (CORS/Auth)   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│            │                    │                    │          │
│            ▼                    ▼                    ▼          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              TimescaleDB Repository                        │ │
│  │         (Primary Data Source + Fallback)                  │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
         │                                            ▲
         │ Fallback HTTP/gRPC                        │ Read/Write
         ▼                                            │
┌─────────────────┐                          ┌─────────────────┐
│   Sequencer     │         gRPC Stream      │   STREAMER      │
│   Service       │◄─────────────────────────┤   SERVICE       │
│   (Fallback)    │                          │                 │
└─────────────────┘                          │  ┌─────────────┐ │
                                             │  │ Sink Layer  │ │
                                             │  │(Pluggable)  │ │
                                             │  └─────────────┘ │
                                             └─────────────────┘
                                                        │
                                                        ▼
                                             ┌─────────────────┐
                                             │   TimescaleDB   │
                                             │   Database      │
                                             │                 │
                                             │ ┌─────────────┐ │
                                             │ │   ticks     │ │
                                             │ │transactions │ │
                                             │ └─────────────┘ │
                                             └─────────────────┘
```

## Core Services

### 1. Streamer Service (`cmd/streamer`)

**Purpose**: High-throughput data ingestion from blockchain sequencer to persistent storage.

**Key Responsibilities**:

- Connect to sequencer gRPC stream
- Parse and validate tick/transaction data
- Batch data for optimal database performance
- Handle reorg scenarios with versioning
- Maintain checkpoint state for recovery

**Performance Characteristics**:

- **Target**: 10,000 transactions/second sustained throughput
- **Latency**: <3s p95 ingest-to-query freshness
- **Memory**: <512MB under steady-state
- **Architecture**: Single binary with pluggable sinks

### 2. API Server (`cmd/api-server`)

**Purpose**: High-performance REST and WebSocket API for querying blockchain data.

**Key Responsibilities**:

- Serve REST API endpoints for tick/transaction queries
- Provide real-time WebSocket streaming
- Proxy external service requests (Match Engine)
- Implement intelligent data source routing

**Data Flow Strategy**:

1. **Primary**: Query TimescaleDB for historical data
2. **Fallback**: Query sequencer REST/gRPC for recent data
3. **Response**: Return 404 if not found in either source

## Data Architecture

### TimescaleDB Schema

The system uses TimescaleDB as the primary time-series database with hypertables and compression for efficient storage:

```sql
-- Ticks hypertable for time-series blockchain data
CREATE TABLE public.ticks (
    tick_number BIGINT NOT NULL,
    height BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    parent_hash TEXT NOT NULL,
    tx_count INTEGER NOT NULL,
    payload_size_bytes BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    proposer_id TEXT NOT NULL,
    proposer_key TEXT NOT NULL,
    chain_id TEXT NOT NULL,
    network TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    
    CONSTRAINT pk_ticks PRIMARY KEY (processed_at, tick_number)
);

-- Convert to hypertable partitioned by time
SELECT create_hypertable('public.ticks', 'processed_at', 
    chunk_time_interval => INTERVAL '1 day');

-- Create indexes for common queries
CREATE INDEX CONCURRENTLY idx_ticks_tick_number ON public.ticks (tick_number);
CREATE INDEX CONCURRENTLY idx_ticks_block_hash ON public.ticks USING HASH (block_hash);
CREATE INDEX CONCURRENTLY idx_ticks_chain_network ON public.ticks (chain_id, network);
CREATE INDEX CONCURRENTLY idx_ticks_version ON public.ticks (version) WHERE version > 0;

-- Enable compression for older chunks (7 days)
ALTER TABLE public.ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'chain_id,network',
    timescaledb.compress_orderby = 'processed_at DESC, tick_number DESC'
);

SELECT add_compression_policy('public.ticks', INTERVAL '7 days');

-- Transactions hypertable
CREATE TABLE public.transactions (
    tick_number BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    tx_id TEXT NOT NULL,
    nonce BIGINT NOT NULL,
    payload TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    public_key TEXT NOT NULL,
    signature TEXT NOT NULL,
    ingestion_timestamp BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_size INTEGER NOT NULL,
    payload_type TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    
    CONSTRAINT pk_transactions PRIMARY KEY (processed_at, tick_number, sequence_number)
);

-- Convert to hypertable
SELECT create_hypertable('public.transactions', 'processed_at',
    chunk_time_interval => INTERVAL '1 day');

-- Create indexes for transactions
CREATE INDEX CONCURRENTLY idx_transactions_tx_hash ON public.transactions USING HASH (tx_hash);
CREATE INDEX CONCURRENTLY idx_transactions_public_key ON public.transactions USING HASH (public_key);
CREATE INDEX CONCURRENTLY idx_transactions_payload_type ON public.transactions (payload_type);
CREATE INDEX CONCURRENTLY idx_transactions_version ON public.transactions (version) WHERE version > 0;

-- Enable compression
ALTER TABLE public.transactions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'payload_type',
    timescaledb.compress_orderby = 'processed_at DESC, tick_number DESC, sequence_number DESC'
);

SELECT add_compression_policy('public.transactions', INTERVAL '7 days');
```

### Data Consistency Model

**Reorg Handling**:

- Old data marked with `version = -1`
- New canonical data inserted with `version = 1+`
- Queries use `WHERE version > 0` or `FINAL` modifier
- Atomic batch operations ensure consistency

**Query Patterns**:

```sql
-- Get canonical tick data (latest version)
SELECT * FROM public.ticks
WHERE tick_number = ? AND version > 0
ORDER BY version DESC LIMIT 1;

-- Get recent transactions
SELECT * FROM public.transactions
WHERE version > 0
ORDER BY processed_at DESC, tick_number DESC, sequence_number DESC
LIMIT ?;
```

## Component Architecture

### Streamer Service Components

```
┌─────────────────────────────────────────────────────────────┐
│                    STREAMER SERVICE                         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │    gRPC     │  │   Parser/   │  │      Batcher        │  │
│  │   Client    │──┤ Transformer │──┤  (Size + Time)      │  │
│  │             │  │             │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                                              │               │
│                                              ▼               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Checkpoint  │  │    Sink     │  │    Sink Interface   │  │
│  │   System    │◄─┤  Manager    │◄─┤   (Pluggable)      │  │
│  │             │  │             │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                                              │               │
│                                              ▼               │
│              ┌─────────────┐  ┌─────────────┐  ┌───────────┐ │
│              │ TimescaleDB │  │   Debug     │  │   Mock    │ │
│              │    Sink     │  │    Sink     │  │   Sink    │ │
│              └─────────────┘  └─────────────┘  └───────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Key Components**:

- **gRPC Client**: Maintains persistent connection to sequencer with automatic reconnection
- **Parser/Transformer**: Converts protobuf data to internal models with validation
- **Batcher**: Aggregates data using configurable size and time-based triggers
- **Sink Interface**: Pluggable storage backends (TimescaleDB, Debug, Mock)
- **Checkpoint System**: SQLite-based persistence for recovery and exactly-once processing

### API Server Components

```
┌─────────────────────────────────────────────────────────────┐
│                     API SERVER                             │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │    Gin      │  │ Middleware  │  │     Handlers        │  │
│  │   Router    │──┤ (CORS/Auth/ │──┤  (REST Endpoints)   │  │
│  │             │  │  Logging)   │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                                              │               │
│                                              ▼               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ WebSocket   │  │ Repository  │  │   TimescaleDB       │  │
│  │    Hub      │  │   Layer     │──┤   Repository        │  │
│  │             │  │             │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         │                                   │               │
│         ▼                                   ▼               │
│  ┌─────────────┐           ┌─────────────────────────────┐  │
│  │    gRPC     │           │        Fallback             │  │
│  │   Client    │           │    (REST/gRPC Proxy)        │  │
│  │             │           │                             │  │
│  └─────────────┘           └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Key Components**:

- **Gin Router**: High-performance HTTP router with middleware support
- **Repository Layer**: Data access abstraction with intelligent source routing
- **WebSocket Hub**: Real-time tick streaming with client management
- **Fallback System**: Automatic failover to external REST/gRPC services

## Configuration Management

### Environment-Based Configuration

Both services use mandatory `.env` files for security:

```env
# Sequencer connection settings
SEQUENCER_ADDR=sequencer-host:9090
SEQUENCER_TLS=false
SEQUENCER_MTLS=false

# Database sink configuration
SINK_KIND=timescaledb
SINK_DSN=postgres://user:pass@host:port/db

# TimescaleDB configuration
TIMESCALEDB_HOST=timescaledb-host
TIMESCALEDB_PORT=5432
TIMESCALEDB_DATABASE=continuum
TIMESCALEDB_USERNAME=postgres
TIMESCALEDB_PASSWORD=secure-password

# API server configuration
API_PORT=3001
REST_BASE_URL=http://sequencer-rest:8080/api/v1
MATCH_ENGINE_URL=http://match-engine:8081/api/v1

# CORS and security
CORS_ALLOWED_ORIGINS=https://frontend.example.com
CORS_ALLOW_CREDENTIALS=false

# Performance tuning
BATCH_ROWS_TX=20000
BATCH_ROWS_TICK=1000
BATCH_MAX_WAIT_MS=100
```

### Security Model

- **No Hardcoded Defaults**: All sensitive values must be in `.env`
- **Fail-Fast**: Applications exit if required configuration missing
- **Principle of Least Privilege**: Database users have minimal required permissions
- **TLS Support**: Configurable encryption for all network connections

## API Design

### REST API Endpoints

The API server provides a comprehensive REST interface:

```
GET  /                              - Service information
GET  /api/v1/health                 - Health check
GET  /api/v1/status                 - Sequencer status
GET  /api/v1/tx/{hash}              - Get transaction by hash
POST /api/v1/tx                     - Submit transaction
POST /api/v1/tx/batch               - Submit batch transactions
GET  /api/v1/tick/{number}          - Get tick by number
GET  /api/v1/ticks/recent           - Get recent ticks
GET  /api/v1/chain/state            - Get chain state
GET  /api/v1/me/markets             - Get markets (proxy)
GET  /api/v1/me/markets/{id}/orderbook - Get orderbook (proxy)
WS   /ws/ticks                      - Real-time tick stream
```

### Response Headers

All responses include data source tracking:

```http
X-Data-Source: timescaledb   # Data from TimescaleDB
X-Data-Source: rest-api      # Data from REST fallback
X-Data-Source: grpc          # Data from gRPC fallback
Cache-Control: private, max-age=600  # Caching directives
```

### WebSocket Protocol

Real-time tick streaming with backpressure handling:

```javascript
// Connection with start tick parameter
ws://api-server:3001/ws/ticks?start_tick=12345

// Tick message format
{
  "type": "tick",
  "tick_number": "541456555",
  "timestamp": "1755945316395000",
  "transaction_count": 1,
  "transactions": [...]
}

// Error message format
{
  "type": "error",
  "error": "Connection lost to sequencer"
}
```

## Performance Characteristics

### Streamer Service

| Metric     | Target        | Notes                         |
| ---------- | ------------- | ----------------------------- |
| Throughput | 10,000 tx/sec | Sustained under normal load   |
| Latency    | <3s p95       | Ingest-to-query freshness     |
| Memory     | <512MB        | Steady-state with batching    |
| Recovery   | <30s          | From checkpoint after restart |

### API Server

| Metric               | Target        | Notes                    |
| -------------------- | ------------- | ------------------------ |
| Response Time        | <100ms p95    | For TimescaleDB queries  |
| Throughput           | 1,000 req/sec | Per endpoint sustained   |
| Concurrent WebSocket | 10,000        | Connections per instance |
| Fallback Latency     | <500ms        | REST/gRPC proxy calls    |

### Database Performance

**TimescaleDB Optimizations**:

- Automatic time-based partitioning with hypertables
- Compression policies for efficient storage
- Time-series optimized indexes
- Continuous aggregates for fast analytics
- Background compression and retention policies

## Deployment Architecture

### Production Deployment

```yaml
# docker-compose.yml structure
version: "3.8"
services:
  streamer:
    image: continuum/streamer:latest
    environment:
      - SINK_KIND=timescaledb
      - TIMESCALEDB_HOST=timescaledb
    depends_on:
      - timescaledb
      - sequencer

  api-server:
    image: continuum/api-server:latest
    ports:
      - "3001:3001"
    environment:
      - TIMESCALEDB_HOST=timescaledb
      - REST_BASE_URL=http://sequencer:8080/api/v1
    depends_on:
      - timescaledb
      - streamer

  timescaledb:
    image: timescale/timescaledb:latest-pg16
    ports:
      - "5432:5432" # PostgreSQL port
    volumes:
      - timescaledb_data:/var/lib/postgresql/data
    environment:
      - POSTGRES_DB=continuum
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=secure-password
```

### Scaling Patterns

**Horizontal Scaling**:

- Multiple API server instances behind load balancer
- Single streamer instance (stateful checkpoint system)
- TimescaleDB cluster for high-availability
- WebSocket sticky sessions for connection management

**Vertical Scaling**:

- Streamer: CPU-bound (parsing) + Memory (batching)
- API Server: CPU-bound (concurrent requests)
- TimescaleDB: I/O and Memory intensive

## Monitoring and Observability

### Health Checks

```bash
# Streamer health (internal metrics)
curl http://streamer:8080/health

# API Server health
curl http://api-server:3001/api/v1/health

# TimescaleDB health
psql -h timescaledb -U postgres -d continuum -c "SELECT 1;"
```

### Metrics Collection

**Key Metrics**:

- `stream_ticks_received_total` - Ticks processed by streamer
- `api_requests_total` - API requests by endpoint and status
- `websocket_connections_active` - Current WebSocket connections
- `timescaledb_query_duration_seconds` - Database query performance
- `batch_size_bytes` - Ingestion batch sizes
- `checkpoint_lag_seconds` - Recovery point objective

### Logging Strategy

**Structured JSON Logging**:

```json
{
  "timestamp": "2025-08-24T11:20:08Z",
  "level": "info",
  "service": "api-server",
  "component": "handlers",
  "action": "get_tick",
  "tick_number": 12345,
  "data_source": "timescaledb",
  "latency_ms": 45,
  "client_ip": "192.168.1.100"
}
```

## Error Handling and Recovery

### Failure Modes

1. **Sequencer Disconnection**

   - Exponential backoff reconnection
   - Checkpoint-based recovery
   - Graceful degradation of API responses

2. **TimescaleDB Unavailability**

   - Automatic fallback to REST/gRPC
   - Connection pooling with health checks
   - Circuit breaker pattern

3. **API Server Overload**
   - Request rate limiting
   - Graceful WebSocket connection drops
   - Load shedding for non-essential endpoints

### Recovery Procedures

**Streamer Recovery**:

```bash
# Check last checkpoint
sqlite3 checkpoint.db "SELECT * FROM checkpoints ORDER BY tick_number DESC LIMIT 1;"

# Resume from specific tick
RESTART_FROM_TICK=12345 make run
```

**Data Integrity Verification**:

```sql
-- Verify data consistency
SELECT
  tick_number,
  COUNT(*) as versions,
  MAX(version) as latest_version
FROM ticks
GROUP BY tick_number
HAVING COUNT(*) > 1
ORDER BY tick_number DESC
LIMIT 10;
```

## Security Considerations

### Network Security

- TLS termination at load balancer
- Internal service mesh with mTLS
- Network policies for service isolation
- Rate limiting and DDoS protection

### Data Security

- Encryption at rest for TimescaleDB
- Connection string encryption in configs
- Audit logging for data access
- Regular security updates and patches

### Authentication & Authorization

- API key-based authentication for external access
- Service-to-service authentication via certificates
- Role-based access control for database users
- CORS configuration for web client access

## Development Workflow

### Local Development

```bash
# Setup development environment
make dev-setup

# Run services locally
make setup-env    # Copy .env.example to .env
vim .env          # Configure local settings
make run-api      # Start API server
make run          # Start streamer (separate terminal)
```

### Testing Strategy

```bash
# Unit tests
go test ./...

# Integration tests
make test-timescaledb    # Requires TimescaleDB instance
make test-sinks        # Test all sink implementations

# Load testing
make run &
curl -X POST http://localhost:3001/api/v1/tx/batch -d @test_batch.json
```

### Code Organization

```
tick-streamer/
├── cmd/                    # Application entry points
│   ├── streamer/          # Streamer service main
│   └── api-server/        # API server main
├── internal/              # Private application code
│   ├── api/              # API server components
│   │   ├── handlers/     # HTTP request handlers
│   │   ├── middleware/   # HTTP middleware
│   │   ├── repository/   # Data access layer
│   │   └── websocket/    # WebSocket hub
│   ├── config/           # Configuration management
│   ├── models/           # Data models and types
│   ├── parser/           # Data parsing and transformation
│   ├── sink/             # Pluggable storage backends
│   └── streamer/         # Streaming and batching logic
├── proto/                # Protocol buffer definitions
├── .env.example          # Environment template
├── Makefile             # Development commands
└── docker-compose.yml   # Local deployment
```

## Future Roadmap

### Performance Enhancements

- [ ] Parallel batch processing in streamer
- [ ] Connection pooling for TimescaleDB
- [ ] Query result caching layer
- [ ] Metrics-based auto-scaling

### Feature Additions

- [ ] Historical data export API
- [ ] Advanced filtering and search
- [ ] Real-time analytics dashboards
- [ ] Multi-tenant support

### Operational Improvements

- [ ] Prometheus metrics integration
- [ ] Distributed tracing support
- [ ] Automated backup and restore
- [ ] Performance regression testing

---

This architecture provides a solid foundation for high-performance blockchain data ingestion and serving, with built-in scalability, reliability, and maintainability features.
