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
│  │              ClickHouse Repository                         │ │
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
                                             │   ClickHouse    │
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

1. **Primary**: Query ClickHouse for historical data
2. **Fallback**: Query sequencer REST/gRPC for recent data
3. **Response**: Return 404 if not found in either source

## Data Architecture

### ClickHouse Schema

The system uses ClickHouse as the primary analytical database with ReplacingMergeTree engines for handling reorgs:

```sql
CREATE TABLE default.ticks
(
    `tick_number` UInt64,
    `height` UInt64,
    `block_hash` String CODEC(ZSTD(6)),
    `parent_hash` String CODEC(ZSTD(6)),
    `tx_count` UInt32,
    `payload_size_bytes` UInt64,
    `size_bytes` UInt64,
    `timestamp` UInt64,
    `processed_at` DateTime64(6),
    `proposer_id` String CODEC(ZSTD(6)),
    `proposer_key` String CODEC(ZSTD(6)),
    `chain_id` LowCardinality(String),
    `network` LowCardinality(String),
    `version` Int32,
    INDEX idx_block_hash block_hash TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_parent_hash parent_hash TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_chain chain_id TYPE set(100) GRANULARITY 1,
    INDEX idx_network network TYPE set(100) GRANULARITY 1,
    PROJECTION p_by_processed_full
    (
        SELECT
            processed_at,
            tick_number,
            height,
            block_hash,
            parent_hash,
            tx_count,
            payload_size_bytes,
            size_bytes,
            timestamp,
            proposer_id,
            proposer_key,
            chain_id,
            network,
            version
        ORDER BY
            processed_at,
            tick_number
    ),
    PROJECTION p_recent_slim
    (
        SELECT
            processed_at,
            tick_number,
            height,
            block_hash,
            tx_count,
            network,
            chain_id
        ORDER BY
            processed_at,
            tick_number
    ),
    PROJECTION p_by_block_hash
    (
        SELECT
            block_hash,
            processed_at,
            tick_number,
            height,
            parent_hash,
            tx_count,
            network,
            chain_id,
            version
        ORDER BY block_hash
    )
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
PARTITION BY toYYYYMMDD(processed_at)
PRIMARY KEY (processed_at, tick_number)
ORDER BY (processed_at, tick_number)
TTL processed_at + toIntervalDay(7) RECOMPRESS CODEC(ZSTD(15))
SETTINGS index_granularity = 2048, allow_nullable_key = 0, deduplicate_merge_projection_mode = 'rebuild'

-- Transactions table with compound ordering
CREATE TABLE default.transactions
(
    `tick_number` UInt64,
    `sequence_number` UInt64,
    `tx_hash` String CODEC(ZSTD(6)),
    `tx_id` String CODEC(ZSTD(6)),
    `nonce` UInt64,
    `payload` String CODEC(ZSTD(6)),
    `timestamp` UInt64,
    `public_key` String CODEC(ZSTD(6)),
    `signature` String CODEC(ZSTD(6)),
    `ingestion_timestamp` UInt64,
    `processed_at` DateTime64(6),
    `payload_size` Int32,
    `payload_type` LowCardinality(String),
    `version` Int32,
    INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_public_key public_key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_payload_type payload_type TYPE set(100) GRANULARITY 1,
    INDEX idx_tx_timestamp timestamp TYPE minmax GRANULARITY 8,
    PROJECTION p_by_processed_full
    (
        SELECT
            processed_at,
            tick_number,
            sequence_number,
            tx_hash,
            tx_id,
            nonce,
            payload,
            timestamp,
            public_key,
            signature,
            ingestion_timestamp,
            payload_size,
            payload_type,
            version
        ORDER BY
            processed_at,
            tick_number,
            sequence_number
    ),
    PROJECTION p_recent_slim
    (
        SELECT
            processed_at,
            tx_hash,
            public_key,
            payload_type,
            payload_size
        ORDER BY
            processed_at,
            tick_number,
            sequence_number
    )
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
PARTITION BY toYYYYMMDD(processed_at)
PRIMARY KEY (processed_at, tick_number)
ORDER BY (processed_at, tick_number, sequence_number)
TTL processed_at + toIntervalDay(7) RECOMPRESS CODEC(ZSTD(15))
SETTINGS index_granularity = 2048, allow_nullable_key = 0, deduplicate_merge_projection_mode = 'rebuild'
```

### Data Consistency Model

**Reorg Handling**:

- Old data marked with `version = -1`
- New canonical data inserted with `version = 1+`
- Queries use `WHERE version > 0` or `FINAL` modifier
- Atomic batch operations ensure consistency

**Query Patterns**:

```sql
-- Get canonical tick data
SELECT * FROM ticks FINAL
WHERE tick_number = ? AND version > 0
ORDER BY version DESC LIMIT 1;

-- Get recent transactions
SELECT * FROM transactions FINAL
WHERE version > 0
ORDER BY tick_number DESC, sequence_number DESC
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
│              │ ClickHouse  │  │   SQLite    │  │   Mock    │ │
│              │    Sink     │  │    Sink     │  │   Sink    │ │
│              └─────────────┘  └─────────────┘  └───────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Key Components**:

- **gRPC Client**: Maintains persistent connection to sequencer with automatic reconnection
- **Parser/Transformer**: Converts protobuf data to internal models with validation
- **Batcher**: Aggregates data using configurable size and time-based triggers
- **Sink Interface**: Pluggable storage backends (ClickHouse, SQLite, Mock)
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
│  │ WebSocket   │  │ Repository  │  │   ClickHouse        │  │
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
SINK_KIND=clickhouse
SINK_DSN=clickhouse://user:pass@host:port/db

# ClickHouse configuration
CLICKHOUSE_HOST=clickhouse-host
CLICKHOUSE_PORT=9440
CLICKHOUSE_DATABASE=continuum
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=secure-password

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
X-Data-Source: clickhouse    # Data from ClickHouse
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
| Response Time        | <100ms p95    | For ClickHouse queries   |
| Throughput           | 1,000 req/sec | Per endpoint sustained   |
| Concurrent WebSocket | 10,000        | Connections per instance |
| Fallback Latency     | <500ms        | REST/gRPC proxy calls    |

### Database Performance

**ClickHouse Optimizations**:

- Partitioning by month for time-based queries
- Compound ordering for multi-dimensional access
- ReplacingMergeTree for automatic deduplication
- Index granularity tuned for query patterns
- Background merges for optimal storage

## Deployment Architecture

### Production Deployment

```yaml
# docker-compose.yml structure
version: "3.8"
services:
  streamer:
    image: continuum/streamer:latest
    environment:
      - SINK_KIND=clickhouse
      - CLICKHOUSE_HOST=clickhouse
    depends_on:
      - clickhouse
      - sequencer

  api-server:
    image: continuum/api-server:latest
    ports:
      - "3001:3001"
    environment:
      - CLICKHOUSE_HOST=clickhouse
      - REST_BASE_URL=http://sequencer:8080/api/v1
    depends_on:
      - clickhouse
      - streamer

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "9440:9440" # Secure native TCP
      - "8123:8123" # HTTP interface
    volumes:
      - clickhouse_data:/var/lib/clickhouse
```

### Scaling Patterns

**Horizontal Scaling**:

- Multiple API server instances behind load balancer
- Single streamer instance (stateful checkpoint system)
- ClickHouse cluster for high-availability
- WebSocket sticky sessions for connection management

**Vertical Scaling**:

- Streamer: CPU-bound (parsing) + Memory (batching)
- API Server: CPU-bound (concurrent requests)
- ClickHouse: I/O and Memory intensive

## Monitoring and Observability

### Health Checks

```bash
# Streamer health (internal metrics)
curl http://streamer:8080/health

# API Server health
curl http://api-server:3001/api/v1/health

# ClickHouse health
curl http://clickhouse:8123/ping
```

### Metrics Collection

**Key Metrics**:

- `stream_ticks_received_total` - Ticks processed by streamer
- `api_requests_total` - API requests by endpoint and status
- `websocket_connections_active` - Current WebSocket connections
- `clickhouse_query_duration_seconds` - Database query performance
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
  "data_source": "clickhouse",
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

2. **ClickHouse Unavailability**

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

- Encryption at rest for ClickHouse
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
make test-clickhouse    # Requires ClickHouse instance
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
- [ ] Connection pooling for ClickHouse
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
