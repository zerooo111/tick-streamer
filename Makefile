# Continuum Streamer Makefile
# Provides convenient commands for development and deployment

.PHONY: help run run-api build build-all install clean clean-data test proto health run-log run-sqlite run-clickhouse run-clickhouse-env logs show-logs test-sinks test-clickhouse setup-env dev-setup api-health

# Default target
help:
	@echo "Continuum Streamer - Available Commands:"
	@echo ""
	@echo "Development:"
	@echo "  make run        - Run the streamer from source (default: mock sink)"
	@echo "  make run-api    - Run the API server from source"
	@echo "  make run-log    - Run streamer with log file sink"
	@echo "  make run-sqlite - Run streamer with SQLite sink"
	@echo "  make run-clickhouse - Run streamer with ClickHouse sink"
	@echo "  make run-clickhouse-env - Run streamer with ClickHouse using .env file"
	@echo "  make logs       - Watch log files in real-time"
	@echo "  make build      - Build both binaries to bin/"
	@echo "  make build-all  - Build for multiple platforms"
	@echo "  make install    - Install to GOPATH/bin"
	@echo "  make clean      - Clean build artifacts"
	@echo ""
	@echo "Testing:"
	@echo "  make test       - Run tests"
	@echo "  make test-sinks - Test all database sinks"
	@echo "  make test-clickhouse - Test ClickHouse integration"
	@echo "  make race       - Run with race detection"
	@echo "  make health     - Check streamer health endpoint"
	@echo "  make api-health - Check API server health endpoint"
	@echo ""
	@echo "Data Management:"
	@echo "  make show-logs  - Show recent log entries"
	@echo "  make clean-data - Clean log files and databases"
	@echo "  make setup-env  - Copy .env.example to .env"
	@echo "  make dev-setup  - Set up development environment"
	@echo ""
	@echo "Protocol Buffers:"
	@echo "  make proto      - Regenerate protobuf code"
	@echo "  make proto-setup - Install protobuf tools"
	@echo ""
	@echo "Examples:"
	@echo "  make setup-env          # Copy .env.example to .env and configure"
	@echo "  make run-api           # Run API server (requires .env file)"
	@echo "  make run-log           # Run streamer with log sink (requires .env file)"
	@echo "  make setup-env && vim .env  # Setup and customize configuration"

# Run the application from source
run:
	go run cmd/streamer/main.go

# Run the API server from source
run-api:
	@echo "🚀 Starting API server..."
	@echo "📋 Loading configuration from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Run 'make setup-env' first"; \
		exit 1; \
	fi
	go run cmd/api-server/main.go

# Run with log file sink
run-log:
	@echo "🚀 Starting streamer with Log File sink..."
	@echo "📋 Loading configuration from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Run 'make setup-env' first"; \
		exit 1; \
	fi
	@mkdir -p logs
	go run cmd/streamer/main.go

# Run with SQLite sink
run-sqlite:
	@echo "🚀 Starting streamer with SQLite sink..."
	@echo "📋 Loading configuration from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Run 'make setup-env' first"; \
		exit 1; \
	fi
	go run cmd/streamer/main.go

# Run with ClickHouse sink
run-clickhouse:
	@echo "🚀 Starting streamer with ClickHouse sink..."
	@echo "📋 Loading configuration from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Run 'make setup-env' first"; \
		exit 1; \
	fi
	go run cmd/streamer/main.go

# Run with ClickHouse sink using environment file
run-clickhouse-env:
	@echo "🚀 Starting streamer with ClickHouse sink (using .env)..."
	@echo "📊 Loading configuration from .env file..."
	@if [ -f ".env" ]; then \
		export $$(cat .env | grep -v '^#' | xargs) && SINK_KIND=clickhouse go run cmd/streamer/main.go; \
	else \
		echo "❌ .env file not found. Copy .env.example to .env first"; \
		exit 1; \
	fi

# Watch log files in real-time
logs:
	@echo "👀 Watching log files in real-time..."
	@echo "Press Ctrl+C to stop"
	@if [ -d "logs" ]; then \
		tail -f logs/*.jsonl 2>/dev/null || echo "No log files found in ./logs/"; \
	else \
		echo "❌ logs/ directory not found. Run 'make run-log' first."; \
	fi

# Build binary
build:
	@mkdir -p bin
	go build -o bin/streamer cmd/streamer/main.go
	go build -o bin/api-server cmd/api-server/main.go
	@echo "Binaries built: bin/streamer, bin/api-server"

# Build for multiple platforms
build-all:
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 go build -o bin/streamer-linux cmd/streamer/main.go
	GOOS=windows GOARCH=amd64 go build -o bin/streamer.exe cmd/streamer/main.go
	GOOS=darwin GOARCH=amd64 go build -o bin/streamer-darwin cmd/streamer/main.go
	GOOS=linux GOARCH=amd64 go build -o bin/api-server-linux cmd/api-server/main.go
	GOOS=windows GOARCH=amd64 go build -o bin/api-server.exe cmd/api-server/main.go
	GOOS=darwin GOARCH=amd64 go build -o bin/api-server-darwin cmd/api-server/main.go
	@echo "Built for multiple platforms in bin/"

# Install to system
install:
	go install ./cmd/streamer
	go install ./cmd/api-server
	@echo "Installed to GOPATH/bin"

# Clean build artifacts
clean:
	rm -rf bin/
	go clean
	@echo "Cleaned build artifacts"

# Clean log files and databases
clean-data:
	@echo "🧹 Cleaning data files..."
	rm -rf logs/
	rm -f tick_streamer.db*
	@echo "Cleaned log files and databases"

# Run tests
test:
	go test ./...

# Run with race detection
race:
	go run -race cmd/streamer/main.go

# Check health endpoint
health:
	@echo "Checking streamer health endpoint..."
	@echo "📋 Reading HTTP_BIND from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Cannot determine health endpoint."; \
		exit 1; \
	fi
	@HTTP_BIND=$$(grep "^HTTP_BIND=" .env | cut -d'=' -f2 | tr -d '"' | sed 's/0.0.0.0/localhost/') && \
	curl -f "http://$$HTTP_BIND/health" 2>/dev/null && echo " ✅ Healthy" || echo " ❌ Unhealthy"

# Check API server health endpoint
api-health:
	@echo "Checking API server health endpoint..."
	@echo "📋 Reading API_PORT from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Cannot determine API endpoint."; \
		exit 1; \
	fi
	@API_PORT=$$(grep "^API_PORT=" .env | cut -d'=' -f2 | tr -d '"') && \
	curl -f "http://localhost:$$API_PORT/api/v1/health" 2>/dev/null && echo " ✅ API Healthy" || echo " ❌ API Unhealthy"

# Protocol buffer setup (one-time)
proto-setup:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Protocol buffer tools installed"

# Regenerate protocol buffer code
proto:
	protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative proto/continuum.proto
	@echo "Protocol buffer code regenerated"

# Development helpers
deps:
	go mod tidy
	go mod verify
	@echo "Dependencies updated and verified"

# Format code
fmt:
	go fmt ./...
	@echo "Code formatted"

# Run linter (if available)
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed, skipping..."; \
		go vet ./...; \
	fi

# Show log file contents (pretty printed JSON)
show-logs:
	@echo "📄 Recent tick data:"
	@if [ -f "logs/ticks.jsonl" ]; then \
		echo "--- Last 5 ticks ---"; \
		tail -5 logs/ticks.jsonl | jq '.' 2>/dev/null || tail -5 logs/ticks.jsonl; \
	else \
		echo "No tick log file found"; \
	fi
	@echo ""
	@echo "📄 Recent transaction data:"
	@if [ -f "logs/transactions.jsonl" ]; then \
		echo "--- Last 5 transactions ---"; \
		tail -5 logs/transactions.jsonl | jq '.' 2>/dev/null || tail -5 logs/transactions.jsonl; \
	else \
		echo "No transaction log file found"; \
	fi

# Test all database sinks
test-sinks:
	@echo "🧪 Testing all database sinks..."
	go test ./internal/sink -v -run="TestSinkFactory|TestLogFileSink|TestSQLiteSink" -timeout=60s

# Test ClickHouse integration specifically
test-clickhouse:
	@echo "🧪 Testing ClickHouse integration..."
	@echo "🔐 Using ClickHouse credentials from environment"
	CLICKHOUSE_PASSWORD=$${CLICKHOUSE_PASSWORD:-1O4~txzDw_LZl} go test ./internal/sink -v -run="TestClickHouseSinkIntegration" -timeout=60s

# Setup environment file
setup-env:
	@echo "📋 Setting up environment configuration..."
	@if [ -f ".env" ]; then \
		echo "⚠️ .env file already exists"; \
		echo "Use 'cp .env.example .env' to overwrite if needed"; \
	else \
		cp .env.example .env; \
		echo "✅ Copied .env.example to .env"; \
		echo "📝 You can now edit .env with your specific settings"; \
	fi

# Development setup
dev-setup:
	@echo "🔧 Setting up development environment..."
	go mod tidy
	make proto-setup
	@echo "✅ Development environment ready!"
	@echo ""
	@echo "Quick start:"
	@echo "  make run-log     # Start with log file sink"
	@echo "  make logs        # Watch logs in another terminal"