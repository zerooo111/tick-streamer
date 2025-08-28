# Continuum Streamer Makefile
# Essential commands for development and deployment

.PHONY: help run-streamer run-api-server build clean test proto setup-env health logs

# Default target
help:
	@echo "Continuum Streamer - Available Commands:"
	@echo ""
	@echo "Development:"
	@echo "  make run-streamer   - Run the ClickHouse streamer from source"
	@echo "  make run-api-server - Run the API server from source" 
	@echo "  make build          - Build both binaries to bin/"
	@echo "  make clean          - Clean build artifacts"
	@echo ""
	@echo "Testing:"
	@echo "  make test           - Run tests"
	@echo "  make health         - Check API server health"
	@echo ""
	@echo "Setup:"
	@echo "  make setup-env      - Copy .env.example to .env"
	@echo "  make proto          - Regenerate protobuf code"
	@echo ""
	@echo "Logs:"
	@echo "  make logs           - Watch log files (or stdout if file logging disabled)"
	@echo ""
	@echo "Examples:"
	@echo "  make setup-env && vim .env    # Setup and configure environment"
	@echo "  make run-streamer             # Run ClickHouse streamer"
	@echo "  make run-api-server           # Run API server"

# Run the ClickHouse streamer from source
run-streamer:
	@echo "🚀 Starting ClickHouse streamer..."
	@echo "📋 Loading configuration from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Run 'make setup-env' first"; \
		exit 1; \
	fi
	go run cmd/streamer/main.go

# Run the API server from source  
run-api-server:
	@echo "🚀 Starting API server..."
	@echo "📋 Loading configuration from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Run 'make setup-env' first"; \
		exit 1; \
	fi
	go run cmd/api-server/main.go

# Watch log files in real-time (or suggest stdout monitoring if disabled)
logs:
	@echo "👀 Checking for log files..."
	@if [ -d "logs" ]; then \
		tail -f logs/*.log logs/*.jsonl 2>/dev/null || echo "No log files found in ./logs/ (file logging is disabled by default)"; \
	else \
		echo "📋 File logging is disabled by default (LOG_FILE_DISABLE=true)"; \
		echo "💡 To monitor logs, use:"; \
		echo "   make run-streamer | grep 'pattern'"; \
		echo "   make run-api-server | grep 'error'"; \
		echo "💡 To enable file logging, set LOG_FILE_DISABLE=false in .env"; \
	fi

# Build binaries
build:
	@mkdir -p bin
	go build -o bin/streamer cmd/streamer/main.go
	go build -o bin/api-server cmd/api-server/main.go
	@echo "✅ Binaries built: bin/streamer, bin/api-server"

# Clean build artifacts
clean:
	rm -rf bin/
	go clean
	@echo "✅ Cleaned build artifacts"

# Run tests
test:
	go test ./...

# Check API server health endpoint
health:
	@echo "🔍 Checking API server health endpoint..."
	@echo "📋 Reading API_PORT from .env file..."
	@if [ ! -f ".env" ]; then \
		echo "❌ .env file not found. Cannot determine API endpoint."; \
		exit 1; \
	fi
	@API_PORT=$$(grep "^API_PORT=" .env | cut -d'=' -f2 | tr -d '"') && \
	curl -f "http://localhost:$$API_PORT/api/v1/health" 2>/dev/null && echo " ✅ API Healthy" || echo " ❌ API Unhealthy"

# Regenerate protocol buffer code
proto:
	protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative proto/continuum.proto
	@echo "✅ Protocol buffer code regenerated"

# Setup environment file
setup-env:
	@echo "📋 Setting up environment configuration..."
	@if [ -f ".env" ]; then \
		echo "⚠️ .env file already exists"; \
		echo "Use 'cp .env.example .env' to overwrite if needed"; \
	else \
		cp .env.example .env; \
		echo "✅ Copied .env.example to .env"; \
		echo "📝 Edit .env with your ClickHouse credentials"; \
	fi