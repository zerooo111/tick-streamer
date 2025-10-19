package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	apiErrors "github.com/zerooo111/tick-streamer/internal/api/errors"
	"github.com/zerooo111/tick-streamer/internal/api/handlers"
	"github.com/zerooo111/tick-streamer/internal/api/middleware"
	"github.com/zerooo111/tick-streamer/internal/api/repository"
	"github.com/zerooo111/tick-streamer/internal/config"
)

type Server struct {
	router         *gin.Engine
	httpServer     *http.Server
	handler        *handlers.Handler
	config         *config.Config
	repository     repository.Repository
	logWriter      *middleware.LogWriter
}

func NewServer(cfg *config.Config) (*Server, error) {
	// Create log writer with rotation
	logConfig := &middleware.LogConfig{
		LogDir:        cfg.LogDir,
		MaxSize:       cfg.LogMaxSize,
		MaxBackups:    cfg.LogMaxBackups,
		MaxAge:        cfg.LogMaxAge,
		Compress:      cfg.LogCompress,
		EnableConsole: cfg.LogConsole,
		DisableFile:   cfg.LogFileDisable,
	}
	
	logWriter, err := middleware.NewLogWriter(logConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create log writer: %w", err)
	}

	// Create repository based on SINK_KIND configuration
	repo, err := repository.NewRepository(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	// Create handlers
	handler, err := handlers.New(cfg.SequencerAddr, cfg.RestBaseURL, cfg.MatchEngineURL, cfg.RollupRestEndpoint, repo)
	if err != nil {
		return nil, fmt.Errorf("failed to create handlers: %w", err)
	}

	// Set Gin mode based on debug setting
	if !cfg.Debug {
		gin.SetMode(gin.ReleaseMode)
	}

	// Create Gin router
	router := gin.New()

	s := &Server{
		router:         router,
		handler:        handler,
		config:         cfg,
		repository:     repo,
		logWriter:      logWriter,
	}

	s.setupMiddleware()
	s.setupRoutes()

	return s, nil
}

func (s *Server) setupMiddleware() {
	// Add log writer to context for error handling
	s.router.Use(func(c *gin.Context) {
		c.Set("logWriter", s.logWriter)
		c.Next()
	})

	// Custom error recovery middleware (must be first)
	s.router.Use(s.logWriter.ErrorLoggingMiddleware())

	// Request logging middleware
	s.router.Use(s.logWriter.LoggingMiddleware())

	// Error handler middleware to catch panics and convert to proper error responses
	s.router.Use(apiErrors.ErrorHandlerMiddleware())

	// Global rate limiting (100 requests per second with burst of 200)
	s.router.Use(middleware.GlobalRateLimiter(100, 200))

	// CORS middleware
	s.router.Use(middleware.CORS(s.config.CORSAllowedOrigins, s.config.CORSAllowCredentials))
}

func (s *Server) setupRoutes() {
	// Create per-endpoint rate limiter
	endpointLimiter := middleware.CreateDefaultRateLimits()

	// Root endpoint
	s.router.GET("/", s.handler.Root)

	// API v1 routes with per-endpoint rate limiting
	v1 := s.router.Group("/api/v1")
	v1.Use(endpointLimiter.Middleware())
	{
		// Health and status
		v1.GET("/health", s.handler.Health)
		v1.GET("/status", s.handler.Status)

		// Transaction routes
		v1.GET("/tx/:hash", s.handler.GetTransaction)
		v1.GET("/tx/recent", s.handler.GetRecentTransactions)
		v1.POST("/tx", middleware.RequestSizeLimit(), s.handler.SubmitTransaction)
		v1.POST("/tx/batch", middleware.RequestSizeLimit(), s.handler.SubmitBatch)

		// Tick routes
		v1.GET("/tick/:number", s.handler.GetTick)
		v1.GET("/ticks/recent", s.handler.GetRecentTicks)

		// Chain state routes
		v1.GET("/chain/state", s.handler.GetChainState)
		v1.GET("/chain-state", s.handler.GetSequencerStatus)

		// Market routes (proxy to match engine)
		me := v1.Group("/me")
		{
			me.GET("/markets", s.handler.GetMarkets)
			me.GET("/markets/:marketId/orderbook", s.handler.GetMarketOrderbook)
			me.GET("/markets/:marketId/orderbook/summary", s.handler.GetMarketOrderbookSummary)
			me.GET("/markets/:marketId/trades", s.handler.GetMarketTrades)
			me.GET("/markets/:marketId/stats", s.handler.GetMarketStats)
			me.GET("/markets/:marketId/candles", s.handler.GetMarketCandles)
			me.GET("/orders/user/:pubkey", s.handler.GetUserOrders)
			me.GET("/balances/:pubkey", s.handler.GetUserBalances)
			me.GET("/accounts/:pubkey", s.handler.GetUserAccounts)
			me.GET("/users/:pubkey/pnl", s.handler.GetUserPNL)
			me.POST("/airdrop/:receiverPubKey/:tokenName", s.handler.PostAirdrop)
		}

		// Rollup routes (proxy to rollup REST API)
		rollup := v1.Group("/rollup")
		{
			rollup.GET("/status", s.handler.GetRollupStatus)
			rollup.GET("/blocks/latest", s.handler.GetRollupLatestBlock)
			rollup.GET("/blocks", s.handler.GetRollupBlocks)
			rollup.GET("/blocks/:height", s.handler.GetRollupBlockByHeight)
			rollup.GET("/transactions/:id", s.handler.GetRollupTransactionById)
		}
	}
}

func (s *Server) Start() error {
	addr := ":" + s.config.APIPort

	s.httpServer = &http.Server{
		Addr:           addr,
		Handler:        s.router,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	log.Printf("🚀 Starting fermi-explorer-go-backend...")
	log.Printf("🌐 REST API: http://localhost%s/api/v1", addr)
	log.Printf("📡 Proxying gRPC from: %s", s.config.SequencerAddr)
	log.Printf("🔗 Proxying REST from: %s", s.config.RestBaseURL)
	log.Printf("🎯 Proxying Match Engine from: %s", s.config.MatchEngineURL)
	
	if s.config.Debug {
		log.Printf("🐛 Debug mode enabled")
	}
	
	log.Printf("✅ Server started successfully")

	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("🛑 Starting graceful shutdown...")

	// Shutdown HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		return err
	}

	// Close handlers
	if s.handler != nil {
		log.Println("📡 Closing gRPC client...")
		s.handler.Close()
		log.Println("✅ gRPC client closed")
	}

	// Close database repository  
	if s.repository != nil {
		log.Println("🗄️ Closing database repository...")
		s.repository.Close()
		log.Println("✅ Database repository closed")
	}

	// Close log writer
	if s.logWriter != nil {
		log.Println("📋 Closing log writer...")
		if err := s.logWriter.Close(); err != nil {
			log.Printf("❌ Error closing log writer: %v", err)
		} else {
			log.Println("✅ Log writer closed")
		}
	}

	log.Println("🏁 Server stopped gracefully")
	return nil
}