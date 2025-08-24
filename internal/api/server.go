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
	"github.com/zerooo111/tick-streamer/internal/api/websocket"
	"github.com/zerooo111/tick-streamer/internal/config"
)

type Server struct {
	router     *gin.Engine
	httpServer *http.Server
	handler    *handlers.Handler
	wsHub      *websocket.Hub
	config     *config.Config
	repository *repository.ClickHouseRepository
}

func NewServer(cfg *config.Config) (*Server, error) {
	// Create ClickHouse repository
	repo, err := repository.NewClickHouseRepository(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse repository: %w", err)
	}

	// Create handlers
	handler, err := handlers.New(cfg.SequencerAddr, cfg.RestBaseURL, cfg.MatchEngineURL, repo)
	if err != nil {
		return nil, fmt.Errorf("failed to create handlers: %w", err)
	}

	// Create WebSocket hub with allowed origins from config
	wsHub, err := websocket.NewHub(cfg.SequencerAddr, cfg.CORSAllowedOrigins)
	if err != nil {
		return nil, fmt.Errorf("failed to create WebSocket hub: %w", err)
	}

	// Set Gin mode based on debug setting
	if !cfg.Debug {
		gin.SetMode(gin.ReleaseMode)
	}

	// Create Gin router
	router := gin.New()

	s := &Server{
		router:     router,
		handler:    handler,
		wsHub:      wsHub,
		config:     cfg,
		repository: repo,
	}

	s.setupMiddleware()
	s.setupRoutes()

	return s, nil
}

func (s *Server) setupMiddleware() {
	// Recovery middleware (must be first)
	s.router.Use(middleware.Recovery())

	// Error handler middleware to catch panics and convert to proper error responses
	s.router.Use(apiErrors.ErrorHandlerMiddleware())

	// Global rate limiting (100 requests per second with burst of 200)
	s.router.Use(middleware.GlobalRateLimiter(100, 200))

	// CORS middleware
	s.router.Use(middleware.CORS(s.config.CORSAllowedOrigins, s.config.CORSAllowCredentials))

	// Logging middleware (only in debug mode)
	if s.config.Debug {
		s.router.Use(middleware.Logging())
	}
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
		v1.POST("/tx", middleware.RequestSizeLimit(), s.handler.SubmitTransaction)
		v1.POST("/tx/batch", middleware.RequestSizeLimit(), s.handler.SubmitBatch)

		// Tick routes
		v1.GET("/tick/:number", s.handler.GetTick)
		v1.GET("/ticks/recent", s.handler.GetRecentTicks)

		// Chain state route
		v1.GET("/chain/state", s.handler.GetChainState)

		// Market routes (proxy to match engine)
		me := v1.Group("/me")
		{
			me.GET("/markets", s.handler.GetMarkets)
			me.GET("/markets/:marketId/orderbook", s.handler.GetMarketOrderbook)
		}
	}

	// WebSocket route
	s.router.GET("/ws/ticks", s.wsHub.HandleWebSocket)
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
	log.Printf("🔌 WebSocket: ws://localhost%s/ws/ticks", addr)
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

	// Shutdown WebSocket hub
	if s.wsHub != nil {
		log.Println("🔌 Shutting down WebSocket hub...")
		s.wsHub.Shutdown()
		log.Println("✅ WebSocket hub shutdown complete")
	}

	// Close handlers
	if s.handler != nil {
		log.Println("📡 Closing gRPC client...")
		s.handler.Close()
		log.Println("✅ gRPC client closed")
	}

	// Close ClickHouse repository
	if s.repository != nil {
		log.Println("🗄️ Closing ClickHouse repository...")
		s.repository.Close()
		log.Println("✅ ClickHouse repository closed")
	}

	log.Println("🏁 Server stopped gracefully")
	return nil
}