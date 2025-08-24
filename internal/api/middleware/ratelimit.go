package middleware

import (
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	apiErrors "github.com/zerooo111/tick-streamer/internal/api/errors"
)

// RateLimiter implements a token bucket algorithm for rate limiting
type RateLimiter struct {
	mu       sync.RWMutex
	visitors map[string]*visitor
	// Configuration
	requestsPerSecond int
	burstSize         int
	cleanupInterval   time.Duration
}

// visitor tracks the rate limit state for a single client
type visitor struct {
	tokens    float64
	lastVisit time.Time
	mu        sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(requestsPerSecond, burstSize int) *RateLimiter {
	rl := &RateLimiter{
		visitors:          make(map[string]*visitor),
		requestsPerSecond: requestsPerSecond,
		burstSize:         burstSize,
		cleanupInterval:   5 * time.Minute,
	}
	
	// Start cleanup goroutine to remove old visitors
	go rl.cleanupVisitors()
	
	return rl
}

// cleanupVisitors removes visitors that haven't been seen recently
func (rl *RateLimiter) cleanupVisitors() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()
	
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for ip, v := range rl.visitors {
			v.mu.Lock()
			// Remove visitors that haven't been seen for 10 minutes
			if now.Sub(v.lastVisit) > 10*time.Minute {
				delete(rl.visitors, ip)
			}
			v.mu.Unlock()
		}
		rl.mu.Unlock()
	}
}

// getVisitor retrieves or creates a visitor for the given IP
func (rl *RateLimiter) getVisitor(ip string) *visitor {
	rl.mu.RLock()
	v, exists := rl.visitors[ip]
	rl.mu.RUnlock()
	
	if !exists {
		rl.mu.Lock()
		// Double-check after acquiring write lock
		v, exists = rl.visitors[ip]
		if !exists {
			v = &visitor{
				tokens:    float64(rl.burstSize),
				lastVisit: time.Now(),
			}
			rl.visitors[ip] = v
		}
		rl.mu.Unlock()
	}
	
	return v
}

// Allow checks if a request from the given IP should be allowed
func (rl *RateLimiter) Allow(ip string) bool {
	v := rl.getVisitor(ip)
	
	v.mu.Lock()
	defer v.mu.Unlock()
	
	now := time.Now()
	elapsed := now.Sub(v.lastVisit).Seconds()
	v.lastVisit = now
	
	// Refill tokens based on time elapsed
	tokensToAdd := elapsed * float64(rl.requestsPerSecond)
	v.tokens += tokensToAdd
	
	// Cap tokens at burst size
	if v.tokens > float64(rl.burstSize) {
		v.tokens = float64(rl.burstSize)
	}
	
	// Check if we have tokens available
	if v.tokens >= 1.0 {
		v.tokens--
		return true
	}
	
	return false
}

// Middleware returns a Gin middleware handler for rate limiting
func (rl *RateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get client IP
		ip := c.ClientIP()
		
		// Check rate limit
		if !rl.Allow(ip) {
			apiErrors.RateLimitError(c)
			c.Abort()
			return
		}
		
		c.Next()
	}
}

// PerEndpointRateLimiter allows different rate limits for different endpoints
type PerEndpointRateLimiter struct {
	limiters map[string]*RateLimiter
	mu       sync.RWMutex
}

// NewPerEndpointRateLimiter creates a rate limiter with per-endpoint limits
func NewPerEndpointRateLimiter() *PerEndpointRateLimiter {
	return &PerEndpointRateLimiter{
		limiters: make(map[string]*RateLimiter),
	}
}

// AddEndpoint adds a rate limit for a specific endpoint
func (p *PerEndpointRateLimiter) AddEndpoint(path string, requestsPerSecond, burstSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.limiters[path] = NewRateLimiter(requestsPerSecond, burstSize)
}

// Middleware returns a Gin middleware handler for per-endpoint rate limiting
func (p *PerEndpointRateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.FullPath()
		
		p.mu.RLock()
		limiter, exists := p.limiters[path]
		p.mu.RUnlock()
		
		if !exists {
			// No rate limit configured for this endpoint
			c.Next()
			return
		}
		
		// Apply the rate limit for this endpoint
		ip := c.ClientIP()
		if !limiter.Allow(ip) {
			apiErrors.RateLimitError(c)
			c.Abort()
			return
		}
		
		c.Next()
	}
}

// CreateDefaultRateLimits creates sensible default rate limits for API endpoints
func CreateDefaultRateLimits() *PerEndpointRateLimiter {
	limiter := NewPerEndpointRateLimiter()
	
	// High-frequency endpoints (streaming, real-time data)
	limiter.AddEndpoint("/api/v1/ws", 10, 20)                    // WebSocket connections
	limiter.AddEndpoint("/api/v1/ticks/recent", 30, 60)          // Recent ticks
	limiter.AddEndpoint("/api/v1/markets/:id/orderbook", 20, 40) // Orderbook data
	
	// Medium-frequency endpoints (queries)
	limiter.AddEndpoint("/api/v1/ticks/:tick", 10, 20)        // Individual tick
	limiter.AddEndpoint("/api/v1/transactions/:hash", 10, 20) // Individual transaction
	limiter.AddEndpoint("/api/v1/markets", 10, 20)            // Markets list
	
	// Low-frequency endpoints (writes)
	limiter.AddEndpoint("/api/v1/transactions", 5, 10)       // Submit transaction
	limiter.AddEndpoint("/api/v1/transactions/batch", 2, 5)  // Submit batch
	
	// Health endpoints (higher limits)
	limiter.AddEndpoint("/health", 100, 200)
	limiter.AddEndpoint("/metrics", 50, 100)
	
	return limiter
}

// GlobalRateLimiter creates a simple global rate limiter
func GlobalRateLimiter(requestsPerSecond, burstSize int) gin.HandlerFunc {
	limiter := NewRateLimiter(requestsPerSecond, burstSize)
	return limiter.Middleware()
}