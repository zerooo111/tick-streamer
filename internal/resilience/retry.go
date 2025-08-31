package resilience

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"
)

// RetryConfig defines the configuration for retry operations
type RetryConfig struct {
	MaxAttempts     int           // Maximum number of retry attempts
	BaseDelay       time.Duration // Base delay between retries
	MaxDelay        time.Duration // Maximum delay between retries
	BackoffFactor   float64       // Exponential backoff multiplier
	JitterEnabled   bool          // Add random jitter to prevent thundering herd
	RetryableErrors []string      // List of error messages that should trigger retries
}

// DefaultRetryConfig returns a sensible default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:     5,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        30 * time.Second,
		BackoffFactor:   2.0,
		JitterEnabled:   true,
		RetryableErrors: []string{"connection refused", "timeout", "unavailable", "network"},
	}
}

// RetryableFunc is a function that can be retried
type RetryableFunc func(ctx context.Context, attempt int) error

// RetryWithBackoff executes a function with exponential backoff retry logic
func RetryWithBackoff(ctx context.Context, config RetryConfig, fn RetryableFunc, operation string) error {
	var lastErr error
	
	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		// Execute the function
		err := fn(ctx, attempt)
		if err == nil {
			// Success on first try or after retries
			if attempt > 1 {
				log.Printf("✅ %s succeeded on attempt %d/%d", operation, attempt, config.MaxAttempts)
			}
			return nil
		}
		
		lastErr = err
		
		// Check if error is retryable
		if !isRetryableError(err, config.RetryableErrors) {
			log.Printf("❌ %s failed with non-retryable error: %v", operation, err)
			return fmt.Errorf("non-retryable error: %w", err)
		}
		
		// Don't sleep after the last attempt
		if attempt >= config.MaxAttempts {
			break
		}
		
		// Calculate delay with exponential backoff
		delay := calculateDelay(attempt-1, config)
		
		log.Printf("⏳ %s failed (attempt %d/%d): %v - retrying in %v", 
			operation, attempt, config.MaxAttempts, err, delay.Truncate(time.Millisecond))
		
		// Sleep with context cancellation support
		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			return fmt.Errorf("retry cancelled: %w", ctx.Err())
		}
	}
	
	log.Printf("❌ %s failed after %d attempts, giving up: %v", 
		operation, config.MaxAttempts, lastErr)
	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// calculateDelay computes the delay for exponential backoff with jitter
func calculateDelay(attempt int, config RetryConfig) time.Duration {
	// Calculate base exponential delay
	delay := time.Duration(float64(config.BaseDelay) * math.Pow(config.BackoffFactor, float64(attempt)))
	
	// Cap at maximum delay
	if delay > config.MaxDelay {
		delay = config.MaxDelay
	}
	
	// Add jitter if enabled (±25% randomness)
	if config.JitterEnabled {
		jitter := time.Duration(rand.Float64() * float64(delay) * 0.5) // ±25%
		if rand.Float64() < 0.5 {
			delay = delay - jitter
		} else {
			delay = delay + jitter
		}
		
		// Ensure delay is not negative
		if delay < 0 {
			delay = config.BaseDelay
		}
	}
	
	return delay
}

// isRetryableError checks if an error should trigger a retry
func isRetryableError(err error, retryableErrors []string) bool {
	if err == nil {
		return false
	}
	
	errorStr := err.Error()
	for _, retryablePattern := range retryableErrors {
		if containsIgnoreCase(errorStr, retryablePattern) {
			return true
		}
	}
	
	return false
}

// containsIgnoreCase checks if a string contains a substring (case insensitive)
func containsIgnoreCase(s, substr string) bool {
	sLower := stringToLower(s)
	substrLower := stringToLower(substr)
	
	// Simple substring search
	return len(sLower) >= len(substrLower) && 
		   (sLower == substrLower || hasSubstring(sLower, substrLower))
}

// hasSubstring checks if haystack contains needle
func hasSubstring(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	
	for i := 0; i <= len(haystack)-len(needle); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// stringToLower converts a string to lowercase (simple implementation)
func stringToLower(s string) string {
	result := make([]byte, len(s))
	for i, c := range []byte(s) {
		if c >= 'A' && c <= 'Z' {
			result[i] = c + 32
		} else {
			result[i] = c
		}
	}
	return string(result)
}

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

// CircuitBreaker implements the circuit breaker pattern for resilience
type CircuitBreaker struct {
	maxFailures  int
	resetTimeout time.Duration
	
	// State tracking
	state           CircuitBreakerState
	failures        int
	lastFailureTime time.Time
	
	// Statistics
	totalRequests  int64
	successCount   int64
	failureCount   int64
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		state:        CircuitClosed,
	}
}

// Execute runs a function through the circuit breaker
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error, operation string) error {
	cb.totalRequests++
	
	// Check if circuit should transition from Open to Half-Open
	if cb.state == CircuitOpen {
		if time.Since(cb.lastFailureTime) > cb.resetTimeout {
			log.Printf("🔄 Circuit breaker for %s transitioning to HALF-OPEN", operation)
			cb.state = CircuitHalfOpen
		} else {
			timeUntilReset := time.Until(cb.lastFailureTime.Add(cb.resetTimeout))
			return fmt.Errorf("circuit breaker open for %s (failures: %d/%d, resets in %v)", 
				operation, cb.failures, cb.maxFailures, timeUntilReset.Truncate(time.Second))
		}
	}
	
	// Execute the function
	err := fn()
	
	if err != nil {
		cb.onFailure(operation)
		cb.failureCount++
		return err
	} else {
		cb.onSuccess(operation)
		cb.successCount++
		return nil
	}
}

// onFailure handles a failure case
func (cb *CircuitBreaker) onFailure(operation string) {
	cb.failures++
	cb.lastFailureTime = time.Now()
	
	if cb.state == CircuitHalfOpen {
		// Half-open state failed, go back to open
		log.Printf("🚨 Circuit breaker for %s: HALF-OPEN → OPEN (test failed)", operation)
		cb.state = CircuitOpen
	} else if cb.failures >= cb.maxFailures {
		// Too many failures, open the circuit
		nextReset := cb.lastFailureTime.Add(cb.resetTimeout)
		log.Printf("🚨 Circuit breaker for %s: CLOSED → OPEN (%d consecutive failures, will reset at %s)", 
			operation, cb.failures, nextReset.Format("15:04:05"))
		cb.state = CircuitOpen
	}
}

// onSuccess handles a success case
func (cb *CircuitBreaker) onSuccess(operation string) {
	if cb.state == CircuitHalfOpen {
		// Half-open state succeeded, close the circuit
		log.Printf("✅ Circuit breaker for %s: HALF-OPEN → CLOSED (recovery confirmed)", operation)
		cb.state = CircuitClosed
		cb.failures = 0
	}
}

// GetState returns the current circuit breaker state
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	return cb.state
}

// GetStats returns circuit breaker statistics
func (cb *CircuitBreaker) GetStats() CircuitBreakerStats {
	return CircuitBreakerStats{
		State:         cb.state,
		TotalRequests: cb.totalRequests,
		SuccessCount:  cb.successCount,
		FailureCount:  cb.failureCount,
		CurrentFailures: cb.failures,
		FailureThreshold: cb.maxFailures,
		LastFailureTime: cb.lastFailureTime,
		ResetTimeout:    cb.resetTimeout,
	}
}

// GetNextResetTime returns when the circuit breaker will next attempt to reset
func (cb *CircuitBreaker) GetNextResetTime() time.Time {
	if cb.state == CircuitOpen {
		return cb.lastFailureTime.Add(cb.resetTimeout)
	}
	return time.Time{} // Return zero time if not open
}

// CircuitBreakerStats contains circuit breaker statistics
type CircuitBreakerStats struct {
	State           CircuitBreakerState
	TotalRequests   int64
	SuccessCount    int64
	FailureCount    int64
	CurrentFailures int
	FailureThreshold int
	LastFailureTime time.Time
	ResetTimeout    time.Duration
}

// HealthChecker defines an interface for health checking
type HealthChecker interface {
	IsHealthy(ctx context.Context) bool
	GetHealthStatus() string
}

// ComponentHealth tracks the health status of a system component
type ComponentHealth struct {
	name            string
	healthChecker   HealthChecker
	isHealthy       bool
	lastCheck       time.Time
	consecutiveFails int
	maxFailsBeforeUnhealthy int
}

// NewComponentHealth creates a new component health tracker
func NewComponentHealth(name string, checker HealthChecker, maxFails int) *ComponentHealth {
	return &ComponentHealth{
		name:                    name,
		healthChecker:           checker,
		isHealthy:               true,
		maxFailsBeforeUnhealthy: maxFails,
	}
}

// CheckHealth performs a health check and updates the component status
func (ch *ComponentHealth) CheckHealth(ctx context.Context) bool {
	ch.lastCheck = time.Now()
	
	healthy := ch.healthChecker.IsHealthy(ctx)
	
	if healthy {
		if !ch.isHealthy && ch.consecutiveFails > 0 {
			log.Printf("✅ %s component recovered (was unhealthy for %d checks)", 
				ch.name, ch.consecutiveFails)
		}
		ch.consecutiveFails = 0
		ch.isHealthy = true
	} else {
		ch.consecutiveFails++
		if ch.isHealthy && ch.consecutiveFails >= ch.maxFailsBeforeUnhealthy {
			log.Printf("❌ %s component marked unhealthy after %d consecutive failures", 
				ch.name, ch.consecutiveFails)
			ch.isHealthy = false
		}
	}
	
	return ch.isHealthy
}

// IsHealthy returns the current health status
func (ch *ComponentHealth) IsHealthy() bool {
	return ch.isHealthy
}

// GetStatus returns a detailed health status string
func (ch *ComponentHealth) GetStatus() string {
	if ch.isHealthy {
		return fmt.Sprintf("%s: healthy (last check: %v)", ch.name, ch.lastCheck.Format(time.RFC3339))
	}
	return fmt.Sprintf("%s: unhealthy (%d consecutive failures, last check: %v)", 
		ch.name, ch.consecutiveFails, ch.lastCheck.Format(time.RFC3339))
}