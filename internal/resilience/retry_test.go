package resilience

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestRetryWithBackoff tests the basic retry functionality
func TestRetryWithBackoff(t *testing.T) {
	config := DefaultRetryConfig()
	config.MaxAttempts = 3
	config.BaseDelay = 10 * time.Millisecond
	config.JitterEnabled = false // Disable for predictable testing

	// Test successful operation on first attempt
	t.Run("SuccessOnFirstAttempt", func(t *testing.T) {
		attemptCount := 0
		
		err := RetryWithBackoff(context.Background(), config, func(ctx context.Context, attempt int) error {
			attemptCount++
			return nil // Success immediately
		}, "test operation")
		
		if err != nil {
			t.Errorf("Expected success, got error: %v", err)
		}
		if attemptCount != 1 {
			t.Errorf("Expected 1 attempt, got %d", attemptCount)
		}
	})

	// Test success after retries
	t.Run("SuccessAfterRetries", func(t *testing.T) {
		attemptCount := 0
		
		err := RetryWithBackoff(context.Background(), config, func(ctx context.Context, attempt int) error {
			attemptCount++
			if attemptCount < 3 {
				return errors.New("connection refused") // Retryable error
			}
			return nil // Success on 3rd attempt
		}, "test operation")
		
		if err != nil {
			t.Errorf("Expected success after retries, got error: %v", err)
		}
		if attemptCount != 3 {
			t.Errorf("Expected 3 attempts, got %d", attemptCount)
		}
	})

	// Test max attempts exceeded
	t.Run("MaxAttemptsExceeded", func(t *testing.T) {
		attemptCount := 0
		
		err := RetryWithBackoff(context.Background(), config, func(ctx context.Context, attempt int) error {
			attemptCount++
			return errors.New("timeout") // Always fail with retryable error
		}, "test operation")
		
		if err == nil {
			t.Error("Expected error after max attempts exceeded")
		}
		if !strings.Contains(err.Error(), "max retries exceeded") {
			t.Errorf("Expected max retries error, got: %v", err)
		}
		if attemptCount != config.MaxAttempts {
			t.Errorf("Expected %d attempts, got %d", config.MaxAttempts, attemptCount)
		}
	})

	// Test non-retryable error
	t.Run("NonRetryableError", func(t *testing.T) {
		attemptCount := 0
		
		err := RetryWithBackoff(context.Background(), config, func(ctx context.Context, attempt int) error {
			attemptCount++
			return errors.New("invalid input") // Non-retryable error
		}, "test operation")
		
		if err == nil {
			t.Error("Expected error for non-retryable failure")
		}
		if !strings.Contains(err.Error(), "non-retryable error") {
			t.Errorf("Expected non-retryable error, got: %v", err)
		}
		if attemptCount != 1 {
			t.Errorf("Expected 1 attempt for non-retryable error, got %d", attemptCount)
		}
	})

	// Test context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		
		attemptCount := 0
		
		err := RetryWithBackoff(ctx, config, func(ctx context.Context, attempt int) error {
			attemptCount++
			time.Sleep(100 * time.Millisecond) // Longer than context timeout
			return errors.New("timeout")
		}, "test operation")
		
		if err == nil {
			t.Error("Expected context cancellation error")
		}
		if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "retry cancelled") {
			t.Errorf("Expected context cancellation error, got: %v", err)
		}
	})
}

// TestCalculateDelay tests the exponential backoff calculation
func TestCalculateDelay(t *testing.T) {
	config := RetryConfig{
		BaseDelay:     100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		JitterEnabled: false,
	}

	tests := []struct {
		attempt      int
		expectedMin  time.Duration
		expectedMax  time.Duration
	}{
		{0, 100 * time.Millisecond, 100 * time.Millisecond},
		{1, 200 * time.Millisecond, 200 * time.Millisecond},
		{2, 400 * time.Millisecond, 400 * time.Millisecond},
		{3, 800 * time.Millisecond, 800 * time.Millisecond},
		{10, 5 * time.Second, 5 * time.Second}, // Should be capped at MaxDelay
	}

	for _, test := range tests {
		delay := calculateDelay(test.attempt, config)
		if delay < test.expectedMin || delay > test.expectedMax {
			t.Errorf("Attempt %d: expected delay between %v and %v, got %v",
				test.attempt, test.expectedMin, test.expectedMax, delay)
		}
	}
}

// TestCircuitBreaker tests the circuit breaker functionality
func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	t.Run("InitialState", func(t *testing.T) {
		if cb.GetState() != CircuitClosed {
			t.Errorf("Expected initial state to be CLOSED, got %v", cb.GetState())
		}
	})

	t.Run("SuccessfulOperations", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			err := cb.Execute(context.Background(), func() error {
				return nil
			}, "test")
			
			if err != nil {
				t.Errorf("Successful operation %d failed: %v", i, err)
			}
			
			if cb.GetState() != CircuitClosed {
				t.Errorf("Circuit should remain CLOSED after success, got %v", cb.GetState())
			}
		}
	})

	t.Run("CircuitOpening", func(t *testing.T) {
		// Cause failures to open the circuit
		for i := 0; i < 3; i++ {
			err := cb.Execute(context.Background(), func() error {
				return errors.New("failure")
			}, "test")
			
			if err == nil {
				t.Errorf("Expected failure on attempt %d", i)
			}
		}
		
		if cb.GetState() != CircuitOpen {
			t.Errorf("Expected circuit to be OPEN after failures, got %v", cb.GetState())
		}
	})

	t.Run("CircuitOpenRejection", func(t *testing.T) {
		// Circuit should reject calls while open
		err := cb.Execute(context.Background(), func() error {
			return nil // This shouldn't be called
		}, "test")
		
		if err == nil {
			t.Error("Expected circuit breaker to reject call while OPEN")
		}
		if !strings.Contains(err.Error(), "circuit breaker open") {
			t.Errorf("Expected circuit open error, got: %v", err)
		}
	})

	t.Run("HalfOpenTransition", func(t *testing.T) {
		// Wait for reset timeout
		time.Sleep(150 * time.Millisecond)
		
		// Next call should transition to HALF-OPEN
		err := cb.Execute(context.Background(), func() error {
			return nil // Success
		}, "test")
		
		if err != nil {
			t.Errorf("Expected successful transition to HALF-OPEN, got error: %v", err)
		}
		
		if cb.GetState() != CircuitClosed {
			t.Errorf("Expected circuit to be CLOSED after successful recovery, got %v", cb.GetState())
		}
	})
}

// TestComponentHealth tests the component health tracking
func TestComponentHealth(t *testing.T) {
	// Mock health checker
	mockHealthy := true
	checker := &MockHealthChecker{isHealthy: &mockHealthy}
	
	health := NewComponentHealth("test-component", checker, 2)

	t.Run("InitiallyHealthy", func(t *testing.T) {
		if !health.IsHealthy() {
			t.Error("Expected component to be initially healthy")
		}
	})

	t.Run("SingleFailureStillHealthy", func(t *testing.T) {
		mockHealthy = false
		
		result := health.CheckHealth(context.Background())
		if !result || !health.IsHealthy() {
			t.Error("Expected component to remain healthy after single failure")
		}
	})

	t.Run("MultipleFailuresUnhealthy", func(t *testing.T) {
		// Still unhealthy
		health.CheckHealth(context.Background())
		
		// This should mark it as unhealthy
		result := health.CheckHealth(context.Background())
		if result || health.IsHealthy() {
			t.Error("Expected component to be unhealthy after multiple failures")
		}
	})

	t.Run("Recovery", func(t *testing.T) {
		mockHealthy = true
		
		result := health.CheckHealth(context.Background())
		if !result || !health.IsHealthy() {
			t.Error("Expected component to recover when health check passes")
		}
	})
}

// TestRetryableErrorDetection tests error classification
func TestRetryableErrorDetection(t *testing.T) {
	retryableErrors := []string{"connection refused", "timeout", "unavailable", "network error"}

	tests := []struct {
		error     error
		retryable bool
	}{
		{errors.New("connection refused"), true},
		{errors.New("Connection Refused"), true}, // Case insensitive
		{errors.New("request timeout"), true},
		{errors.New("service unavailable"), true},
		{errors.New("network error"), true},
		{errors.New("invalid input"), false},
		{errors.New("permission denied"), false},
		{nil, false},
	}

	for _, test := range tests {
		result := isRetryableError(test.error, retryableErrors)
		if result != test.retryable {
			t.Errorf("Error '%v': expected retryable=%v, got %v", 
				test.error, test.retryable, result)
		}
	}
}

// TestJitterCalculation tests jitter functionality
func TestJitterCalculation(t *testing.T) {
	config := RetryConfig{
		BaseDelay:     100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		JitterEnabled: true,
	}

	// Calculate delays multiple times to ensure jitter works
	delays := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		delays[i] = calculateDelay(1, config) // Should be around 200ms with jitter
	}

	// Check that we got different values (jitter is working)
	allSame := true
	firstDelay := delays[0]
	for _, delay := range delays[1:] {
		if delay != firstDelay {
			allSame = false
			break
		}
	}

	if allSame {
		t.Error("Expected jitter to produce different delay values")
	}

	// Check that all delays are within reasonable bounds (±50% of base)
	expectedDelay := 200 * time.Millisecond
	minExpected := time.Duration(float64(expectedDelay) * 0.5)  // 50% less
	maxExpected := time.Duration(float64(expectedDelay) * 1.5)  // 50% more

	for i, delay := range delays {
		if delay < minExpected || delay > maxExpected {
			t.Errorf("Delay %d (%v) outside expected range [%v, %v]", 
				i, delay, minExpected, maxExpected)
		}
	}
}

// MockHealthChecker for testing
type MockHealthChecker struct {
	isHealthy *bool
}

func (m *MockHealthChecker) IsHealthy(ctx context.Context) bool {
	return *m.isHealthy
}

func (m *MockHealthChecker) GetHealthStatus() string {
	if *m.isHealthy {
		return "mock: healthy"
	}
	return "mock: unhealthy"
}

// Benchmark retry operations
func BenchmarkRetryWithBackoff(b *testing.B) {
	config := DefaultRetryConfig()
	config.MaxAttempts = 3
	config.BaseDelay = 1 * time.Millisecond
	config.JitterEnabled = false

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RetryWithBackoff(context.Background(), config, func(ctx context.Context, attempt int) error {
			return nil // Always succeed
		}, "benchmark")
	}
}

func BenchmarkCircuitBreakerSuccess(b *testing.B) {
	cb := NewCircuitBreaker(5, 1*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Execute(context.Background(), func() error {
			return nil
		}, "benchmark")
	}
}