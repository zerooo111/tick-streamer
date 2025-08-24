package errors

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"

	"github.com/gin-gonic/gin"
)

// ErrorResponse represents a structured error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
	TraceID string `json:"trace_id,omitempty"`
}

// Common error codes
const (
	ErrCodeValidation    = "VALIDATION_ERROR"
	ErrCodeNotFound      = "NOT_FOUND"
	ErrCodeUnauthorized  = "UNAUTHORIZED"
	ErrCodeForbidden     = "FORBIDDEN"
	ErrCodeInternal      = "INTERNAL_ERROR"
	ErrCodeBadRequest    = "BAD_REQUEST"
	ErrCodeRateLimit     = "RATE_LIMIT_EXCEEDED"
	ErrCodeServiceUnavailable = "SERVICE_UNAVAILABLE"
)

// HandleError creates a safe error response that doesn't leak internal details
func HandleError(c *gin.Context, statusCode int, errCode string, userMessage string, internalErr error) {
	// Log the full error internally with context
	if internalErr != nil {
		// Get the calling function for better debugging
		pc, file, line, _ := runtime.Caller(1)
		fn := runtime.FuncForPC(pc)
		
		// Sanitize the file path to remove absolute paths
		parts := strings.Split(file, "/")
		if len(parts) > 2 {
			file = strings.Join(parts[len(parts)-2:], "/")
		}
		
		log.Printf("Error at %s:%d in %s: %v", file, line, fn.Name(), internalErr)
	}
	
	// Create safe error response for the client
	response := ErrorResponse{
		Error:   http.StatusText(statusCode),
		Message: userMessage,
		Code:    errCode,
	}
	
	// Add trace ID if available in context
	if traceID, exists := c.Get("trace_id"); exists {
		response.TraceID = traceID.(string)
	}
	
	c.JSON(statusCode, response)
}

// ValidationError handles validation errors
func ValidationError(c *gin.Context, message string) {
	HandleError(c, http.StatusBadRequest, ErrCodeValidation, message, nil)
}

// NotFoundError handles not found errors
func NotFoundError(c *gin.Context, resource string) {
	message := fmt.Sprintf("%s not found", resource)
	HandleError(c, http.StatusNotFound, ErrCodeNotFound, message, nil)
}

// UnauthorizedError handles unauthorized access
func UnauthorizedError(c *gin.Context) {
	HandleError(c, http.StatusUnauthorized, ErrCodeUnauthorized, "Authentication required", nil)
}

// ForbiddenError handles forbidden access
func ForbiddenError(c *gin.Context) {
	HandleError(c, http.StatusForbidden, ErrCodeForbidden, "Access denied", nil)
}

// InternalError handles internal server errors
func InternalError(c *gin.Context, err error) {
	// Never expose internal error details to the client
	HandleError(c, http.StatusInternalServerError, ErrCodeInternal, 
		"An internal error occurred. Please try again later.", err)
}

// BadRequestError handles bad request errors
func BadRequestError(c *gin.Context, message string) {
	HandleError(c, http.StatusBadRequest, ErrCodeBadRequest, message, nil)
}

// RateLimitError handles rate limit exceeded errors
func RateLimitError(c *gin.Context) {
	HandleError(c, http.StatusTooManyRequests, ErrCodeRateLimit, 
		"Rate limit exceeded. Please try again later.", nil)
}

// ServiceUnavailableError handles service unavailable errors
func ServiceUnavailableError(c *gin.Context, err error) {
	HandleError(c, http.StatusServiceUnavailable, ErrCodeServiceUnavailable,
		"Service temporarily unavailable. Please try again later.", err)
}

// ErrorHandlerMiddleware is a middleware that catches panics and converts them to proper error responses
func ErrorHandlerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Log the panic with stack trace
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				log.Printf("Panic recovered: %v\nStack trace:\n%s", err, buf[:n])
				
				// Return a safe error response
				InternalError(c, fmt.Errorf("panic: %v", err))
				c.Abort()
			}
		}()
		
		c.Next()
		
		// Handle errors that weren't caught
		if len(c.Errors) > 0 {
			err := c.Errors.Last()
			InternalError(c, err)
		}
	}
}