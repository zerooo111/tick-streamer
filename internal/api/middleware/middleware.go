package middleware

import (
	"fmt"
	"html"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
)

const (
	MaxRequestSize = 10 * 1024 * 1024 // 10MB
	RequestTimeout = 30 * time.Second
)

type ErrorResponse struct {
	Error     string      `json:"error"`
	Message   string      `json:"message"`
	Errors    interface{} `json:"errors,omitempty"`
	Timestamp int64       `json:"timestamp"`
}

func CORS(allowedOrigins []string, allowCredentials bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		
		// Check if origin is allowed
		allowed := false
		for _, allowedOrigin := range allowedOrigins {
			if allowedOrigin == "*" || allowedOrigin == origin {
				allowed = true
				break
			}
		}
		
		if allowed {
			c.Header("Access-Control-Allow-Origin", origin)
		}
		
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		
		if allowCredentials {
			c.Header("Access-Control-Allow-Credentials", "true")
		}
		
		// Handle preflight requests
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusOK)
			return
		}
		
		c.Next()
	}
}

func Logging() gin.HandlerFunc {
	return gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("📡 %s - [%s] \"%s %s %s\" %d %s \"%s\" %s\n",
			param.ClientIP,
			param.TimeStamp.Format(time.RFC3339),
			param.Method,
			param.Path,
			param.Request.Proto,
			param.StatusCode,
			param.Latency,
			param.Request.UserAgent(),
			param.ErrorMessage,
		)
	})
}

func Recovery() gin.HandlerFunc {
	return gin.RecoveryWithWriter(gin.DefaultWriter, func(c *gin.Context, recovered interface{}) {
		log.Printf("❌ Panic recovered: %v", recovered)
		SendErrorResponse(c, http.StatusInternalServerError, "Internal server error", nil)
	})
}

func RequestSizeLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > MaxRequestSize {
			SendErrorResponse(c, http.StatusRequestEntityTooLarge, 
				fmt.Sprintf("Request body too large. Maximum size: %d bytes", MaxRequestSize), nil)
			c.Abort()
			return
		}
		c.Next()
	}
}

// Validation helpers
func ValidateTransactionHash(hash string) error {
	if hash == "" {
		return fmt.Errorf("transaction hash is required")
	}
	
	return nil
}

func ValidateTickNumber(tickStr string) (uint64, error) {
	if tickStr == "" {
		return 0, fmt.Errorf("tick number is required")
	}
	
	tickNum, err := strconv.ParseUint(tickStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("tick number must be a valid unsigned integer")
	}
	
	return tickNum, nil
}

// Regular expressions for input validation
var (
	// Pattern for SQL injection attempts
	sqlInjectionPattern = regexp.MustCompile(`(?i)(union|select|insert|update|delete|drop|create|alter|exec|execute|script|javascript|onload|onerror|onclick)`)
	// Pattern for script tags and common XSS patterns
	xssPattern = regexp.MustCompile(`(?i)(<script|<iframe|javascript:|on\w+=)`)
	// Pattern for path traversal attempts
	pathTraversalPattern = regexp.MustCompile(`\.\.[\\/]|\.\.\\|%2e%2e|%252e%252e`)
)

func SanitizeInput(input string) string {
	// First, trim whitespace
	input = strings.TrimSpace(input)
	
	// Check for empty input
	if input == "" {
		return ""
	}
	
	// Limit input length to prevent buffer overflow attacks
	const maxInputLength = 10000
	if len(input) > maxInputLength {
		input = input[:maxInputLength]
	}
	
	// HTML escape to prevent XSS
	input = html.EscapeString(input)
	
	// Remove or replace dangerous patterns
	if sqlInjectionPattern.MatchString(input) {
		log.Printf("Warning: Potential SQL injection attempt detected and sanitized")
		input = sqlInjectionPattern.ReplaceAllString(input, "")
	}
	
	if xssPattern.MatchString(input) {
		log.Printf("Warning: Potential XSS attempt detected and sanitized")
		input = xssPattern.ReplaceAllString(input, "")
	}
	
	if pathTraversalPattern.MatchString(input) {
		log.Printf("Warning: Potential path traversal attempt detected and sanitized")
		input = pathTraversalPattern.ReplaceAllString(input, "")
	}
	
	// Remove null bytes and other control characters
	input = strings.Map(func(r rune) rune {
		if r == 0 || (unicode.IsControl(r) && r != '\t' && r != '\n' && r != '\r') {
			return -1 // Remove the character
		}
		return r
	}, input)
	
	return input
}

// SanitizeURLParam sanitizes URL parameters specifically
func SanitizeURLParam(param string) string {
	// Apply general sanitization
	param = SanitizeInput(param)
	
	// Additional URL-specific sanitization
	// Remove any URL encoding that might hide malicious content
	param = strings.ReplaceAll(param, "%00", "")
	param = strings.ReplaceAll(param, "\x00", "")
	
	return param
}

// SanitizeJSON sanitizes JSON string values
func SanitizeJSON(jsonStr string) string {
	// Apply general sanitization
	jsonStr = SanitizeInput(jsonStr)
	
	// Ensure the JSON doesn't contain script injections
	// This is a basic check - for production, use a proper JSON validator
	jsonStr = strings.ReplaceAll(jsonStr, "</script>", "")
	jsonStr = strings.ReplaceAll(jsonStr, "<script>", "")
	
	return jsonStr
}

func ValidateQueryParams(c *gin.Context) []string {
	var errors []string
	
	if limit := c.Query("limit"); limit != "" {
		if val, err := strconv.Atoi(limit); err != nil || val < 0 || val > 1000 {
			errors = append(errors, "limit must be a positive integer <= 1000")
		}
	}
	
	if offset := c.Query("offset"); offset != "" {
		if val, err := strconv.Atoi(offset); err != nil || val < 0 {
			errors = append(errors, "offset must be a non-negative integer")
		}
	}
	
	return errors
}

func SendErrorResponse(c *gin.Context, statusCode int, message string, validationErrors interface{}) {
	response := ErrorResponse{
		Error:     getStatusText(statusCode),
		Message:   message,
		Errors:    validationErrors,
		Timestamp: time.Now().Unix(),
	}
	
	// Log error to console for development
	log.Printf("❌ [%s %s] %d: %s", c.Request.Method, c.Request.URL.Path, statusCode, message)
	if validationErrors != nil {
		log.Printf("   Validation errors: %v", validationErrors)
	}
	
	// Log to error file if logger exists in context
	if logWriter, exists := c.Get("logWriter"); exists {
		if lw, ok := logWriter.(*LogWriter); ok {
			errorDetail := message
			if validationErrors != nil {
				errorDetail = fmt.Sprintf("%s - Validation errors: %v", message, validationErrors)
			}
			lw.LogError(c.Request.Method, c.Request.URL.Path, c.ClientIP(), c.Request.UserAgent(), errorDetail, statusCode)
		}
	}
	
	c.JSON(statusCode, response)
}

func getStatusText(code int) string {
	statusTexts := map[int]string{
		400: "Bad Request",
		401: "Unauthorized",
		403: "Forbidden",
		404: "Not Found",
		405: "Method Not Allowed",
		413: "Request Entity Too Large",
		500: "Internal Server Error",
		502: "Bad Gateway",
		503: "Service Unavailable",
		504: "Gateway Timeout",
	}
	if text, ok := statusTexts[code]; ok {
		return text
	}
	return "Unknown Error"
}