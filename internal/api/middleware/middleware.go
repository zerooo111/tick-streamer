package middleware

import (
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

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
	
	// Check if it's a valid hex string (allow both with and without 0x prefix)
	cleanHash := strings.TrimPrefix(hash, "0x")
	if len(cleanHash) != 64 {
		return fmt.Errorf("transaction hash must be 64 hex characters")
	}
	
	matched, _ := regexp.MatchString("^[0-9a-fA-F]{64}$", cleanHash)
	if !matched {
		return fmt.Errorf("transaction hash must be valid hex")
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

func SanitizeInput(input string) string {
	// Remove any potentially dangerous characters for basic sanitization
	// This is a simple implementation - expand as needed
	return strings.TrimSpace(input)
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
	
	log.Printf("❌ [%s %s] %d: %s", c.Request.Method, c.Request.URL.Path, statusCode, message)
	if validationErrors != nil {
		log.Printf("   Validation errors: %v", validationErrors)
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