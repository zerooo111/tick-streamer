package middleware

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LogConfig struct {
	LogDir         string
	MaxSize        int  // megabytes
	MaxBackups     int
	MaxAge         int  // days
	Compress       bool
	EnableConsole  bool
	DisableFile    bool  // Disable file logging completely
}

type LogWriter struct {
	requestLogger *lumberjack.Logger
	errorLogger   *lumberjack.Logger
	config        *LogConfig
}

func NewLogWriter(config *LogConfig) (*LogWriter, error) {
	var requestLogger, errorLogger *lumberjack.Logger

	// Only create file loggers if file logging is not disabled
	if !config.DisableFile {
		// Create log directory if it doesn't exist
		if err := os.MkdirAll(config.LogDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		requestLogger = &lumberjack.Logger{
			Filename:   filepath.Join(config.LogDir, "requests.log"),
			MaxSize:    config.MaxSize,
			MaxBackups: config.MaxBackups,
			MaxAge:     config.MaxAge,
			Compress:   config.Compress,
		}

		errorLogger = &lumberjack.Logger{
			Filename:   filepath.Join(config.LogDir, "errors.log"),
			MaxSize:    config.MaxSize,
			MaxBackups: config.MaxBackups,
			MaxAge:     config.MaxAge,
			Compress:   config.Compress,
		}
	}

	return &LogWriter{
		requestLogger: requestLogger,
		errorLogger:   errorLogger,
		config:        config,
	}, nil
}

func (lw *LogWriter) getWriter(isError bool) io.Writer {
	var fileWriter io.Writer
	
	// Only use file writers if file logging is enabled
	if !lw.config.DisableFile {
		if isError {
			fileWriter = lw.errorLogger
		} else {
			fileWriter = lw.requestLogger
		}
	}

	if lw.config.EnableConsole {
		if fileWriter != nil {
			return io.MultiWriter(fileWriter, os.Stdout)
		}
		return os.Stdout
	}
	
	// If file logging is disabled and console is disabled, return stdout as fallback
	if fileWriter == nil {
		return os.Stdout
	}
	
	return fileWriter
}

func (lw *LogWriter) LoggingMiddleware() gin.HandlerFunc {
	return gin.LoggerWithConfig(gin.LoggerConfig{
		Formatter: func(param gin.LogFormatterParams) string {
			return fmt.Sprintf(`{"time":"%s","method":"%s","path":"%s","protocol":"%s","status_code":%d,"latency":"%s","client_ip":"%s","user_agent":"%s","response_size":%d,"error_message":"%s"}`+"\n",
				param.TimeStamp.Format(time.RFC3339),
				param.Method,
				param.Path,
				param.Request.Proto,
				param.StatusCode,
				param.Latency,
				param.ClientIP,
				param.Request.UserAgent(),
				param.BodySize,
				param.ErrorMessage,
			)
		},
		Output: func() io.Writer {
			return lw.getWriter(false)
		}(),
		SkipPaths: []string{"/health"},
	})
}

func (lw *LogWriter) ErrorLoggingMiddleware() gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(lw.getWriter(true), func(c *gin.Context, recovered interface{}) {
		if recovered != nil {
			errorMsg := fmt.Sprintf(`{"time":"%s","level":"ERROR","type":"panic","method":"%s","path":"%s","client_ip":"%s","user_agent":"%s","error":"%v"}`+"\n",
				time.Now().Format(time.RFC3339),
				c.Request.Method,
				c.Request.URL.Path,
				c.ClientIP(),
				c.Request.UserAgent(),
				recovered,
			)
			
			writer := lw.getWriter(true)
			writer.Write([]byte(errorMsg))
		}
		c.AbortWithStatus(500)
	})
}

func (lw *LogWriter) LogError(method, path, clientIP, userAgent, errorMsg string, statusCode int) {
	logEntry := fmt.Sprintf(`{"time":"%s","level":"ERROR","type":"application","method":"%s","path":"%s","status_code":%d,"client_ip":"%s","user_agent":"%s","error":"%s"}`+"\n",
		time.Now().Format(time.RFC3339),
		method,
		path,
		statusCode,
		clientIP,
		userAgent,
		errorMsg,
	)
	
	writer := lw.getWriter(true)
	writer.Write([]byte(logEntry))
}

func (lw *LogWriter) Close() error {
	var err error
	
	// Only close file loggers if they exist
	if lw.requestLogger != nil {
		if closeErr := lw.requestLogger.Close(); closeErr != nil {
			err = closeErr
		}
	}
	
	if lw.errorLogger != nil {
		if closeErr := lw.errorLogger.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("%v; %v", err, closeErr)
			} else {
				err = closeErr
			}
		}
	}
	
	return err
}