package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zerooo111/tick-streamer/internal/api/middleware"
	"github.com/zerooo111/tick-streamer/internal/api/repository"
	pb "github.com/zerooo111/tick-streamer/proto"
)

type Handler struct {
	grpcClient     pb.SequencerServiceClient
	grpcConn       *grpc.ClientConn
	restBaseURL    string
	matchEngineURL string
	httpClient     *http.Client
	repository     *repository.ClickHouseRepository
}

func New(grpcAddr, restBaseURL, matchEngineURL string, repo *repository.ClickHouseRepository) (*Handler, error) {
	// Create gRPC connection
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	h := &Handler{
		grpcClient:     pb.NewSequencerServiceClient(conn),
		grpcConn:       conn,
		restBaseURL:    restBaseURL,
		matchEngineURL: matchEngineURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		repository: repo,
	}

	return h, nil
}

func (h *Handler) Close() {
	if h.grpcConn != nil {
		h.grpcConn.Close()
	}
}

// Health check endpoint
func (h *Handler) Health(c *gin.Context) {
	response := gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"version":   "1.0.0",
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, response)
}

// Sequencer status endpoint
func (h *Handler) Status(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := h.grpcClient.GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusServiceUnavailable, "Failed to get status from sequencer", nil)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, resp)
}

// Get transaction by hash
func (h *Handler) GetTransaction(c *gin.Context) {
	txHash := middleware.SanitizeInput(c.Param("hash"))

	if err := middleware.ValidateTransactionHash(txHash); err != nil {
		middleware.SendErrorResponse(c, http.StatusBadRequest, "Invalid transaction hash", []string{err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try ClickHouse first
	tx, err := h.repository.GetTransaction(ctx, txHash)
	if err == nil {
		c.Header("Cache-Control", "private, max-age=1800")
		c.Header("X-Data-Source", "clickhouse")
		c.JSON(http.StatusOK, tx)
		return
	}

	// Fallback to REST API
	resp, err := h.makeSecureRequest("GET", h.restBaseURL+"/tx/"+txHash, nil)
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to get transaction", nil)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		middleware.SendErrorResponse(c, http.StatusNotFound, "Transaction not found", nil)
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get transaction", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to decode response", nil)
		return
	}

	c.Header("Cache-Control", "private, max-age=1800")
	c.Header("X-Data-Source", "rest-api")
	c.JSON(http.StatusOK, data)
}

// Submit single transaction
func (h *Handler) SubmitTransaction(c *gin.Context) {
	var body struct {
		Transaction struct {
			TxID      string `json:"tx_id" binding:"required"`
			Payload   []byte `json:"payload" binding:"required"`
			Signature []byte `json:"signature" binding:"required"`
			PublicKey []byte `json:"public_key" binding:"required"`
			Nonce     uint64 `json:"nonce" binding:"required"`
			Timestamp uint64 `json:"timestamp" binding:"required"`
		} `json:"transaction" binding:"required"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		middleware.SendErrorResponse(c, http.StatusBadRequest, "Invalid request body", []string{err.Error()})
		return
	}

	// Convert to gRPC transaction
	grpcTx := &pb.Transaction{
		TxId:      body.Transaction.TxID,
		Payload:   body.Transaction.Payload,
		Signature: body.Transaction.Signature,
		PublicKey: body.Transaction.PublicKey,
		Nonce:     body.Transaction.Nonce,
		Timestamp: body.Transaction.Timestamp,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := h.grpcClient.SubmitTransaction(ctx, &pb.SubmitTransactionRequest{
		Transaction: grpcTx,
	})
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to submit transaction", nil)
		return
	}

	c.JSON(http.StatusOK, resp)
}

// Submit batch of transactions
func (h *Handler) SubmitBatch(c *gin.Context) {
	var body struct {
		Transactions []struct {
			TxID      string `json:"tx_id" binding:"required"`
			Payload   []byte `json:"payload" binding:"required"`
			Signature []byte `json:"signature" binding:"required"`
			PublicKey []byte `json:"public_key" binding:"required"`
			Nonce     uint64 `json:"nonce" binding:"required"`
			Timestamp uint64 `json:"timestamp" binding:"required"`
		} `json:"transactions" binding:"required,min=1"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		middleware.SendErrorResponse(c, http.StatusBadRequest, "Invalid request body", []string{err.Error()})
		return
	}

	// Convert to gRPC transactions
	var grpcTxs []*pb.Transaction
	for _, tx := range body.Transactions {
		grpcTxs = append(grpcTxs, &pb.Transaction{
			TxId:      tx.TxID,
			Payload:   tx.Payload,
			Signature: tx.Signature,
			PublicKey: tx.PublicKey,
			Nonce:     tx.Nonce,
			Timestamp: tx.Timestamp,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := h.grpcClient.SubmitBatch(ctx, &pb.SubmitBatchRequest{
		Transactions: grpcTxs,
	})
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to submit batch", nil)
		return
	}

	c.JSON(http.StatusOK, resp)
}

// Get tick by number
func (h *Handler) GetTick(c *gin.Context) {
	tickStr := middleware.SanitizeInput(c.Param("number"))

	tickNumber, err := middleware.ValidateTickNumber(tickStr)
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusBadRequest, "Invalid tick number", []string{err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try ClickHouse first
	tick, err := h.repository.GetTick(ctx, tickNumber)
	if err == nil {
		c.Header("Cache-Control", "private, max-age=600")
		c.Header("X-Data-Source", "clickhouse")
		c.JSON(http.StatusOK, tick)
		return
	}

	// Fallback to REST API
	resp, err := h.makeSecureRequest("GET", h.restBaseURL+"/tick/"+tickStr, nil)
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to get tick", nil)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		middleware.SendErrorResponse(c, http.StatusNotFound, "Tick not found", nil)
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get tick", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to decode response", nil)
		return
	}

	c.Header("Cache-Control", "private, max-age=600")
	c.Header("X-Data-Source", "rest-api")
	c.JSON(http.StatusOK, data)
}

// Get recent ticks
func (h *Handler) GetRecentTicks(c *gin.Context) {
	validationErrors := middleware.ValidateQueryParams(c)
	if len(validationErrors) > 0 {
		middleware.SendErrorResponse(c, http.StatusBadRequest, "Invalid query parameters", validationErrors)
		return
	}

	// Get limit parameter
	limit := 50 // default
	if limitStr := c.Query("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 && parsedLimit <= 1000 {
			limit = parsedLimit
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try ClickHouse first
	ticks, err := h.repository.GetRecentTicks(ctx, limit)
	if err == nil {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		c.Header("X-Data-Source", "clickhouse")
		c.JSON(http.StatusOK, ticks)
		return
	}

	// Fallback to REST API
	targetURL := h.restBaseURL + "/ticks/recent"
	if c.Request.URL.RawQuery != "" {
		targetURL += "?" + c.Request.URL.RawQuery
	}

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to get recent ticks", nil)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get recent ticks", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to decode response", nil)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Header("X-Data-Source", "rest-api")
	c.JSON(http.StatusOK, data)
}

// Get chain state
func (h *Handler) GetChainState(c *gin.Context) {
	tickLimitStr := c.Query("tick_limit")
	var tickLimit *int

	if tickLimitStr != "" {
		parsed, err := strconv.Atoi(tickLimitStr)
		if err != nil || parsed < 0 {
			middleware.SendErrorResponse(c, http.StatusBadRequest, "Invalid tick_limit parameter", nil)
			return
		}
		tickLimit = &parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try ClickHouse first
	chainState, err := h.repository.GetChainState(ctx, tickLimit)
	if err == nil {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		c.Header("X-Data-Source", "clickhouse")
		c.JSON(http.StatusOK, chainState)
		return
	}

	// Fallback to gRPC
	var grpcTickLimit uint32
	if tickLimit != nil {
		grpcTickLimit = uint32(*tickLimit)
	}

	resp, err := h.grpcClient.GetChainState(ctx, &pb.GetChainStateRequest{
		TickLimit: grpcTickLimit,
	})
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusServiceUnavailable, "Failed to get chain state from sequencer", nil)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Header("X-Data-Source", "grpc")
	c.JSON(http.StatusOK, resp)
}

// Get markets
func (h *Handler) GetMarkets(c *gin.Context) {
	resp, err := h.makeSecureRequest("GET", h.matchEngineURL+"/markets", nil)
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to get markets", nil)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get markets", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to decode response", nil)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get market orderbook
func (h *Handler) GetMarketOrderbook(c *gin.Context) {
	marketID := middleware.SanitizeInput(c.Param("marketId"))

	if marketID == "" {
		middleware.SendErrorResponse(c, http.StatusBadRequest, "Market ID is required", nil)
		return
	}

	targetURL := h.matchEngineURL + "/markets/" + url.PathEscape(marketID) + "/orderbook"

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to get market orderbook", nil)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		middleware.SendErrorResponse(c, http.StatusNotFound, "Market not found", nil)
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get market orderbook", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		middleware.SendErrorResponse(c, http.StatusInternalServerError, "Failed to decode response", nil)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Root endpoint with API information
func (h *Handler) Root(c *gin.Context) {
	response := gin.H{
		"service":  "fermi-explorer-go-backend",
		"version":  "1.0.0",
		"endpoints": gin.H{
			"health":           "/api/v1/health",
			"status":           "/api/v1/status",
			"transaction":      "/api/v1/tx/{hash}",
			"submit_transaction": "/api/v1/tx",
			"submit_batch":     "/api/v1/tx/batch",
			"tick":             "/api/v1/tick/{number}",
			"recent_ticks":     "/api/v1/ticks/recent",
			"chain_state":      "/api/v1/chain/state",
			"websocket":        "/ws/ticks",
			"markets":          "/api/v1/me/markets",
			"market_orderbook": "/api/v1/me/markets/{marketId}/orderbook",
		},
	}

	c.JSON(http.StatusOK, response)
}

// Helper function to make secure HTTP requests
func (h *Handler) makeSecureRequest(method, url string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader

	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "fermi-explorer-go-proxy/1.0")
	req.Header.Set("Accept", "application/json")
	
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return h.httpClient.Do(req)
}