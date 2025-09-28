package handlers

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apiErrors "github.com/zerooo111/tick-streamer/internal/api/errors"
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
	repository     repository.Repository
}

func New(grpcAddr, restBaseURL, matchEngineURL string, repo repository.Repository) (*Handler, error) {
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

// TransactionRequest handles the JSON unmarshaling with proper type conversions
type TransactionRequest struct {
	TxID         string `json:"tx_id" binding:"required"`
	Payload      []byte `json:"payload" binding:"required"`
	SignatureHex string `json:"signature" binding:"required"`
	PublicKeyB58 string `json:"public_key" binding:"required"`
	Nonce        uint64 `json:"nonce" binding:"required"`
	TimestampStr string `json:"timestamp" binding:"required"`
}


// ToProtobuf converts the request to a protobuf transaction
func (tr *TransactionRequest) ToProtobuf() (*pb.Transaction, error) {
	// Convert hex signature to bytes
	signature, err := hex.DecodeString(tr.SignatureHex)
	if err != nil {
		return nil, fmt.Errorf("invalid signature hex: %w", err)
	}

	// Convert base58 public key to bytes (for now, treat as string bytes)
	// TODO: Proper base58 decoding if needed
	publicKey := []byte(tr.PublicKeyB58)

	// Convert timestamp string to uint64
	timestamp, err := strconv.ParseUint(tr.TimestampStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}

	return &pb.Transaction{
		TxId:      tr.TxID,
		Payload:   tr.Payload,
		Signature: signature,
		PublicKey: publicKey,
		Nonce:     tr.Nonce,
		Timestamp: timestamp,
	}, nil
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
		apiErrors.ValidationError(c, "Invalid transaction hash: " + err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try database first
	tx, err := h.repository.GetTransaction(ctx, txHash)
	if err == nil {
		c.Header("Cache-Control", "private, max-age=1800")
		c.Header("X-Data-Source", "database")
		c.JSON(http.StatusOK, gin.H{
			"source": "db",
			"data":   tx,
		})
		return
	}

	// Fallback to REST API
	resp, err := h.makeSecureRequest("GET", h.restBaseURL+"/tx/"+txHash, nil)
	if err != nil {
		apiErrors.ServiceUnavailableError(c, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "Transaction")
		return
	}

	if resp.StatusCode != http.StatusOK {
		apiErrors.ServiceUnavailableError(c, fmt.Errorf("upstream returned status %d", resp.StatusCode))
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "private, max-age=1800")
	c.Header("X-Data-Source", "rest-api")
	c.JSON(http.StatusOK, gin.H{
		"source": "continuum",
		"data":   data,
	})
}

// Submit single transaction
func (h *Handler) SubmitTransaction(c *gin.Context) {
	var body struct {
		Transaction TransactionRequest `json:"transaction" binding:"required"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		apiErrors.BadRequestError(c, "Invalid request body: " + err.Error())
		return
	}

	// Convert to gRPC transaction
	grpcTx, err := body.Transaction.ToProtobuf()
	if err != nil {
		apiErrors.BadRequestError(c, "Invalid transaction data: " + err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := h.grpcClient.SubmitTransaction(ctx, &pb.SubmitTransactionRequest{
		Transaction: grpcTx,
	})
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to submit transaction"))
		return
	}

	c.JSON(http.StatusOK, resp)
}

// Submit batch of transactions
func (h *Handler) SubmitBatch(c *gin.Context) {
	var body struct {
		Transactions []TransactionRequest `json:"transactions" binding:"required,min=1"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		apiErrors.BadRequestError(c, "Invalid request body: " + err.Error())
		return
	}

	// Convert to gRPC transactions
	var grpcTxs []*pb.Transaction
	for i, tx := range body.Transactions {
		grpcTx, err := tx.ToProtobuf()
		if err != nil {
			apiErrors.BadRequestError(c, fmt.Sprintf("Invalid transaction data at index %d: %s", i, err.Error()))
			return
		}
		grpcTxs = append(grpcTxs, grpcTx)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := h.grpcClient.SubmitBatch(ctx, &pb.SubmitBatchRequest{
		Transactions: grpcTxs,
	})
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to submit batch"))
		return
	}

	c.JSON(http.StatusOK, resp)
}

// Get tick by number
func (h *Handler) GetTick(c *gin.Context) {
	tickStr := middleware.SanitizeInput(c.Param("number"))

	tickNumber, err := middleware.ValidateTickNumber(tickStr)
	if err != nil {
		apiErrors.BadRequestError(c, "Invalid tick number: " + err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try database first
	tick, err := h.repository.GetTick(ctx, tickNumber)
	if err == nil {
		c.Header("Cache-Control", "private, max-age=600")
		c.Header("X-Data-Source", "database")
		c.JSON(http.StatusOK, gin.H{
			"source": "db",
			"data":   tick,
		})
		return
	}

	// Fallback to REST API
	resp, err := h.makeSecureRequest("GET", h.restBaseURL+"/tick/"+tickStr, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get tick"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "Tick")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get tick", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "private, max-age=600")
	c.Header("X-Data-Source", "rest-api")
	c.JSON(http.StatusOK, gin.H{
		"source": "continuum",
		"data":   data,
	})
}

// Get recent ticks
func (h *Handler) GetRecentTicks(c *gin.Context) {
	validationErrors := middleware.ValidateQueryParams(c)
	if len(validationErrors) > 0 {
		apiErrors.BadRequestError(c, "Invalid query parameters: " + strings.Join(validationErrors, ", "))
		return
	}

	// Get limit parameter
	limit := 50 // default
	if limitStr := c.Query("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 && parsedLimit <= 1000 {
			limit = parsedLimit
		}
	}

	// Proxy to continuum REST API
	continuumURL := fmt.Sprintf("%s/ticks/recent?limit=%d", h.restBaseURL, limit)
	
	resp, err := h.httpClient.Get(continuumURL)
	if err != nil {
		apiErrors.ServiceUnavailableError(c, fmt.Errorf("failed to get recent ticks from continuum"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		apiErrors.ServiceUnavailableError(c, fmt.Errorf("continuum service returned status %d", resp.StatusCode))
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Header("X-Data-Source", "continuum")
	c.JSON(http.StatusOK, data)
}

// Get recent transactions
func (h *Handler) GetRecentTransactions(c *gin.Context) {
	validationErrors := middleware.ValidateQueryParams(c)
	if len(validationErrors) > 0 {
		apiErrors.BadRequestError(c, "Invalid query parameters: " + strings.Join(validationErrors, ", "))
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

	// Try database first
	transactions, err := h.repository.GetRecentTransactions(ctx, limit)
	if err == nil {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		c.Header("X-Data-Source", "database")
		c.JSON(http.StatusOK, gin.H{
			"transactions": transactions,
			"count":        len(transactions),
		})
		return
	}
	
	// Log database error for debugging  
	fmt.Printf("Database query failed for recent transactions: %v\n", err)

	// Return empty result - database may not have data yet
	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Header("X-Data-Source", "database_unavailable")
	c.JSON(http.StatusOK, gin.H{
		"transactions": []interface{}{},
		"count":        0,
		"message":      "Recent transactions unavailable - database may not be populated yet",
	})
}

// Get chain state
func (h *Handler) GetChainState(c *gin.Context) {
	tickLimitStr := c.Query("tick_limit")
	var tickLimit *int

	if tickLimitStr != "" {
		parsed, err := strconv.Atoi(tickLimitStr)
		if err != nil || parsed < 0 {
			apiErrors.BadRequestError(c, "Invalid tick_limit parameter")
			return
		}
		tickLimit = &parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
		apiErrors.InternalError(c, fmt.Errorf("failed to get markets"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get markets", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get market orderbook
func (h *Handler) GetMarketOrderbook(c *gin.Context) {
	marketID := middleware.SanitizeInput(c.Param("marketId"))

	if marketID == "" {
		apiErrors.BadRequestError(c, "Market ID is required")
		return
	}

	targetURL := h.matchEngineURL + "/markets/" + url.PathEscape(marketID) + "/orderbook"

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get market orderbook"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "Market")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get market orderbook", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get market trades
func (h *Handler) GetMarketTrades(c *gin.Context) {
	marketID := middleware.SanitizeInput(c.Param("marketId"))

	if marketID == "" {
		apiErrors.BadRequestError(c, "Market ID is required")
		return
	}

	targetURL := h.matchEngineURL + "/markets/" + url.PathEscape(marketID) + "/trades"

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get market trades"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "Market")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get market trades", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get market orderbook summary
func (h *Handler) GetMarketOrderbookSummary(c *gin.Context) {
	marketID := middleware.SanitizeInput(c.Param("marketId"))

	if marketID == "" {
		apiErrors.BadRequestError(c, "Market ID is required")
		return
	}

	targetURL := h.matchEngineURL + "/markets/" + url.PathEscape(marketID) + "/orderbook/summary"

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get market orderbook summary"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "Market")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get market orderbook summary", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get user orders
func (h *Handler) GetUserOrders(c *gin.Context) {
	pubkey := middleware.SanitizeInput(c.Param("pubkey"))

	if pubkey == "" {
		apiErrors.BadRequestError(c, "Public key is required")
		return
	}

	targetURL := h.matchEngineURL + "/orders/user/" + url.PathEscape(pubkey)

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get user orders"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "User orders")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get user orders", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get user balances
func (h *Handler) GetUserBalances(c *gin.Context) {
	pubkey := middleware.SanitizeInput(c.Param("pubkey"))

	if pubkey == "" {
		apiErrors.BadRequestError(c, "Public key is required")
		return
	}

	targetURL := h.matchEngineURL + "/balances/" + url.PathEscape(pubkey)

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get user balances"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "User balances")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get user balances", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}


// Get user accounts (full account summary)
func (h *Handler) GetUserAccounts(c *gin.Context) {
	pubkey := middleware.SanitizeInput(c.Param("pubkey"))

	if pubkey == "" {
		apiErrors.BadRequestError(c, "Public key is required")
		return
	}

	targetURL := h.matchEngineURL + "/accounts/" + url.PathEscape(pubkey)

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get user accounts"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "User accounts")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get user accounts", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get positions (position snapshots)
func (h *Handler) GetPositions(c *gin.Context) {
	targetURL := h.matchEngineURL + "/positions"

	// Add optional owner query parameter
	owner := c.Query("owner")
	if owner != "" {
		targetURL += "?owner=" + url.QueryEscape(owner)
	}

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get positions"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get positions", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}


// Get liquidations
func (h *Handler) GetLiquidations(c *gin.Context) {
	targetURL := h.matchEngineURL + "/liquidations"

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get liquidations"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get liquidations", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get market stats
func (h *Handler) GetMarketStats(c *gin.Context) {
	marketUUID := middleware.SanitizeInput(c.Param("marketId"))

	if marketUUID == "" {
		apiErrors.BadRequestError(c, "Market UUID is required")
		return
	}

	targetURL := h.matchEngineURL + "/markets/" + url.PathEscape(marketUUID) + "/stats"

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get market stats"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "Market stats")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get market stats", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Get user PNL
func (h *Handler) GetUserPNL(c *gin.Context) {
	ownerPubkey := middleware.SanitizeInput(c.Param("pubkey"))

	if ownerPubkey == "" {
		apiErrors.BadRequestError(c, "Owner public key is required")
		return
	}

	targetURL := h.matchEngineURL + "/users/" + url.PathEscape(ownerPubkey) + "/pnl"

	resp, err := h.makeSecureRequest("GET", targetURL, nil)
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to get user PNL"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "User PNL")
		return
	}

	if resp.StatusCode != http.StatusOK {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to get user PNL", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.JSON(http.StatusOK, data)
}

// Post airdrop
func (h *Handler) PostAirdrop(c *gin.Context) {
	receiverPubKey := middleware.SanitizeInput(c.Param("receiverPubKey"))
	tokenName := middleware.SanitizeInput(c.Param("tokenName"))

	if receiverPubKey == "" {
		apiErrors.BadRequestError(c, "Receiver public key is required")
		return
	}

	if tokenName == "" {
		apiErrors.BadRequestError(c, "Token name is required")
		return
	}

	targetURL := h.matchEngineURL + "/airdrop/" + url.PathEscape(receiverPubKey) + "/" + url.PathEscape(tokenName)

	// Read request body if any
	var body []byte
	if c.Request.Body != nil {
		var err error
		body, err = io.ReadAll(c.Request.Body)
		if err != nil {
			apiErrors.BadRequestError(c, "Failed to read request body")
			return
		}
	}

	resp, err := h.makeSecureRequest("POST", targetURL, bytes.NewReader(body))
	if err != nil {
		apiErrors.InternalError(c, fmt.Errorf("failed to post airdrop"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		apiErrors.NotFoundError(c, "Airdrop")
		return
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		middleware.SendErrorResponse(c, http.StatusBadGateway, "Failed to post airdrop", nil)
		return
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		apiErrors.InternalError(c, err)
		return
	}

	c.JSON(resp.StatusCode, data)
}

// Root endpoint with API information
func (h *Handler) Root(c *gin.Context) {
	response := gin.H{
		"service":  "fermi-explorer-go-backend",
		"version":  "1.0.0",
		"endpoints": gin.H{
			"health":           "/api/v1/health",
			"status":           "/api/v1/status",
			"sequencer_status": "/api/v1/sequencer/status",
			"transaction":      "/api/v1/tx/{hash}",
			"recent_transactions": "/api/v1/tx/recent",
			"submit_transaction": "/api/v1/tx",
			"submit_batch":     "/api/v1/tx/batch",
			"tick":             "/api/v1/tick/{number}",
			"recent_ticks":     "/api/v1/ticks/recent",
			"chain_state":      "/api/v1/chain/state",
			"websocket_ticks":  "/ws/ticks",
			"websocket_market_stats": "/ws/market-stats",
			"markets":          "/api/v1/me/markets",
			"create_market":    "/api/v1/me/markets",
			"market_orderbook": "/api/v1/me/markets/{marketId}/orderbook",
			"market_orderbook_summary": "/api/v1/me/markets/{marketId}/orderbook/summary",
			"market_trades":    "/api/v1/me/markets/{marketId}/trades",
			"market_stats":     "/api/v1/me/markets/{marketId}/stats",
			"market_candles":   "/api/v1/me/markets/{marketId}/candles?tf=1h&from=ISO&to=ISO",
			"user_orders":      "/api/v1/me/orders/user/{pubkey}",
			"user_balances":    "/api/v1/me/balances/{pubkey}",
			"user_accounts":    "/api/v1/me/accounts/{pubkey}",
			"user_pnl":         "/api/v1/me/users/{pubkey}/pnl",
			"positions":        "/api/v1/me/positions",
			"margin_deposit":   "/api/v1/me/margin/deposit",
			"margin_withdraw":  "/api/v1/me/margin/withdraw",
			"airdrop":          "/api/v1/me/airdrop/{receiverPubKey}/{tokenName}",
			"liquidations":     "/api/v1/me/liquidations",
		},
	}

	c.JSON(http.StatusOK, response)
}

// GetMarketCandles retrieves OHLC candles for a market
func (h *Handler) GetMarketCandles(c *gin.Context) {
	marketID := middleware.SanitizeInput(c.Param("marketId"))
	if marketID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"data":       nil,
			"statusCode": http.StatusBadRequest,
			"error":      "Market ID is required",
		})
		return
	}

	// Parse and validate timeframe
	timeframe := c.Query("tf")
	if timeframe == "" {
		timeframe = "1h" // default timeframe
	}
	
	// Validate timeframe
	allowedTimeframes := map[string]bool{
		"1m": true, "5m": true, "15m": true, 
		"1h": true, "4h": true, "1d": true,
	}
	if !allowedTimeframes[timeframe] {
		c.JSON(http.StatusBadRequest, gin.H{
			"data":       nil,
			"statusCode": http.StatusBadRequest,
			"error":      "Invalid timeframe. Allowed values: 1m, 5m, 15m, 1h, 4h, 1d",
		})
		return
	}

	// Parse from/to dates
	var from, to time.Time
	var err error

	if fromStr := c.Query("from"); fromStr != "" {
		from, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"data":       nil,
				"statusCode": http.StatusBadRequest,
				"error":      "Invalid 'from' date format. Use RFC3339 format (e.g., 2023-01-01T00:00:00Z)",
			})
			return
		}
	} else {
		// Default to 24 hours ago if no 'from' specified
		from = time.Now().UTC().Add(-24 * time.Hour)
	}

	if toStr := c.Query("to"); toStr != "" {
		to, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"data":       nil,
				"statusCode": http.StatusBadRequest,
				"error":      "Invalid 'to' date format. Use RFC3339 format (e.g., 2023-01-01T00:00:00Z)",
			})
			return
		}
	} else {
		// Default to now if no 'to' specified
		to = time.Now().UTC()
	}

	// Validate date range
	if from.After(to) {
		c.JSON(http.StatusBadRequest, gin.H{
			"data":       nil,
			"statusCode": http.StatusBadRequest,
			"error":      "'from' date must be before 'to' date",
		})
		return
	}

	// Limit date range to prevent excessive queries
	if to.Sub(from) > 30*24*time.Hour {
		c.JSON(http.StatusBadRequest, gin.H{
			"data":       nil,
			"statusCode": http.StatusBadRequest,
			"error":      "Date range cannot exceed 30 days",
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Query candles from repository
	candles, err := h.repository.GetMarketCandles(ctx, marketID, timeframe, from, to)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"data":       nil,
			"statusCode": http.StatusInternalServerError,
			"error":      "Failed to get market candles",
		})
		return
	}

	// Set caching headers as specified
	c.Header("Cache-Control", "public, max-age=5")
	c.Header("X-Data-Source", "database")
	
	c.JSON(http.StatusOK, gin.H{
		"data":       candles,
		"statusCode": http.StatusOK,
	})
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