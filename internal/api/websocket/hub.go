package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/zerooo111/tick-streamer/proto"
)

type Client struct {
	ID        string
	Conn      *websocket.Conn
	Hub       *Hub
	Send      chan []byte
	StartTick uint64
	mu        sync.Mutex
	ClientIP  string  // Track client IP for cleanup
}

type Hub struct {
	clients        map[*Client]bool
	register       chan *Client
	unregister     chan *Client
	broadcast      chan []byte
	grpcAddr       string
	grpcClient     pb.SequencerServiceClient
	grpcConn       *grpc.ClientConn
	mu             sync.RWMutex
	metrics        HubMetrics
	allowedOrigins []string
	upgrader       websocket.Upgrader
	
	// Connection limits
	maxConnections    int
	maxConnectionsPerIP int
	clientsByIP       map[string]int
}

type HubMetrics struct {
	ActiveConnections   int64
	TotalConnections    int64
	DroppedConnections  int64
	BroadcastErrors     int64
}

type TickMessage struct {
	Type                 string                    `json:"type"`
	TickNumber          string                    `json:"tick_number"`
	Timestamp           string                    `json:"timestamp"`
	TransactionCount    int                       `json:"transaction_count"`
	TransactionBatchHash string                   `json:"transaction_batch_hash"`
	PreviousOutput      string                    `json:"previous_output"`
	VDFProof            *VDFProofMessage          `json:"vdf_proof"`
	Transactions        []TransactionMessage      `json:"transactions"`
}

type VDFProofMessage struct {
	Input      string `json:"input"`
	Output     string `json:"output"`
	Proof      string `json:"proof"`
	Iterations string `json:"iterations"`
}

type TransactionMessage struct {
	TxID               string `json:"tx_id"`
	SequenceNumber     string `json:"sequence_number"`
	Nonce              string `json:"nonce"`
	IngestionTimestamp string `json:"ingestion_timestamp"`
	PayloadSize        int    `json:"payload_size"`
}

type ErrorMessage struct {
	Type  string `json:"type"`
	Error string `json:"error"`
}

func NewHub(grpcAddr string, allowedOrigins []string) (*Hub, error) {
	// Create gRPC connection
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	hub := &Hub{
		clients:             make(map[*Client]bool),
		register:            make(chan *Client),
		unregister:          make(chan *Client),
		broadcast:           make(chan []byte, 256),
		grpcAddr:            grpcAddr,
		grpcClient:          pb.NewSequencerServiceClient(conn),
		grpcConn:            conn,
		allowedOrigins:      allowedOrigins,
		maxConnections:      100,  // Default maximum total connections
		maxConnectionsPerIP: 10,   // Default maximum connections per IP
		clientsByIP:         make(map[string]int),
	}

	// Configure WebSocket upgrader with CORS check
	hub.upgrader = websocket.Upgrader{
		CheckOrigin:     hub.checkOrigin,
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	go hub.run()
	return hub, nil
}

// checkOrigin validates the origin of WebSocket connections
func (h *Hub) checkOrigin(r *http.Request) bool {
	// If no allowed origins are configured, reject all connections for security
	if len(h.allowedOrigins) == 0 {
		log.Println("Warning: No allowed origins configured, rejecting WebSocket connection")
		return false
	}

	// Special case: "*" allows all origins (use with caution)
	for _, allowed := range h.allowedOrigins {
		if allowed == "*" {
			return true
		}
	}

	// Get the origin from the request
	origin := r.Header.Get("Origin")
	if origin == "" {
		// No origin header, could be a non-browser client
		// You may want to allow or deny based on your security requirements
		return false
	}

	// Parse the origin URL
	originURL, err := url.Parse(origin)
	if err != nil {
		log.Printf("Invalid origin URL: %s", origin)
		return false
	}

	// Check if the origin matches any allowed origin
	for _, allowed := range h.allowedOrigins {
		allowedURL, err := url.Parse(allowed)
		if err != nil {
			// If allowed origin is not a valid URL, try direct string comparison
			if strings.EqualFold(origin, allowed) {
				return true
			}
			continue
		}

		// Compare scheme, host, and port
		if originURL.Scheme == allowedURL.Scheme &&
			strings.EqualFold(originURL.Host, allowedURL.Host) {
			return true
		}
	}

	log.Printf("Rejected WebSocket connection from origin: %s", origin)
	return false
}

// SetConnectionLimits configures the maximum connection limits
func (h *Hub) SetConnectionLimits(maxTotal, maxPerIP int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.maxConnections = maxTotal
	h.maxConnectionsPerIP = maxPerIP
}

// canAcceptConnection checks if a new connection from the given IP can be accepted
func (h *Hub) canAcceptConnection(clientIP string) (bool, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Check total connection limit
	activeConnections := len(h.clients)
	if activeConnections >= h.maxConnections {
		return false, fmt.Sprintf("maximum connections reached (%d)", h.maxConnections)
	}
	
	// Check per-IP connection limit
	connectionsFromIP := h.clientsByIP[clientIP]
	if connectionsFromIP >= h.maxConnectionsPerIP {
		return false, fmt.Sprintf("maximum connections per IP reached (%d)", h.maxConnectionsPerIP)
	}
	
	return true, ""
}

// registerClient registers a new client connection
func (h *Hub) registerClient(client *Client, clientIP string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	h.clients[client] = true
	h.clientsByIP[clientIP]++
	h.metrics.ActiveConnections++
	h.metrics.TotalConnections++
}

// unregisterClient removes a client connection
func (h *Hub) unregisterClient(client *Client, clientIP string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.Send)
		
		// Decrement per-IP counter
		if count := h.clientsByIP[clientIP]; count > 0 {
			if count == 1 {
				delete(h.clientsByIP, clientIP)
			} else {
				h.clientsByIP[clientIP]--
			}
		}
		
		h.metrics.ActiveConnections--
		h.metrics.DroppedConnections++
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.metrics.ActiveConnections++
			h.metrics.TotalConnections++
			h.mu.Unlock()
			
			log.Printf("WebSocket client %s connected, starting from tick %d", client.ID, client.StartTick)
			
			// Start streaming ticks for this client
			go h.streamTicksToClient(client)

		case client := <-h.unregister:
			// Use the unregisterClient method to properly clean up
			h.unregisterClient(client, client.ClientIP)
			log.Printf("Client %s disconnected", client.ID)

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.Send <- message:
				default:
					// Client's send channel is full, remove it
					delete(h.clients, client)
					close(client.Send)
					h.metrics.ActiveConnections--
					h.metrics.DroppedConnections++
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Hub) streamTicksToClient(client *Client) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := h.grpcClient.StreamTicks(ctx, &pb.StreamTicksRequest{
		StartTick: client.StartTick,
	})
	if err != nil {
		h.sendErrorToClient(client, err.Error())
		return
	}

	for {
		tick, err := stream.Recv()
		if err != nil {
			if ctx.Err() == context.Canceled {
				return // Client disconnected
			}
			log.Printf("gRPC stream error for client %s: %v", client.ID, err)
			h.sendErrorToClient(client, err.Error())
			return
		}

		h.sendTickToClient(client, tick)
	}
}

func (h *Hub) sendTickToClient(client *Client, tick *pb.Tick) {
	tickMsg := TickMessage{
		Type:                 "tick",
		TickNumber:          strconv.FormatUint(tick.TickNumber, 10),
		Timestamp:           strconv.FormatUint(tick.Timestamp, 10),
		TransactionCount:    len(tick.Transactions),
		TransactionBatchHash: tick.TransactionBatchHash,
		PreviousOutput:      tick.PreviousOutput,
		VDFProof: &VDFProofMessage{
			Input:      tick.VdfProof.Input,
			Output:     tick.VdfProof.Output,
			Proof:      tick.VdfProof.Proof,
			Iterations: strconv.FormatUint(tick.VdfProof.Iterations, 10),
		},
		Transactions: make([]TransactionMessage, len(tick.Transactions)),
	}

	for i, tx := range tick.Transactions {
		payloadSize := 0
		if tx.Transaction.Payload != nil {
			payloadSize = len(tx.Transaction.Payload)
		}
		
		tickMsg.Transactions[i] = TransactionMessage{
			TxID:               tx.Transaction.TxId,
			SequenceNumber:     strconv.FormatUint(tx.SequenceNumber, 10),
			Nonce:              strconv.FormatUint(tx.Transaction.Nonce, 10),
			IngestionTimestamp: strconv.FormatUint(tx.IngestionTimestamp, 10),
			PayloadSize:        payloadSize,
		}
	}

	data, err := json.Marshal(tickMsg)
	if err != nil {
		log.Printf("Failed to marshal tick for client %s: %v", client.ID, err)
		return
	}

	select {
	case client.Send <- data:
	default:
		// Client's send channel is full
		h.unregister <- client
	}
}

func (h *Hub) sendErrorToClient(client *Client, errorMsg string) {
	errMsg := ErrorMessage{
		Type:  "error",
		Error: errorMsg,
	}

	data, err := json.Marshal(errMsg)
	if err != nil {
		log.Printf("Failed to marshal error for client %s: %v", client.ID, err)
		return
	}

	select {
	case client.Send <- data:
	default:
		// Client's send channel is full
		h.unregister <- client
	}
}

func (h *Hub) HandleWebSocket(c *gin.Context) {
	// Get client IP for connection tracking
	clientIP := c.ClientIP()
	
	// Check connection limits before upgrading
	canAccept, reason := h.canAcceptConnection(clientIP)
	if !canAccept {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": reason})
		log.Printf("Rejected WebSocket connection from %s: %s", clientIP, reason)
		return
	}
	
	startTickStr := c.DefaultQuery("start_tick", "0")
	startTick, err := strconv.ParseUint(startTickStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start_tick parameter"})
		return
	}

	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	client := &Client{
		ID:        generateClientID(),
		Conn:      conn,
		Hub:       h,
		Send:      make(chan []byte, 256),
		StartTick: startTick,
		ClientIP:  clientIP,
	}

	// Register the client with IP tracking
	h.registerClient(client, clientIP)
	log.Printf("WebSocket client %s connected from %s, starting from tick %d", client.ID, clientIP, client.StartTick)
	
	// Start streaming ticks for this client
	go h.streamTicksToClient(client)

	// Start goroutines to handle reading and writing
	go client.writePump()
	
	// When readPump returns, unregister the client
	go func() {
		client.readPump()
		h.unregisterClient(client, clientIP)
	}()
}

func (c *Client) readPump() {
	defer func() {
		c.Hub.unregister <- c
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(512)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued messages to the current message
			n := len(c.Send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.Send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Hub) GetMetrics() HubMetrics {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.metrics
}

func (h *Hub) Shutdown() {
	if h.grpcConn != nil {
		h.grpcConn.Close()
	}
	
	h.mu.Lock()
	for client := range h.clients {
		close(client.Send)
		client.Conn.Close()
	}
	h.clients = make(map[*Client]bool)
	h.mu.Unlock()
}

func generateClientID() string {
	return "client_" + strconv.FormatInt(time.Now().UnixNano(), 10)
}