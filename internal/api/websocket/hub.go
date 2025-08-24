package websocket

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/zerooo111/tick-streamer/proto"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins for now - should be configurable in production
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type Client struct {
	ID        string
	Conn      *websocket.Conn
	Hub       *Hub
	Send      chan []byte
	StartTick uint64
	mu        sync.Mutex
}

type Hub struct {
	clients     map[*Client]bool
	register    chan *Client
	unregister  chan *Client
	broadcast   chan []byte
	grpcAddr    string
	grpcClient  pb.SequencerServiceClient
	grpcConn    *grpc.ClientConn
	mu          sync.RWMutex
	metrics     HubMetrics
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

func NewHub(grpcAddr string) (*Hub, error) {
	// Create gRPC connection
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	hub := &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte, 256),
		grpcAddr:   grpcAddr,
		grpcClient: pb.NewSequencerServiceClient(conn),
		grpcConn:   conn,
	}

	go hub.run()
	return hub, nil
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
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
				h.metrics.ActiveConnections--
				h.metrics.DroppedConnections++
			}
			h.mu.Unlock()
			
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
	startTickStr := c.DefaultQuery("start_tick", "0")
	startTick, err := strconv.ParseUint(startTickStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start_tick parameter"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
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
	}

	// Register the client
	h.register <- client

	// Start goroutines to handle reading and writing
	go client.writePump()
	go client.readPump()
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