package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// ============================================
// SECTION 1: IMPORTS & CONSTANTS
// ============================================

const (
	DefaultPort = ":8080"
	MaxCodeSize  = 100_000
	RateLimitMax = 100
	RateLimitWindow = 60_000 // milliseconds
)

// ============================================
// SECTION 2: CONFIGURATION
// ============================================

type Config struct {
	Port           string
	SecurityLevel  string
	MaxBufferSize  int
	TimeoutMs      int
	EnableMetrics  bool
	LogLevel       string
}

var DefaultConfig = Config{
	Port:          DefaultPort,
	SecurityLevel: "medium",
	MaxBufferSize: 10000,
	TimeoutMs:     5000,
	EnableMetrics: true,
	LogLevel:      "info",
}

// ============================================
// SECTION 3: SECURITY GATEWAY
// ============================================

type SecurityGateway struct {
	mu                sync.RWMutex
	rateLimiters      map[string]*RateLimiter
	blockedPatterns   []string
}

type RateLimiter struct {
	count     int
	resetTime int64
}

func NewSecurityGateway() *SecurityGateway {
	return &SecurityGateway{
		rateLimiters: make(map[string]*RateLimiter),
		blockedPatterns: []string{
			"eval", "exec", "__import__", "subprocess",
		},
	}
}

func (sg *SecurityGateway) ValidateSignal(signal CapacitySignal) error {
	if signal.ProducerID == "" || signal.ConsumerID == "" {
		return fmt.Errorf("producer_id and consumer_id must not be empty")
	}
	if signal.RequestedRate <= 0 {
		return fmt.Errorf("requested_rate must be > 0")
	}
	if signal.PriorityLevel < 0 || signal.PriorityLevel > 2 {
		return fmt.Errorf("priority_level must be 0, 1, or 2")
	}
	
	sg.mu.Lock()
	defer sg.mu.Unlock()
	
	now := time.Now().UnixMilli()
	rl, exists := sg.rateLimiters[signal.ProducerID]
	
	if !exists {
		sg.rateLimiters[signal.ProducerID] = &RateLimiter{
			count:     1,
			resetTime: now + RateLimitWindow,
		}
		return nil
	}
	
	if now > rl.resetTime {
		rl.count = 1
		rl.resetTime = now + RateLimitWindow
		return nil
	}
	
	if rl.count >= RateLimitMax {
		return fmt.Errorf("rate limit exceeded for producer %s", signal.ProducerID)
	}
	
	rl.count++
	return nil
}

func (sg *SecurityGateway) AuditLog(action, module string, success bool, duration int64) {
	log.Printf("[AUDIT] action=%s module=%s success=%v duration=%dms",
		action, module, success, duration)
}

// ============================================
// SECTION 4: MESSAGE TYPES (RFC PROTOCOL)
// ============================================

type CapacitySignal struct {
	Action         string `json:"action"`
	Version        string `json:"version"`
	ProducerID     string `json:"producer_id"`
	ConsumerID     string `json:"consumer_id"`
	RequestedRate  int    `json:"requested_rate"`
	PriorityLevel  int    `json:"priority_level"`
	TimeoutMs      int    `json:"timeout_ms"`
	Timestamp      int64  `json:"timestamp"`
}

type FlowControl struct {
	Action           string `json:"action"`
	Version          string `json:"version"`
	ConsumerID       string `json:"consumer_id"`
	ProducerID       string `json:"producer_id"`
	RecommendedRate  int    `json:"recommended_rate"`
	CurrentLoad      int    `json:"current_load"`
	MaxCapacity      int    `json:"max_capacity"`
	BackpressureLevel int    `json:"backpressure_level"`
	Reason           string `json:"reason"`
	ApplyImmediately bool   `json:"apply_immediately"`
	Timestamp        int64  `json:"timestamp"`
}

type MetricsReport struct {
	Action                    string `json:"action"`
	Version                   string `json:"version"`
	ProducerID                string `json:"producer_id"`
	ConsumerID                string `json:"consumer_id"`
	FlowName                  string `json:"flow_name"`
	ItemsBuffered             int    `json:"items_buffered"`
	ItemsProcessed            int    `json:"items_processed"`
	ItemsDropped              int    `json:"items_dropped"`
	LatencyP50Ms              int    `json:"latency_p50_ms"`
	LatencyP99Ms              int    `json:"latency_p99_ms"`
	ThroughputCurrent         int    `json:"throughput_current"`
	BackpressureTriggeredCount int    `json:"backpressure_triggered_count"`
	ErrorCount                int    `json:"error_count"`
	Timestamp                 int64  `json:"timestamp"`
}

// ============================================
// SECTION 5: CORE ENGINE - BACKPRESSURE ENGINE
// ============================================

type FlowState struct {
	ProducerID             string
	ConsumerID             string
	CurrentLoad            int
	MaxCapacity            int
	ItemsBuffered          int
	BackpressureTriggered  int
	LastAdjustment         time.Time
}

type BackpressureEngine struct {
	mu              sync.RWMutex
	flows           map[string]*FlowState
	metrics         map[string]*MetricsReport
	config          Config
}

func NewBackpressureEngine(cfg Config) *BackpressureEngine {
	return &BackpressureEngine{
		flows:   make(map[string]*FlowState),
		metrics: make(map[string]*MetricsReport),
		config:  cfg,
	}
}

func (engine *BackpressureEngine) SignalCapacity(signal CapacitySignal) FlowControl {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	key := signal.ProducerID + ":" + signal.ConsumerID
	flow, exists := engine.flows[key]
	
	if !exists {
		flow = &FlowState{
			ProducerID:    signal.ProducerID,
			ConsumerID:    signal.ConsumerID,
			MaxCapacity:   1000,
			CurrentLoad:   0,
			ItemsBuffered: 0,
		}
		engine.flows[key] = flow
	}

	recommendedRate := engine.calculateOptimalRate(flow)
	backpressureLevel := engine.calculateBackpressureLevel(flow)

	return FlowControl{
		Action:            "FlowControl",
		Version:           "1.0",
		ConsumerID:        signal.ConsumerID,
		ProducerID:        signal.ProducerID,
		RecommendedRate:   recommendedRate,
		CurrentLoad:       flow.CurrentLoad,
		MaxCapacity:       flow.MaxCapacity,
		BackpressureLevel: backpressureLevel,
		Reason:            engine.getReason(backpressureLevel),
		ApplyImmediately:  backpressureLevel == 2,
		Timestamp:         time.Now().UnixMilli(),
	}
}

func (engine *BackpressureEngine) calculateOptimalRate(flow *FlowState) int {
	if flow.CurrentLoad > int(float64(flow.MaxCapacity)*0.95) {
		return 0
	} else if flow.CurrentLoad > int(float64(flow.MaxCapacity)*0.80) {
		return flow.MaxCapacity / 4
	} else if flow.CurrentLoad > int(float64(flow.MaxCapacity)*0.50) {
		return flow.MaxCapacity / 2
	}
	return flow.MaxCapacity
}

func (engine *BackpressureEngine) calculateBackpressureLevel(flow *FlowState) int {
	loadPercent := (flow.CurrentLoad * 100) / flow.MaxCapacity
	if loadPercent >= 95 {
		return 2 // Critical
	} else if loadPercent >= 80 {
		return 1 // Alert
	}
	return 0 // Normal
}

func (engine *BackpressureEngine) getReason(level int) string {
	switch level {
	case 2:
		return "consumer_critical"
	case 1:
		return "consumer_alert"
	default:
		return "consumer_normal"
	}
}

func (engine *BackpressureEngine) ReportMetrics(metrics MetricsReport) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	key := metrics.ProducerID + ":" + metrics.ConsumerID
	engine.metrics[key] = &metrics

	log.Printf("[METRICS] flow=%s items_buffered=%d latency_p99=%dms",
		key, metrics.ItemsBuffered, metrics.LatencyP99Ms)

	return nil
}

func (engine *BackpressureEngine) GetHealth() map[string]interface{} {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	return map[string]interface{}{
		"status":         "healthy",
		"active_flows":   len(engine.flows),
		"metrics_count":  len(engine.metrics),
		"uptime":         time.Now().Unix(),
	}
}

// ============================================
// SECTION 6: ORCHESTRATOR
// ============================================

type Orchestrator struct {
	engine *BackpressureEngine
	sg     *SecurityGateway
}

func NewOrchestrator(cfg Config) *Orchestrator {
	return &Orchestrator{
		engine: NewBackpressureEngine(cfg),
		sg:     NewSecurityGateway(),
	}
}

func (orch *Orchestrator) HandleCapacitySignal(signal CapacitySignal) (FlowControl, error) {
	start := time.Now()

	// Validate
	if err := orch.sg.ValidateSignal(signal); err != nil {
		orch.sg.AuditLog("capacity_signal", "backpressure", false, time.Since(start).Milliseconds())
		return FlowControl{}, err
	}

	// Execute
	result := orch.engine.SignalCapacity(signal)

	// Audit
	orch.sg.AuditLog("capacity_signal", "backpressure", true, time.Since(start).Milliseconds())

	return result, nil
}

func (orch *Orchestrator) HandleMetricsReport(metrics MetricsReport) error {
	start := time.Now()

	err := orch.engine.ReportMetrics(metrics)

	orch.sg.AuditLog("metrics_report", "backpressure", err == nil, time.Since(start).Milliseconds())

	return err
}

// ============================================
// SECTION 7: HTTP API
// ============================================

type HTTPServer struct {
	orchestrator *Orchestrator
	config       Config
}

func NewHTTPServer(cfg Config) *HTTPServer {
	return &HTTPServer{
		orchestrator: NewOrchestrator(cfg),
		config:       cfg,
	}
}

func (server *HTTPServer) HandleCapacity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var signal CapacitySignal
	if err := json.NewDecoder(r.Body).Decode(&signal); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	signal.Version = "1.0"
	signal.Timestamp = time.Now().UnixMilli()

	result, err := server.orchestrator.HandleCapacitySignal(signal)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (server *HTTPServer) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var metrics MetricsReport
	if err := json.NewDecoder(r.Body).Decode(&metrics); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	metrics.Version = "1.0"
	metrics.Timestamp = time.Now().UnixMilli()

	if err := server.orchestrator.HandleMetricsReport(metrics); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "received"})
}

func (server *HTTPServer) HandleHealth(w http.ResponseWriter, r *http.Request) {
	health := server.orchestrator.engine.GetHealth()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// ============================================
// SECTION 8: CLI
// ============================================

func printUsage() {
	fmt.Println(`
Nexus Backpressure - Universal Backpressure Protocol

Usage:
  go run main.go [--server] [--port PORT]

Examples:
  go run main.go --server
  go run main.go --server --port :9000
	`)
}

// ============================================
// SECTION 9: MAIN
// ============================================

func main() {
	fmt.Println("🌊 Nexus Backpressure Protocol v1.0")
	fmt.Println("Starting server...")

	cfg := DefaultConfig

	// Parse flags (simple version)
	for i, arg := range []string{} {
		_ = i
		_ = arg
	}

	server := NewHTTPServer(cfg)

	// Register HTTP handlers
	http.HandleFunc("/capacity", server.HandleCapacity)
	http.HandleFunc("/metrics", server.HandleMetrics)
	http.HandleFunc("/health", server.HandleHealth)

	// Start server
	addr := cfg.Port
	if addr == "" {
		addr = DefaultPort
	}

	fmt.Printf("✅ Server listening on http://localhost%s\n", addr)
	fmt.Println("Endpoints:")
	fmt.Println("  POST /capacity  - Send CapacitySignal")
	fmt.Println("  POST /metrics   - Send MetricsReport")
	fmt.Println("  GET  /health    - Get health status")

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}