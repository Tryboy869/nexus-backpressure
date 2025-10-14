/**
 * NEXUS BACKPRESSURE - Rust Implementation
 * Architecture: 9-section mono-file, identical to Go/Python/Node
 */

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================
// SECTION 1: IMPORTS & CONSTANTS
// ============================================

const DEFAULT_PORT: u16 = 8080;
const RATE_LIMIT_MAX: u32 = 100;
const RATE_LIMIT_WINDOW: u64 = 60_000; // milliseconds
const DEFAULT_MAX_CAPACITY: u32 = 1000;

// ============================================
// SECTION 2: CONFIGURATION
// ============================================

#[derive(Clone, Debug)]
pub struct Config {
    pub port: u16,
    pub host: String,
    pub security_level: String,
    pub max_buffer_size: u32,
    pub timeout_ms: u32,
    pub enable_metrics: bool,
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: DEFAULT_PORT,
            host: "0.0.0.0".to_string(),
            security_level: "medium".to_string(),
            max_buffer_size: 10_000,
            timeout_ms: 5_000,
            enable_metrics: true,
            log_level: "info".to_string(),
        }
    }
}

// ============================================
// SECTION 3: SECURITY GATEWAY
// ============================================

#[derive(Clone)]
pub struct RateLimiter {
    count: u32,
    reset_time: u64,
}

pub struct SecurityGateway {
    rate_limiters: Arc<Mutex<HashMap<String, RateLimiter>>>,
    blocked_patterns: Vec<String>,
}

impl SecurityGateway {
    fn new() -> Self {
        SecurityGateway {
            rate_limiters: Arc::new(Mutex::new(HashMap::new())),
            blocked_patterns: vec![
                "eval".to_string(),
                "exec".to_string(),
                "__import__".to_string(),
                "subprocess".to_string(),
            ],
        }
    }

    fn validate_signal(&self, signal: &CapacitySignal) -> Result<(), String> {
        if signal.producer_id.is_empty() || signal.consumer_id.is_empty() {
            return Err("producer_id and consumer_id required".to_string());
        }

        if signal.requested_rate == 0 {
            return Err("requested_rate must be > 0".to_string());
        }

        if signal.priority_level > 2 {
            return Err("priority_level must be 0, 1, or 2".to_string());
        }

        // Rate limiting check
        if !self.check_rate_limit(&signal.producer_id) {
            return Err("rate limit exceeded".to_string());
        }

        Ok(())
    }

    fn check_rate_limit(&self, producer_id: &str) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut limiters = self.rate_limiters.lock().unwrap();

        if !limiters.contains_key(producer_id) {
            limiters.insert(
                producer_id.to_string(),
                RateLimiter {
                    count: 1,
                    reset_time: now + RATE_LIMIT_WINDOW,
                },
            );
            return true;
        }

        let limiter = limiters.get_mut(producer_id).unwrap();

        if now > limiter.reset_time {
            limiter.count = 1;
            limiter.reset_time = now + RATE_LIMIT_WINDOW;
            return true;
        }

        if limiter.count >= RATE_LIMIT_MAX {
            return false;
        }

        limiter.count += 1;
        true
    }

    fn audit_log(&self, action: &str, module: &str, success: bool, duration_ms: u128) {
        let level = if success { "INFO" } else { "WARN" };
        println!(
            "[AUDIT] action={} module={} success={} duration={}ms",
            action, module, success, duration_ms
        );
    }
}

// ============================================
// SECTION 4: MESSAGE TYPES (RFC PROTOCOL)
// ============================================

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CapacitySignal {
    pub action: String,
    pub version: String,
    pub producer_id: String,
    pub consumer_id: String,
    pub requested_rate: u32,
    pub priority_level: u32,
    pub timeout_ms: u32,
    pub timestamp: u64,
}

impl Default for CapacitySignal {
    fn default() -> Self {
        CapacitySignal {
            action: "CapacitySignal".to_string(),
            version: "1.0".to_string(),
            producer_id: String::new(),
            consumer_id: String::new(),
            requested_rate: 0,
            priority_level: 0,
            timeout_ms: 5_000,
            timestamp: current_timestamp_ms(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FlowControl {
    pub action: String,
    pub version: String,
    pub consumer_id: String,
    pub producer_id: String,
    pub recommended_rate: u32,
    pub current_load: u32,
    pub max_capacity: u32,
    pub backpressure_level: u32,
    pub reason: String,
    pub apply_immediately: bool,
    pub timestamp: u64,
}

impl Default for FlowControl {
    fn default() -> Self {
        FlowControl {
            action: "FlowControl".to_string(),
            version: "1.0".to_string(),
            consumer_id: String::new(),
            producer_id: String::new(),
            recommended_rate: 0,
            current_load: 0,
            max_capacity: 0,
            backpressure_level: 0,
            reason: String::new(),
            apply_immediately: false,
            timestamp: current_timestamp_ms(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetricsReport {
    pub action: String,
    pub version: String,
    pub producer_id: String,
    pub consumer_id: String,
    pub flow_name: String,
    pub items_buffered: u32,
    pub items_processed: u32,
    pub items_dropped: u32,
    pub latency_p50_ms: u32,
    pub latency_p99_ms: u32,
    pub throughput_current: u32,
    pub backpressure_triggered_count: u32,
    pub error_count: u32,
    pub timestamp: u64,
}

impl Default for MetricsReport {
    fn default() -> Self {
        MetricsReport {
            action: "MetricsReport".to_string(),
            version: "1.0".to_string(),
            producer_id: String::new(),
            consumer_id: String::new(),
            flow_name: String::new(),
            items_buffered: 0,
            items_processed: 0,
            items_dropped: 0,
            latency_p50_ms: 0,
            latency_p99_ms: 0,
            throughput_current: 0,
            backpressure_triggered_count: 0,
            error_count: 0,
            timestamp: current_timestamp_ms(),
        }
    }
}

// ============================================
// SECTION 5: CORE ENGINE - BACKPRESSURE
// ============================================

#[derive(Clone)]
pub struct FlowState {
    pub producer_id: String,
    pub consumer_id: String,
    pub current_load: u32,
    pub max_capacity: u32,
    pub items_buffered: u32,
    pub backpressure_triggered: u32,
}

pub struct BackpressureEngine {
    flows: Arc<Mutex<HashMap<String, FlowState>>>,
    metrics: Arc<Mutex<HashMap<String, MetricsReport>>>,
}

impl BackpressureEngine {
    fn new() -> Self {
        BackpressureEngine {
            flows: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn signal_capacity(&self, signal: &CapacitySignal) -> FlowControl {
        let key = format!("{}:{}", signal.producer_id, signal.consumer_id);

        let mut flows = self.flows.lock().unwrap();

        if !flows.contains_key(&key) {
            flows.insert(
                key.clone(),
                FlowState {
                    producer_id: signal.producer_id.clone(),
                    consumer_id: signal.consumer_id.clone(),
                    current_load: 0,
                    max_capacity: DEFAULT_MAX_CAPACITY,
                    items_buffered: 0,
                    backpressure_triggered: 0,
                },
            );
        }

        let flow = flows.get(&key).unwrap();
        let recommended_rate = self.calculate_optimal_rate(flow);
        let backpressure_level = self.calculate_backpressure_level(flow);

        FlowControl {
            consumer_id: signal.consumer_id.clone(),
            producer_id: signal.producer_id.clone(),
            recommended_rate,
            current_load: flow.current_load,
            max_capacity: flow.max_capacity,
            backpressure_level,
            reason: self.get_reason(backpressure_level),
            apply_immediately: backpressure_level == 2,
            timestamp: current_timestamp_ms(),
            ..Default::default()
        }
    }

    fn calculate_optimal_rate(&self, flow: &FlowState) -> u32 {
        if flow.current_load > (flow.max_capacity * 95) / 100 {
            0
        } else if flow.current_load > (flow.max_capacity * 80) / 100 {
            flow.max_capacity / 4
        } else if flow.current_load > (flow.max_capacity * 50) / 100 {
            flow.max_capacity / 2
        } else {
            flow.max_capacity
        }
    }

    fn calculate_backpressure_level(&self, flow: &FlowState) -> u32 {
        let load_percent = (flow.current_load * 100) / flow.max_capacity;

        if load_percent >= 95 {
            2 // Critical
        } else if load_percent >= 80 {
            1 // Alert
        } else {
            0 // Normal
        }
    }

    fn get_reason(&self, level: u32) -> String {
        match level {
            0 => "consumer_normal".to_string(),
            1 => "consumer_alert".to_string(),
            2 => "consumer_critical".to_string(),
            _ => "unknown".to_string(),
        }
    }

    fn report_metrics(&self, metrics: &MetricsReport) {
        let key = format!("{}:{}", metrics.producer_id, metrics.consumer_id);
        let mut m = self.metrics.lock().unwrap();
        m.insert(key.clone(), metrics.clone());

        println!(
            "[METRICS] flow={} items_buffered={} latency_p99={}ms",
            key, metrics.items_buffered, metrics.latency_p99_ms
        );
    }

    fn get_health(&self) -> serde_json::Value {
        let flows = self.flows.lock().unwrap();
        let metrics = self.metrics.lock().unwrap();

        serde_json::json!({
            "status": "healthy",
            "active_flows": flows.len(),
            "metrics_count": metrics.len(),
            "uptime": uptime_seconds()
        })
    }
}

// ============================================
// SECTION 6: ORCHESTRATOR
// ============================================

pub struct Orchestrator {
    engine: BackpressureEngine,
    sg: SecurityGateway,
}

impl Orchestrator {
    fn new() -> Self {
        Orchestrator {
            engine: BackpressureEngine::new(),
            sg: SecurityGateway::new(),
        }
    }

    fn handle_capacity_signal(&self, signal: &CapacitySignal) -> Result<FlowControl, String> {
        let start = std::time::Instant::now();

        // Validate
        self.sg.validate_signal(signal)?;

        // Execute
        let result = self.engine.signal_capacity(signal);

        // Audit
        let duration = start.elapsed().as_millis();
        self.sg.audit_log("capacity_signal", "backpressure", true, duration);

        Ok(result)
    }

    fn handle_metrics_report(&self, metrics: &MetricsReport) -> Result<(), String> {
        let start = std::time::Instant::now();

        self.engine.report_metrics(metrics);

        let duration = start.elapsed().as_millis();
        self.sg.audit_log("metrics_report", "backpressure", true, duration);

        Ok(())
    }
}

// ============================================
// SECTION 7: HTTP API (Placeholder)
// ============================================

// For production, use actix-web or similar framework
// This is a simple CLI demonstration

// ============================================
// SECTION 8: CLI
// ============================================

fn print_usage() {
    println!(
        r#"
Nexus Backpressure - Universal Backpressure Protocol

Usage:
  cargo run [--server] [--port PORT]

Examples:
  cargo run -- --server
  cargo run -- --server --port 9000
    "#
    );
}

// ============================================
// SECTION 9: MAIN & UTILITIES
// ============================================

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn uptime_seconds() -> u64 {
    std::process::id() as u64 // Simplified, use proper uptime tracking
}

fn main() {
    println!("🌊 Nexus Backpressure Protocol v1.0");

    let args: Vec<String> = std::env::args().collect();
    let is_server = args.contains(&"--server".to_string());

    if is_server {
        println!("Note: This Rust version is a CLI demo.");
        println!("For HTTP server, add actix-web to Cargo.toml:");
        println!("  actix-web = \"4\"");
        println!("");

        let orchestrator = Orchestrator::new();

        // Demo: Simulate capacity signal
        let signal = CapacitySignal {
            producer_id: "producer-1".to_string(),
            consumer_id: "consumer-1".to_string(),
            requested_rate: 1000,
            ..Default::default()
        };

        match orchestrator.handle_capacity_signal(&signal) {
            Ok(flow_control) => {
                println!("\n✅ Capacity Signal Response:");
                println!("{}", serde_json::to_string_pretty(&flow_control).unwrap());
            }
            Err(e) => println!("❌ Error: {}", e),
        }

        // Demo: Report metrics
        let metrics = MetricsReport {
            producer_id: "producer-1".to_string(),
            consumer_id: "consumer-1".to_string(),
            items_buffered: 450,
            latency_p99_ms: 250,
            throughput_current: 95,
            ..Default::default()
        };

        match orchestrator.handle_metrics_report(&metrics) {
            Ok(_) => println!("✅ Metrics reported"),
            Err(e) => println!("❌ Error: {}", e),
        }

        // Print health
        println!("\nHealth Status:");
        println!("{}", serde_json::to_string_pretty(&orchestrator.engine.get_health()).unwrap());
    } else {
        print_usage();
    }
}