#!/usr/bin/env python3
"""
NEXUS BACKPRESSURE - Python Implementation
Architecture: 9-section mono-file, identical to Go/Node/Rust
"""

import json
import time
import logging
from typing import Dict, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict
from threading import Lock
from http.server import HTTPServer, BaseHTTPRequestHandler
import argparse

# ============================================
# SECTION 1: IMPORTS & SETUP
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# SECTION 2: CONFIGURATION
# ============================================

@dataclass
class Config:
    port: int = 8080
    host: str = "0.0.0.0"
    security_level: str = "medium"
    max_buffer_size: int = 10000
    timeout_ms: int = 5000
    enable_metrics: bool = True
    log_level: str = "info"

DEFAULT_CONFIG = Config()

# ============================================
# SECTION 3: SECURITY GATEWAY
# ============================================

class SecurityGateway:
    """Centralized security validation and audit logging"""
    
    RATE_LIMIT_MAX = 100
    RATE_LIMIT_WINDOW = 60000  # milliseconds
    
    def __init__(self):
        self._rate_limiters: Dict[str, Dict] = {}
        self._lock = Lock()
        self._blocked_patterns = [
            "eval", "exec", "__import__", "subprocess", "os.system"
        ]
    
    def validate_signal(self, signal: Dict) -> Tuple[bool, Optional[str]]:
        """Validate CapacitySignal"""
        if not signal.get("producer_id") or not signal.get("consumer_id"):
            return False, "producer_id and consumer_id required"
        
        if signal.get("requested_rate", 0) <= 0:
            return False, "requested_rate must be > 0"
        
        if signal.get("priority_level", 0) not in (0, 1, 2):
            return False, "priority_level must be 0, 1, or 2"
        
        # Rate limiting check
        if not self._check_rate_limit(signal.get("producer_id")):
            return False, "rate limit exceeded"
        
        return True, None
    
    def _check_rate_limit(self, producer_id: str) -> bool:
        """Check rate limit for producer"""
        with self._lock:
            now = int(time.time() * 1000)
            
            if producer_id not in self._rate_limiters:
                self._rate_limiters[producer_id] = {
                    "count": 1,
                    "reset_time": now + self.RATE_LIMIT_WINDOW
                }
                return True
            
            rl = self._rate_limiters[producer_id]
            
            if now > rl["reset_time"]:
                rl["count"] = 1
                rl["reset_time"] = now + self.RATE_LIMIT_WINDOW
                return True
            
            if rl["count"] >= self.RATE_LIMIT_MAX:
                return False
            
            rl["count"] += 1
            return True
    
    def audit_log(self, action: str, module: str, success: bool, duration_ms: int):
        """Log audit trail"""
        level = logging.INFO if success else logging.WARNING
        logger.log(
            level,
            f"[AUDIT] action={action} module={module} success={success} duration={duration_ms}ms"
        )

# ============================================
# SECTION 4: MESSAGE TYPES (RFC PROTOCOL)
# ============================================

@dataclass
class CapacitySignal:
    action: str = "CapacitySignal"
    version: str = "1.0"
    producer_id: str = ""
    consumer_id: str = ""
    requested_rate: int = 0
    priority_level: int = 0
    timeout_ms: int = 5000
    timestamp: int = 0
    
    def to_dict(self):
        return asdict(self)

@dataclass
class FlowControl:
    action: str = "FlowControl"
    version: str = "1.0"
    consumer_id: str = ""
    producer_id: str = ""
    recommended_rate: int = 0
    current_load: int = 0
    max_capacity: int = 0
    backpressure_level: int = 0
    reason: str = ""
    apply_immediately: bool = False
    timestamp: int = 0
    
    def to_dict(self):
        return asdict(self)

@dataclass
class MetricsReport:
    action: str = "MetricsReport"
    version: str = "1.0"
    producer_id: str = ""
    consumer_id: str = ""
    flow_name: str = ""
    items_buffered: int = 0
    items_processed: int = 0
    items_dropped: int = 0
    latency_p50_ms: int = 0
    latency_p99_ms: int = 0
    throughput_current: int = 0
    backpressure_triggered_count: int = 0
    error_count: int = 0
    timestamp: int = 0
    
    def to_dict(self):
        return asdict(self)

# ============================================
# SECTION 5: CORE ENGINE - BACKPRESSURE
# ============================================

@dataclass
class FlowState:
    producer_id: str
    consumer_id: str
    current_load: int = 0
    max_capacity: int = 1000
    items_buffered: int = 0
    backpressure_triggered: int = 0
    last_adjustment: float = 0.0

class BackpressureEngine:
    """Core backpressure coordination engine"""
    
    def __init__(self, config: Config):
        self._flows: Dict[str, FlowState] = {}
        self._metrics: Dict[str, MetricsReport] = {}
        self._config = config
        self._lock = Lock()
    
    def signal_capacity(self, signal: CapacitySignal) -> FlowControl:
        """Process capacity signal and return flow control"""
        with self._lock:
            key = f"{signal.producer_id}:{signal.consumer_id}"
            
            if key not in self._flows:
                self._flows[key] = FlowState(
                    producer_id=signal.producer_id,
                    consumer_id=signal.consumer_id
                )
            
            flow = self._flows[key]
            recommended_rate = self._calculate_optimal_rate(flow)
            backpressure_level = self._calculate_backpressure_level(flow)
            
            return FlowControl(
                consumer_id=signal.consumer_id,
                producer_id=signal.producer_id,
                recommended_rate=recommended_rate,
                current_load=flow.current_load,
                max_capacity=flow.max_capacity,
                backpressure_level=backpressure_level,
                reason=self._get_reason(backpressure_level),
                apply_immediately=(backpressure_level == 2),
                timestamp=int(time.time() * 1000)
            )
    
    def _calculate_optimal_rate(self, flow: FlowState) -> int:
        """Calculate optimal rate based on load"""
        if flow.current_load > int(flow.max_capacity * 0.95):
            return 0
        elif flow.current_load > int(flow.max_capacity * 0.80):
            return flow.max_capacity // 4
        elif flow.current_load > int(flow.max_capacity * 0.50):
            return flow.max_capacity // 2
        return flow.max_capacity
    
    def _calculate_backpressure_level(self, flow: FlowState) -> int:
        """Calculate backpressure level (0=normal, 1=alert, 2=critical)"""
        load_percent = (flow.current_load * 100) // flow.max_capacity
        
        if load_percent >= 95:
            return 2
        elif load_percent >= 80:
            return 1
        return 0
    
    def _get_reason(self, level: int) -> str:
        """Get reason for backpressure level"""
        reasons = {
            0: "consumer_normal",
            1: "consumer_alert",
            2: "consumer_critical"
        }
        return reasons.get(level, "unknown")
    
    def report_metrics(self, metrics: MetricsReport) -> None:
        """Report metrics from producer/consumer"""
        with self._lock:
            key = f"{metrics.producer_id}:{metrics.consumer_id}"
            self._metrics[key] = metrics
            logger.info(
                f"[METRICS] flow={key} items_buffered={metrics.items_buffered} "
                f"latency_p99={metrics.latency_p99_ms}ms"
            )
    
    def get_health(self) -> Dict:
        """Get engine health status"""
        with self._lock:
            return {
                "status": "healthy",
                "active_flows": len(self._flows),
                "metrics_count": len(self._metrics),
                "uptime": int(time.time())
            }

# ============================================
# SECTION 6: ORCHESTRATOR
# ============================================

class Orchestrator:
    """Coordinates security, engine, and logging"""
    
    def __init__(self, config: Config):
        self.engine = BackpressureEngine(config)
        self.sg = SecurityGateway()
    
    def handle_capacity_signal(self, signal_dict: Dict) -> Tuple[Optional[Dict], Optional[str]]:
        """Handle incoming capacity signal"""
        start = time.time()
        
        # Create signal object
        signal = CapacitySignal(
            producer_id=signal_dict.get("producer_id", ""),
            consumer_id=signal_dict.get("consumer_id", ""),
            requested_rate=signal_dict.get("requested_rate", 0),
            priority_level=signal_dict.get("priority_level", 0),
            timeout_ms=signal_dict.get("timeout_ms", 5000),
            timestamp=int(time.time() * 1000)
        )
        
        # Validate
        valid, error = self.sg.validate_signal(signal.to_dict())
        if not valid:
            duration = int((time.time() - start) * 1000)
            self.sg.audit_log("capacity_signal", "backpressure", False, duration)
            return None, error
        
        # Execute
        flow_control = self.engine.signal_capacity(signal)
        
        # Audit
        duration = int((time.time() - start) * 1000)
        self.sg.audit_log("capacity_signal", "backpressure", True, duration)
        
        return flow_control.to_dict(), None
    
    def handle_metrics_report(self, metrics_dict: Dict) -> Tuple[bool, Optional[str]]:
        """Handle incoming metrics report"""
        start = time.time()
        
        metrics = MetricsReport(
            producer_id=metrics_dict.get("producer_id", ""),
            consumer_id=metrics_dict.get("consumer_id", ""),
            flow_name=metrics_dict.get("flow_name", ""),
            items_buffered=metrics_dict.get("items_buffered", 0),
            items_processed=metrics_dict.get("items_processed", 0),
            items_dropped=metrics_dict.get("items_dropped", 0),
            latency_p50_ms=metrics_dict.get("latency_p50_ms", 0),
            latency_p99_ms=metrics_dict.get("latency_p99_ms", 0),
            throughput_current=metrics_dict.get("throughput_current", 0),
            backpressure_triggered_count=metrics_dict.get("backpressure_triggered_count", 0),
            error_count=metrics_dict.get("error_count", 0),
            timestamp=int(time.time() * 1000)
        )
        
        self.engine.report_metrics(metrics)
        
        duration = int((time.time() - start) * 1000)
        self.sg.audit_log("metrics_report", "backpressure", True, duration)
        
        return True, None

# ============================================
# SECTION 7: HTTP API HANDLER
# ============================================

class RequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Nexus protocol"""
    
    def do_POST(self):
        """Handle POST requests"""
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')
        
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return
        
        if self.path == "/capacity":
            self._handle_capacity(data)
        elif self.path == "/metrics":
            self._handle_metrics(data)
        else:
            self.send_error(404, "Not Found")
    
    def do_GET(self):
        """Handle GET requests"""
        if self.path == "/health":
            self._handle_health()
        else:
            self.send_error(404, "Not Found")
    
    def _handle_capacity(self, data: Dict):
        """Handle capacity signal"""
        result, error = self.orchestrator.handle_capacity_signal(data)
        
        if error:
            self.send_response(400)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": error}).encode())
        else:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())
    
    def _handle_metrics(self, data: Dict):
        """Handle metrics report"""
        success, error = self.orchestrator.handle_metrics_report(data)
        
        self.send_response(200 if success else 400)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
        if success:
            self.wfile.write(json.dumps({"status": "received"}).encode())
        else:
            self.wfile.write(json.dumps({"error": error}).encode())
    
    def _handle_health(self):
        """Handle health check"""
        health = self.orchestrator.engine.get_health()
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(health).encode())
    
    def log_message(self, format, *args):
        """Suppress default logging"""
        pass

# ============================================
# SECTION 8: CLI
# ============================================

def print_usage():
    """Print usage information"""
    print("""
Nexus Backpressure - Universal Backpressure Protocol

Usage:
  python app.py [--server] [--port PORT] [--host HOST]

Examples:
  python app.py --server
  python app.py --server --port 9000 --host 0.0.0.0
    """)

# ============================================
# SECTION 9: MAIN
# ============================================

def main():
    """Main entry point"""
    print("🌊 Nexus Backpressure Protocol v1.0")
    
    parser = argparse.ArgumentParser(description="Nexus Backpressure Server")
    parser.add_argument("--server", action="store_true", help="Run as server")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    
    args = parser.parse_args()
    
    if args.server:
        config = Config(port=args.port, host=args.host)
        orchestrator = Orchestrator(config)
        
        # Attach orchestrator to handler
        RequestHandler.orchestrator = orchestrator
        
        server = HTTPServer((args.host, args.port), RequestHandler)
        
        print(f"✅ Server listening on http://{args.host}:{args.port}")
        print("Endpoints:")
        print("  POST /capacity  - Send CapacitySignal")
        print("  POST /metrics   - Send MetricsReport")
        print("  GET  /health    - Get health status")
        print()
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\n✅ Server shutdown")
            server.shutdown()
    else:
        print_usage()

if __name__ == "__main__":
    main()