#!/usr/bin/env node

/**
 * NEXUS BACKPRESSURE - Node.js Implementation
 * Architecture: 9-section mono-file, identical to Go/Python/Rust
 */

import { createServer } from 'http';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// ============================================
// SECTION 1: IMPORTS & SETUP
// ============================================

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================
// SECTION 2: CONFIGURATION
// ============================================

const DEFAULT_CONFIG = {
  port: 8080,
  host: '0.0.0.0',
  securityLevel: 'medium',
  maxBufferSize: 10000,
  timeoutMs: 5000,
  enableMetrics: true,
  logLevel: 'info'
};

// ============================================
// SECTION 3: SECURITY GATEWAY
// ============================================

class SecurityGateway {
  static RATE_LIMIT_MAX = 100;
  static RATE_LIMIT_WINDOW = 60000; // milliseconds

  constructor() {
    this.rateLimiters = new Map();
    this.blockedPatterns = [
      'eval', 'Function', 'require(', 'import(', '__proto__'
    ];
  }

  validateSignal(signal) {
    if (!signal.producer_id || !signal.consumer_id) {
      return { valid: false, error: 'producer_id and consumer_id required' };
    }

    if ((signal.requested_rate || 0) <= 0) {
      return { valid: false, error: 'requested_rate must be > 0' };
    }

    if (![0, 1, 2].includes(signal.priority_level || 0)) {
      return { valid: false, error: 'priority_level must be 0, 1, or 2' };
    }

    // Rate limiting check
    if (!this.checkRateLimit(signal.producer_id)) {
      return { valid: false, error: 'rate limit exceeded' };
    }

    return { valid: true };
  }

  checkRateLimit(producerId) {
    const now = Date.now();

    if (!this.rateLimiters.has(producerId)) {
      this.rateLimiters.set(producerId, {
        count: 1,
        resetTime: now + SecurityGateway.RATE_LIMIT_WINDOW
      });
      return true;
    }

    const rl = this.rateLimiters.get(producerId);

    if (now > rl.resetTime) {
      rl.count = 1;
      rl.resetTime = now + SecurityGateway.RATE_LIMIT_WINDOW;
      return true;
    }

    if (rl.count >= SecurityGateway.RATE_LIMIT_MAX) {
      return false;
    }

    rl.count++;
    return true;
  }

  auditLog(action, module, success, durationMs) {
    const level = success ? 'INFO' : 'WARN';
    console.log(
      `[AUDIT] action=${action} module=${module} success=${success} duration=${durationMs}ms`
    );
  }
}

// ============================================
// SECTION 4: MESSAGE TYPES (RFC PROTOCOL)
// ============================================

class CapacitySignal {
  constructor(data = {}) {
    this.action = 'CapacitySignal';
    this.version = '1.0';
    this.producer_id = data.producer_id || '';
    this.consumer_id = data.consumer_id || '';
    this.requested_rate = data.requested_rate || 0;
    this.priority_level = data.priority_level || 0;
    this.timeout_ms = data.timeout_ms || 5000;
    this.timestamp = data.timestamp || Date.now();
  }
}

class FlowControl {
  constructor(data = {}) {
    this.action = 'FlowControl';
    this.version = '1.0';
    this.consumer_id = data.consumer_id || '';
    this.producer_id = data.producer_id || '';
    this.recommended_rate = data.recommended_rate || 0;
    this.current_load = data.current_load || 0;
    this.max_capacity = data.max_capacity || 0;
    this.backpressure_level = data.backpressure_level || 0;
    this.reason = data.reason || '';
    this.apply_immediately = data.apply_immediately || false;
    this.timestamp = data.timestamp || Date.now();
  }
}

class MetricsReport {
  constructor(data = {}) {
    this.action = 'MetricsReport';
    this.version = '1.0';
    this.producer_id = data.producer_id || '';
    this.consumer_id = data.consumer_id || '';
    this.flow_name = data.flow_name || '';
    this.items_buffered = data.items_buffered || 0;
    this.items_processed = data.items_processed || 0;
    this.items_dropped = data.items_dropped || 0;
    this.latency_p50_ms = data.latency_p50_ms || 0;
    this.latency_p99_ms = data.latency_p99_ms || 0;
    this.throughput_current = data.throughput_current || 0;
    this.backpressure_triggered_count = data.backpressure_triggered_count || 0;
    this.error_count = data.error_count || 0;
    this.timestamp = data.timestamp || Date.now();
  }
}

// ============================================
// SECTION 5: CORE ENGINE - BACKPRESSURE
// ============================================

class FlowState {
  constructor(producerId, consumerId) {
    this.producer_id = producerId;
    this.consumer_id = consumerId;
    this.current_load = 0;
    this.max_capacity = 1000;
    this.items_buffered = 0;
    this.backpressure_triggered = 0;
    this.last_adjustment = Date.now();
  }
}

class BackpressureEngine {
  constructor(config) {
    this.flows = new Map();
    this.metrics = new Map();
    this.config = config;
  }

  signalCapacity(signal) {
    const key = `${signal.producer_id}:${signal.consumer_id}`;

    if (!this.flows.has(key)) {
      this.flows.set(key, new FlowState(signal.producer_id, signal.consumer_id));
    }

    const flow = this.flows.get(key);
    const recommendedRate = this.calculateOptimalRate(flow);
    const backpressureLevel = this.calculateBackpressureLevel(flow);

    return new FlowControl({
      consumer_id: signal.consumer_id,
      producer_id: signal.producer_id,
      recommended_rate: recommendedRate,
      current_load: flow.current_load,
      max_capacity: flow.max_capacity,
      backpressure_level: backpressureLevel,
      reason: this.getReason(backpressureLevel),
      apply_immediately: backpressureLevel === 2,
      timestamp: Date.now()
    });
  }

  calculateOptimalRate(flow) {
    if (flow.current_load > flow.max_capacity * 0.95) {
      return 0;
    } else if (flow.current_load > flow.max_capacity * 0.80) {
      return Math.floor(flow.max_capacity / 4);
    } else if (flow.current_load > flow.max_capacity * 0.50) {
      return Math.floor(flow.max_capacity / 2);
    }
    return flow.max_capacity;
  }

  calculateBackpressureLevel(flow) {
    const loadPercent = Math.floor((flow.current_load * 100) / flow.max_capacity);

    if (loadPercent >= 95) {
      return 2; // Critical
    } else if (loadPercent >= 80) {
      return 1; // Alert
    }
    return 0; // Normal
  }

  getReason(level) {
    const reasons = {
      0: 'consumer_normal',
      1: 'consumer_alert',
      2: 'consumer_critical'
    };
    return reasons[level] || 'unknown';
  }

  reportMetrics(metrics) {
    const key = `${metrics.producer_id}:${metrics.consumer_id}`;
    this.metrics.set(key, metrics);
    console.log(
      `[METRICS] flow=${key} items_buffered=${metrics.items_buffered} latency_p99=${metrics.latency_p99_ms}ms`
    );
  }

  getHealth() {
    return {
      status: 'healthy',
      active_flows: this.flows.size,
      metrics_count: this.metrics.size,
      uptime: process.uptime()
    };
  }
}

// ============================================
// SECTION 6: ORCHESTRATOR
// ============================================

class Orchestrator {
  constructor(config) {
    this.engine = new BackpressureEngine(config);
    this.sg = new SecurityGateway();
  }

  handleCapacitySignal(signalData) {
    const start = Date.now();

    const signal = new CapacitySignal(signalData);

    // Validate
    const validation = this.sg.validateSignal(signal);
    if (!validation.valid) {
      const duration = Date.now() - start;
      this.sg.auditLog('capacity_signal', 'backpressure', false, duration);
      return { error: validation.error };
    }

    // Execute
    const flowControl = this.engine.signalCapacity(signal);

    // Audit
    const duration = Date.now() - start;
    this.sg.auditLog('capacity_signal', 'backpressure', true, duration);

    return flowControl;
  }

  handleMetricsReport(metricsData) {
    const start = Date.now();

    const metrics = new MetricsReport(metricsData);
    this.engine.reportMetrics(metrics);

    const duration = Date.now() - start;
    this.sg.auditLog('metrics_report', 'backpressure', true, duration);

    return { status: 'received' };
  }
}

// ============================================
// SECTION 7: HTTP API
// ============================================

class HTTPServer {
  constructor(config) {
    this.config = config;
    this.orchestrator = new Orchestrator(config);
  }

  start() {
    const server = createServer((req, res) => {
      res.setHeader('Content-Type', 'application/json');

      if (req.method === 'POST' && req.url === '/capacity') {
        this.handleCapacity(req, res);
      } else if (req.method === 'POST' && req.url === '/metrics') {
        this.handleMetrics(req, res);
      } else if (req.method === 'GET' && req.url === '/health') {
        this.handleHealth(req, res);
      } else {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Not Found' }));
      }
    });

    server.listen(this.config.port, this.config.host, () => {
      console.log(
        `✅ Server listening on http://${this.config.host}:${this.config.port}`
      );
      console.log('Endpoints:');
      console.log('  POST /capacity  - Send CapacitySignal');
      console.log('  POST /metrics   - Send MetricsReport');
      console.log('  GET  /health    - Get health status');
      console.log('');
    });

    return server;
  }

  handleCapacity(req, res) {
    let body = '';

    req.on('data', chunk => {
      body += chunk.toString();
    });

    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        const result = this.orchestrator.handleCapacitySignal(data);

        if (result.error) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: result.error }));
        } else {
          res.writeHead(200);
          res.end(JSON.stringify(result));
        }
      } catch (error) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });
  }

  handleMetrics(req, res) {
    let body = '';

    req.on('data', chunk => {
      body += chunk.toString();
    });

    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        const result = this.orchestrator.handleMetricsReport(data);

        res.writeHead(200);
        res.end(JSON.stringify(result));
      } catch (error) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });
  }

  handleHealth(req, res) {
    const health = this.orchestrator.engine.getHealth();
    res.writeHead(200);
    res.end(JSON.stringify(health));
  }
}

// ============================================
// SECTION 8: CLI
// ============================================

function printUsage() {
  console.log(`
Nexus Backpressure - Universal Backpressure Protocol

Usage:
  node app.js [--server] [--port PORT] [--host HOST]

Examples:
  node app.js --server
  node app.js --server --port 9000 --host 0.0.0.0
  `);
}

// ============================================
// SECTION 9: MAIN
// ============================================

function main() {
  console.log('🌊 Nexus Backpressure Protocol v1.0');

  const args = process.argv.slice(2);
  const isServer = args.includes('--server');
  const portIdx = args.indexOf('--port');
  const hostIdx = args.indexOf('--host');

  const config = {
    ...DEFAULT_CONFIG,
    port: portIdx !== -1 ? parseInt(args[portIdx + 1]) : DEFAULT_CONFIG.port,
    host: hostIdx !== -1 ? args[hostIdx + 1] : DEFAULT_CONFIG.host
  };

  if (isServer) {
    const httpServer = new HTTPServer(config);
    httpServer.start();
  } else {
    printUsage();
  }
}

main();
