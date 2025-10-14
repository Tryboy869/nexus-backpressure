# Nexus Backpressure - Python Implementation

Reference implementation of the Nexus Backpressure Protocol in Python.

## Features

- ✅ Complete protocol implementation (9 sections)
- ✅ HTTP API (POST /capacity, /metrics, GET /health)
- ✅ Security gateway with rate limiting
- ✅ Thread-safe flow management
- ✅ Audit logging
- ✅ Metrics collection

## Requirements

- Python 3.8+

## Setup

```bash
# Navigate to this directory
cd reference-implementations/python

# (Optional) Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Run the server
python app.py --server

# Or specify port/host
python app.py --server --port 9000 --host 0.0.0.0
```

## Server Output

```
🌊 Nexus Backpressure Protocol v1.0
✅ Server listening on http://0.0.0.0:8080
Endpoints:
  POST /capacity  - Send CapacitySignal
  POST /metrics   - Send MetricsReport
  GET  /health    - Get health status
```

## API Endpoints

### POST /capacity

Send a capacity signal from producer:

```bash
curl -X POST http://localhost:8080/capacity \
  -H "Content-Type: application/json" \
  -d '{
    "producer_id": "kafka-consumer-1",
    "consumer_id": "mongodb",
    "requested_rate": 5000,
    "priority_level": 1,
    "timeout_ms": 3000
  }'
```

Response:

```json
{
  "action": "FlowControl",
  "version": "1.0",
  "consumer_id": "mongodb",
  "producer_id": "kafka-consumer-1",
  "recommended_rate": 500,
  "current_load": 85,
  "max_capacity": 1000,
  "backpressure_level": 1,
  "reason": "consumer_alert",
  "apply_immediately": false,
  "timestamp": 1729000000000
}
```

### POST /metrics

Report metrics from producer/consumer:

```bash
curl -X POST http://localhost:8080/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "producer_id": "kafka-consumer-1",
    "consumer_id": "mongodb",
    "flow_name": "data-pipeline",
    "items_buffered": 120,
    "items_processed": 50000,
    "items_dropped": 0,
    "latency_p50_ms": 45,
    "latency_p99_ms": 250,
    "throughput_current": 450,
    "backpressure_triggered_count": 5,
    "error_count": 0
  }'
```

Response:

```json
{
  "status": "received"
}
```

### GET /health

Check server health:

```bash
curl http://localhost:8080/health
```

Response:

```json
{
  "status": "healthy",
  "active_flows": 2,
  "metrics_count": 8,
  "uptime": 3456
}
```

## Architecture

```
app.py (500 lines)
├── Section 1: Imports & Setup
├── Section 2: Configuration (@dataclass)
├── Section 3: Security Gateway (rate limiting, validation)
├── Section 4: Message Types (dataclasses)
├── Section 5: Backpressure Engine
├── Section 6: Orchestrator
├── Section 7: HTTP API Handler
├── Section 8: CLI (argparse)
└── Section 9: Main & Entry Point
```

## Configuration

Edit `pyproject.toml` or modify at runtime:

```python
config = Config(
    port=8080,
    host="0.0.0.0",
    security_level="medium",
    max_buffer_size=10000,
    timeout_ms=5000,
    enable_metrics=True,
    log_level="info"
)
```

## Performance

- Response time: <10ms
- Concurrent flows: 1k+
- Memory usage: ~100MB for 1k flows

## Testing

```bash
# Run server in background
python app.py --server &

# Test endpoints
curl http://localhost:8080/health

# Load test with Apache Bench
ab -n 1000 -c 50 http://localhost:8080/health
```

## Extending

To add functionality:

1. Create new class inheriting from dataclass/base
2. Implement required methods
3. Register in Orchestrator (Section 6)
4. Add HTTP handler in Section 7

## Production Deployment

### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY app.py pyproject.toml ./

RUN pip install --no-cache-dir -e .

EXPOSE 8080
CMD ["python", "app.py", "--server"]
```

### Kubernetes

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: nexus-backpressure
spec:
  containers:
  - name: nexus
    image: nexus:1.0.0-python
    ports:
    - containerPort: 8080
    livenessProbe:
      httpGet:
        path: /health
        port: 8080
      initialDelaySeconds: 5
      periodSeconds: 10
```

### Systemd Service

```ini
[Unit]
Description=Nexus Backpressure
After=network.target

[Service]
Type=simple
User=nexus
WorkingDirectory=/opt/nexus
ExecStart=/usr/bin/python3 app.py --server
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

### Port already in use

```bash
# Find process using port 8080
lsof -i :8080

# Kill process
kill -9 <PID>

# Or use different port
python app.py --server --port 9000
```

### Rate limiting errors

If you're getting "rate limit exceeded", wait 60 seconds or change limit in Section 3:

```python
RATE_LIMIT_MAX = 100  # Change this
RATE_LIMIT_WINDOW = 60000  # milliseconds
```

## License

Apache 2.0