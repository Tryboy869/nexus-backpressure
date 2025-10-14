# Nexus Backpressure - Getting Started Guide

5-minute quickstart to launch your first Nexus server.

## Prerequisites

Choose ONE language:

- **Go**: `go version` ≥ 1.21
- **Python**: `python3 --version` ≥ 3.8
- **Node.js**: `node --version` ≥ 18
- **Rust**: `cargo --version` ≥ 1.70

## Step 1: Clone the Repository

```bash
git clone https://github.com/Tryboy869/nexus-backpressure.git
cd nexus-backpressure
```

## Step 2: Choose Your Language

### Go

```bash
cd reference-implementations/go
go run main.go
```

Expected output:
```
✅ Server listening on http://localhost:8080
Endpoints:
  POST /capacity  - Send CapacitySignal
  POST /metrics   - Send MetricsReport
  GET  /health    - Get health status
```

### Python

```bash
cd reference-implementations/python
pip install -r requirements.txt  # if requirements.txt exists
python app.py --server
```

Expected output:
```
🌊 Nexus Backpressure Protocol v1.0
✅ Server listening on http://0.0.0.0:8080
```

### Node.js

```bash
cd reference-implementations/node
npm install
npm start
```

Expected output:
```
🌊 Nexus Backpressure Protocol v1.0
✅ Server listening on http://0.0.0.0:8080
```

### Rust

```bash
cd reference-implementations/rust
cargo run -- --server
```

Expected output:
```
🌊 Nexus Backpressure Protocol v1.0
[Demo output showing protocol in action]
```

## Step 3: Test Your Server

### Health Check

```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "healthy",
  "active_flows": 0,
  "metrics_count": 0,
  "uptime": 123
}
```

### Send Capacity Signal

```bash
curl -X POST http://localhost:8080/capacity \
  -H "Content-Type: application/json" \
  -d '{
    "producer_id": "test-producer",
    "consumer_id": "test-consumer",
    "requested_rate": 1000,
    "priority_level": 0,
    "timeout_ms": 5000
  }'
```

Response:
```json
{
  "action": "FlowControl",
  "version": "1.0",
  "consumer_id": "test-consumer",
  "producer_id": "test-producer",
  "recommended_rate": 1000,
  "current_load": 0,
  "max_capacity": 1000,
  "backpressure_level": 0,
  "reason": "consumer_normal",
  "apply_immediately": false,
  "timestamp": 1729000000000
}
```

### Report Metrics

```bash
curl -X POST http://localhost:8080/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "producer_id": "test-producer",
    "consumer_id": "test-consumer",
    "items_buffered": 50,
    "latency_p99_ms": 100,
    "throughput_current": 500
  }'
```

Response:
```json
{
  "status": "received"
}
```

## Step 4: Read Documentation

After server is running, explore:

1. **Protocol**: Read `SPECIFICATION.md`
2. **Integration**: Check `integrations/` folder
3. **Examples**: Look at `examples/` folder
4. **Case Studies**: Review `case-studies/`

## What's Next?

### For Developers

- Integrate with your framework (RxJS, Kafka, etc.)
- Read integration examples in `integrations/`
- Build your own client library

### For DevOps

- Deploy with Docker (see README in each language)
- Configure Kubernetes (YAML in integrations/kubernetes/)
- Monitor with Prometheus (configs in integrations/prometheus/)

### For Architects

- Review SPECIFICATION.md for complete protocol details
- Study case studies for real-world usage
- Plan integration into your systems

## Troubleshooting

### Port 8080 already in use

Use a different port:

```bash
# Go
go run main.go --port :9000

# Python
python app.py --server --port 9000

# Node
node app.js --server --port 9000

# Rust
cargo run -- --server --port 9000
```

### Certificate/SSL errors

For development, HTTP is fine. For production, use reverse proxy:

```nginx
server {
    listen 443 ssl;
    server_name nexus.example.com;
    
    ssl_certificate /path/to/cert;
    ssl_certificate_key /path/to/key;
    
    location / {
        proxy_pass http://localhost:8080;
    }
}
```

### Performance issues

Check resource limits and load:

```bash
# Monitor CPU/Memory
top
# or
htop

# Load test
ab -n 10000 -c 100 http://localhost:8080/health
```

## Common Use Cases

### 1. Monitor Production Logs

**Problem**: Logs → Filebeat → Elasticsearch (crashes)

**Solution**: Use Nexus Backpressure to control flow

See: `case-studies/elasticsearch-log-pipeline.md`

### 2. Payment Processing

**Problem**: High-volume transactions → memory overflow

**Solution**: Rate-limit with backpressure

See: `case-studies/stripe-like-payment-processor.md`

### 3. Microservices Chain

**Problem**: Service A → B → C, C slows → cascade failure

**Solution**: Each service respects capacity signals

See: `case-studies/microservices-chain.md`

### 4. Kafka Streaming

**Problem**: Consumer can't keep up, tuning nightmare

**Solution**: Automatic adaptive flow control

See: `case-studies/kafka-streaming-pipeline.md`

## Next Steps

1. Choose a use case from above
2. Review corresponding case study
3. Integrate Nexus into your system
4. Monitor improvements

## Getting Help

- **Questions**: GitHub Discussions
- **Bugs**: GitHub Issues
- **Chat**: Discord server
- **Docs**: Full documentation in `docs/` folder

## Contributing

Want to help? See `CONTRIBUTING.md` for:
- Adding new language implementations
- Improving documentation
- Building integrations
- Reporting issues

---

**You now have Nexus Backpressure running! 🎉**

Next: Read `SPECIFICATION.md` to understand the protocol, or jump to integrations for your framework.