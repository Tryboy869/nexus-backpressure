# Nexus Backpressure - Integration README Files

## integrations/rxjs/README.md

### RxJS Integration

This directory contains examples and guides for integrating Nexus Backpressure with RxJS streams.

#### What is RxJS?

RxJS is a library for reactive programming using Observables. It's widely used in Angular applications and modern JavaScript frameworks for managing asynchronous data streams.

#### Why Nexus Backpressure + RxJS?

RxJS has throttle and debounce operators, but they're not universally understood. Nexus provides a standard protocol that works across all streaming frameworks.

**Benefits:**
- No more `throttleTime()` guessing
- Unified approach across frameworks
- Server-aware rate limiting
- Better memory management

#### Getting Started

```bash
# Install dependencies
npm install rxjs

# Run examples
npx ts-node example.ts
```

#### Examples

1. **simple-example**: Basic producer-consumer with backpressure
2. **http-stream-example**: HTTP requests with adaptive rate limiting
3. **buffer-example**: Buffer management with automatic throttling
4. **error-handling**: Error recovery with backpressure

#### Integration Guide

```typescript
import { NexusBackpressureClient } from './nexus';

const backpressure = new NexusBackpressureClient('producer', 'consumer');

source$.pipe(
  tap(async (item) => {
    const flowControl = await backpressure.getCapacity(1);
    if (flowControl.backpressure_level >= 2) {
      // Pause or throttle
    }
  })
).subscribe(process);
```

---

## integrations/kafka/README.md

### Kafka Integration

This directory contains examples for integrating Nexus Backpressure with Apache Kafka consumers.

#### What is Kafka?

Apache Kafka is a distributed event streaming platform. Consumers read from topics and process messages in batches.

#### Why Nexus Backpressure + Kafka?

Kafka's fetch configuration (`fetch.max.bytes`, `fetch.min.bytes`) is complex and non-intuitive. Nexus automates this.

**Benefits:**
- No more tuning `fetch.max.bytes`
- Automatic adaptive batching
- Coordinate multiple consumer groups
- Prevent consumer lag

#### Getting Started

```bash
# Install Kafka client
pip install kafka-python

# Start Kafka (use docker-compose in parent directory)
docker-compose up kafka

# Run examples
python example.py
```

#### Examples

1. **simple-consumer**: Basic Kafka consumer with backpressure
2. **high-load**: Consumer under heavy load, throttles automatically
3. **multi-consumer**: Coordinating multiple consumer groups

#### Integration Guide

```python
from nexus import NexusBackpressureClient

backpressure = NexusBackpressureClient('kafka-topic', 'consumer-group')

for message in consumer:
    flow_control = backpressure.get_capacity(batch_size)
    if flow_control.backpressure_level >= 2:
        # Skip or delay processing
        continue
    process(message)
```

---

## integrations/nodejs-streams/README.md

### Node.js Streams Integration

This directory contains examples for integrating Nexus Backpressure with Node.js streams.

#### What are Node.js Streams?

Streams are objects that let you read/write data in chunks, handling backpressure automatically through the `highWaterMark` property.

#### Why Nexus Backpressure + Node Streams?

`highWaterMark` is a magic number. Nexus makes backpressure explicit and measurable.

**Benefits:**
- No more guessing `highWaterMark` values
- Observable backpressure levels
- Integrate with external systems
- Better debugging

#### Getting Started

```bash
# Node.js 18+ included
node example.js
```

#### Examples

1. **file-stream**: Read/write files with backpressure
2. **http-stream**: Handle HTTP responses efficiently
3. **dual-stream**: Producer-consumer coordination
4. **error-handling**: Error recovery in streams

#### Integration Guide

```javascript
import { NexusBackpressureClient } from './nexus';

const backpressure = new NexusBackpressureClient('producer', 'consumer');

readable
  .pipe(
    new Transform({
      async transform(chunk, enc, callback) {
        const flowControl = await backpressure.getCapacity(1);
        if (flowControl.backpressure_level >= 2) {
          // Pause and retry
          setTimeout(() => callback(null, chunk), 100);
        } else {
          callback(null, chunk);
        }
      }
    })
  )
  .pipe(writable);
```

---

## integrations/kubernetes/README.md

### Kubernetes Integration

This directory contains Kubernetes manifests for deploying Nexus Backpressure in production.

#### Deployment Files

- **deployment.yaml**: Complete Kubernetes deployment with 3 replicas
- Includes: HPA, PDB, ServiceMonitor for Prometheus
- Security context and resource limits
- Health checks and monitoring

#### Deploying

```bash
# Deploy to Kubernetes
kubectl apply -f deployment.yaml

# Check status
kubectl get pods -n nexus-backpressure
kubectl logs -f deployment/nexus-backpressure -n nexus-backpressure

# Port forward to access locally
kubectl port-forward svc/nexus-backpressure 8080:80 -n nexus-backpressure
```

#### Configuration

Edit the ConfigMap in deployment.yaml:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: nexus-config
data:
  config.yaml: |
    security:
      rate_limit: 100
```

#### Monitoring

Deployment includes:
- Prometheus ServiceMonitor (scrapes metrics every 30s)
- Pod Disruption Budget (minimum 1 pod available)
- HorizontalPodAutoscaler (scales 3-10 pods based on CPU/memory)

---

## integrations/docker-compose/README.md

### Docker Compose Stack

Complete local development stack with all optional services.

#### Services

- **nexus**: Nexus Backpressure server (Go)
- **prometheus**: Metrics collection
- **grafana**: Visualization (admin:admin)
- **kafka**: Kafka broker (for testing)
- **zookeeper**: Kafka coordinator
- **redis**: Caching (optional)
- **elasticsearch**: Log storage (optional)
- **kibana**: Elasticsearch UI (optional)

#### Getting Started

```bash
# Start the stack
docker-compose up

# Access services
- Nexus: http://localhost:8080
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Kibana: http://localhost:5601
- Kafka: localhost:9092
```

#### Run Examples

```bash
# In separate terminal:
cd ../rxjs
npx ts-node example.ts

# Or Python example:
cd ../kafka
python example.py

# Or Node example:
cd ../nodejs-streams
node example.js
```

#### Volumes

Persistent data stored in:
- `prometheus_data/`
- `grafana_data/`
- `redis_data/`
- `elasticsearch_data/`

#### Cleanup

```bash
# Stop services
docker-compose down

# Remove all data
docker-compose down -v
```

---

## Accessing All Integrations

From any integration directory:

```bash
# Get back to root
cd ../../

# See all integrations
ls integrations/

# Read any integration
cat integrations/[name]/README.md
```