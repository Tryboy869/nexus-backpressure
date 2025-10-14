# Nexus Backpressure Protocol Specification v1.0

**RFC-Style Document**

---

## ABSTRACT

This document specifies the Nexus Backpressure Protocol, a universal framework for coordinating flow control between producers and consumers in distributed systems. The protocol enables framework-agnostic management of streaming data without requiring each framework to implement its own backpressure solution.

---

## STATUS

Proposed Standard

---

## BACKGROUND

### Historical Problem

Backpressure—the mechanism by which a consumer signals to a producer that it cannot process data at the current rate—has been implemented differently in every major framework:

| Framework | Solution | Problem |
|-----------|----------|---------|
| RxJS | `throttle()`, `debounce()`, custom | No standard, confusion about what to use |
| Node Streams | `highWaterMark` | Magic number, poorly documented |
| Kafka | `fetch.max.bytes`, configuration | Complex tuning, non-intuitive |
| Apache Spark | `shuffle.spill`, `memoryFraction` | Infrastructure-focused, not applicative |
| Python asyncio | Custom per-library | No universality |

### Consequences

- **Memory leaks**: Producers fill buffers faster than consumers drain them
- **Crashes**: Out-of-memory errors bring down entire systems
- **Developer burden**: Each framework requires separate learning
- **Coordination failure**: Multi-framework systems have no unified approach

### Solution Approach

Define a universal protocol that:
1. Works with ANY producer/consumer pair
2. Is language-agnostic
3. Is simple to implement
4. Provides observability
5. Guarantees data integrity

---

## CORE CONCEPTS

### Producer
An entity generating data at a certain rate (items/second).

### Consumer
An entity processing data. Has limited capacity (items/second).

### Backpressure
A signal from consumer to producer indicating reduced capacity.

### Capacity
The number of items a consumer can process without overflowing.

### Flow
A named producer-consumer data pipeline.

### Metrics
Observability data about a flow (latency, throughput, load).

---

## PROTOCOL OVERVIEW

The Nexus Backpressure Protocol uses a **request-response** model:

```
Producer: "How much can you take?"
   ↓
Nexus Controller (stateless) receives CapacitySignal
   ↓
Controller calculates optimal rate based on consumer state
   ↓
Consumer: "I can take this much"
   ↓
Producer receives FlowControl and adjusts rate
```

---

## MESSAGE TYPES

### 1. CapacitySignal (Producer → Controller)

**Purpose**: Producer asks for capacity information

**Format** (JSON):
```json
{
  "action": "CapacitySignal",
  "version": "1.0",
  "producer_id": "string (unique identifier)",
  "consumer_id": "string (unique identifier)",
  "requested_rate": "integer (items/sec)",
  "priority_level": "integer (0=normal, 1=important, 2=critical)",
  "timeout_ms": "integer (milliseconds to wait)",
  "timestamp": "integer (epoch milliseconds)"
}
```

**Validation Rules**:
- `producer_id` and `consumer_id` must be non-empty strings
- `requested_rate` must be > 0
- `priority_level` must be 0, 1, or 2
- `timeout_ms` must be between 100 and 30000

**Example**:
```json
{
  "action": "CapacitySignal",
  "version": "1.0",
  "producer_id": "filebeat-1",
  "consumer_id": "elasticsearch-cluster",
  "requested_rate": 10000,
  "priority_level": 0,
  "timeout_ms": 5000,
  "timestamp": 1729000000000
}
```

---

### 2. FlowControl (Controller → Producer)

**Purpose**: Controller responds with capacity and rate recommendations

**Format** (JSON):
```json
{
  "action": "FlowControl",
  "version": "1.0",
  "consumer_id": "string",
  "producer_id": "string",
  "recommended_rate": "integer (items/sec to send)",
  "current_load": "integer (0-100, % capacity)",
  "max_capacity": "integer (items/sec max)",
  "backpressure_level": "integer (0=normal, 1=alert, 2=critical)",
  "reason": "string (why this rate)",
  "apply_immediately": "boolean (force immediate adjustment)",
  "timestamp": "integer (epoch milliseconds)"
}
```

**Semantics**:
- `recommended_rate`: Producer should not exceed this rate
- `backpressure_level`: 0 = normal, 1 = starting to fill, 2 = nearly full
- `current_load`: Percentage of consumer's capacity in use
- `apply_immediately`: If true, producer must stop all traffic until new signal

**Example**:
```json
{
  "action": "FlowControl",
  "version": "1.0",
  "consumer_id": "elasticsearch-cluster",
  "producer_id": "filebeat-1",
  "recommended_rate": 100,
  "current_load": 95,
  "max_capacity": 105,
  "backpressure_level": 2,
  "reason": "consumer_near_capacity",
  "apply_immediately": false,
  "timestamp": 1729000000001
}
```

---

### 3. MetricsReport (Producer/Consumer → Monitoring)

**Purpose**: Report performance metrics for observability

**Format** (JSON):
```json
{
  "action": "MetricsReport",
  "version": "1.0",
  "producer_id": "string",
  "consumer_id": "string",
  "flow_name": "string (optional, human-readable)",
  "items_buffered": "integer (current buffer size)",
  "items_processed": "integer (cumulative)",
  "items_dropped": "integer (due to backpressure)",
  "latency_p50_ms": "integer (median latency)",
  "latency_p99_ms": "integer (99th percentile)",
  "throughput_current": "integer (items/sec right now)",
  "backpressure_triggered_count": "integer (number of times)",
  "error_count": "integer",
  "timestamp": "integer (epoch milliseconds)"
}
```

**Example**:
```json
{
  "action": "MetricsReport",
  "version": "1.0",
  "producer_id": "filebeat-1",
  "consumer_id": "elasticsearch-cluster",
  "flow_name": "logs-pipeline",
  "items_buffered": 450,
  "items_processed": 1000000,
  "items_dropped": 0,
  "latency_p50_ms": 45,
  "latency_p99_ms": 250,
  "throughput_current": 95,
  "backpressure_triggered_count": 12,
  "error_count": 0,
  "timestamp": 1729000000500
}
```

---

## PROTOCOL FLOW (State Machine)

```
┌─────────────┐
│   START     │
└──────┬──────┘
       │
       ▼
┌─────────────────────────┐
│ Producer sends          │
│ CapacitySignal          │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────┐
│ Controller calculates   │
│ optimal rate            │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────┐
│ Consumer responds with  │
│ FlowControl             │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────┐
│ Producer receives       │
│ FlowControl             │
└──────┬──────────────────┘
       │
       ├─ Recommended rate > 0?
       │  YES ──► Send items at recommended rate
       │  NO  ──► WAIT (backpressure)
       │
       ├─ Buffer filling?
       │  YES ──► Report MetricsReport
       │  NO  ──► Continue
       │
       └─ Loop (repeat CapacitySignal every N seconds)
```

---

## RATE CALCULATION ALGORITHM

The Nexus Controller uses an adaptive algorithm to calculate `recommended_rate`:

```
IF current_load < 50%:
    recommended_rate = max_capacity  (full speed)
ELSE IF current_load < 80%:
    recommended_rate = max_capacity / 2  (half speed)
ELSE IF current_load < 95%:
    recommended_rate = max_capacity / 4  (quarter speed)
ELSE:
    recommended_rate = 0  (pause, full backpressure)
```

**Rationale**: Gradual reduction prevents oscillation and allows buffer to drain.

---

## GUARANTEES

### Safety
- No data loss without explicit consumer consent
- Producer MUST respect `apply_immediately` flag
- Buffer will never grow unbounded

### Fairness
- Multiple producers competing for same consumer receive proportional capacity
- Priority levels allow differentiation (optional)

### Responsiveness
- CapacitySignal latency should be <100ms
- Rate adjustments should apply within 1 second

### Observability
- All significant state changes logged via MetricsReport
- Timestamp precision: milliseconds
- All errors reported with reason string

---

## IMPLEMENTATION REQUIREMENTS

### Required for Conformance

1. **Support all three message types** (CapacitySignal, FlowControl, MetricsReport)
2. **Validate all JSON fields** per specification
3. **Implement rate calculation algorithm** (or compatible)
4. **Support HTTP POST** (at minimum) for message exchange
5. **Include timestamp** in all messages
6. **Support priority_level** (at least 0/1/2)
7. **Report metrics** at least every 60 seconds

### Optional for Enhancement

- gRPC support
- WebSocket support
- Custom rate calculation
- Machine learning-based rate optimization
- Multi-region coordination

---

## SECURITY CONSIDERATIONS

### Input Validation

Implement strict validation for all inputs:
```
- Strings: max 256 characters, alphanumeric+underscore+dash
- Integers: range checking
- JSON: schema validation
```

### Rate Limiting

Apply per-producer rate limiting to prevent DoS:
```
- Max 100 CapacitySignals per minute per producer
- Max 1000 MetricsReports per minute per producer
```

### Authentication

Recommended (not required):
- API tokens for producers
- Service-to-service authentication
- TLS for transport

---

## EXAMPLES

### Example 1: Simple Producer-Consumer

Producer starts:
```json
{"action": "CapacitySignal", "producer_id": "p1", "consumer_id": "c1", "requested_rate": 1000, ...}
```

Controller responds:
```json
{"action": "FlowControl", "recommended_rate": 500, "backpressure_level": 1, ...}
```

Producer sends 500 items/sec. Consumer drains them. Later...

```json
{"action": "MetricsReport", "items_buffered": 50, "latency_p99_ms": 120, ...}
```

---

### Example 2: Cascade with Backpressure

A → B → C

C slows down. B detects it. A respects it:

```
C sends CapacitySignal: recommended_rate = 10
B receives: adjusts to 10
B sends to A: recommended_rate = 10
A receives: adjusts to 10

Result: No cascade failure, system stable
```

---

## VERSIONING

This is version 1.0 of the Nexus Backpressure Protocol.

Future versions may add:
- Cancellation/timeout semantics
- Transaction support
- Multi-language protocol negotiation

Implementations must set `"version": "1.0"` in all messages.

---

## REFERENCES

- TCP Flow Control (RFC 793)
- HTTP/2 Flow Control (RFC 7540)
- gRPC Flow Control
- Node.js Streams Backpressure

---

**Specification written by Anzize Daouda**
**Status: Proposed Standard (October 2025)**
**Next review: Q2 2026**