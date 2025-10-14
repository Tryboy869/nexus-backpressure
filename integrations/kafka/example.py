#!/usr/bin/env python3
"""
Nexus Backpressure - Kafka Integration Example

Demonstrates how to use Nexus Backpressure Protocol with Apache Kafka
to handle consumer lag and prevent processing bottlenecks.
"""

import json
import time
import requests
from typing import Dict, Optional
from dataclasses import dataclass

# ============================================
# SECTION 1: NEXUS BACKPRESSURE CLIENT
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

class NexusBackpressureClient:
    """Client for Nexus Backpressure Protocol"""
    
    def __init__(self, producer_id: str, consumer_id: str, nexus_url: str = "http://localhost:8080"):
        self.producer_id = producer_id
        self.consumer_id = consumer_id
        self.nexus_url = nexus_url
    
    def get_capacity(self, requested_rate: int) -> Optional[FlowControl]:
        """Request capacity from Nexus controller"""
        signal = CapacitySignal(
            producer_id=self.producer_id,
            consumer_id=self.consumer_id,
            requested_rate=requested_rate,
            priority_level=0,
            timeout_ms=5000,
            timestamp=int(time.time() * 1000)
        )
        
        try:
            response = requests.post(
                f"{self.nexus_url}/capacity",
                json={
                    "producer_id": signal.producer_id,
                    "consumer_id": signal.consumer_id,
                    "requested_rate": signal.requested_rate,
                    "priority_level": signal.priority_level,
                    "timeout_ms": signal.timeout_ms,
                    "timestamp": signal.timestamp
                },
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                return FlowControl(**data)
            else:
                print(f"Error: {response.status_code} - {response.text}")
                return None
        except requests.RequestException as e:
            print(f"Connection error: {e}")
            return None
    
    def report_metrics(self, metrics: Dict) -> bool:
        """Report metrics to Nexus"""
        report = {
            "action": "MetricsReport",
            "version": "1.0",
            "producer_id": self.producer_id,
            "consumer_id": self.consumer_id,
            "timestamp": int(time.time() * 1000),
            **metrics
        }
        
        try:
            response = requests.post(
                f"{self.nexus_url}/metrics",
                json=report,
                timeout=5
            )
            return response.status_code == 200
        except requests.RequestException as e:
            print(f"Metrics error: {e}")
            return False

# ============================================
# SECTION 2: KAFKA CONSUMER WITH BACKPRESSURE
# ============================================

class KafkaConsumerWithBackpressure:
    """
    Kafka consumer that respects Nexus Backpressure signals
    
    NOTE: This is a conceptual example. In production, use kafka-python:
    pip install kafka-python
    """
    
    def __init__(self, topic: str, consumer_group: str):
        self.topic = topic
        self.consumer_group = consumer_group
        self.backpressure = NexusBackpressureClient(
            producer_id=f"kafka-topic-{topic}",
            consumer_id=f"consumer-{consumer_group}"
        )
        
        self.metrics = {
            "messages_consumed": 0,
            "messages_skipped": 0,
            "errors": 0,
            "total_latency": 0
        }
    
    def consume_with_backpressure(self, batch_size: int = 100, duration_sec: int = 30):
        """
        Consume messages while respecting backpressure
        """
        print(f"\n🌊 Kafka Consumer with Backpressure")
        print(f"   Topic: {self.topic}")
        print(f"   Group: {self.consumer_group}")
        print(f"   Batch size: {batch_size}")
        print(f"   Duration: {duration_sec}s\n")
        
        start_time = time.time()
        iteration = 0
        
        while time.time() - start_time < duration_sec:
            iteration += 1
            
            # Check capacity before consuming
            flow_control = self.backpressure.get_capacity(batch_size)
            
            if not flow_control:
                print("[ERROR] Could not get flow control")
                self.metrics["errors"] += 1
                time.sleep(1)
                continue
            
            # Respect backpressure level
            if flow_control.backpressure_level >= 2:
                print(f"[{iteration}] ⚠️  CRITICAL BACKPRESSURE - Skipping batch")
                self.metrics["messages_skipped"] += batch_size
                time.sleep(2)
                continue
            
            elif flow_control.backpressure_level == 1:
                print(f"[{iteration}] ⚠️  ALERT - Reducing rate")
                batch_size = max(10, batch_size // 2)
            
            # Simulate consuming messages
            messages_consumed = self._simulate_consume(batch_size)
            self.metrics["messages_consumed"] += messages_consumed
            
            # Log status
            print(f"[{iteration}] Consumed {messages_consumed} messages | "
                  f"Load: {flow_control.current_load}% | "
                  f"Rate: {flow_control.recommended_rate}/s | "
                  f"Level: {flow_control.backpressure_level}")
            
            # Report metrics every 5 iterations
            if iteration % 5 == 0:
                self._report_metrics()
            
            time.sleep(0.5)
        
        # Final report
        self._report_metrics()
        self._print_summary()
    
    def _simulate_consume(self, batch_size: int) -> int:
        """Simulate consuming messages from Kafka"""
        # Simulate message processing latency
        latency_ms = 50
        time.sleep(latency_ms / 1000)
        self.metrics["total_latency"] += latency_ms
        return batch_size
    
    def _report_metrics(self):
        """Report metrics to Nexus"""
        avg_latency = (
            self.metrics["total_latency"] // self.metrics["messages_consumed"]
            if self.metrics["messages_consumed"] > 0
            else 0
        )
        
        self.backpressure.report_metrics({
            "flow_name": f"kafka-{self.topic}",
            "items_processed": self.metrics["messages_consumed"],
            "items_dropped": self.metrics["messages_skipped"],
            "error_count": self.metrics["errors"],
            "latency_p99_ms": avg_latency,
            "throughput_current": self.metrics["messages_consumed"] // 10
        })
    
    def _print_summary(self):
        """Print consumption summary"""
        print(f"\n📊 Summary:")
        print(f"   Messages consumed: {self.metrics['messages_consumed']}")
        print(f"   Messages skipped: {self.metrics['messages_skipped']}")
        print(f"   Errors: {self.metrics['errors']}")
        if self.metrics["messages_consumed"] > 0:
            avg_latency = self.metrics["total_latency"] // self.metrics["messages_consumed"]
            print(f"   Avg latency: {avg_latency}ms")

# ============================================
# SECTION 3: EXAMPLE 1 - SIMPLE CONSUMER
# ============================================

def simple_consumer_example():
    """Example 1: Simple Kafka consumer with backpressure"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Simple Kafka Consumer with Backpressure")
    print("="*60)
    
    consumer = KafkaConsumerWithBackpressure(
        topic="user-events",
        consumer_group="event-processor"
    )
    
    consumer.consume_with_backpressure(
        batch_size=100,
        duration_sec=10
    )

# ============================================
# SECTION 4: EXAMPLE 2 - HIGH LOAD SCENARIO
# ============================================

def high_load_example():
    """Example 2: High load scenario with backpressure"""
    print("\n" + "="*60)
    print("EXAMPLE 2: High Load Scenario")
    print("="*60)
    
    consumer = KafkaConsumerWithBackpressure(
        topic="transactions",
        consumer_group="payment-processor"
    )
    
    # Start with large batch size, should throttle down under load
    consumer.consume_with_backpressure(
        batch_size=500,
        duration_sec=15
    )

# ============================================
# SECTION 5: EXAMPLE 3 - MULTIPLE CONSUMERS
// ============================================

def multiple_consumers_example():
    """Example 3: Multiple consumers coordinating via Nexus"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Multiple Consumers Coordinating")
    print("="*60)
    
    consumers = [
        KafkaConsumerWithBackpressure("logs", "log-aggregator-1"),
        KafkaConsumerWithBackpressure("logs", "log-aggregator-2"),
        KafkaConsumerWithBackpressure("logs", "log-aggregator-3"),
    ]
    
    print("\n🔄 Running 3 consumers concurrently...\n")
    
    for i, consumer in enumerate(consumers):
        print(f"Consumer {i+1} starting...")
        consumer.consume_with_backpressure(
            batch_size=100,
            duration_sec=10
        )

# ============================================
# SECTION 6: MAIN ENTRY POINT
// ============================================

def main():
    """Main entry point"""
    print("\n🌊 Nexus Backpressure - Kafka Integration Examples")
    print("   Make sure Nexus server is running on http://localhost:8080\n")
    
    try:
        # Run examples
        simple_consumer_example()
        
        print("\n" + "="*60 + "\n")
        
        high_load_example()
        
        print("\n" + "="*60 + "\n")
        
        multiple_consumers_example()
        
        print("\n✅ All examples completed!")
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()