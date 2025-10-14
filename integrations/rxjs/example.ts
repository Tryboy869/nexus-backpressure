/**
 * Nexus Backpressure - RxJS Integration Example
 * 
 * Demonstrates how to use Nexus Backpressure Protocol with RxJS streams
 * to handle backpressure and prevent memory leaks.
 */

import { Observable, interval, Subject, throttleTime, tap } from 'rxjs';

// ============================================
// SECTION 1: NEXUS BACKPRESSURE CLIENT
// ============================================

interface CapacitySignal {
  action: string;
  version: string;
  producer_id: string;
  consumer_id: string;
  requested_rate: number;
  priority_level: number;
  timeout_ms: number;
  timestamp: number;
}

interface FlowControl {
  action: string;
  version: string;
  consumer_id: string;
  producer_id: string;
  recommended_rate: number;
  current_load: number;
  max_capacity: number;
  backpressure_level: number;
  reason: string;
  apply_immediately: boolean;
  timestamp: number;
}

class NexusBackpressureClient {
  private producerId: string;
  private consumerId: string;
  private nexusUrl: string = 'http://localhost:8080';

  constructor(producerId: string, consumerId: string) {
    this.producerId = producerId;
    this.consumerId = consumerId;
  }

  async getCapacity(requestedRate: number): Promise<FlowControl> {
    const signal: CapacitySignal = {
      action: 'CapacitySignal',
      version: '1.0',
      producer_id: this.producerId,
      consumer_id: this.consumerId,
      requested_rate: requestedRate,
      priority_level: 0,
      timeout_ms: 5000,
      timestamp: Date.now()
    };

    const response = await fetch(`${this.nexusUrl}/capacity`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(signal)
    });

    if (!response.ok) {
      throw new Error(`Backpressure error: ${response.statusText}`);
    }

    return response.json();
  }

  async reportMetrics(metrics: Record<string, any>): Promise<void> {
    const report = {
      action: 'MetricsReport',
      version: '1.0',
      producer_id: this.producerId,
      consumer_id: this.consumerId,
      ...metrics,
      timestamp: Date.now()
    };

    await fetch(`${this.nexusUrl}/metrics`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(report)
    });
  }
}

// ============================================
// SECTION 2: EXAMPLE 1 - SIMPLE PRODUCER-CONSUMER
// ============================================

function simpleExample() {
  console.log('\n🌊 EXAMPLE 1: Simple Producer-Consumer with Backpressure\n');

  const backpressure = new NexusBackpressureClient('rxjs-producer-1', 'console-consumer');
  let itemsProcessed = 0;

  // Create a fast producer (1000 items/sec)
  const fastProducer$ = interval(1).pipe(
    tap(() => itemsProcessed++)
  );

  // Subscribe with adaptive backpressure
  fastProducer$.pipe(
    throttleTime(10), // Limit to ~100 items/sec
    tap(async (item) => {
      // Check capacity every 100 items
      if (itemsProcessed % 100 === 0) {
        try {
          const flowControl = await backpressure.getCapacity(100);
          
          console.log(`[${itemsProcessed}] Load: ${flowControl.current_load}% | Rate: ${flowControl.recommended_rate}/s | Level: ${flowControl.backpressure_level}`);

          // Respect recommended rate
          if (flowControl.backpressure_level === 2) {
            console.warn('⚠️  CRITICAL BACKPRESSURE - Slowing down');
          }
        } catch (err) {
          console.error('Backpressure error:', err);
        }
      }
    })
  ).subscribe(
    (item) => {
      // Consumer processing
      if (itemsProcessed % 1000 === 0) {
        console.log(`✓ Processed ${itemsProcessed} items`);
      }
    },
    (error) => console.error('Stream error:', error)
  );

  // Report metrics periodically
  setInterval(async () => {
    try {
      await backpressure.reportMetrics({
        items_processed: itemsProcessed,
        flow_name: 'simple-producer-consumer',
        throughput_current: Math.round(itemsProcessed / 10),
        latency_p99_ms: 50
      });
    } catch (err) {
      console.error('Metrics error:', err);
    }
  }, 10000); // Every 10 seconds
}

// ============================================
// SECTION 3: EXAMPLE 2 - HTTP STREAM CONSUMER
// ============================================

async function httpStreamExample() {
  console.log('\n🌊 EXAMPLE 2: HTTP Stream with Backpressure\n');

  const backpressure = new NexusBackpressureClient('http-client', 'backend-api');
  const requests$ = new Subject<string>();

  let successCount = 0;
  let errorCount = 0;

  requests$.pipe(
    tap(async (url) => {
      try {
        // Check backpressure before making request
        const flowControl = await backpressure.getCapacity(1);

        if (flowControl.backpressure_level >= 2) {
          console.warn(`⚠️  Skipping request - critical backpressure`);
          return;
        }

        // Make HTTP request
        const response = await fetch(url);
        
        if (response.ok) {
          successCount++;
          console.log(`✓ Request succeeded (${successCount} total)`);
        } else {
          errorCount++;
        }
      } catch (err) {
        errorCount++;
        console.error('Request failed:', err);
      }
    })
  ).subscribe();

  // Simulate incoming requests
  for (let i = 0; i < 10; i++) {
    await new Promise(resolve => setTimeout(resolve, 100));
    requests$.next('https://api.example.com/data');
  }

  // Report metrics
  await backpressure.reportMetrics({
    items_processed: successCount,
    error_count: errorCount,
    flow_name: 'http-stream',
    throughput_current: successCount
  });
}

// ============================================
// SECTION 4: EXAMPLE 3 - BUFFER WITH BACKPRESSURE
// ============================================

function bufferWithBackpressure() {
  console.log('\n🌊 EXAMPLE 3: Buffer Management with Backpressure\n');

  const backpressure = new NexusBackpressureClient('buffer-producer', 'buffer-consumer');
  const buffer: any[] = [];
  const maxBuffer = 1000;

  const producer$ = interval(1).pipe(
    tap(async (item) => {
      // Check if buffer is getting full
      if (buffer.length > maxBuffer * 0.8) {
        try {
          const flowControl = await backpressure.getCapacity(buffer.length);

          // Slow down if backpressure is high
          if (flowControl.backpressure_level > 0) {
            await new Promise(resolve => 
              setTimeout(resolve, 10 * flowControl.backpressure_level)
            );
          }
        } catch (err) {
          console.error('Backpressure check failed:', err);
        }
      }

      // Add to buffer
      buffer.push({ id: item, timestamp: Date.now() });

      if (buffer.length % 100 === 0) {
        console.log(`Buffer size: ${buffer.length}/${maxBuffer}`);
      }
    })
  );

  // Consumer drains buffer
  setInterval(() => {
    const batchSize = Math.min(50, buffer.length);
    const batch = buffer.splice(0, batchSize);

    if (batch.length > 0) {
      console.log(`Drained ${batch.length} items. Buffer now: ${buffer.length}`);
    }

    // Report metrics
    backpressure.reportMetrics({
      items_buffered: buffer.length,
      items_processed: batch.length,
      flow_name: 'buffer-backpressure'
    }).catch(err => console.error('Metrics error:', err));
  }, 1000); // Drain every 1 second

  producer$.subscribe();
}

// ============================================
// SECTION 5: EXAMPLE 4 - ERROR HANDLING
// ============================================

async function errorHandlingExample() {
  console.log('\n🌊 EXAMPLE 4: Error Handling with Backpressure\n');

  const backpressure = new NexusBackpressureClient('error-producer', 'error-consumer');
  let errors = 0;

  const unreliableSource$ = new Subject<number>();

  unreliableSource$.pipe(
    tap(async (item) => {
      try {
        // Random errors (10% chance)
        if (Math.random() < 0.1) {
          throw new Error('Random processing error');
        }

        const flowControl = await backpressure.getCapacity(1);
        console.log(`✓ Item ${item} processed`);
      } catch (err) {
        errors++;
        console.error(`✗ Item ${item} failed: ${err.message}`);

        // Report error metrics
        await backpressure.reportMetrics({
          error_count: errors,
          flow_name: 'error-handling'
        });
      }
    })
  ).subscribe();

  // Send items
  for (let i = 1; i <= 20; i++) {
    await new Promise(resolve => setTimeout(resolve, 100));
    unreliableSource$.next(i);
  }

  console.log(`\nTotal errors: ${errors}/20`);
}

// ============================================
// SECTION 6: MAIN ENTRY POINT
// ============================================

async function main() {
  console.log('🌊 Nexus Backpressure - RxJS Integration Examples\n');
  console.log('Make sure Nexus server is running on http://localhost:8080\n');

  try {
    // Run examples
    simpleExample();

    // Wait a bit then run next example
    await new Promise(resolve => setTimeout(resolve, 5000));
    await httpStreamExample();

    await new Promise(resolve => setTimeout(resolve, 5000));
    bufferWithBackpressure();

    await new Promise(resolve => setTimeout(resolve, 5000));
    await errorHandlingExample();
  } catch (err) {
    console.error('Example error:', err);
  }
}

// Run if executed directly
if (typeof module !== 'undefined' && require.main === module) {
  main().catch(console.error);
}

export { NexusBackpressureClient };