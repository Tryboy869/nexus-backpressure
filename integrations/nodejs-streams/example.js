#!/usr/bin/env node

/**
 * Nexus Backpressure - Node.js Streams Integration Example
 * 
 * Demonstrates how to use Nexus Backpressure Protocol with Node.js streams
 * to handle backpressure and prevent memory leaks.
 */

import { Readable, Writable, Transform, pipeline } from 'stream';
import fetch from 'node-fetch';

// ============================================
// SECTION 1: NEXUS BACKPRESSURE CLIENT
// ============================================

class NexusBackpressureClient {
  constructor(producerId, consumerId, nexusUrl = 'http://localhost:8080') {
    this.producerId = producerId;
    this.consumerId = consumerId;
    this.nexusUrl = nexusUrl;
  }

  async getCapacity(requestedRate) {
    const signal = {
      action: 'CapacitySignal',
      version: '1.0',
      producer_id: this.producerId,
      consumer_id: this.consumerId,
      requested_rate: requestedRate,
      priority_level: 0,
      timeout_ms: 5000,
      timestamp: Date.now()
    };

    try {
      const response = await fetch(`${this.nexusUrl}/capacity`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(signal)
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      return await response.json();
    } catch (err) {
      console.error('Capacity check error:', err.message);
      return null;
    }
  }

  async reportMetrics(metrics) {
    const report = {
      action: 'MetricsReport',
      version: '1.0',
      producer_id: this.producerId,
      consumer_id: this.consumerId,
      timestamp: Date.now(),
      ...metrics
    };

    try {
      await fetch(`${this.nexusUrl}/metrics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(report)
      });
    } catch (err) {
      console.error('Metrics error:', err.message);
    }
  }
}

// ============================================
// SECTION 2: EXAMPLE 1 - FILE STREAM WITH BACKPRESSURE
// ============================================

function simpleFileStreamExample() {
  console.log('\n🌊 EXAMPLE 1: File Stream with Backpressure\n');

  const backpressure = new NexusBackpressureClient(
    'file-reader',
    'file-processor'
  );

  // Create readable stream (source)
  const readable = new Readable({
    read() {
      // Simulate file reading
      for (let i = 0; i < 1000; i++) {
        this.push(`Line ${i}: Sample data\n`);
      }
      this.push(null); // End stream
    }
  });

  // Transform stream with backpressure handling
  let itemsProcessed = 0;
  const transform = new Transform({
    async transform(chunk, encoding, callback) {
      itemsProcessed++;

      // Check backpressure every 100 items
      if (itemsProcessed % 100 === 0) {
        const flowControl = await backpressure.getCapacity(100);

        if (flowControl) {
          if (flowControl.backpressure_level >= 2) {
            console.log(`⚠️  CRITICAL BACKPRESSURE at ${itemsProcessed} items`);
            // Slow down by delaying processing
            setTimeout(() => callback(null, chunk), 500);
            return;
          } else if (flowControl.backpressure_level === 1) {
            console.log(`[${itemsProcessed}] Alert: Load at ${flowControl.current_load}%`);
          }
        }
      }

      callback(null, chunk);
    }
  });

  // Writable stream (sink)
  let bytesWritten = 0;
  const writable = new Writable({
    write(chunk, encoding, callback) {
      bytesWritten += chunk.length;

      if (itemsProcessed % 500 === 0) {
        console.log(`✓ Processed ${itemsProcessed} items (${bytesWritten} bytes)`);
      }

      callback();
    },
    final(callback) {
      console.log(`\n✅ Stream complete: ${itemsProcessed} items, ${bytesWritten} bytes\n`);
      backpressure.reportMetrics({
        items_processed: itemsProcessed,
        flow_name: 'file-stream',
        throughput_current: itemsProcessed / 10
      });
      callback();
    }
  });

  // Use pipeline for proper backpressure handling
  pipeline(readable, transform, writable, (err) => {
    if (err) console.error('Pipeline error:', err);
  });
}

// ============================================
// SECTION 3: EXAMPLE 2 - HTTP STREAM WITH BACKPRESSURE
// ============================================

function httpStreamExample() {
  console.log('\n🌊 EXAMPLE 2: HTTP Stream with Backpressure\n');

  const backpressure = new NexusBackpressureClient(
    'http-producer',
    'http-consumer'
  );

  let itemsProcessed = 0;

  // Simulate HTTP response stream
  const httpStream = new Readable({
    async read() {
      if (itemsProcessed < 1000) {
        itemsProcessed++;

        // Check backpressure
        if (itemsProcessed % 50 === 0) {
          const flowControl = await backpressure.getCapacity(50);

          if (flowControl) {
            console.log(
              `[${itemsProcessed}] Load: ${flowControl.current_load}% | ` +
              `Rate: ${flowControl.recommended_rate}/s`
            );

            if (flowControl.backpressure_level >= 2) {
              console.warn('⚠️  Pausing stream');
              this.pause();
              setTimeout(() => this.resume(), 1000);
              return;
            }
          }
        }

        this.push(JSON.stringify({ id: itemsProcessed, data: 'test' }) + '\n');
      } else {
        this.push(null);
      }
    }
  });

  // Process each line
  const lineProcessor = new Transform({
    transform(chunk, encoding, callback) {
      callback(null, chunk);
    }
  });

  // Log output
  const logger = new Writable({
    write(chunk, encoding, callback) {
      if (itemsProcessed % 200 === 0) {
        console.log(`✓ Logged item ${itemsProcessed}`);
      }
      callback();
    },
    final(callback) {
      console.log(`\n✅ HTTP stream complete: ${itemsProcessed} items\n`);
      backpressure.reportMetrics({
        items_processed: itemsProcessed,
        flow_name: 'http-stream'
      });
      callback();
    }
  });

  pipeline(httpStream, lineProcessor, logger, (err) => {
    if (err) console.error('Pipeline error:', err);
  });
}

// ============================================
// SECTION 4: EXAMPLE 3 - DUAL STREAM WITH BACKPRESSURE
// ============================================

function dualStreamExample() {
  console.log('\n🌊 EXAMPLE 3: Dual Stream (Producer-Consumer) with Backpressure\n');

  const backpressure = new NexusBackpressureClient(
    'producer-stream',
    'consumer-stream'
  );

  let produced = 0;
  let consumed = 0;

  // Producer stream
  const producer = new Readable({
    read() {
      if (produced < 500) {
        produced++;
        this.push(`data-${produced}\n`);
      } else {
        this.push(null);
      }
    }
  });

  // Consumer stream with backpressure
  const consumer = new Transform({
    async transform(chunk, encoding, callback) {
      consumed++;

      if (consumed % 50 === 0) {
        const flowControl = await backpressure.getCapacity(50);

        if (flowControl) {
          console.log(
            `[${consumed}] Processing | Load: ${flowControl.current_load}% | ` +
            `Level: ${flowControl.backpressure_level}`
          );

          if (flowControl.backpressure_level >= 2) {
            setTimeout(() => callback(null, chunk), 200);
            return;
          }
        }
      }

      callback(null, chunk);
    }
  });

  // Output
  const output = new Writable({
    write(chunk, encoding, callback) {
      if (consumed % 100 === 0) {
        console.log(`✓ Consumed ${consumed} items`);
      }
      callback();
    },
    final(callback) {
      console.log(`\n✅ Dual stream complete: produced=${produced}, consumed=${consumed}\n`);
      backpressure.reportMetrics({
        items_processed: consumed,
        flow_name: 'dual-stream',
        throughput_current: consumed / 10
      });
      callback();
    }
  });

  pipeline(producer, consumer, output, (err) => {
    if (err) console.error('Pipeline error:', err);
  });
}

// ============================================
// SECTION 5: EXAMPLE 4 - ERROR HANDLING IN STREAMS
// ============================================

function errorHandlingExample() {
  console.log('\n🌊 EXAMPLE 4: Stream Error Handling with Backpressure\n');

  const backpressure = new NexusBackpressureClient(
    'error-producer',
    'error-consumer'
  );

  let itemsProcessed = 0;
  let errors = 0;

  // Stream that may have errors
  const unreliableStream = new Readable({
    read() {
      for (let i = 0; i < 100; i++) {
        itemsProcessed++;
        // 5% error rate
        if (Math.random() < 0.05) {
          this.destroy(new Error('Random stream error'));
          return;
        }
        this.push(`item-${itemsProcessed}\n`);
      }
      this.push(null);
    }
  });

  // Error recovery handler
  const errorRecovery = new Transform({
    async transform(chunk, encoding, callback) {
      try {
        const flowControl = await backpressure.getCapacity(1);

        if (flowControl && flowControl.backpressure_level >= 2) {
          errors++;
          console.log(`⚠️  Skipping due to backpressure`);
          callback(); // Skip this item
          return;
        }

        callback(null, chunk);
      } catch (err) {
        errors++;
        console.error('Transform error:', err.message);
        callback(); // Continue despite error
      }
    }
  });

  // Output with error tracking
  const errorOutput = new Writable({
    write(chunk, encoding, callback) {
      if (itemsProcessed % 200 === 0) {
        console.log(
          `✓ ${itemsProcessed} items processed (${errors} errors)`
        );
      }
      callback();
    },
    final(callback) {
      console.log(`\n✅ Error handling complete`);
      console.log(`   Processed: ${itemsProcessed}`);
      console.log(`   Errors: ${errors}\n`);
      backpressure.reportMetrics({
        items_processed: itemsProcessed,
        error_count: errors,
        flow_name: 'error-recovery'
      });
      callback();
    }
  });

  pipeline(unreliableStream, errorRecovery, errorOutput, (err) => {
    if (err) {
      console.error('Pipeline error:', err.message);
      backpressure.reportMetrics({
        error_count: errors + 1,
        flow_name: 'error-recovery'
      });
    }
  });
}

// ============================================
// SECTION 6: MAIN ENTRY POINT
// ============================================

async function main() {
  console.log('🌊 Nexus Backpressure - Node.js Streams Integration Examples');
  console.log('   Make sure Nexus server is running on http://localhost:8080\n');

  try {
    // Run examples with delays between them
    simpleFileStreamExample();

    await new Promise(resolve => setTimeout(resolve, 3000));
    httpStreamExample();

    await new Promise(resolve => setTimeout(resolve, 3000));
    dualStreamExample();

    await new Promise(resolve => setTimeout(resolve, 3000));
    errorHandlingExample();
  } catch (err) {
    console.error('Example error:', err);
  }
}

// Run if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error);
}

export { NexusBackpressureClient };