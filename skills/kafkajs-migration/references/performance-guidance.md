# Performance Guidance for KafkaJS to confluent-kafka-javascript Migration

Recommendations derived from production migrations and community-reported issues. Surface these when migrating high-throughput applications.

## Producer Performance

- **Use fire-and-forget + flush** instead of awaiting each `send()`. Awaiting individual sends without setting `linger.ms: 0` can cause up to a 1000x performance decrease vs KafkaJS ([Issue #42](https://github.com/confluentinc/confluent-kafka-javascript/issues/42)). The underlying librdkafka batching needs time to accumulate messages — awaiting each send defeats this.
  ```javascript
  for (const msg of messages) {
    producer.send({ topic: 'test', messages: [{ value: msg }] });
  }
  await producer.flush({ timeout: 5000 });
  ```
- If awaiting each send is required, set `linger.ms` to `0` outside the `kafkaJS` block to disable batching delays:
  ```javascript
  const producer = kafka.producer({ kafkaJS: { /* ... */ }, 'linger.ms': 0 });
  ```
- The producer does not currently parallelize writes across multiple partitions. For workloads spread across many partitions, this can be a bottleneck compared to KafkaJS.

## Consumer Performance

- **Batch size**: `eachBatch` has a default max size of 32 (configurable via `js.consumer.max.batch.size`). This is significantly smaller than KafkaJS's batches and can be a bottleneck for high-throughput applications. Tune this value upward for workloads that depend on batch processing efficiency ([Issue #286](https://github.com/confluentinc/confluent-kafka-javascript/issues/286), [Issue #350](https://github.com/confluentinc/confluent-kafka-javascript/issues/350)).
- **`rebalanceTimeout` also sets max poll interval**: Setting it too low causes rebalance loops. Setting it too high delays rebalancing. Default is 300000ms (5 min). Message processing in `eachMessage`/`eachBatch` must complete within this time.
- **`maxWaitTimeInMs`**: Default changed from KafkaJS. Setting to 0 causes 100% CPU — avoid this. Tune carefully based on latency requirements.
- **Auto-commit interval**: Users migrating with long auto-commit intervals (e.g., 30s) should consider lowering to 5-10s to reduce consumer lag.
- **`partitionsConsumedConcurrently`**: Understand how this interacts with partition assignment and revocation during rebalances, especially with auto-scaling. Scaling consumers while this is set can lead to unexpected behavior during rebalance events.

## Memory Management

confluent-kafka-javascript (via librdkafka) typically shows higher resident memory than KafkaJS. This is usually memory fragmentation, not a leak — librdkafka allocates and frees memory in patterns that can cause the OS to retain pages.

- On Linux with glibc, set `MALLOC_TRIM_THRESHOLD_=131072` (128KB) as an environment variable to return memory to the OS more aggressively. In production migrations, this has reduced consumer memory from 2-3GB down to ~300MB.
- Setting this value too low increases CPU utilization from frequent malloc/free cycles. Benchmark to find the right balance for your workload.
- High resident memory can cause Kubernetes to interpret pods as resource-exhausted, triggering unnecessary scaling or liveness probe failures.

## Kubernetes Auto-Scaling

Node.js is single-threaded, so CPU-based auto-scaling behaves differently than with multi-threaded runtimes:

- **Lower CPU autoscaling threshold**: Consider `targetCPUUtilizationPercentage` around 25% rather than the typical 50-70%. By the time a single-core Node.js process hits 50% utilization, the consumer thread may already be maxed out, causing queues to build up and rebalance times to increase.
- **Reduce stabilization window**: A shorter `stabilizationWindowSeconds` (e.g., 15s) prevents flapping between scale-up and scale-down while still reacting quickly to load changes.
- **Liveness probes**: High message processing load can block the event loop, causing liveness probe timeouts and pod restarts. Ensure probes have generous timeouts during load spikes.

## Metrics and Observability

- **Consumer lag calculation differs** between KafkaJS and confluent-kafka-javascript. The two clients count lag differently, which can make KafkaJS appear to have lower lag in side-by-side comparisons. Validate your metrics methodology during migration rather than assuming a regression.
- Enable librdkafka statistics for detailed broker and consumer metrics:
  ```javascript
  const consumer = kafka.consumer({
    kafkaJS: { groupId: 'my-group' },
    'statistics.interval.ms': 5000,
  });
  ```
- Key metrics to monitor during migration validation: batch size, end-to-end latency, consumer offset lag (P50/P90/P99), CPU and memory utilization, producer and consumer queue sizes, requests per second, data skew across partitions, and rebalance frequency/duration.

## Known Community Issues

Reference these GitHub issues when troubleshooting performance:
- [#42](https://github.com/confluentinc/confluent-kafka-javascript/issues/42) — 1000x producer slowdown (resolved with fire-and-forget pattern)
- [#286](https://github.com/confluentinc/confluent-kafka-javascript/issues/286) / [#350](https://github.com/confluentinc/confluent-kafka-javascript/issues/350) — eachBatch size limitations
- [#284](https://github.com/confluentinc/confluent-kafka-javascript/issues/284) — High-performance configuration with 1000+ consumers
- [#326](https://github.com/confluentinc/confluent-kafka-javascript/issues/326) — CPU spikes
- [#369](https://github.com/confluentinc/confluent-kafka-javascript/issues/369) — Memory usage reports
