# WarpStream Client Optimization (.NET / librdkafka)

WarpStream is Kafka-protocol-compatible but architecturally different from Apache Kafka: stateless
Agents write directly to object storage (e.g., S3) instead of broker-local disks. **A few small
client configuration changes can result in 10-20x higher throughput.** Read this reference whenever
the user's target environment is WarpStream, and apply these overrides on top of the standard
config baseline from `KafkaConfig.cs`.

**Reference docs:** [WarpStream configuration recommendations](https://docs.warpstream.com/warpstream/kafka/configure-kafka-client/tuning-for-performance.md)

Confluent's .NET client (`Confluent.Kafka`) wraps **librdkafka** -- the same underlying library used
by `confluent-kafka-python`, `confluent-kafka-go`, and `node-rdkafka`. The config keys below are
librdkafka keys; where `ProducerConfig`/`ConsumerConfig` expose a typed C# property, use it. For keys
without a dedicated property, use `config.Set("key", "value")` (both `ProducerConfig` and
`ConsumerConfig` inherit this from `Confluent.Kafka.Config`).

---

## Why Defaults Must Change

| Kafka assumption | WarpStream reality |
|---|---|
| Produce latency is single-digit ms | Produce latency is ~250ms p50 / ~500ms p99 (data must flush to object storage) |
| Each broker owns specific partitions | Any Agent can serve any partition -- Agents are stateless and interchangeable |
| `fetch.min.bytes` controls batching | `fetch.min.bytes` is **not supported** by WarpStream |
| Idempotent producers have minimal overhead | Idempotent producers reduce throughput on WarpStream (see below) |

---

## Important: `message.max.bytes` is client-global

Unlike the Java client (where the producer's `max.request.size` and the consumer's `fetch.max.bytes`
are independent), librdkafka's `message.max.bytes` applies to **both** producer and consumer
instances built from configs that set it. If `KafkaConfig.BaseProducerConfig` and
`BaseConsumerConfig` both derive from a shared base that sets `message.max.bytes`, the consumer
inherits it. librdkafka then enforces `fetch.max.bytes >= message.max.bytes` and
`max.partition.fetch.bytes >= message.max.bytes` at consumer construction -- violating either raises
`KafkaException` with `_INVALID_ARG` ("fetch.max.bytes must be >= message.max.bytes"). The consumer
values below are sized to satisfy this constraint against the producer's `message.max.bytes =
64000000`.

## Producer Overrides

```csharp
var producerConfig = KafkaConfig.BaseProducerConfig(env);
producerConfig.EnableIdempotence = false;               // disable idempotence for throughput (see EOS note below)
producerConfig.MaxInFlight = 1_000_000;                 // with idempotence off, raise in-flight requests dramatically
producerConfig.LingerMs = 100;                          // larger batches amortize object-storage write latency
producerConfig.BatchSize = 16_000_000;
producerConfig.CompressionType = CompressionType.Lz4;   // WarpStream decompresses and recompresses for storage
producerConfig.Set("queue.buffering.max.kbytes", "1048576");
producerConfig.Set("queue.buffering.max.messages", "1000000");
producerConfig.Set("message.max.bytes", "64000000");
producerConfig.Set("batch.num.messages", "100000");
producerConfig.Set("sticky.partitioning.linger.ms", "25");
producerConfig.Set("partitioner", "consistent_random");
producerConfig.Set("request.timeout.ms", "30000");
producerConfig.Set("metadata.max.age.ms", "60000");        // reduce unnecessary metadata refreshes
producerConfig.Set("metadata.recovery.strategy", "rebootstrap"); // recover cleanly when Agents scale
```

## Consumer Overrides

```csharp
var consumerConfig = KafkaConfig.BaseConsumerConfig(env);
// Must be >= producer message.max.bytes (64000000) -- see the client-global note above.
consumerConfig.FetchMaxBytes = 67_108_864;
consumerConfig.MaxPartitionFetchBytes = 67_108_864;
consumerConfig.Set("fetch.wait.max.ms", "10000"); // fetch.min.bytes is NOT supported -- this controls wait time instead
// Do NOT set FetchMinBytes / "fetch.min.bytes" -- unsupported by WarpStream.
```

## librdkafka Version Notes

- **librdkafka < 2.8** (bundled transitively via `Confluent.Kafka`'s native `librdkafka.redist`
  dependency): leader epoch mismatch errors. WarpStream returns epoch 0; librdkafka 2.4+ expects
  monotonically increasing epochs. Workaround: append `ws_sle=true` to `client.id`. Best fix: pin a
  `Confluent.Kafka` version that bundles librdkafka 2.8+.
- **librdkafka < 2.10:** retains stale Agent IP addresses during rolling restarts. Pin a
  `Confluent.Kafka` version that bundles librdkafka 2.10.0+.

---

## Idempotent Producers and EOS

Enabling idempotent producers (`EnableIdempotence = true`) reduces throughput on WarpStream, because
idempotence limits `max.in.flight.requests.per.connection` to 5, and WarpStream's higher produce
latency means those slots are occupied longer. The overrides above already set
`EnableIdempotence = false` -- this is the recommended default. If exactly-once semantics are
required, it will work; plan for additional capacity to compensate for the reduced concurrency.

---

## Zone-Aware Routing

WarpStream charges for cross-AZ network transfer. Append `ws_az=<availability-zone>` to the client
ID to route the client to an Agent in the same availability zone:

```csharp
producerConfig.ClientId = "my-app,ws_az=us-east-1a";
```

Without it, clients may be routed cross-AZ, incurring ~$0.05/GB on AWS. **Do NOT** enable rack-aware
consumer assignment (`client.rack`) -- it causes unnecessary rebalances since Agents are stateless.

---

## Sticky Partitioning

WarpStream Agents batch records across topics and partitions into combined object-storage files.
Larger batches mean fewer S3 PUTs, which means lower cost and higher throughput.

- **Use a `null` message key** whenever ordering between records is not required -- this enables
  sticky partitioning, where the client accumulates records for a single partition before rotating.
- Only use message keys when entity-based ordering is genuinely needed (e.g., all events for the
  same `orderId` must land on the same partition).

---

## Things That Don't Apply on WarpStream

| Config | Why irrelevant |
|---|---|
| `replication.factor` | Always returns 3 (cosmetic). Durability is from object storage. |
| `fetch.min.bytes` | Not supported. Use `fetch.wait.max.ms` instead. |
| `log.dirs` / `broker.id` | No local disks, no static broker identities. |

---

## Quick Checklist

When generating code or configs for a WarpStream target, verify:

- [ ] `EnableIdempotence = false` (unless the user requires exactly-once semantics -- inform them of the throughput tradeoff)
- [ ] `MaxInFlight` raised to 1,000,000
- [ ] `LingerMs = 100` (or 10-25 for low-latency)
- [ ] `BatchSize` increased (16MB)
- [ ] `FetchMaxBytes` / `MaxPartitionFetchBytes` >= the producer's `message.max.bytes` (67108864 to satisfy 64000000)
- [ ] `fetch.wait.max.ms = 10000` set via `.Set(...)`
- [ ] `fetch.min.bytes` / `FetchMinBytes` NOT set (unsupported)
- [ ] `ClientId` includes `ws_az=<az>` for zone-aware routing
- [ ] `CompressionType = CompressionType.Lz4`
- [ ] No `replication.factor` tuning (cosmetic on WarpStream)
