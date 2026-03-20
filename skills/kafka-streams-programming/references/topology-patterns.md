# Topology Patterns Reference

Use-case driven guide. Start from what the user wants to accomplish, then map to the right KS primitives.

**Docs:** [DSL API](https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html) | [Tutorials](https://github.com/confluentinc/tutorials)

## Table of Contents
- [Stateless Patterns](#stateless-patterns) — filter, map, route, split, merge
- [Enrichment Patterns](#enrichment-patterns) — stream-table join, GlobalKTable, FK join
- [Joins Decision Tree](#joins-decision-tree) — choosing the right join type
- [Aggregation Patterns](#aggregation-patterns) — count, sum, reduce, cogroup
- [Windowing Decision Tree](#windowing-decision-tree) — tumbling, hopping, session, sliding
- [Suppression (Final Results)](#suppression) — emit only when window closes
- [Deduplication](#deduplication) — exactly-once and idempotent patterns
- [Stream Splitting and Routing](#stream-splitting) — modern split() API
- [Processor API](#processor-api) — when DSL isn't enough
- [Exactly-Once Semantics](#exactly-once) — decision tree, when you need it vs at-least-once, EOS config
- [Interactive Queries](#interactive-queries) — queryable state stores
- [Versioned KTables](#versioned-ktables) — temporal correctness for joins
- [Named Operator Rules](#named-operator-rules) — which operators accept Named.as()
- [Recovery Configuration](#recovery) — standby replicas, restoration tuning
- [Assignment Strategy](#assignment-strategy) — KIP-1071, static membership

---

## Stateless Patterns

For: filtering, transforming, routing records without maintaining state. Simplest to operate.

### Filter
```java
KStream<String, Order> orders = builder.stream("orders",
    Consumed.with(Serdes.String(), orderSerde).withName("source-orders"));

orders.filter((key, order) -> order.getAmount() > 100.0, Named.as("filter-large"))
    .to("large-orders", Produced.with(Serdes.String(), orderSerde).withName("sink-large"));
```

### Map / Transform
```java
orders.mapValues(order -> new OrderSummary(order.getId(), order.getTotal()),
        Named.as("map-to-summary"))
    .to("order-summaries", Produced.with(Serdes.String(), summarySerde).withName("sink-summaries"));
```

### FlatMap (one-to-many)
```java
// Split an order with multiple line items into individual records
orders.flatMapValues(order -> order.getLineItems(), Named.as("explode-items"))
    .to("line-items", Produced.with(Serdes.String(), itemSerde).withName("sink-items"));
```

### Stateless Config
```properties
num.stream.threads=4              # Match CPU cores — no state overhead
commit.interval.ms=1000           # Lower latency (no state store flush)
consumer.fetch.min.bytes=1048576  # 1MB — batch for throughput
producer.batch.size=65536         # 64KB batch
producer.linger.ms=20
producer.compression.type=lz4
```

---

## Enrichment Patterns

For: "I need to add information from another topic to my stream."

### Pattern 1: Stream-Table Join (co-partitioned lookup)
**Use when:** Lookup topic is keyed by the same key as the stream, and you want automatic updates when lookup data changes.

```java
KStream<String, Order> orders = builder.stream("orders",
    Consumed.with(Serdes.String(), orderSerde).withName("source-orders"));
KTable<String, Customer> customers = builder.table("customers",
    Consumed.with(Serdes.String(), customerSerde).withName("source-customers"));

// Stream-table join — no Named.as() parameter (won't compile)
KStream<String, EnrichedOrder> enriched = orders.leftJoin(customers,
    (order, customer) -> new EnrichedOrder(order, customer));

enriched.to("enriched-orders",
    Produced.with(Serdes.String(), enrichedSerde).withName("sink-enriched"));
```

**Requirements:**
- Both topics MUST be co-partitioned (same key, same number of partitions)
- If the stream key doesn't match the table key, use `selectKey()` first (triggers repartition)
- Stream-table joins do NOT accept `Named.as()` — see [Named Operator Rules](#named-operator-rules)

### Pattern 2: GlobalKTable Join (broadcast lookup)
**Use when:** Lookup data is small, the join key isn't the stream key, or you can't co-partition.

```java
GlobalKTable<String, Product> products = builder.globalTable("products",
    Consumed.with(Serdes.String(), productSerde).withName("source-products"));

KStream<String, Order> orders = builder.stream("orders",
    Consumed.with(Serdes.String(), orderSerde).withName("source-orders"));

// KeyValueMapper extracts the join key from the stream record
KStream<String, EnrichedOrder> enriched = orders.join(products,
    (orderId, order) -> order.getProductId(),   // extract FK from stream record
    (order, product) -> new EnrichedOrder(order, product));
```

**Key differences from KTable join:**
- GlobalKTable is replicated to ALL instances (not partitioned)
- No co-partitioning requirement — can join on any field
- Higher memory cost — entire table on every instance
- No changelog topic — reads directly from source topic on startup

### Pattern 3: Foreign Key Table-Table Join
**Use when:** Both sides are KTables and the join key is a foreign key (not the primary key).

```java
KTable<String, TrackPurchase> purchases = builder.table("purchases",
    Consumed.with(Serdes.String(), purchaseSerde).withName("source-purchases"));
KTable<Long, Album> albums = builder.table("albums",
    Consumed.with(Serdes.Long(), albumSerde).withName("source-albums"));

// FK join — extractor pulls the FK from the left table's value
KTable<String, MusicInterest> joined = purchases.join(albums,
    TrackPurchase::getAlbumId,    // foreign key extractor
    (purchase, album) -> new MusicInterest(purchase, album));
```

**Requirements:**
- The FK extractor must return the type of the right table's key
- Creates internal repartition topics — generates cross-partition traffic
- Right table updates propagate to matching left table records

---

## Joins Decision Tree

```
"I need to combine data from two topics"
    │
    ├─ Both are unbounded event streams? ──→ Stream-Stream Join
    │   └─ Requires a time window (JoinWindows)
    │   └─ Both must be co-partitioned
    │
    ├─ One is a stream, one is reference/lookup data?
    │   ├─ Same key? ──→ Stream-Table Join (KTable)
    │   ├─ Different key but lookup is small? ──→ GlobalKTable Join
    │   └─ Different key and too large for GlobalKTable? ──→ selectKey() + Stream-Table Join
    │
    └─ Both are compacted topics (latest value per key)?
        ├─ Same key? ──→ Table-Table Join
        └─ Different key (foreign key)? ──→ FK Table-Table Join
```

### Stream-Stream Join
```java
KStream<String, OrderEvent> orders = builder.stream("orders", ...);
KStream<String, PaymentEvent> payments = builder.stream("payments", ...);

// Join orders with payments within a 5-minute window
KStream<String, OrderPayment> joined = orders.join(payments,
    (order, payment) -> new OrderPayment(order, payment),
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
    StreamJoined.with(Serdes.String(), orderSerde, paymentSerde)
        .withName("order-payment-join"));
```

**Join types:**
| Method | Left record arrives, right not yet | Right arrives, left not yet |
|--------|-------|-------|
| `join()` | Waits | Waits |
| `leftJoin()` | Emits with null right | Waits |
| `outerJoin()` | Emits with null right | Emits with null left |

### Table-Table Join
```java
KTable<String, UserProfile> profiles = builder.table("profiles", ...);
KTable<String, UserPrefs> prefs = builder.table("preferences", ...);

KTable<String, UserFull> full = profiles.join(prefs,
    (profile, pref) -> new UserFull(profile, pref),
    Named.as("profile-prefs-join"));
```

---

## Aggregation Patterns

### Simple Aggregation (count/sum/reduce)
```java
KTable<String, Long> orderCounts = orders
    .groupByKey(Grouped.with(Serdes.String(), orderSerde).withName("group-orders"))
    .count(Named.as("count-orders"),
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-counts-store"));
```

### Custom Aggregation
```java
KTable<String, AccountSummary> summary = transactions
    .groupByKey(Grouped.with(Serdes.String(), txnSerde).withName("group-by-account"))
    .aggregate(
        AccountSummary::new,                          // initializer
        (accountId, txn, agg) -> agg.add(txn),       // adder
        Named.as("aggregate-account"),
        Materialized.<String, AccountSummary, KeyValueStore<Bytes, byte[]>>
            as("account-summary-store")
            .withKeySerde(Serdes.String())
            .withValueSerde(summarySerde));
```

### Re-keying Before Aggregation
```java
// If you need to aggregate by a different key than the record key
orders
    .selectKey((key, order) -> order.getRegion(), Named.as("rekey-by-region"))
    // selectKey triggers automatic repartition
    .groupByKey(Grouped.with(Serdes.String(), orderSerde).withName("group-by-region"))
    .count(Named.as("count-by-region"), Materialized.as("region-counts-store"));
```

### Cogrouping (multiple inputs into one aggregation)
```java
// Aggregate login events from three different app streams into one rollup
KGroupedStream<String, LoginEvent> app1 = app1Stream.groupByKey(...);
KGroupedStream<String, LoginEvent> app2 = app2Stream.groupByKey(...);
KGroupedStream<String, LoginEvent> app3 = app3Stream.groupByKey(...);

Aggregator<String, LoginEvent, LoginRollup> aggregator =
    (key, event, rollup) -> rollup.add(event);

KTable<String, LoginRollup> rollup = app1.cogroup(aggregator)
    .cogroup(app2, aggregator)
    .cogroup(app3, aggregator)
    .aggregate(LoginRollup::new, Materialized.with(Serdes.String(), rollupSerde));
```

### Stateful Config
```properties
statestore.cache.max.bytes=52428800   # 50MB — deduplicates updates within cache
state.dir=/var/kafka-streams/state    # Fast local storage, NOT NFS
processing.guarantee=at_least_once    # Default. Use exactly_once_v2 if needed.
```

### RocksDB Memory Estimation
```
Per-instance memory = block_cache(50MB) + write_buffers(16MB x 3) = 98 MB
Non-windowed: 1 RocksDB instance per partition per store
Windowed: up to 3 RocksDB instances per partition (time segments)

Example: 10 partitions, 1 store = 10 x 98 MB = ~1 GB off-heap
Example: 10 partitions, 1 windowed store = 10 x 3 x 98 MB = ~3 GB off-heap
```

This is OFF-HEAP memory — not controlled by -Xmx. Size containers accordingly.

### State Store TTL
```java
// For KTables that accumulate unbounded data, expire old entries
builder.table("input", Consumed.with(...).withName("source"),
    Materialized.<String, Value, KeyValueStore<Bytes, byte[]>>as("my-store")
        .withRetention(Duration.ofDays(30)));   // TTL
```

---

## Windowing Decision Tree

```
"I need to aggregate over time periods"
    │
    ├─ Fixed, non-overlapping intervals? (e.g., "per hour", "per day")
    │   └─ Tumbling Window
    │
    ├─ Fixed intervals that overlap? (e.g., "5-min window every 1 min")
    │   └─ Hopping Window
    │
    ├─ Continuous monitoring — every record sees the full window around it?
    │   (e.g., "flag if >5 events in any 10-min period", fraud detection, rate limiting)
    │   └─ Sliding Window (SlidingWindows — NOT TimeWindows)
    │
    ├─ Based on activity gaps? (e.g., "user session ends after 30 min idle")
    │   └─ Session Window
    │
    └─ For joining two streams within a time range?
        └─ JoinWindows (different API — see Joins Decision Tree)
```

### Tumbling Window
```java
.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
// or with grace period for late events:
.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
```

### Hopping Window
```java
.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1))
    .advanceBy(Duration.ofMinutes(1)))
```

### Sliding Window
Use `SlidingWindows` for continuous monitoring where every record needs to see its full time context. Unlike hopping windows (which use `TimeWindows.advanceBy()`), sliding windows create a new window for every distinct pair of record timestamps — no events are missed at window boundaries.

```java
// Fraud detection: flag if >5 transactions in any 10-min period
.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)))
// or with grace period:
.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(10), Duration.ofSeconds(30)))
```

**When to use SlidingWindows vs TimeWindows:**
- **SlidingWindows** — fraud detection, rate limiting, anomaly detection — any case where you need "in any N-minute period" semantics. No events slip through boundary gaps.
- **TimeWindows (hopping)** — periodic reports, dashboards — when you want results at regular intervals and minor boundary effects are acceptable.

### Session Window
```java
// Groups records into sessions based on activity — gap = inactivity threshold
.windowedBy(SessionWindows.ofInactivityGapAndGrace(
    Duration.ofMinutes(5),    // inactivity gap
    Duration.ofSeconds(30)))  // grace period for late arrivals
```

**Session windows and suppression:** Session windows emit intermediate results as new records extend or merge sessions. If you only want the final session summary (emitted when the session closes), add suppression:

```java
.windowedBy(SessionWindows.ofInactivityGapAndGrace(
    Duration.ofMinutes(30), Duration.ofSeconds(30)))
.aggregate(initializer, aggregator, sessionMerger,
    Materialized.as("session-store"))
.suppress(Suppressed.untilWindowCloses(
    Suppressed.BufferConfig.maxRecords(10000).shutDownWhenFull()))
```

This is the recommended pattern for sessionization use cases where downstream consumers should only see completed sessions.

### Window Retention
```java
// Control how long windowed state is kept
Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts-store")
    .withRetention(Duration.ofDays(7))   // 7 days of windows
```

### Windowed Key Handling
Windowed aggregations produce `Windowed<K>` keys. Unwrap before writing to output:
```java
windowedTable.toStream(Named.as("to-stream"))
    .map((windowed, count) -> KeyValue.pair(windowed.key(), count),
        Named.as("unwrap-key"))
    .to("output", Produced.with(Serdes.String(), Serdes.Long()).withName("sink"));
```

### Windowed Changelog Topics
Windowed changelogs automatically use `cleanup.policy=compact,delete` (not just `compact`). This ensures expired window segments are cleaned up. Non-windowed changelogs use `compact` only.

---

## Suppression

Emit only the **final** result of a window (not intermediate updates):

```java
windowedCounts
    .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
    .toStream(Named.as("final-to-stream"))
    .map((windowed, count) -> KeyValue.pair(windowed.key(), count),
        Named.as("unwrap-key"))
    .to("final-counts", Produced.with(Serdes.String(), Serdes.Long()).withName("sink"));
```

**Production: use bounded buffers** to prevent OOM:
```java
.suppress(Suppressed.untilWindowCloses(
    Suppressed.BufferConfig.maxRecords(10000).shutDownWhenFull()))
```

Monitor: `suppression-buffer-size-avg`, `suppression-buffer-size-max`.

---

## Deduplication

### Using State Store (within a time window)
```java
// Track seen IDs in a windowed store, filter duplicates
KStream<String, Event> deduped = events
    .transform(() -> new DeduplicationTransformer<>(
        Duration.ofMinutes(10),    // dedup window
        event -> event.getId()),   // key extractor
    "dedup-store");
```

### Using Processor API
```java
// More control with Processor API
builder.addStateStore(Stores.windowStoreBuilder(
    Stores.persistentWindowStore("dedup-store",
        Duration.ofMinutes(10), Duration.ofMinutes(10), false),
    Serdes.String(), Serdes.String()));

events.process(() -> new DeduplicationProcessor(), "dedup-store");
```

---

## Stream Splitting

Modern API (replaces deprecated `branch()`):

```java
builder.stream("appearances", Consumed.with(Serdes.String(), appearanceSerde)
        .withName("source-appearances"))
    .split(Named.as("split-"))
    .branch((key, v) -> "drama".equals(v.getGenre()),
        Branched.withConsumer(ks -> ks.to("drama-topic",
            Produced.with(Serdes.String(), appearanceSerde).withName("sink-drama"))))
    .branch((key, v) -> "fantasy".equals(v.getGenre()),
        Branched.withConsumer(ks -> ks.to("fantasy-topic",
            Produced.with(Serdes.String(), appearanceSerde).withName("sink-fantasy"))))
    .defaultBranch(Branched.withConsumer(ks -> ks.to("other-topic",
        Produced.with(Serdes.String(), appearanceSerde).withName("sink-other"))));
```

---

## Processor API

Use when the DSL can't express your logic — e.g., conditional forwarding, accessing record metadata, scheduled operations (punctuators), or custom state store interactions.

### FixedKeyProcessor (key doesn't change)
```java
public class EnrichmentProcessor implements FixedKeyProcessor<String, Order, EnrichedOrder> {
    private FixedKeyProcessorContext<String, EnrichedOrder> context;
    private KeyValueStore<String, Customer> customerStore;

    @Override
    public void init(FixedKeyProcessorContext<String, EnrichedOrder> context) {
        this.context = context;
        this.customerStore = context.getStateStore("customer-store");
    }

    @Override
    public void process(FixedKeyRecord<String, Order> record) {
        Customer customer = customerStore.get(record.key());
        if (customer != null) {
            context.forward(record.withValue(new EnrichedOrder(record.value(), customer)));
        }
    }
}

// Wire into topology
stream.processValues(() -> new EnrichmentProcessor(),
    Named.as("enrich"), "customer-store");
```

### Processor (key can change)
```java
public class RekeyProcessor implements Processor<String, Event, String, Event> {
    private ProcessorContext<String, Event> context;

    @Override
    public void init(ProcessorContext<String, Event> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Event> record) {
        // Forward creates a SHARED reference — use record.withValue() to copy
        context.forward(record.withKey(record.value().getRegion()));
    }
}
```

**Record sharing gotcha:** `context.forward(record)` shares the record reference. If you forward the same record multiple times with modifications, use `record.withValue(newVal)` to create a copy.

### Punctuators (scheduled operations)
```java
@Override
public void init(ProcessorContext<String, Event> context) {
    // STREAM_TIME: fires based on event timestamps (advances with data)
    context.schedule(Duration.ofSeconds(5), PunctuationType.STREAM_TIME,
        timestamp -> flushBuffer());

    // WALL_CLOCK_TIME: fires based on system clock (independent of data flow)
    context.schedule(Duration.ofSeconds(20), PunctuationType.WALL_CLOCK_TIME,
        timestamp -> emitHeartbeat());
}
```

**STREAM_TIME vs WALL_CLOCK_TIME:**
- STREAM_TIME only advances when records arrive — punctuator won't fire if there's no data
- WALL_CLOCK_TIME fires regardless of data flow — use for heartbeats, timeouts, periodic flushes

### Custom TimestampExtractor
```java
public class EventTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof Event) {
            long eventTime = ((Event) record.value()).getTimestamp();
            if (eventTime > 0) return eventTime;
        }
        // Fall back to partition time if event time is invalid
        return partitionTime;
    }
}
```

Built-in extractors:
- `FailOnInvalidTimestamp` — throws on negative timestamps (default)
- `LogAndSkipOnInvalidTimestamp` — logs and skips bad records
- `UsePartitionTimeOnInvalidTimestamp` — falls back to partition time
- `WallclockTimestampExtractor` — ignores event time, uses system clock

---

## Exactly-Once

### Do You Actually Need EOS?

Most users who ask for "exactly-once" don't need Kafka Streams EOS. EOS is specifically about **atomic read-process-write within a Kafka Streams topology** — it guarantees that consuming an input record, updating state stores, and producing output records either all happen or none happen, even through failures and rebalances.

**Decision tree:**

```
"I need exactly-once processing"
    │
    ├─ Are duplicates in your OUTPUT topic the actual problem?
    │   │
    │   ├─ Yes, and the output consumer can't handle duplicates
    │   │   (e.g., financial ledger, billing events, cross-system sync)
    │   │   │
    │   │   ├─ Is the output consumed by another Kafka Streams app or Kafka consumer?
    │   │   │   └─ EOS is appropriate. Use exactly_once_v2.
    │   │   │
    │   │   └─ Is the output consumed by an external system (database, API)?
    │   │       └─ EOS only helps the Kafka side. The external write still needs
    │   │          idempotency (upsert, dedup key, conditional write).
    │   │
    │   └─ No — the consumer can tolerate or deduplicate occasional repeats
    │       └─ at_least_once + idempotent consumer design.
    │           Cheaper, simpler, higher throughput.
    │
    ├─ Is the concern about STATE STORE correctness during failures?
    │   └─ State stores are already protected by changelog topics.
    │       at_least_once replays may reprocess records, but the state
    │       converges to the same result for idempotent operations
    │       (count, sum, max, overwrite). EOS is only needed when
    │       reprocessing would produce DIFFERENT state (e.g., appending
    │       to a list, non-idempotent side effects).
    │
    └─ Is the concern about processing records MORE THAN ONCE?
        └─ at_least_once may reprocess after a failure, but each record
           is processed AT LEAST once. If your logic is idempotent
           (same input → same output), duplicates don't matter.
```

**When EOS is the right choice:**
- Financial transactions where duplicate output records cause real money problems
- Cross-topic atomic writes — output to multiple topics must be all-or-nothing
- Non-idempotent state mutations where replay would corrupt state
- Regulatory/compliance requirements mandating exactly-once guarantees

**When at-least-once is sufficient (most apps):**
- Aggregations (count, sum, min, max) — idempotent by nature, state converges
- Enrichment joins — a duplicate enriched record is harmless
- Filtering/routing — duplicates get filtered or routed again identically
- Any case where the downstream consumer upserts by key

### EOS as an Architectural Choice

EOS is a significant architectural decision, not a configuration toggle. Understand the real costs before enabling it.

**The real cost (from production experience):**
- **Throughput:** 10-30% lower than at-least-once. Every commit becomes a transaction requiring coordination with the transaction coordinator.
- **Commit frequency:** EOS defaults to 100ms commit intervals (vs 30s for at-least-once), increasing transactional overhead.
- **Write amplification:** ~2x for broker sizing. EOS adds transactional markers and coordination overhead to changelog writes.
- **Broker resources:** Each transaction requires coordination with the transaction coordinator. Transaction state is replicated (default `transaction.state.log.replication.factor=3`), requiring at least 3 brokers.
- **Error amplification:** With EOS, any unhandled exception (NPE, ClassCastException) forces KS to abort the transaction, wipe local state, and restore from changelog. A simple bug becomes a multi-hour outage for large state stores. In production, EOS + NPE has caused 105M+ consumer lag. Switching to at-least-once is the immediate mitigation.

**CC-specific considerations:**
- Transactional IDs expire after 7 days idle on CC (non-configurable `transactional.id.expiration.ms`). App restart after 7+ days idle → `InvalidPidMappingException`. This has caused P1 outages.
- On CC, users cannot run `kafka-transactions` CLI to detect hanging transactions — they must contact support.

### EOS Configuration

See `config-baseline.md` § EOS Configuration for the full configuration reference, checklist, and enforced producer properties.

**Key points:**
- Always use `exactly_once_v2` (KIP-447, AK 3.0+) — v1 is deprecated
- Do NOT set `commit.interval.ms` — EOS overrides to 100ms for correctness
- Set `transaction.timeout.ms` >= your worst-case processing time (default 10s is too low for most apps)
- Downstream consumers must set `isolation.level=read_committed`

**How it works internally:** Each commit is a Kafka transaction that atomically writes: (1) output records to output topics, (2) changelog records for state stores, (3) consumer offset commits. If anything fails, the entire transaction is aborted and the task replays from the last committed offset. Producer fencing ensures that after a rebalance, the old task owner can't write stale data.

### EOS Performance Impact

| Metric | At-Least-Once | EOS (exactly_once_v2) |
|--------|--------------|----------------------|
| Commit interval | 30,000ms (default) | 100ms (fixed for correctness) |
| Throughput | Baseline | 10-30% lower |
| Broker write amplification | ~1.5x (changelog) | ~2x (changelog + transaction markers) |
| Min brokers required | 1 (dev) | 3 (transaction state replication) |
| Producer overhead per commit | Offset commit only | Full transaction: begin, produce, commit offsets, end |

### EOS Failure Modes

For detailed diagnostics, root cause analysis, and emergency procedures for each failure mode, see `debugging.md` § EOS / Transaction Issues.

**Common failure patterns:**
- **Transaction timeout cascade** — processing exceeds `transaction.timeout.ms` → abort → rebalance → state restoration → timeout loop
- **InvalidPidMappingException (CC)** — transactional ID expires after 7 days idle
- **Error amplification** — unhandled exceptions + EOS = state wipe + multi-hour restoration
- **COORDINATOR_NOT_AVAILABLE** — transaction coordinator broker failure

### EOS Gotchas

1. **`transaction.timeout.ms` defaults to 10s** — too low for most production workloads. Start with 60s.
2. **Single-broker dev:** Set `transaction.state.log.replication.factor=1` in docker-compose.
3. **EOS + interactive queries:** IQ reads may include uncommitted data during a transaction (committed within 100ms).
4. **EOS + broker failure:** Apps may require manual restart after broker recovery. In production, single broker failures have disconnected 40+ KStreams apps simultaneously.
5. **EOS + state restoration:** If restoration exceeds `transaction.timeout.ms`, creates a restore-timeout-rebalance loop. Use `num.standby.replicas=1` and persistent volumes.

---

## Interactive Queries

Turn your Streams app into both a processor and a serving layer.

### Setup
```properties
application.server=localhost:8080   # Unique per instance
```

### Query State Stores
```java
ReadOnlyKeyValueStore<String, AccountSummary> store = streams.store(
    StoreQueryParameters.fromNameAndType("account-store",
        QueryableStoreTypes.keyValueStore()));

AccountSummary result = store.get("account-123");
```

### IQv2 (typed queries — Kafka 3.2+)
```java
StateQueryRequest<KeyValueIterator<String, AccountSummary>> request =
    StateQueryRequest.inStore("account-store")
        .withQuery(RangeQuery.withNoBounds())
        .withPartitions(Set.of(0, 1, 2));

StateQueryResult<KeyValueIterator<String, AccountSummary>> result =
    streams.query(request);
```

Query types: `KeyQuery`, `RangeQuery`, `TimestampedKeyQuery`, `WindowKeyQuery`, `WindowRangeQuery`, `VersionedKeyQuery`, `MultiVersionedKeyQuery`

### Multi-Instance Discovery
```java
KeyQueryMetadata metadata = streams.queryMetadataForKey(
    "account-store", key, Serdes.String().serializer());

if (metadata.activeHost().equals(thisHost)) {
    // Query local store
} else {
    // Proxy to metadata.activeHost()
}
```

### Test IQ with TopologyTestDriver
```java
@Test
void testStoreContents() {
    try (var driver = new TopologyTestDriver(topology, props)) {
        var inputTopic = driver.createInputTopic("input", ...);
        inputTopic.pipeInput("key1", record1);

        var store = driver.getKeyValueStore("my-store");
        assertNotNull(store.get("key1"));
    }
}
```

---

## Versioned KTables

For temporal join correctness — ensures the KTable version used in a join matches the stream record's timestamp, not the latest version.

```java
VersionedBytesStoreSupplier supplier = Stores.persistentVersionedKeyValueStore(
    "versioned-store", Duration.ofMinutes(10));

KTable<String, String> table = builder.table("input",
    Consumed.with(Serdes.String(), Serdes.String()),
    Materialized.<String, String>as(supplier)
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String()));
```

**When to use:** Stream-table joins where the table is updated frequently and you need the value as of the stream record's timestamp, not the latest value. Without versioning, a late-arriving table update can retroactively change join results.

---

## Named Operator Rules

Not all operators accept `Named.as()`. Getting this wrong causes compilation errors.

**Accepts `Named.as()`:** `filter`, `mapValues`, `map`, `flatMap`, `flatMapValues`, `selectKey`, `peek`, `merge`, `split`, `aggregate`, `count`, `reduce`, `toStream`

**Uses `Grouped.as()`:** `groupByKey`, `groupBy`

**Stream-table joins — NO `Named.as()`:**
```java
// CORRECT
stream.leftJoin(table, (streamVal, tableVal) -> merge(streamVal, tableVal));
// WRONG — won't compile
stream.leftJoin(table, joiner, Named.as("my-join"));
```

**Stream-stream joins — use `StreamJoined.withName()`:**
```java
stream1.join(stream2, joiner, JoinWindows.of(...),
    StreamJoined.with(keySerde, val1Serde, val2Serde).withName("my-join"));
```

**Table-table joins — use `Named.as()`:**
```java
table1.join(table2, joiner, Named.as("table-join"));
```

---

## Recovery

Applies to all stateful patterns. Recovery is the #1 production issue.

### Standby Replicas

**KIP-1071 note:** Client-side `num.standby.replicas` is NOT supported with `group.protocol=streams` (see § Assignment Strategy). If the user needs standby replicas, remove `group.protocol=streams` to use the classic protocol. Inform the user to file an issue (see § Assignment Strategy for details).

```properties
# Requires classic protocol (no group.protocol=streams)
num.standby.replicas=1              # Warm standby on other instances
acceptable.recovery.lag=10000       # Max lag before standby is "caught up"
probing.rebalance.interval.ms=300000  # 5 min — check standby readiness
```

### State Restoration Tuning
```properties
consumer.max.poll.interval.ms=600000  # 10 min — tolerate slow restoration
consumer.max.poll.records=500
restore.consumer.fetch.max.bytes=52428800  # 50 MB — speed up restoration
```

Root cause of rebalancing loops: RocksDB compaction during restoration blocks the poll thread → exceeds `max.poll.interval.ms` → evicted → rebalance → restore again.

### Custom RocksDB Config
```java
public class CustomRocksDBConfig implements RocksDBConfigSetter {
    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        tableConfig.setBlockCacheSize(25 * 1024 * 1024L);  // 25 MB
        options.setMaxWriteBufferNumber(2);
        options.setWriteBufferSize(8 * 1024 * 1024L);     // 8 MB
        options.setTableFormatConfig(tableConfig);
    }
    @Override
    public void close(String storeName, Options options) {}
}
// Register: rocksdb.config.setter=com.example.CustomRocksDBConfig
```

---

## Assignment Strategy

### KIP-1071 (default)
```properties
group.protocol=streams   # Default in all generated apps. AK 4.2+ / CP 8.2+ required.
```

Does NOT fall back gracefully — crashes with `UnsupportedVersionException` if broker doesn't support it.

**KIP-1071 limitations (as of AK 4.2):** The following features are NOT yet supported with `group.protocol=streams`. Using them throws a `ConfigException` or `UnsupportedOperationException` at startup:

| Feature | What happens | JIRA |
|---|---|---|
| **Static membership** (`group.instance.id`) | `ConfigException` at startup | [KAFKA-20169](https://issues.apache.org/jira/browse/KAFKA-20169) (In Progress) |
| **Regex topic patterns** (pattern subscriptions) | `UnsupportedOperationException` | [KAFKA-20171](https://issues.apache.org/jira/browse/KAFKA-20171) (Open) |
| **Standby replicas** via client config (`num.standby.replicas`) | Must be configured broker-side | [KAFKA-20116](https://issues.apache.org/jira/browse/KAFKA-20116) (In Progress) |
| **Warm-up replicas** (`max.warmup.replicas`) | Ignored, warning logged | [KAFKA-20116](https://issues.apache.org/jira/browse/KAFKA-20116) (In Progress) |
| **Online protocol migration** (classic → streams) | Must stop all instances, wait for session timeout, reconfigure, restart | [KAFKA-20172](https://issues.apache.org/jira/browse/KAFKA-20172) (Open) |
| **Topology updates** without new consumer group | Must create new `application.id` | [KAFKA-20170](https://issues.apache.org/jira/browse/KAFKA-20170) (Open) |
| **Non-default KafkaClientSupplier** | Not supported | — |

**Workaround:** Remove `group.protocol=streams` to fall back to the classic protocol, which supports all of the above.

**If the user genuinely needs any of these features:** Tell them to fall back to the classic protocol for now, and also **open an issue on [Apache Kafka GitHub](https://github.com/apache/kafka/issues)** describing their use case and referencing the relevant JIRA ticket above.

These features are all on the KIP-1071 roadmap (several are already in progress). Community demand determines what gets prioritized next.

### Static Membership (classic protocol only)
```properties
# Requires: group.protocol NOT set to "streams"
group.instance.id=${HOSTNAME}     # Unique, stable per instance
session.timeout.ms=60000          # Hold assignment during restarts
heartbeat.interval.ms=10000       # < 1/3 of session.timeout
```

Use for: rolling deploys (K8s), stateful apps with large state, transient network issues.
Don't use for: stateless apps, dynamic autoscaling.

Static membership + standby replicas = strongest combination for minimizing rebalance impact on the classic protocol.
