# Consumer Pattern (`IConsumer<TKey,TValue>`)

Use `references/JsonSchemaConsumer.cs` (or `references/AvroConsumer.cs` for the Avro path) as the
code template.

Reference docs:
- Consumer configuration: https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/index.md

## Use the new consumer group protocol: `GroupProtocol = GroupProtocol.Consumer`

On **Apache Kafka 4.0+ clients and brokers**, set:

```csharp
consumerConfig.GroupProtocol = GroupProtocol.Consumer;
```

This opts into the **next-generation consumer rebalance protocol** (KIP-848), which became generally
available in AK 4.0. It replaces the legacy `Classic` protocol (`GroupProtocol.Classic`, the default).

Why it matters:
- **Eliminates the "stop-the-world" rebalance barrier.** Under the classic protocol every rebalance
  is a global synchronization barrier -- *all* consumers in the group revoke their partitions and stop
  processing while the group re-forms. The new protocol moves partition-assignment computation to the
  **group coordinator on the broker** and reconciles assignments **incrementally**, so consumers keep
  processing the partitions they retain while only the moving partitions are handed off.
- **Smoother rebalance experience.** Adding or removing a consumer (scaling, restarts, deploys) no
  longer pauses the whole group. Rebalances are faster and far less disruptive.

`GroupProtocol.Consumer` requires **AK 4.0+ on both the client and the broker**. For **Confluent
Cloud**, the new protocol is supported on current clusters. For **local Docker / self-managed**, use
a 4.0+ broker. If the user is pinned to a 3.x broker, fall back to omitting the setting (defaults to
`Classic`) and note the limitation.

## Graceful shutdown: `CancellationToken`, not Java's `wakeup()`

Java's `KafkaConsumer` uses `consumer.wakeup()` + catching `WakeupException` to break the poll loop
from a shutdown hook. **Do not port that pattern to .NET.** The .NET idiom is a `CancellationToken`:
`consumer.Consume(cancellationToken)` throws `OperationCanceledException` when the token is cancelled.
Register the cancellation from `Console.CancelKeyPress` (or a host's `IHostApplicationLifetime` if
integrating into an existing ASP.NET Core / Worker Service app), catch `OperationCanceledException`
around the loop, and `Close()` the consumer in a `finally` block so offsets commit cleanly and the
consumer leaves the group without triggering an unnecessary rebalance.

## Delivery Semantics: Choosing a Commit Strategy

Two `ConsumerConfig` booleans -- `EnableAutoCommit` and `EnableAutoOffsetStore` -- control *when* an
offset becomes eligible to commit and *when* it's actually written to the broker. They are
independent knobs, not a single on/off switch, and the combination you pick **is** the delivery
semantic. Always set both explicitly rather than relying on defaults -- the defaults are the
least-safe option.

To understand the two knobs, split "committing" into two steps:
1. **Store** -- mark an offset (locally, in-memory) as the next one to commit for that partition.
2. **Commit** -- actually write the stored offset(s) to the broker (`__consumer_offsets`).

`EnableAutoOffsetStore` controls step 1 (does `Consume()` auto-store the offset the instant it
returns a record, or do you call `StoreOffset()` yourself?). `EnableAutoCommit` controls step 2 (does
a background timer periodically flush whatever's stored, on `AutoCommitIntervalMs`, default 5000ms?).
`consumer.Commit(...)` calls are always available regardless of these settings and let you commit on
your own schedule instead of the timer's.

### At-most-once (the default -- avoid unless you mean it)

```csharp
consumerConfig.EnableAutoCommit = true;      // default
consumerConfig.EnableAutoOffsetStore = true; // default
```

This is what you get if you don't touch either setting. `Consume()` stores the offset **the instant
it returns the record** -- before your code has done anything with it. The background timer commits
that stored offset every `AutoCommitIntervalMs`, completely decoupled from whether processing
finished or even started. If the process crashes after `Consume()` returns but before `Process()`
finishes, the offset may already be committed: on restart, that message is skipped and never
reprocessed. **A message can be lost.** This is only appropriate when losing an occasional message is
acceptable (e.g. best-effort metrics, sampled telemetry) -- never for anything durable. Do not leave a
generated consumer on these defaults implicitly; the user should choose this deliberately.

### At-least-once via `StoreOffset` (recommended default for this skill)

```csharp
consumerConfig.EnableAutoCommit = true;
consumerConfig.EnableAutoOffsetStore = false;
```

```csharp
var record = consumer.Consume(cts.Token);
Process(record);
consumer.StoreOffset(record); // marks record.Offset + 1 as ready; does NOT talk to the broker
```

Now `Consume()` no longer auto-stores anything -- you call `StoreOffset(record)` yourself, **after**
processing succeeds. The background timer still runs and commits whatever has been stored, but since
nothing gets stored until your code says so, the timer can only ever commit offsets for records that
were actually processed. On crash, at worst the last few processed-but-not-yet-committed records get
reprocessed on restart (duplicates are possible, downstream idempotency handles that) -- **no message
is skipped before being processed.** `StoreOffset` is a cheap, local, non-blocking call (no network
round-trip); the network cost of committing is amortized by the timer. This is the best throughput
option that still gives you at-least-once, and is Confluent's own recommended pattern for the common
case. `enable.auto.offset.store` must be `false` for `StoreOffset` to be valid -- the client throws
if you call it while auto offset store is still on.

### At-least-once via manual `Commit` (stronger per-record guarantee, lower throughput)

```csharp
consumerConfig.EnableAutoCommit = false;
```

```csharp
var record = consumer.Consume(cts.Token);
Process(record);
consumer.Commit(record); // synchronous: blocks until the broker acknowledges, throws KafkaException on failure
```

This is what `JsonSchemaConsumer.cs` uses. Disabling `EnableAutoCommit` turns off the background
timer entirely -- there is no periodic flush, so *you* decide exactly when a commit round-trips to
the broker. `consumer.Commit(record)` blocks until the broker acknowledges, giving the strongest
per-call guarantee, at the cost of throughput since the loop pauses on every commit. For higher
throughput with this approach, commit every N records or every few seconds instead of after each one
-- the tradeoff is a wider (but still bounded) reprocessing window on crash. Always do a final
`consumer.Commit()` (no arguments -- commits the last stored/consumed offsets) in the `finally` block
before `Close()`, so the last processed offset is durable before the process exits.

**When to pick this over `StoreOffset`:** when you want commit timing under explicit application
control (e.g. commit only at defined batch boundaries) rather than an interval timer, or when the
simplicity of "one call does both store and commit" outweighs the throughput cost. Default to the
`StoreOffset` pattern above unless the user has a reason to want explicit per-record/per-batch commit
control.

### Exactly-once (requires Kafka transactions -- a read-process-write pipeline)

Neither pattern above can give exactly-once on its own: **committing a consumer offset and producing
an output record are two separate operations against two different systems**, and without a
transaction a crash between them causes either a lost output or a reprocessed input. Kafka's
exactly-once semantics (EOS) close this gap by putting the **consumer's offset commit and the
producer's writes in one atomic transaction** -- this only works for Kafka-to-Kafka pipelines (read
from Kafka, write to Kafka), not for arbitrary external side effects.

```csharp
consumerConfig.EnableAutoCommit = false;
consumerConfig.EnableAutoOffsetStore = false;
consumerConfig.IsolationLevel = IsolationLevel.ReadCommitted; // on the DOWNSTREAM consumer of the output topic -- already librdkafka's default, set explicitly for clarity

producerConfig.TransactionalId = "my-app-1"; // unique per producer instance; stable across restarts of the same logical instance
```

```csharp
producer.InitTransactions(TimeSpan.FromSeconds(10)); // once, at startup

var record = consumer.Consume(cts.Token);
producer.BeginTransaction();
try
{
    producer.Produce("output-topic", new Message<string, TValue> { Key = record.Message.Key, Value = Transform(record.Message.Value) });

    // Commits the consumer's offset AS PART OF the transaction -- do NOT also call
    // consumer.Commit()/StoreOffset() here, the transaction owns the offset commit.
    var offsets = new[] { new TopicPartitionOffset(record.TopicPartition, record.Offset + 1) };
    producer.SendOffsetsToTransaction(offsets, consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(10));

    producer.CommitTransaction();
}
catch (KafkaException)
{
    producer.AbortTransaction(); // input record will be re-consumed on the next poll
    throw;
}
```

Key points:
- `InitTransactions` is called once at startup (fences off any previous zombie instance of the same
  `TransactionalId`). `BeginTransaction`/`CommitTransaction`/`AbortTransaction` wrap each unit of work.
- `SendOffsetsToTransaction` -- using `consumer.ConsumerGroupMetadata` -- is what makes the consumer's
  progress part of the same atomic transaction as the produced output. This replaces both
  `StoreOffset` and `Commit` entirely; don't call either alongside it.
- On any failure, call `AbortTransaction()` (not swallow the exception) so the partially-produced
  output is marked aborted and the input offset is not advanced -- the record is re-consumed next
  poll.
- **Any consumer reading the output topic** must use `IsolationLevel.ReadCommitted`, otherwise it may
  see uncommitted/aborted transactional writes. Unlike the Java client -- where `isolation.level`
  **defaults to `read_uncommitted`**, so Java consumers must set `read_committed` explicitly or they
  silently see aborted/uncommitted records -- librdkafka (and therefore every librdkafka-based
  client: `Confluent.Kafka`, `confluent-kafka-python`, `confluent-kafka-go`, `node-rdkafka`)
  **defaults `IsolationLevel` to `ReadCommitted` already**. Practically: if the source topic is
  produced to by a transactional (EOS) producer, any .NET consumer reading it is safe against
  duplicates/aborted-transaction artifacts **even without setting `IsolationLevel` explicitly** --
  the line in the config block above is there for clarity, not because it changes behavior from the
  default. Don't assume the same is true if a teammate's code is on the Java client, or if this
  consumer is ever ported there -- call out `read_committed` explicitly in that case since its
  default genuinely differs.
- This is meaningfully more complex and has lower throughput than the at-least-once patterns above
  (transaction coordinator round-trips per batch). Only reach for it when the user explicitly needs
  exactly-once guarantees across a read-transform-write pipeline -- for a plain producer or consumer
  generated by this skill, default to the `StoreOffset` at-least-once pattern instead.

## What stays the same

Schema Registry usage and the deserializer wiring (`.AsSyncOverAsync()` on the Confluent
`JsonDeserializer<T>`/`AvroDeserializer<T>`, since `IConsumer<TKey,TValue>` only accepts a synchronous
`IDeserializer<TValue>`) are exactly as in `JsonSchemaConsumer.cs`. The consumer remains a
single-threaded `Consume()` loop; scale by running more instances in the same consumer group
(parallelism is capped at the partition count). The .NET client does not currently support Kafka's
Share Consumer API (KIP-932 "Queues for Kafka") -- that feature is Java-client-only as of Kafka 4.x;
if the user wants queue-like cooperative consumption beyond partition count, tell them this isn't yet
available via `Confluent.Kafka` and point them at the Java client skill instead.
