# Consumer API (regular `KafkaConsumer`)

Read this when the user chooses the **regular Consumer API** (the default) instead of the Share
Consumer API. Use `references/AvroConsumer.java` as the code template (it pairs with the Avro path;
adapt the deserializer for JSON Schema or Protobuf exactly as the share consumer would).

Reference docs:
- Consumer configs: https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.md

## Use the new consumer group protocol: `group.protocol=consumer`

On **Apache Kafka 4.0+ clients and brokers**, set:

```java
props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
```

This opts into the **next-generation consumer rebalance protocol** (KIP-848), which became generally
available in AK 4.0. It replaces the legacy `classic` protocol.

Why it matters:
- **Eliminates the "stop-the-world" rebalance barrier.** Under the classic protocol every rebalance
  is a global synchronization barrier — *all* consumers in the group revoke their partitions and stop
  processing while the group re-forms. The new protocol moves partition-assignment computation to the
  **group coordinator on the broker** and reconciles assignments **incrementally**, so consumers keep
  processing the partitions they retain while only the moving partitions are handed off.
- **Smoother rebalance experience.** Adding or removing a consumer (scaling, restarts, deploys) no
  longer pauses the whole group. Rebalances are faster and far less disruptive, which means lower
  end-to-end latency spikes during membership changes.

`group.protocol=consumer` requires **AK 4.0+ on both the client and the broker**. The reference build
files already pin Kafka 4.x clients, so the client side is covered. For **Confluent Cloud**, the new
protocol is supported on current clusters. For **local Docker / self-managed**, use a 4.0+ broker. If
the user is pinned to a 3.x broker, fall back to omitting the config (defaults to `classic`) and note
the limitation.

## Disable auto-commit: `enable.auto.commit=false`

By default `enable.auto.commit=true` commits offsets automatically on a timer
(`auto.commit.interval.ms`, default 5s). That timer is decoupled from whether you have actually
*finished processing* the records — offsets can be committed for records that are still in flight or
that fail downstream, which **widens the window for duplicate or lost message processing**.

For tighter control and stronger prevention of duplicate processing, disable it:

```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
```

Then commit **explicitly, after** your processing for a batch has succeeded, so an offset is only
advanced once the work behind it is durably done.

## Committing offsets: `commitSync` vs `commitAsync`

With auto-commit off you choose when and how to commit. Both commit the offsets returned by the most
recent `poll()`, but they trade off differently:

- **`commitSync()`** — blocks until the broker acknowledges the commit (and retries automatically on
  retriable errors). Strongest guarantee that the offset is committed, at the cost of throughput: the
  poll loop stalls on every commit, so you typically get **lower throughput**. Best when **order of
  processing matters more than speed**, and for the **final commit before shutting down** a consumer,
  where you want to be certain the last processed offset is recorded before the process exits.

- **`commitAsync()`** — fires the commit and returns immediately, handling the result in a callback.
  Higher throughput because the poll loop keeps moving, but it does **not** retry on failure (a later
  commit may supersede it) and offers a weaker guarantee that any individual commit landed. Best for
  the **steady-state** of a high-throughput loop where occasional reprocessing on failure is
  acceptable.

A common pattern combines them: `commitAsync()` during normal processing for throughput, and a final
`commitSync()` in a `finally` block (or just before `close()`) to make the last commit durable:

```java
try {
    while (true) {
        ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, Transaction> record : records) {
            // ... process the record ...
        }
        consumer.commitAsync();          // steady state: don't block the loop
    }
} catch (WakeupException e) {
    // Expected on shutdown — fall through.
} finally {
    try {
        consumer.commitSync();           // final commit: block until durable
    } finally {
        consumer.close();
    }
}
```

## What stays the same

Schema Registry usage, the deserializer config (`specific.avro.reader=true` for Avro), and the
wakeup + `close()`-in-`finally` graceful-shutdown pattern are exactly as in `AvroConsumer.java`. The
consumer remains a single-threaded `poll()` loop; scale by running more instances in the same
consumer group (parallelism is capped at the partition count). For queue-like semantics that scale
beyond the partition count, see `references/share-consumer.md` instead.
