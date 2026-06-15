# Share Consumer API ("Queues for Kafka")

Read this when the user chooses the **Share Consumer API** instead of the regular Consumer API. Use
`references/AvroShareConsumer.java` as the code template (it pairs with the Avro path; adapt the
deserializer for JSON Schema or Protobuf exactly as the regular consumer would).

Reference docs:
- Share Consumer API: https://docs.confluent.io/kafka/kafka-apis.md
- End-to-end Confluent Cloud / Docker tutorial: https://developer.confluent.io/confluent-tutorials/queues-for-kafka/

## Consumer API vs. Share Consumer API — the key distinction

With the **regular Consumer API** (`KafkaConsumer`, consumer groups), each partition is assigned to
**exactly one** consumer in the group. Parallelism is capped at the partition count — extra
consumers sit idle. Offsets advance per partition and per-key ordering is preserved.

With the **Share Consumer API** (`KafkaShareConsumer`, share groups), **multiple share consumers can
consume from the same partition**. The broker cooperatively distributes individual records across
all members of the share group, giving **queue-like messaging semantics**: many consumers
concurrently process messages off the same topic and you can scale consumers **beyond** the number
of partitions. The trade-off is that there is **no per-partition / per-key ordering** guarantee
across members, and each record must be **acknowledged**.

Choose the Share Consumer API when the user wants:
- to scale consumers independently of partition count,
- queue / work-queue semantics (competing consumers, cooperative concurrent processing),
- per-record acknowledgement with redelivery of un-acked records.

Choose the regular Consumer API (the default) when the user needs per-key ordering, exactly the
classic consumer-group model, or has not asked for queue semantics.

## Acknowledgement modes

- **Implicit (default):** records are acknowledged automatically on the next `poll()` (or on
  `commitSync()`/`commitAsync()`).
- **Explicit** (`share.acknowledgement.mode=explicit`): call
  `consumer.acknowledge(record, AcknowledgeType.ACCEPT)` after handling each record. Use
  `AcknowledgeType.RELEASE` to redeliver (transient failure) and `AcknowledgeType.REJECT` for poison
  records. `AvroShareConsumer.java` uses explicit mode because it makes the per-record lifecycle obvious.

## Where consumption starts

`auto.offset.reset` on the client `Properties` is **ignored** for share consumers. Starting position
is a property of the **share group**, set on the cluster:

```
kafka-configs --bootstrap-server <broker> --alter \
  --group <share-group-id> --add-config share.auto.offset.reset=earliest
```

## Build file: requires Kafka 4.x clients

`KafkaShareConsumer` ships in **Apache Kafka 4.x**. The reference `pom.xml` and `build.gradle` already
pin `kafka.version=4.2.1` and `confluent.version=8.2.1`, so the Share Consumer API is available out of
the box — **no version change is needed**. Just confirm the generated build file keeps these (or
newer 4.x / 8.x) versions; do not downgrade to a 3.x Kafka client, which predates the API.

## Enabling the feature per environment

**Local Docker:** the share feature must be enabled on the cluster once after startup:

```
kafka-features --bootstrap-server localhost:9092 upgrade --feature share.version=1
```

(Run inside the broker container, e.g. `docker compose exec kafka kafka-features ...`.) Use a Kafka
4.x broker image so the feature is available. Then set `share.auto.offset.reset` on the group as
shown above.

**Confluent Cloud:** "Queues for Kafka" is an opt-in feature. It requires a Dedicated cluster and
enablement via Confluent support; confirm with the user that it is enabled on their cluster before
relying on it. Schema Registry, API-key auth, and `KafkaConfig` are otherwise identical to the
regular consumer path.

## Testing

`kafka-clients` has **no `MockShareConsumer`** to mirror `MockConsumer`. Do **not** try to drive the
share-consumer poll loop with a mock. Keep `AppTest.java` focused on what is testable without a live
cluster: `KafkaConfig` (SASL_SSL vs PLAINTEXT), the Avro `value.avsc` shape, and — if you want
coverage of the record-handling/acknowledgement logic — refactor the per-record handling into a small
method and unit-test that directly. Verify by source inspection that `AvroShareConsumer.java` uses
`KafkaShareConsumer`, a Schema Registry deserializer, and the wakeup + `close()` pattern.

## What stays the same

Schema Registry usage is unchanged: the share consumer uses the same `KafkaAvroDeserializer` (with
`specific.avro.reader=true`), `KafkaJsonSchemaDeserializer`, or `KafkaProtobufDeserializer`, reading
`schema.registry.url` from the same `Properties`. Graceful shutdown uses the same wakeup +
`close()`-in-`finally` pattern as `AvroConsumer.java`. The consumer remains a single-threaded
`poll()` loop; scale by running **more instances in the same share group**, not more threads on one
instance.
