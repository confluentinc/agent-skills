# Schema GUID in Header Configuration

The default serialization format is JSON with Schema GUID in header (`JSON_SR`). The message payload remains pure JSON while a 16-byte schema GUID is stored in the Kafka message header (instead of prepending a magic byte + schema ID to the payload). Deserializers automatically check the header first, then fall back to the payload prefix for backward compatibility.

For details, see [Wire format: schema GUID in header](https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.md).

## Java Client Configuration

```properties
# Producer config
key.serializer=io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
value.serializer=io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
key.schema.id.serializer=io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer
value.schema.id.serializer=io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer

# Consumer config (checks header first, then payload prefix)
key.deserializer=io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer
value.deserializer=io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer
key.schema.id.deserializer=io.confluent.kafka.serializers.schema.id.HeaderSchemaIdDeserializer
value.schema.id.deserializer=io.confluent.kafka.serializers.schema.id.HeaderSchemaIdDeserializer
```

## Confluent Cloud Managed Connectors

```
output.data.format=JSON_SR
output.key.format=JSON_SR
```

## Why Schema GUID in Header?

- Payload remains pure JSON — readable by any JSON consumer without Confluent deserializers
- Schema enforcement still happens via Schema Registry
- Easier migration for clients not currently using Schema Registry
- Two identical schemas always produce the same GUID regardless of which Schema Registry they're registered with

## Limitations

See [docs](https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.md):

- Headers may be dropped in Kafka Streams or ksqlDB state stores
- Verify Kafka Connect preserves headers and doesn't transform them
- Changing from payload prefix to header for record keys may alter partitioning
