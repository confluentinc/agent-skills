# Multi-Event Topics Guide

By default the reference code uses **TopicNameStrategy**: one schema per `<topic>-value` subject. This is correct for the common case where each topic carries a single event type. Only use the patterns below when the user explicitly describes multiple event types on one topic (e.g. `OrderCreated`, `OrderUpdated`, `OrderCancelled` on an `order-events` topic).

Keep **TopicNameStrategy** (one subject) and use a **union schema**. Do NOT switch to `RecordNameStrategy`/`TopicRecordNameStrategy` (multiple subjects) unless the user specifically needs independent per-type subjects — the single-subject union keeps one serializer and one contract per topic.

## Avro (default)

Define a top-level Avro **union of records** in `src/main/avro/value.avsc`, registered under `<topic>-value`. Each branch is a record with an `eventType` discriminator field:

```json
[
  {
    "type": "record",
    "name": "OrderCreated",
    "namespace": "com.example.kafka",
    "doc": "Emitted when a new order is placed.",
    "fields": [
      {"name": "eventType", "type": {"type": "enum", "name": "OrderCreatedType", "symbols": ["OrderCreated"]}, "doc": "Discriminator."},
      {"name": "orderId", "type": "string", "doc": "Unique order identifier."},
      {"name": "customerId", "type": "string", "default": "", "doc": "Customer who placed the order."},
      {"name": "total", "type": "double", "default": 0, "doc": "Order total."},
      {"name": "timestamp", "type": "string", "default": "", "doc": "When the order was placed (ISO 8601)."}
    ]
  },
  {"type": "record", "name": "OrderUpdated", "namespace": "com.example.kafka", "doc": "...", "fields": ["..."]},
  {"type": "record", "name": "OrderCancelled", "namespace": "com.example.kafka", "doc": "...", "fields": ["..."]}
]
```

Key rules:
1. The Avro plugin generates one `SpecificRecord` class per branch (`OrderCreated`, `OrderUpdated`, `OrderCancelled`). The producer's value type is the common supertype `Object` (or `SpecificRecord`): `Producer<String, SpecificRecord>`. Build whichever event class and send it — `KafkaAvroSerializer` selects the matching union branch automatically.
2. Register the union schema explicitly under `<topic>-value` (`new AvroSchema(unionSchemaString)`), keep `auto.register.schemas=false`.
3. The consumer with `specific.avro.reader=true` deserializes into the correct generated class; branch on `instanceof` (or read the `eventType` field) to route.
4. All Schema Generation Rules still apply to each branch (doc on record and every field, defaults on non-identifier fields, enums for discriminators).

## JSON Schema (alternative)

Use a single `value.schema.json` with `oneOf` + `$ref`, registered under `<topic>-value`. Each sub-schema in `$defs` has an `event_type` discriminator with a single-value `enum`:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OrderEvent",
  "description": "Union of all event types on the order-events topic.",
  "oneOf": [
    { "$ref": "#/$defs/OrderCreated" },
    { "$ref": "#/$defs/OrderUpdated" },
    { "$ref": "#/$defs/OrderCancelled" }
  ],
  "$defs": {
    "OrderCreated": {
      "type": "object",
      "title": "OrderCreated",
      "description": "Emitted when a new order is placed.",
      "properties": {
        "event_type": { "type": "string", "enum": ["OrderCreated"], "description": "Discriminator." },
        "order_id":   { "type": "string", "description": "Unique order identifier." },
        "timestamp":  { "type": "string", "format": "date-time", "description": "When the order was placed." }
      },
      "required": ["event_type", "order_id", "timestamp"]
    }
  }
}
```

The producer uses one `KafkaJsonSchemaSerializer`; the `oneOf` validates each POJO against the matching sub-schema at serialization time.
