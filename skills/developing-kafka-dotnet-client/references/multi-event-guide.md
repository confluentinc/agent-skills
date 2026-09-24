# Multi-Event Topics Guide

By default the reference code uses **TopicNameStrategy**: one schema per `<topic>-value` subject.
This is correct for the common case where each topic carries a single event type. Only use the
patterns below when the user explicitly describes multiple event types on one topic (e.g.
`OrderCreated`, `OrderUpdated`, `OrderCancelled` on an `order-events` topic).

Keep **TopicNameStrategy** (one subject) and use a **union schema**. Do NOT switch to
`RecordNameStrategy`/`TopicRecordNameStrategy` (multiple subjects) unless the user specifically needs
independent per-type subjects -- the single-subject union keeps one serializer and one contract per
topic.

## Avro (recommended for multi-event topics)

Define a top-level Avro **union of records** in `Avro/value.avsc`, registered under `<topic>-value`.
Each branch is a record with an `eventType` discriminator field:

```json
[
  {
    "type": "record",
    "name": "OrderCreated",
    "namespace": "ExampleKafka.Avro",
    "doc": "Emitted when a new order is placed.",
    "fields": [
      {"name": "eventType", "type": {"type": "enum", "name": "OrderCreatedType", "symbols": ["OrderCreated"]}, "doc": "Discriminator."},
      {"name": "orderId", "type": "string", "doc": "Unique order identifier."},
      {"name": "customerId", "type": "string", "default": "", "doc": "Customer who placed the order."},
      {"name": "total", "type": "double", "default": 0, "doc": "Order total."},
      {"name": "timestamp", "type": "string", "default": "", "doc": "When the order was placed (ISO 8601)."}
    ]
  },
  {"type": "record", "name": "OrderUpdated", "namespace": "ExampleKafka.Avro", "doc": "...", "fields": ["..."]},
  {"type": "record", "name": "OrderCancelled", "namespace": "ExampleKafka.Avro", "doc": "...", "fields": ["..."]}
]
```

Key rules:
1. `avrogen` generates one class per branch (`OrderCreated`, `OrderUpdated`, `OrderCancelled`), each
   implementing `Avro.Specific.ISpecificRecord`. Parameterize the producer and serializer over the
   common interface, **not** one concrete branch type: `IProducer<string, ISpecificRecord>` and
   `AvroSerializer<ISpecificRecord>`. Each generated class reports its own schema through its instance
   `Schema` property, so `AvroSerializer<ISpecificRecord>` correctly selects the matching union branch
   for whichever concrete instance you pass to `Produce`/`ProduceAsync` -- construct whichever event
   class fits and send it.
2. Register the union schema explicitly under `<topic>-value` (`new Schema(unionSchemaJson,
   SchemaType.Avro)`), keep `AutoRegisterSchemas = false`.
3. The consumer parameterizes `IConsumer<string, ISpecificRecord>` / `AvroDeserializer<ISpecificRecord>`
   the same way; branch on the deserialized value's runtime type (`is OrderCreated oc`, a `switch`
   pattern match, or the `eventType` field) to route.
4. All Schema Generation Rules still apply to each branch (doc on record and every field, defaults on
   non-identifier fields, enums for discriminators).

## JSON Schema (alternative)

.NET's JSON Schema path is code-first (`Value.cs` -> generated schema), which makes a single-subject
union harder to express cleanly than Avro's schema-first union array: NJsonSchema needs an explicit
polymorphic type hierarchy (`[JsonPolymorphic]` / `[JsonDerivedType]` on a common base class) to emit
a `oneOf` schema, and Confluent's `JsonSerializer<T>` would need to be parameterized over that base
type. This is possible but more intricate to get right than the Avro union above -- **prefer Avro for
multi-event topics in .NET** unless the user has a hard requirement for JSON Schema. If they do, model
one base class (e.g. `OrderEvent`) with an `EventType` discriminator property, annotate it with
`[JsonPolymorphic(TypeDiscriminatorPropertyName = "eventType")]` and a `[JsonDerivedType]` per branch,
parameterize the producer/consumer over `OrderEvent`, and verify with `dotnet build`/`dotnet test`
that the generated schema and round-trip serialization actually produce the shape you expect before
shipping it -- this path has fewer prior examples to lean on than the Avro union.
