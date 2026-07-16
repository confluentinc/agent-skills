# Schema Generation Rules

When generating or adapting a schema to the user's domain, follow these rules strictly. Without them, the schema lacks discoverability, breaks on evolution, and creates governance issues. The default format on the JVM is **Avro**; the JSON Schema rules follow for the alternative path.

## Avro (default — `src/main/avro/value.avsc`)

1. **`doc` everywhere.** The record itself and every field MUST have a `doc`. These surface in the Schema Registry UI and governance tooling.
2. **camelCase field names.** Use camelCase (`transactionId`, not `transaction_id`) so the Avro plugin generates idiomatic Java accessors (`getTransactionId()`, `setTransactionId(...)`). The producer/consumer code references these generated names.
3. **`name` and `namespace` drive codegen.** The record `name` becomes the generated Java class name and `namespace` its package — e.g. `"name": "Transaction"`, `"namespace": "com.example.kafka"` generates `com.example.kafka.Transaction`. Keep the namespace aligned with the project package.
4. **Defaults on non-identifier fields.** Every field that is not the entity identifier MUST have a `default`: `""` for strings, `0` for numbers, `false` for booleans, the first symbol for enums, and `null` for nullable unions. Backward-compatible evolution requires defaults.
5. **Enums for fixed value sets.** Status codes, event types, and categories MUST use an Avro `enum` with explicit `symbols`. The enum `name` generates a Java enum (e.g. `Status.completed`).
6. **Nullable fields use a union with `null` first.** Optional fields use `["null", "<type>"]` with `"default": null`. Avro requires `null` to be the first branch when the default is null. Include at least one nullable field (e.g. a `metadata` field) for future extensibility.
7. **Timestamps.** Represent points in time as ISO 8601 strings (`"type": "string"`) for portability, or use the Avro logical type `{"type": "long", "logicalType": "timestamp-millis"}` when the user wants native temporal types. Be consistent and document the choice in the field `doc`.

## JSON Schema (alternative — `src/main/resources/value.schema.json`)

1. **Descriptions everywhere.** The schema and every property MUST have a `description`.
2. **Defaults on non-key fields.** Same rationale as Avro — every non-identifier field needs a `default`.
3. **Timestamps use `format: date-time`.** Any point-in-time field MUST be `"type": "string", "format": "date-time"`. Never bare `"type": "string"`.
4. **Enums for fixed value sets.** Use `"enum"` with explicit values.
5. **Include a nullable field.** Use `"oneOf": [{"type": "null"}, {"type": "..."}]` with `"default": null` for extensibility.
6. **`title` and `$schema`.** `"title"` matches the event name (e.g. `"Transaction"`); `"$schema"` is `"http://json-schema.org/draft-07/schema#"`.
7. **Pair with a POJO.** The Java value type is a POJO whose Jackson-annotated fields match the schema property names. The consumer sets `json.value.type` to that POJO class.

If the user has no specific domain, use a generic event schema with `id`, `type`, `timestamp`, and `payload` fields — but still apply every rule above.
