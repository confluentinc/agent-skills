# Schema Generation Rules

When generating or adapting a schema to the user's domain, follow these rules strictly. Without them, the generated schema will lack discoverability, break on evolution, and produce governance issues.

1. **Descriptions everywhere.** The schema itself and every property MUST have a `description`. Descriptions enable discoverability in Schema Registry UI and governance tools.
2. **Default values on non-key fields.** Every field that is not the primary identifier MUST have a `default` value. Use sensible defaults: `""` for strings, `0` for numbers, `false` for booleans, `null` for nullable unions, and the first enum value for enums. Without defaults, you cannot add or remove fields without breaking consumers (backward-compatible schema evolution requires defaults).
3. **Timestamps use `format: date-time`.** Any field representing a point in time MUST use `"type": "string", "format": "date-time"` (ISO 8601). Do not use bare `"type": "string"` for timestamps.
4. **Enums for fixed value sets.** Fields with a known, fixed set of values (status codes, event types, categories) MUST use `"enum"` with explicit values. This prevents invalid data and enables schema-level validation.
5. **Include nullable fields.** Include at least one nullable field using `"oneOf": [{"type": "null"}, {"type": "..."}]` with `"default": null` for future extensibility. If the user's domain does not suggest one, add a `metadata` field as a nullable object. Without nullable unions, making a field optional later requires a breaking schema change.
6. **Title and `$schema`.** The `"title"` must match the event name (e.g., `"UserSignup"`, `"SensorReading"`). The `"$schema"` must be `"http://json-schema.org/draft-07/schema#"`.

If the user doesn't have a specific domain, use a generic event schema with `id`, `type`, `timestamp`, and `payload` properties — but still apply all the rules above (descriptions, defaults, `format: date-time` on the timestamp, etc.).
