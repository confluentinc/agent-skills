# Multi-Event Topics Guide

By default, the reference code uses **TopicNameStrategy**: one schema per `<topic>-value` subject. This is correct for the common case where each topic carries a single event type.

When a user describes multiple event types on a single topic (e.g., `OrderCreated`, `OrderUpdated`, `OrderCancelled` on an `order-events` topic), keep **TopicNameStrategy** and use a **union schema** with `oneOf` and `$ref`. This keeps a single subject (`<topic>-value`) and a single serializer while allowing multiple event shapes within the same topic. The `oneOf` discriminates between event types, and each referenced sub-schema can evolve independently inside the union.

Use `references/order_events.schema.json` as the template for multi-event schemas. The pattern:

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
        "customer_id": { "type": "string", "description": "Customer who placed the order.", "default": "" },
        "total":      { "type": "number", "description": "Order total.", "default": 0 },
        "currency":   { "type": "string", "description": "ISO 4217 currency code.", "default": "USD" },
        "timestamp":  { "type": "string", "format": "date-time", "description": "When the order was placed." }
      },
      "required": ["event_type", "order_id", "timestamp"]
    },
    "OrderUpdated": { "..." : "..." },
    "OrderCancelled": { "..." : "..." }
  }
}
```

Key rules for union schemas:
1. **Discriminator field.** Every sub-schema MUST include an `event_type` property with a single-value `enum` (e.g., `"enum": ["OrderCreated"]`). This lets consumers route messages without inspecting the full payload.
2. **All Schema Generation Rules still apply** to each sub-schema — descriptions, defaults, `format: date-time`, nullable fields, etc.
3. **One file.** The union schema is a single `schemas/value.schema.json` registered under the default `<topic>-value` subject. No extra subjects or strategy changes are needed.
4. **Producer code is unchanged.** The producer uses the same single serializer — the `oneOf` validation happens at serialization time. The producer just passes the correct dict shape for each event type.

Use `references/producer_multi_event.py` as the template for multi-event async producers and `references/producer_multi_event_sync.py` for synchronous. The key difference from single-event producers is that the `produce()` function accepts messages of varying shapes — the union schema's `oneOf` validates each message against the matching sub-schema at serialization time.

Only suggest multi-event union schemas when the user explicitly describes multiple event types on one topic. For single-event-type topics, use a plain object schema.
