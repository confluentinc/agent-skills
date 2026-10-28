# Streaming semantics (non-dbt concepts that affect correctness)

These are the hidden assumptions of a streaming pipeline. None are dbt concepts; all of them affect correctness.

- **`` `$rowtime` `` is auto-attached to every Confluent Cloud Flink table** as a hidden event-time column with a watermark derived from the underlying Kafka record timestamp. Use it as the event-time column for windowing on existing topics — it works whether or not the topic's schema has its own timestamp field, and it removes the need for the user to declare a watermark. Quote with backticks because of the `$`. Example: `FROM TABLE(TUMBLE(TABLE {{ source('raw','orders') }}, DESCRIPTOR(`` `$rowtime` ``), INTERVAL '5' MINUTES))`.
- **`changelog.mode`** — `'append'` (insert-only stream) vs `'upsert'` (last-value-per-key, requires a `PRIMARY KEY`). Default depends on whether the table has a PK. Pick deliberately; downstream consumers see different streams. Set via `config(with={'changelog.mode': 'append' | 'upsert'})`.
- **`WATERMARK FOR <ts> AS <expr>`** declares an event-time column on a *new* table — used inside `streaming_source` column definitions when the model author owns the schema.
- **`PRIMARY KEY (col) NOT ENFORCED`** is metadata only — Flink does **not** dedupe on insert. It signals to downstream operators (and `upsert` changelog mode) which column identifies a row. State this explicitly to users; it is not a unique constraint at write time.
- **Default time zone is UTC.** `TIMESTAMP` columns are zone-naive. Use `TIMESTAMP_LTZ` or set `sql.local-time-zone` once in a Cloud Console workspace if local-time conversions matter.
- **Streaming joins** require either watermarks + an interval predicate (interval join) or one side as a temporal/lookup table. Plain unbounded joins on streaming tables blow up state.
