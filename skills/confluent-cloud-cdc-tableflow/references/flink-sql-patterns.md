# Flink SQL Patterns for Debezium CDC in Confluent Cloud

CDC source tables are **auto-discovered** from Kafka topics with SR schemas — no manual source table creation needed. Reference them with backtick-quoting: `` `postgres-cdc.public.customers` ``. All patterns work identically with `JSON_SR`, `AVRO`, and `PROTOBUF` formats. If a CDC table doesn't appear in `SHOW TABLES`, the connector may not have produced data yet — wait 2-5 minutes.

---

## Debezium Type Mappings

Debezium uses specific logical types that map to Flink types differently than you might expect. Understanding these is critical to avoid type mismatches.

| Debezium Logical Type | Flink Column Type | Meaning | Conversion |
|---|---|---|---|
| `io.debezium.time.ZonedTimestamp` | `STRING` | ISO 8601 string with timezone (MySQL TIMESTAMP) | No conversion needed — already human-readable |
| `io.debezium.time.NanoTimestamp` | `BIGINT` | Nanoseconds since epoch (SQL Server DATETIME2) | `TO_TIMESTAMP_LTZ(col / 1000000, 3)` |
| `io.debezium.time.MicroTimestamp` | `BIGINT` | Microseconds since epoch (Oracle TIMESTAMP) | `TO_TIMESTAMP_LTZ(col / 1000, 3)` |
| `io.debezium.time.Timestamp` | `BIGINT` | Milliseconds since epoch | `TO_TIMESTAMP_LTZ(col, 3)` |
| `io.debezium.time.Date` | `INT` | Days since epoch | Use as-is or convert |
| `io.debezium.time.MicroTime` | `BIGINT` | Microseconds since midnight | Use as-is or convert |
| Regular `INT`, `BIGINT`, `STRING`, etc. | Direct mapping | Standard types | No conversion needed |
| `DECIMAL` / `NUMERIC` | `STRING` (with `decimal.handling.mode=string`) or `BYTES` (default) | DECIMAL or STRING | Set `decimal.handling.mode: string` on the connector — without it, DECIMAL arrives as raw BYTES that Flink cannot cast. With `string` mode, values arrive as human-readable strings like `"1299.99"` |
| `INTERVAL` (PG/Oracle) | `STRING` (with `interval.handling.mode=string`) or `INT64` (default, lossy micros) | STRING | Set `interval.handling.mode: string` on the connector for lossless ISO 8601 format |
| Binary (`BYTEA`, `BLOB`, etc.) | `STRING` (with `binary.handling.mode=base64`) or `BYTES` (default) | STRING | Set `binary.handling.mode: base64` on the connector for JSON-safe base64 encoding |

---

## Pattern 1: Create Target Table (Minimal WITH Clause)

Target tables are the only tables you need to CREATE. They use a minimal WITH clause -- no connector, format, bootstrap server, or Schema Registry properties are needed.

```sql
CREATE TABLE `target_customers` (
  `id` INT NOT NULL,
  `name` STRING,
  `email` STRING,
  `created_at` TIMESTAMP_LTZ(3),
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert'
);
```

**Key Points:**
- Only `'changelog.mode' = 'upsert'` is needed in the WITH clause for CDC use cases
- Do NOT add `'connector'`, `'value.format'`, `'properties.bootstrap.servers'`, or Schema Registry URL properties -- these are not supported in Confluent Cloud Flink and will cause errors
- Use `TIMESTAMP_LTZ(3)` (not `TIMESTAMP(3)`) for timestamp columns converted from Debezium MicroTimestamp or Timestamp types
- Define `PRIMARY KEY (...) NOT ENFORCED` to maintain upsert semantics for CDC

---

## Pattern 2: INSERT Statement (CDC Envelope Decoded Automatically)

Confluent Cloud Flink recognizes CDC changelog tables natively. You do NOT need to reference `after.*` fields or filter by `op`. Flink handles Debezium envelope decoding and CDC semantics (inserts, updates, deletes) automatically.

```sql
INSERT INTO `target_customers`
SELECT
  `id`,
  `name`,
  `email`,
  TO_TIMESTAMP_LTZ(`created_at` / 1000, 3)
FROM `postgres-cdc.public.customers`;
```

**Key Points:**
- Reference columns directly by name (e.g., `id`, `name`) -- not as `after.id` or `after.name`
- Flink automatically interprets the Debezium changelog stream, handling inserts, updates, and deletes
- Apply timestamp conversions as needed based on the Debezium type mappings above
- No `WHERE op IN ('c', 'u', 'r')` filter needed -- CDC semantics are handled natively

---

## Pattern 3: Multi-Table Pipeline

For each table, create a target table (Pattern 1) and INSERT (Pattern 2). **Each INSERT must be submitted as a separate Flink statement** — they run as independent continuous jobs.

```sql
INSERT INTO `target_customers`
SELECT `id`, `name`, `email`, TO_TIMESTAMP_LTZ(`created_at` / 1000, 3)
FROM `postgres-cdc.public.customers`;

INSERT INTO `target_orders`
SELECT `order_id`, `customer_id`, `amount`, TO_TIMESTAMP_LTZ(`order_date` / 1000, 3)
FROM `postgres-cdc.public.orders`;
```

---

## Pattern 4: Filtering and Transformation

**Filter by column value:**
```sql
INSERT INTO `target_active_customers`
SELECT `id`, `name`, `email`, TO_TIMESTAMP_LTZ(`created_at` / 1000, 3)
FROM `postgres-cdc.public.customers`
WHERE `status` = 'active';
```

**Add computed columns:**
```sql
INSERT INTO `target_customers_enriched`
SELECT
  `id`, `name`, `email`,
  TO_TIMESTAMP_LTZ(`created_at` / 1000, 3),
  CURRENT_TIMESTAMP AS `processed_at`,
  'flink-cdc-pipeline' AS `source_system`
FROM `postgres-cdc.public.customers`;
```

**Join with reference data:**
```sql
INSERT INTO `target_orders_enriched`
SELECT o.`order_id`, o.`customer_id`, o.`region_id`, r.`region_name`, o.`amount`
FROM `postgres-cdc.public.orders` o
LEFT JOIN `dim_regions` r ON o.`region_id` = r.`region_id`;
```

---

## Pattern 5: Schema Evolution

When database schemas change, update Flink tables:

```sql
-- 1. Stop the INSERT statement (via MCP: delete-flink-statements)
-- 2. Drop and recreate target table with new columns
DROP TABLE `target_customers`;
CREATE TABLE `target_customers` (
  `id` INT NOT NULL,
  `name` STRING,
  `email` STRING,
  `phone` STRING,       -- New column
  `created_at` TIMESTAMP_LTZ(3),
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('changelog.mode' = 'upsert');

-- 3. Recreate INSERT with new column
INSERT INTO `target_customers`
SELECT `id`, `name`, `email`, `phone`, TO_TIMESTAMP_LTZ(`created_at` / 1000, 3)
FROM `postgres-cdc.public.customers`;
```

---

## Statement Management via MCP

### Creating a Flink Statement

Use `mcp__confluent__create-flink-statement` with:

| Parameter | Description | Example |
|---|---|---|
| `statement` | The SQL text | `CREATE TABLE ...` or `INSERT INTO ...` |
| `statementName` | Lowercase kebab-case name | `cdc-create-target-customers` |
| `environmentId` | The Confluent environment ID | `env-abc123` |
| `computePoolId` | The Flink compute pool ID | `lfcp-xyz789` |
| `catalogName` | Usually the environment display name | `my-environment` |
| `databaseName` | The Kafka cluster display name | `dedicated_cluster` |

### Workflow

1. `SHOW TABLES` → verify CDC source table is auto-discovered
2. `CREATE TABLE` → create target table with upsert mode
3. `INSERT INTO` → start continuous decode pipeline
4. `read-flink-statement` → verify INSERT is running (no error)

### Other MCP Operations

- **List:** `mcp__confluent__list-flink-statements`
- **Read/results:** `mcp__confluent__read-flink-statement`
- **Delete:** `mcp__confluent__delete-flink-statements`

---

## Common Pitfalls and Solutions

**Pitfall 1: Using explicit connector/format properties**
- **Problem:** `'value.format' = 'avro-confluent'` or `'connector' = 'kafka'` in CREATE TABLE
- **Solution:** Remove all format/connector/bootstrap/SR properties. Cloud Flink handles this automatically.

**Pitfall 2: Creating explicit source tables for CDC topics**
- **Problem:** Manually creating source tables with `before`/`after` ROW types
- **Solution:** CDC source tables are auto-discovered. Just reference them directly.

**Pitfall 3: Type mismatch on timestamp columns**
- **Problem:** Debezium MicroTimestamp is BIGINT, not TIMESTAMP. INSERT fails with type error.
- **Solution:** Use `TO_TIMESTAMP_LTZ(col / 1000, 3)` and `TIMESTAMP_LTZ(3)` in target.

**Pitfall 4: Using TIMESTAMP(3) instead of TIMESTAMP_LTZ(3)**
- **Problem:** `TO_TIMESTAMP_LTZ()` returns `TIMESTAMP_LTZ(3)`, incompatible with `TIMESTAMP(3)`.
- **Solution:** Define target columns as `TIMESTAMP_LTZ(3)`.

For connector-level data type issues (DECIMAL garbled bytes, INTERVAL lossy values, binary breaking JSON, Oracle NUMBER structs), see `references/connector-configs.md` Data Type Serialization and `references/troubleshooting.md` Data Quality Issues. These must be fixed at the connector level, not in Flink SQL.

---

## Pattern 6: Working with Schemaless Topics (No Schema Registry)

When a topic has no schema in SR, Flink cannot auto-discover its structure. The preferred fix is to register a schema in SR or use schema inference — see `references/connector-configs.md` "Handling Topics Without Schema Registry" for all options.

If you need to work with the topic directly in Flink without registering a schema, use the raw BYTES approach:

```sql
-- Flink infers schemaless topics as raw bytes
-- Modify to STRING to work with JSON payloads
ALTER TABLE `raw_topic` MODIFY (`key` STRING, `val` STRING);

-- Extract fields using JSON functions
INSERT INTO `target_customers`
SELECT
  JSON_VALUE(`val`, '$.id' RETURNING INT) AS `id`,
  JSON_VALUE(`val`, '$.name') AS `name`,
  JSON_VALUE(`val`, '$.email') AS `email`,
  TO_TIMESTAMP_LTZ(
    CAST(JSON_VALUE(`val`, '$.created_at') AS BIGINT) / 1000, 3
  ) AS `created_at`
FROM `raw_topic`;
```

This works for any JSON payload regardless of how it was serialized. It's more fragile than schema-based approaches (no type safety, no schema evolution), but useful for ad-hoc exploration or topics you can't modify.

---

## Pattern 7: Multi-Event Topics

Multi-event topics use `RecordNameStrategy` or `TopicRecordNameStrategy` instead of the default `TopicNameStrategy`, putting multiple schemas on one topic. Flink can't auto-discover these. For a detailed breakdown of each strategy, see `references/connector-configs.md` "Subject Name Strategies".

### Splitting Multi-Event Topics into Typed Target Tables

Use the raw BYTES approach with JSON functions to split by event type:

```sql
-- Multi-event topic: Flink infers as raw bytes since it can't resolve the schema
-- Modify to STRING to parse JSON payloads
ALTER TABLE `shared_events` MODIFY (`key` STRING, `val` STRING);

-- Route order events to a typed target table
INSERT INTO `target_orders`
SELECT
  JSON_VALUE(`val`, '$.order_id' RETURNING INT),
  JSON_VALUE(`val`, '$.customer_id' RETURNING INT),
  JSON_VALUE(`val`, '$.amount' RETURNING DOUBLE)
FROM `shared_events`
WHERE JSON_VALUE(`val`, '$.event_type') = 'order';

-- Route customer events to a separate typed target table
INSERT INTO `target_customers`
SELECT
  JSON_VALUE(`val`, '$.customer_id' RETURNING INT),
  JSON_VALUE(`val`, '$.name'),
  JSON_VALUE(`val`, '$.email')
FROM `shared_events`
WHERE JSON_VALUE(`val`, '$.event_type') = 'customer';
```

Each INSERT runs as a separate continuous Flink job. The target topics each use `TopicNameStrategy` (the default for Flink-created topics), so they have a single schema and work normally with Tableflow.

**Long-term recommendation:** Migrate to `TopicNameStrategy` with one topic per event type for any topics that need to flow into Tableflow.

---

## Resources

- Confluent Cloud Flink SQL Reference: https://docs.confluent.io/cloud/current/flink/reference/overview.html
- Confluent Cloud Flink CREATE TABLE: https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html
- Debezium CDC Connectors: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-debezium.html
