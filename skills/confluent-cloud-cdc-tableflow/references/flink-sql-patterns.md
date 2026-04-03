# Flink SQL Patterns for Debezium CDC in Confluent Cloud

This guide covers Flink SQL patterns for working with Debezium CDC events in Confluent Cloud Flink. In Confluent Cloud, Flink is deeply integrated with Schema Registry and Kafka -- source tables from CDC connectors are auto-discovered, and no explicit connector or format properties are needed in CREATE TABLE statements.

---

## Key Principle: Auto-Discovery of CDC Source Tables

When a Debezium CDC connector creates topics with schemas registered in Schema Registry, Confluent Cloud Flink **automatically discovers** those topics as Flink tables. You do NOT need to create source tables manually.

For example, if a CDC connector with `topic.prefix = "postgres-cdc"` captures the `public.customers` table, the topic `postgres-cdc.public.customers` automatically appears as a Flink table:

```sql
-- This table already exists -- no CREATE TABLE needed
-- Reference it with backtick-quoting due to dots in the name:
SELECT * FROM `postgres-cdc.public.customers` LIMIT 5;
```

Verify auto-discovered tables with:

```sql
SHOW TABLES;
```

If the CDC source table does not appear, the connector may not have produced data yet. Wait 2-5 minutes for the connector to fully provision and produce its initial snapshot.

---

## Debezium Type Mappings

Debezium uses specific logical types that map to Flink types differently than you might expect. Understanding these is critical to avoid type mismatches.

| Debezium Logical Type | Flink Column Type | Meaning | Conversion |
|---|---|---|---|
| `io.debezium.time.MicroTimestamp` | `BIGINT` | Microseconds since epoch | `TO_TIMESTAMP_LTZ(col / 1000, 3)` |
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

When capturing multiple tables, create a target table and INSERT statement for each. Each INSERT runs as a separate continuous Flink statement.

```sql
-- Target tables (source tables are auto-discovered)
CREATE TABLE `target_customers` (
  `id` INT NOT NULL,
  `name` STRING,
  `email` STRING,
  `created_at` TIMESTAMP_LTZ(3),
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('changelog.mode' = 'upsert');

CREATE TABLE `target_orders` (
  `order_id` BIGINT NOT NULL,
  `customer_id` INT,
  `amount` DECIMAL(10, 2),
  `order_date` TIMESTAMP_LTZ(3),
  PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH ('changelog.mode' = 'upsert');

-- INSERT statements (each submitted as a separate Flink statement)
INSERT INTO `target_customers`
SELECT `id`, `name`, `email`, TO_TIMESTAMP_LTZ(`created_at` / 1000, 3)
FROM `postgres-cdc.public.customers`;

INSERT INTO `target_orders`
SELECT `order_id`, `customer_id`, `amount`, TO_TIMESTAMP_LTZ(`order_date` / 1000, 3)
FROM `postgres-cdc.public.orders`;
```

**Important:** Each INSERT must be submitted as a separate Flink statement via MCP.

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

**Pitfall 5: DECIMAL columns appear as garbled BYTES**
- **Problem:** DECIMAL/NUMERIC columns show as raw bytes (e.g., `"N\u001f"` instead of `"199.99"`). Flink cannot CAST VARBINARY to DECIMAL.
- **Solution:** This must be fixed at the connector level, not in Flink. Set `"decimal.handling.mode": "string"` in the Debezium connector config. This outputs decimal values as human-readable strings which Flink can handle directly.

**Pitfall 6: INTERVAL columns produce lossy microsecond values**
- **Problem:** PostgreSQL `INTERVAL` or Oracle `INTERVAL YEAR TO MONTH` columns arrive as INT64 microseconds, approximating months as 30 days and years as 365.25 days.
- **Solution:** Set `"interval.handling.mode": "string"` on the connector. Values arrive as ISO 8601 strings (e.g., `P1Y2M3DT4H5M6.78S`).

**Pitfall 7: Binary columns break JSON serialization**
- **Problem:** BYTEA, VARBINARY, BLOB, or RAW columns serialized as raw bytes produce garbled or inconsistent JSON output.
- **Solution:** Set `"binary.handling.mode": "base64"` on the connector. Values arrive as base64-encoded strings.

**Pitfall 8: Oracle NUMBER without precision produces struct, not scalar**
- **Problem:** Oracle `NUMBER` (no precision/scale) and `FLOAT` columns are serialized as `VariableScaleDecimal` — a struct of `{scale: INT32, value: BYTES}`, not a simple value. This breaks differently than regular DECIMAL bytes.
- **Solution:** Set `"decimal.handling.mode": "string"` on the connector. Converts to human-readable string values.

**Pitfall 9: CDC source table not in SHOW TABLES**
- **Problem:** Connector just created, table not visible yet.
- **Solution:** Wait 2-5 minutes for connector provisioning. Verify schemas exist via `mcp__confluent__list-schemas`.

---

## Resources

- Confluent Cloud Flink SQL Reference: https://docs.confluent.io/cloud/current/flink/reference/overview.html
- Confluent Cloud Flink CREATE TABLE: https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html
- Debezium CDC Connectors: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-debezium.html
