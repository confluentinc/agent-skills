# Troubleshooting Guide: CDC to Tableflow Pipeline

This guide covers common issues when setting up and running CDC pipelines with Debezium, Flink, and Tableflow on Confluent Cloud.

## Table of Contents

1. [MCP Server Issues](#mcp-server-issues)
2. [Connector Issues](#connector-issues)
3. [Flink Issues](#flink-issues)
4. [Tableflow Issues](#tableflow-issues)
5. [Schema Registry Issues](#schema-registry-issues)
6. [Performance Issues](#performance-issues)
7. [Data Quality Issues](#data-quality-issues)

---

## MCP Server Issues

### MCP Cluster Targeting

**Problem:** MCP operations (`consume-messages`, `list-topics`, `create-tableflow-topic`) target the wrong Kafka cluster.

**Cause:** The MCP server's `BOOTSTRAP_SERVERS` and `KAFKA_API_KEY` in the MCP config (`~/.config/claude/mcp.json`) determine which cluster it connects to for Kafka operations.

**Diagnosis:** Run `mcp__confluent__list-topics` and check if the expected topics appear. If you see topics from a different cluster, the MCP config points to the wrong cluster.

**Solution:** Update the MCP config to set `BOOTSTRAP_SERVERS`, `KAFKA_API_KEY`, and `KAFKA_API_SECRET` for the target cluster. Then reconnect MCP with `/mcp`.

### MCP Tableflow create-topic Doesn't Accept Cluster/Environment ID

**Problem:** `mcp__confluent__create-tableflow-topic` returns "topic not found" even though the topic exists on the target cluster.

**Cause:** The MCP tool doesn't have cluster/environment ID parameters and defaults based on the MCP server's bootstrap config.

**Solution:** Either:
1. Update MCP config to point to the correct cluster, reconnect with `/mcp`, then retry
2. Enable Tableflow through the Confluent Cloud UI: Environment → Cluster → Topics → topic → Tableflow tab

### MCP read-connector Doesn't Show Status/Errors

**Problem:** Can't see if a connector is RUNNING, FAILED, or PROVISIONING via MCP.

**Cause:** The MCP `read-connector` tool returns config and tasks but not explicit status or error messages.

**Diagnosis:**
- `tasks: []` → Still provisioning (wait 2-5 minutes)
- `tasks: [{...}]` → Tasks assigned, connector is running
- Check `mcp__confluent__list-schemas(subjectPrefix)` → If schemas appear, connector is producing data

**Workaround:** Use the Confluent Cloud UI for detailed connector status and error logs.

---

## Connector Issues

### Missing `topic.prefix`

**Symptom:** Connector creation fails with "topic.prefix is required".

**Solution:** Add `"topic.prefix": "<prefix>"` to connector config. This is a REQUIRED field for all CDC V2 connectors. It controls topic naming: `{topic.prefix}.{schema}.{table}`.

### Managed Connector Provisioning Delay

**Symptom:** Connector shows no tasks (`"tasks": []`) after creation.

**Cause:** Managed connectors on Confluent Cloud take 2-5 minutes to provision.

**Solution:** Poll connector status every 30-60 seconds using `mcp__confluent__read-connector`. Tasks will appear once provisioning completes. Only investigate failures if tasks don't appear after 5 minutes.

### Connector Fails to Start

**Symptom:** Connector status shows `FAILED` immediately after creation.

**Common Causes:**

1. **Database connectivity issues**

   Look for:
   - `Connection refused` → Network/firewall blocking access
   - `Authentication failed` → Wrong username/password
   - `Database not found` → Wrong database name

   **Solution:**
   - Verify database hostname, port, and credentials
   - Ensure firewall allows Confluent Cloud IP ranges
   - For cloud databases (RDS, Cloud SQL), check security groups

2. **Database CDC not properly configured**

   **PostgreSQL:**
   ```sql
   SHOW wal_level;  -- Must be 'logical'
   SHOW max_replication_slots;  -- Must be > 0
   ```

   **MySQL:**
   ```sql
   SHOW VARIABLES LIKE 'log_bin';  -- Must be ON
   SHOW VARIABLES LIKE 'binlog_format';  -- Must be ROW
   ```

   **SQL Server:**
   ```sql
   SELECT is_cdc_enabled FROM sys.databases WHERE name = '<db>';
   SELECT name, is_tracked_by_cdc FROM sys.tables;
   ```

   **Solution:** Follow database prerequisites in `references/database-prerequisites.md`

3. **Schema Registry connection issues**

   Error: `Failed to connect to Schema Registry`

   **Solution:** Verify Schema Registry is enabled for the environment

4. **Invalid configuration**

   Error: `Invalid configuration` or `Missing required property`

   **Solution:** Check connector class name, verify all required fields, ensure table names use `schema.table` format

### MySQL Connector Fails: "LOCK TABLES privilege required"

**Symptom:** Connector fails during initial snapshot with: *"The database user does not have the 'LOCK TABLES' privilege required to obtain a consistent snapshot by preventing concurrent writes to tables."*

**Cause:** This happens on all managed MySQL services (AWS RDS/Aurora, Google Cloud SQL, Azure Database for MySQL). Debezium first tries `FLUSH TABLES WITH READ LOCK` (requires `SUPER`), but managed services don't grant `SUPER`. Debezium then falls back to per-table `LOCK TABLES`, which requires an explicit `LOCK TABLES` grant. On self-managed MySQL where the user has `SUPER`, this error doesn't occur because the global lock succeeds.

**Solution:**
```sql
GRANT LOCK TABLES ON <database>.* TO '<connector_user>'@'%';
FLUSH PRIVILEGES;
```

Then restart or recreate the connector.

### Connector Runs but No Messages

**Symptom:** Connector has tasks assigned but no topics/schemas appear.

**Diagnosis with MCP:**
```
mcp__confluent__list-schemas(subjectPrefix: "<topic-prefix>")
mcp__confluent__search-topics-by-name(topicName: "<topic-prefix>")
```

**Common Causes:**
1. Initial snapshot still in progress (wait a few more minutes)
2. `table.include.list` filter excludes all tables (check case sensitivity)
3. `snapshot.mode = "never"` and no recent database changes

---

## Flink Issues

### Invalid Format Specifier in Cloud Flink

**Symptom:** CREATE TABLE fails with "Unsupported format: avro-confluent".

**Cause:** Confluent Cloud Flink handles formats automatically. Explicit format specs like `'value.format' = 'avro-confluent'` are NOT supported.

**Solution:** Remove ALL format, connector, bootstrap, and Schema Registry properties from CREATE TABLE statements. Only use `'changelog.mode' = 'upsert'` in the WITH clause.

### Type Mismatch on Timestamp Columns

**Symptom:** INSERT fails with "Incompatible types for sink column 'created_at'". Query schema shows BIGINT but sink expects TIMESTAMP.

**Cause:** Debezium uses `io.debezium.time.MicroTimestamp` which maps to BIGINT (microseconds since epoch).

**Solution:**
1. Define target column as `TIMESTAMP_LTZ(3)` (not `TIMESTAMP(3)`)
2. Convert in INSERT: `TO_TIMESTAMP_LTZ(col / 1000, 3)` for MicroTimestamp, `TO_TIMESTAMP_LTZ(col, 3)` for Timestamp

### CDC Source Table Not Appearing in SHOW TABLES

**Symptom:** `SHOW TABLES` doesn't list the CDC topic table after connector creation.

**Cause:** The CDC connector hasn't produced data yet (still provisioning or doing initial snapshot).

**Solution:**
1. Verify connector has tasks: `mcp__confluent__read-connector`
2. Verify schemas exist: `mcp__confluent__list-schemas(subjectPrefix: "<topic-prefix>")`
3. Wait 2-5 minutes, then retry SHOW TABLES
4. Ensure `databaseName` in the Flink statement matches the cluster display name where the topic exists

### Flink Statement Fails to Create

**Common Errors:**

1. **Syntax error** (`SQL parse failed`)
   - Check Confluent Cloud Flink SQL syntax
   - Ensure proper backtick-quoting for table/column names with special chars

2. **Table does not exist**
   - CDC source table not yet auto-discovered
   - Wrong `catalogName` or `databaseName` in MCP statement creation
   - Topic on a private cluster without network access

3. **Statement name invalid**
   - Must be lowercase kebab-case: `[a-z0-9]([-a-z0-9]*[a-z0-9])?`
   - Max 100 characters

### Flink INSERT Runs but No Data Output

**Diagnosis with MCP:**
```
mcp__confluent__consume-messages(topicNames: ["<target-topic>"], value: {useSchemaRegistry: true})
```

Note: Consumer starts at latest offset. If snapshot already completed, insert a new row in the database to verify.

**Common Causes:**
1. WHERE clause filters all records
2. Schema mismatch between source and target (check column types)
3. Source table not receiving data (debug connector first)

### Flink Advisory Warnings (Can Be Ignored)

These warnings appear on INSERT statements but don't prevent execution:
- "Primary key does not match upsert key" — Expected for CDC decode patterns
- "Highly state-intensive operators without TTL" — Set TTL if needed for production

---

## Tableflow Issues

**Important:** Tableflow is a **native topic feature** on Confluent Cloud, NOT a sink connector. It is enabled per-topic and materializes data as Iceberg or Delta tables.

**Storage options:**
- **Managed** — Confluent manages the S3 storage
- **BYOB (Bring Your Own Bucket)** — User provides S3 bucket with Provider Integration

**Requirement:** Tableflow requires a **Dedicated** cluster. Basic and Standard clusters do not support Tableflow.

### Tableflow Topic Fails to Activate

**Symptom:** Status stays `FAILED` after enabling.

**Common Causes:**

1. **Topic format is wrong** — Tableflow requires a plain format (JSON_SR or Avro), NOT Debezium envelope. Ensure Flink decodes the envelope before writing to the target topic.

2. **Storage credentials invalid (BYOB only)** — Check Provider Integration and IAM permissions.

3. **Cluster type** — Must be a Dedicated cluster.

### Tableflow Not Writing Files

**Symptom:** Tableflow is active but no files appear.

**Causes:**
1. Not enough data to trigger flush (wait longer or insert more data)
2. Storage path doesn't exist (BYOB only)

### MCP Tableflow Creation Fails

**Symptom:** `mcp__confluent__create-tableflow-topic` returns "topic not found".

**Cause:** MCP targets the wrong cluster (see MCP Server Issues above).

**Solution:** Update MCP config to point to the dedicated cluster, or use the Confluent Cloud UI.

---

## Schema Registry Issues

### Schema Not Registered

**Diagnosis with MCP:**
```
mcp__confluent__list-schemas(subjectPrefix: "<topic-prefix>")
```

**Solutions:**
1. Wait for connector/Flink to produce first message (auto-registers schema)
2. Check subject naming: `<topic-name>-value` and `<topic-name>-key`
3. Schema compatibility violation — add optional fields, don't remove required ones

### Stale Schema IDs After Deleting and Recreating Schemas

**Symptom:** After deleting schemas and recreating a CDC connector, Flink goes DEGRADED with errors like `"Schema ID 100001 not found"` or `"Could not find schema for subject"`.

**Cause:** Old messages still on the CDC source topics reference schema IDs that were deleted from Schema Registry. When Flink tries to deserialize these messages, it fails because the schema IDs no longer exist.

**Solution:** When doing a clean reset of a CDC pipeline, you must delete resources in this order:
1. Delete the Flink INSERT statements
2. Delete the CDC connector
3. Delete the CDC source topics (not just schemas) — topics like `{topic.prefix}.{schema}.{table}` and any heartbeat topics
4. Hard-delete all associated schemas from Schema Registry (both `-key` and `-value` subjects)
5. Drop the replication slot in PostgreSQL: `SELECT pg_drop_replication_slot('<slot_name>');`
6. Recreate the connector fresh — it will create new topics, register new schemas, and do a clean initial snapshot

**Important:** Simply deleting schemas without deleting the topics does NOT work. The old messages with stale schema IDs remain on the topics and will cause Flink failures.

### Schema Evolution

When database schemas change:
1. Stop Flink INSERT statement (`mcp__confluent__delete-flink-statements`)
2. Drop and recreate target table with new schema
3. Recreate INSERT statement
4. Verify new data flows correctly

---

## Performance Issues

### High Latency

Check latency at each stage:
1. **Connector lag** — `mcp__confluent__read-connector` (check if tasks are healthy)
2. **Flink processing** — `mcp__confluent__read-flink-statement` (check for backpressure)
3. **Tableflow lag** — `mcp__confluent__read-tableflow-topic` (check status)

**Solutions:**
- Increase Flink compute pool CFU
- Optimize connector config (disable schema changes, adjust batch size)
- Check for data skew (hotkey issues)

---

## Data Quality Issues

### DECIMAL/NUMERIC Columns Show Garbled Data

**Symptom:** Price or other DECIMAL columns appear as garbled bytes (e.g., `"N\u001f"` instead of `"199.99"`) in Flink output or consumed messages.

**Cause:** By default, Debezium serializes PostgreSQL DECIMAL/NUMERIC columns as raw bytes (Java BigDecimal unscaled value). This is the default `decimal.handling.mode = precise` behavior.

**Solution:** Add `"decimal.handling.mode": "string"` to the Debezium connector config. This outputs human-readable decimal values like `"1299.99"`. This must be fixed at the connector level — Flink cannot CAST VARBINARY to DECIMAL, so there is no workaround in Flink SQL.

**Note:** After changing this setting, you need to recreate the connector. If existing messages on the topic already have bytes-encoded decimals, follow the "Stale Schema IDs" cleanup procedure above for a clean reset.

### INTERVAL Columns Produce Wrong Values

**Symptom:** PostgreSQL `INTERVAL` or Oracle `INTERVAL YEAR TO MONTH` / `INTERVAL DAY TO SECOND` columns produce large integer values instead of human-readable intervals.

**Cause:** Default `interval.handling.mode = numeric` approximates intervals as microseconds, using lossy conversions (1 month = 30 days, 1 year = 365.25 days). An interval of `1 year` becomes `31557600000000` microseconds.

**Solution:** Add `"interval.handling.mode": "string"` to the connector config. Outputs ISO 8601 format like `P1Y2M3DT4H5M6.78S`.

### Binary Columns Produce Garbled JSON

**Symptom:** BYTEA (PostgreSQL), VARBINARY/BLOB (MySQL), BINARY/IMAGE (SQL Server), or RAW/BLOB (Oracle) columns produce garbled or inconsistent output in consumed messages.

**Cause:** Default `binary.handling.mode = bytes` produces raw byte arrays that break JSON serialization.

**Solution:** Add `"binary.handling.mode": "base64"` to the connector config. Outputs base64-encoded strings that are JSON-safe.

### Oracle NUMBER Without Precision Produces Struct

**Symptom:** Oracle `NUMBER` (no precision/scale) or `FLOAT` columns produce a nested struct instead of a simple value, breaking Flink schemas.

**Cause:** Debezium serializes these as `VariableScaleDecimal` — a struct of `{scale: INT32, value: BYTES}` — because the precision is unknown at schema time.

**Solution:** Add `"decimal.handling.mode": "string"` to the connector config. Converts all numeric types to human-readable strings.

### Duplicate Records

**Causes:**
1. `snapshot.mode = 'always'` → Use `'initial'` instead
2. Flink restart replays records → Ensure target table has PRIMARY KEY

### Missing Records

**Diagnosis:**
1. Check schemas exist for both source and target topics
2. Verify Flink INSERT is running
3. Check for overly restrictive WHERE clauses

---

## Getting Help

- **Confluent Support:** https://support.confluent.io/
- **Confluent Cloud Troubleshooting:** https://docs.confluent.io/cloud/current/troubleshooting/index.html
- **Debezium Troubleshooting:** https://debezium.io/documentation/reference/stable/operations/index.html
- **Flink SQL Debugging:** https://docs.confluent.io/cloud/current/flink/troubleshooting.html
- **Community Forum:** https://forum.confluent.io/
- **Community Slack:** https://confluentcommunity.slack.com/
