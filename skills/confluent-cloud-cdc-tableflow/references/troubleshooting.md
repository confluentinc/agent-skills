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

**Cause:** Managed MySQL services (RDS/Aurora, Cloud SQL, Azure) don't grant `SUPER`, so Debezium falls back to per-table `LOCK TABLES` which needs an explicit grant. See `references/database-prerequisites.md` MySQL section for details.

**Solution:**
```sql
GRANT LOCK TABLES ON <database>.* TO '<connector_user>'@'%';
FLUSH PRIVILEGES;
```

Then restart or recreate the connector.

### Snapshot Takes Too Long / Unclear if Pipeline is Broken

**Symptom:** After creating the connector with `snapshot.mode: initial` on a large table, no data appears in the target topic for hours. Unclear whether the snapshot is still running or the pipeline is broken.

**Cause:** Initial snapshots on large tables (100M+ rows) can take hours or days. The skill's Phase 4 verification can't distinguish "waiting for snapshot" from "broken pipeline."

**Diagnosis:**
1. Check if the connector has tasks assigned (provisioned):
   ```
   mcp__confluent__read-connector(connectorName, environmentId, clusterId)
   ```
   If `tasks: [{...}]` → connector is provisioned and likely running the snapshot.

2. Check if schemas have been registered (snapshot has started producing):
   ```
   mcp__confluent__list-schemas(subjectPrefix: "<topic-prefix>")
   ```
   Schemas appearing means the connector has started producing. No schemas after 5+ minutes with tasks assigned = broken.

3. Monitor the source topic for growing message count — in Confluent Cloud UI, check the topic's "Messages In" metric. A steady stream means the snapshot is progressing.

4. Check the source database for active replication connections:
   ```sql
   -- PostgreSQL: check active replication slots
   SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots;

   -- MySQL: check active binlog connections
   SHOW PROCESSLIST;
   ```

**Mitigation for initial validation:**
Use `snapshot.mode: schema_only` to validate the pipeline end-to-end without waiting for the full snapshot. Once confirmed working, delete the connector and recreate with `snapshot.mode: initial`.

### CDC Connector Broken After Kafka Topic Deletion

**Symptom:** After deleting a CDC source Kafka topic (e.g., `mysql-cdc.cdctest.customers`) while the connector was running, the connector enters a failure loop and cannot recover.

**Cause:** The connector tracks its position (offsets, binlog position, LSN, etc.) relative to the Kafka topic. When the topic is deleted, the connector loses its state and cannot resume or re-snapshot automatically.

**Solution:** Delete the connector entirely and recreate it from scratch. The new connector will perform a fresh initial snapshot. **Never delete CDC source Kafka topics while the connector is running.** Always delete the connector first, then clean up topics.

### DynamoDB CDC: Snapshot Works but CDC Streaming Silently Fails

**Symptom:** The connector completes the initial snapshot (records with `sync_mode: SNAPSHOT` appear in the Kafka topic), but subsequent DML changes never appear. The connector shows as RUNNING with a task assigned — no errors visible.

**Cause:** The IAM user is missing write permissions needed for the CDC checkpointing table. The DynamoDB CDC connector uses a KCL-style DynamoDB table to track shard leases during the CDC phase. Without `CreateTable`, `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem` permissions, the connector cannot create or update this table, so the CDC phase cannot track its position in the stream.

**Diagnosis:**
1. Check the source Kafka topic — if only `sync_mode: SNAPSHOT` records exist and no `sync_mode: CDC` records appear after 5+ minutes, the CDC phase is not working
2. Check the IAM policy for the connector's IAM user — it likely only has read permissions (`Scan`, `GetRecords`, `GetShardIterator`, etc.) but is missing write permissions

**Solution:** Update the IAM policy to include all required permissions for both reading DynamoDB Streams and writing to the checkpointing table. See `references/database-prerequisites.md` "DynamoDB CDC Prerequisites" for the complete IAM policy. After updating the IAM policy, delete and recreate the connector.

Also ensure the connector config includes these explicit properties:
```json
{
  "dynamodb.table.discovery.mode": "INCLUDELIST",
  "dynamodb.table.sync.mode": "SNAPSHOT_CDC",
  "dynamodb.cdc.max.poll.records": "5000",
  "dynamodb.snapshot.max.poll.records": "1000"
}
```

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

### Orphaned PostgreSQL Replication Slot After Connector Deletion

**Symptom:** After deleting a PostgreSQL CDC connector, database disk usage grows steadily. Eventually the database may become read-only (on managed services like RDS).

**Cause:** Deleting the connector does NOT drop the replication slot. The orphaned slot holds WAL segments indefinitely.

**Solution:**
```sql
-- Check for orphaned slots (active = false)
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;

-- Drop the orphaned slot
SELECT pg_drop_replication_slot('debezium_slot');
```

**Prevention:** Always clean up the replication slot when deleting a PostgreSQL CDC connector. See `references/database-prerequisites.md` for monitoring guidance.

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

**Requirement:** Tableflow works across all Confluent Cloud cluster types (Basic, Standard, Enterprise, Dedicated, and Freight).

### Tableflow Suspended: Null Value (Tombstone) on APPEND-Mode Topic

**Symptom:** Tableflow suspends with: *"Tableflow will be suspended because we detected a Kafka record at partition X offset Y with a null value. The changelog mode of this table is APPEND."*

**Cause:** CDC connectors with `tombstones.on.delete=true` produce null-value Kafka records (tombstones) when a row is deleted in the source database. Tableflow in APPEND mode cannot handle null values.

**Solution:** **Never enable Tableflow directly on CDC source topics.** Follow the skill's architecture: use a Flink INSERT job to decode the Debezium envelope into a target topic created with `'changelog.mode' = 'upsert'`, then enable Tableflow on that target topic. The Flink decode layer properly translates DELETE operations into upsert-compatible retract/tombstone messages that Tableflow handles correctly.

**Do NOT try these workarounds — they don't work:**
- `record_failure_strategy: SKIP` — Does not skip changelog mode violations, only data-level errors.
- `after.state.only=true` on the connector — Strips the Debezium envelope but tombstones still produce null values. Also, `OracleXStreamSource` does not support this option.
- Changing `changelog.mode` from APPEND to UPSERT after Tableflow has started — See "Changelog Mode Immutable" below.

### Tableflow Error: Changelog Mode Modified

**Symptom:** *"The changelog mode for this topic has been modified since table materialization began from APPEND to UPSERT. Modifying the changelog mode of an existing table is not supported."*

**Cause:** Tableflow caches the changelog mode at first materialization. The mode was APPEND when Tableflow first read from the topic, and someone subsequently altered the topic's `changelog.mode` property to UPSERT.

**Solution:** The cached mode cannot be changed. You must:
1. Delete the Tableflow topic
2. Delete the underlying Kafka topic (this is required because the S3 `table_path` is keyed by topic name — deleting only the Tableflow topic and recreating it reuses the same cached state)
3. Recreate the Kafka topic with `'changelog.mode' = 'upsert'` set from creation (via Flink CREATE TABLE)
4. Re-enable Tableflow on the new topic

### Tableflow Error: Incompatible Schema Evolution

**Symptom:** *"Incompatible schema evolution detected. Field changed from nullable to non-nullable at key."*

**Cause:** Multiple changelog mode changes corrupted the Tableflow S3 state. Flip-flopping between APPEND and UPSERT modes causes schema conflicts because the key nullability changes between modes.

**Solution:** Full pipeline reset required — delete the Tableflow topic, the Kafka topic, all associated schemas, and recreate from scratch. See SKILL.md "Pipeline cleanup order matters" for the correct deletion sequence.

### Tableflow Topic Fails to Activate

**Symptom:** Status stays `FAILED` after enabling.

**Common Causes:**

1. **Topic format is wrong** — Tableflow requires a plain format (JSON_SR or Avro), NOT Debezium envelope. Ensure Flink decodes the envelope before writing to the target topic.

2. **Storage credentials invalid (BYOB only)** — Check Provider Integration and IAM permissions.

3. **Cluster type** — Verify Tableflow is available on your cluster tier and region.

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

### Schema Registry Compatibility Rejects New Schema

**Symptom:** Connector halts or fails to register a new schema after a database column is dropped or a type changes.

**Cause:** The default Schema Registry compatibility mode is `BACKWARD`. If Debezium registers a schema with a removed field, `BACKWARD` compatibility rejects it because existing consumers might depend on that field.

**Solution:**
1. Check current compatibility:
   ```bash
   confluent schema-registry config describe --environment <env-id>
   ```
2. For CDC subjects, set `FULL_TRANSITIVE` compatibility (allows both forward and backward evolution):
   ```bash
   confluent schema-registry config update --subject "<topic-prefix>.<schema>.<table>-value" --compatibility FULL_TRANSITIVE --environment <env-id>
   ```
3. If you need to apply this globally for all CDC subjects, update the global default — but be aware this affects non-CDC subjects too.

**Prevention:** Check SR compatibility during Phase 1 (Discovery) before creating the connector.

### Topic Has No Schema — Flink Can't Auto-Discover

**Symptom:** A topic exists and has messages, but doesn't appear in `SHOW TABLES` in Flink.

**Cause:** The topic was produced without a Schema Registry serializer (plain `JSON` / `StringSerializer`), so no schema is registered.

**Solution:** Register a JSON schema in SR (partial schemas work for JSON), use [schema inference](https://docs.confluent.io/cloud/current/sr/schemas-manage.html#infer-a-schema-from-messages), or use Flink's raw BYTES approach. See `references/connector-configs.md` "Handling Topics Without Schema Registry" for all options and `references/flink-sql-patterns.md` Pattern 6 for Flink-specific code.

### Multi-Event Topic — Mixed Schemas on One Topic

**Symptom:** A topic has messages from multiple event types. Flink can't auto-discover it, or some records fail deserialization.

**Cause:** The topic uses `RecordNameStrategy` or `TopicRecordNameStrategy` instead of the default `TopicNameStrategy`. Flink expects one schema per topic.

**Diagnosis:** Check which subjects exist:
```bash
confluent schema-registry subject list --environment <env-id> | grep "<topic-name>"
```
Subjects like `com.example.Order-value` instead of `<topic-name>-value` indicate `RecordNameStrategy`.

**Solution:** Split into typed target tables using Flink. See `references/flink-sql-patterns.md` Pattern 7 for code and `references/connector-configs.md` "Subject Name Strategies" for strategy details.

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
