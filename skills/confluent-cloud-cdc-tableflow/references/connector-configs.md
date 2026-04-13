# Connector Configuration Templates

This file contains configuration templates for each supported Debezium CDC connector type. All configurations use Schema Registry and are designed for Confluent Cloud fully-managed connectors.

## Using MCP to Create Connectors

**Preferred method:** Use `mcp__confluent__create-connector` with these parameters:
- `connectorName`: The connector name
- `environmentId`: The Confluent Cloud environment ID
- `clusterId`: The Kafka cluster ID
- `connectorConfig`: A flat map of string key-value pairs (the JSON config below)

**Important:** The `kafka.api.key` and `kafka.api.secret` in the connector config must be scoped to the target Kafka cluster. These may differ from the MCP server's own API keys if the MCP server is configured for a different cluster.

## Common Configuration Elements

All connectors share these common settings:

```json
{
  "connector.class": "<connector-class>",
  "name": "<connector-name>",
  "topic.prefix": "<topic-prefix>",

  "kafka.api.key": "<KAFKA_API_KEY>",
  "kafka.api.secret": "<KAFKA_API_SECRET>",

  "output.data.format": "JSON_SR",
  "output.key.format": "JSON_SR",

  "decimal.handling.mode": "string",
  "binary.handling.mode": "base64",

  "tasks.max": "1"
}
```

**REQUIRED field: `topic.prefix`** — Controls topic naming. All CDC V2 connectors require this. Topics will be named `{topic.prefix}.{schema}.{table}`.

**Schema Format Options:**
- `JSON_SR` (default, recommended — JSON with Schema Registry, uses schema ID in header; human-readable payloads, easiest to debug with `consume-messages`)
- `AVRO` (binary format — more compact and efficient for high-throughput pipelines; requires Avro-compatible consumers; best for large-scale production CDC)
- `PROTOBUF` (binary format — strongly typed with nested message support; good when downstream consumers already use Protobuf)

**Format selection guidance:**
| Criteria | JSON_SR | AVRO | PROTOBUF |
|---|---|---|---|
| Debugging / readability | Best — human-readable payloads | Poor — binary | Poor — binary |
| Throughput / message size | Largest payloads | ~30-50% smaller than JSON_SR | ~30-50% smaller than JSON_SR |
| Flink auto-discovery | Yes | Yes | Yes |
| Tableflow compatibility | Yes | Yes | Yes |
| Schema evolution | Supported via SR | Best support via SR | Supported via SR |

All three formats register schemas in Schema Registry and work identically with Flink auto-discovery and Tableflow. The choice is primarily about payload size vs. debuggability.

**Important:** `output.data.format` and `output.key.format` must both use a Schema Registry-backed format (`JSON_SR`, `AVRO`, or `PROTOBUF`). Using plain `JSON` (without SR) means no schema is registered, which breaks Flink auto-discovery and Tableflow. See "Handling Topics Without Schema Registry" below.

To use Avro or Protobuf, change both format fields in the connector config:
```json
{
  "output.data.format": "AVRO",
  "output.key.format": "AVRO"
}
```
or:
```json
{
  "output.data.format": "PROTOBUF",
  "output.key.format": "PROTOBUF"
}
```
All other connector settings (type mappings, snapshot mode, etc.) remain the same regardless of format.

**Note on managed connectors:** Fields like `kafka.auth.mode` and `kafka.endpoint` are auto-configured by Confluent Cloud when using MCP or the CLI. You only need to provide `kafka.api.key` and `kafka.api.secret`.

**Provisioning time:** Managed connectors take 2-5 minutes to provision. The connector will show `tasks: []` until provisioning completes. Poll status until tasks appear.

---

## PostgreSQL CDC Source V2

**Connector Class:** `PostgresCdcSourceV2`

```json
{
  "connector.class": "PostgresCdcSourceV2",
  "name": "cdc-pipeline-skill-postgres-connector",

  "kafka.api.key": "${file:/secrets.properties:KAFKA_API_KEY}",
  "kafka.api.secret": "${file:/secrets.properties:KAFKA_API_SECRET}",

  "database.hostname": "<postgres-host>",
  "database.port": "5432",
  "database.user": "<postgres-user>",
  "database.password": "<postgres-password>",
  "database.dbname": "<database-name>",
  "database.server.name": "<logical-server-name>",
  "topic.prefix": "<topic-prefix>",

  "table.include.list": "<schema>.<table1>,<schema>.<table2>",

  "plugin.name": "pgoutput",
  "publication.name": "dbz_publication",
  "slot.name": "debezium_slot",

  "output.data.format": "JSON_SR",
  "output.key.format": "JSON_SR",

  "snapshot.mode": "initial",

  "tombstones.on.delete": "true",

  "heartbeat.interval.ms": "30000",
  "heartbeat.action.query": "INSERT INTO heartbeat (ts) VALUES (NOW())",

  "decimal.handling.mode": "string",
  "binary.handling.mode": "base64",
  "interval.handling.mode": "string",
  "hstore.handling.mode": "map",

  "tasks.max": "1"
}
```

**PostgreSQL-specific parameters:**
- `plugin.name`: Use `pgoutput` (native PostgreSQL logical replication)
- `publication.name`: Publication created in PostgreSQL
- `slot.name`: Replication slot name (must be unique)
- `interval.handling.mode`: Set to `string` for INTERVAL columns (default `numeric` is lossy)
- `hstore.handling.mode`: Set to `map` for HSTORE columns

For `topic.prefix`, `decimal.handling.mode`, `binary.handling.mode`, `snapshot.mode`, and other shared settings, see Common Configuration Elements and Configuration Best Practices above.

**Topic Naming:**
Topic pattern: `<topic.prefix>.<schema>.<table>`

Example: `postgres-cdc.public.users`

**Documentation:**
https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html

---

## MySQL CDC Source V2

**Connector Class:** `MySqlCdcSourceV2`

```json
{
  "connector.class": "MySqlCdcSourceV2",
  "name": "cdc-pipeline-skill-mysql-connector",

  "kafka.api.key": "${file:/secrets.properties:KAFKA_API_KEY}",
  "kafka.api.secret": "${file:/secrets.properties:KAFKA_API_SECRET}",

  "database.hostname": "<mysql-host>",
  "database.port": "3306",
  "database.user": "<mysql-user>",
  "database.password": "<mysql-password>",
  "database.server.id": "184054",
  "database.server.name": "<logical-server-name>",
  "topic.prefix": "<topic-prefix>",

  "database.include.list": "<database-name>",
  "table.include.list": "<database>.<table1>,<database>.<table2>",

  "output.data.format": "JSON_SR",
  "output.key.format": "JSON_SR",

  "snapshot.mode": "initial",

  "tombstones.on.delete": "true",

  "include.schema.changes": "false",

  "gtid.source.includes": ".*",

  "decimal.handling.mode": "string",
  "binary.handling.mode": "base64",

  "tasks.max": "1"
}
```

**MySQL-specific parameters:**
- `database.server.id`: Unique numeric ID for this connector (5-10 digits)
- `database.include.list`: Comma-separated list of databases
- `table.include.list`: Format: `database.table` (not `schema.table`)
- `gtid.source.includes`: GTID filter (use `.*` for all)

**Topic Naming:**
Topic pattern: `<topic.prefix>.<database>.<table>`

Example: `mysql-cdc.inventory.customers`

**Documentation:**
https://docs.confluent.io/cloud/current/connectors/cc-mysql-cdc-source-v2-debezium/cc-mysql-cdc-source-v2-debezium.html

---

## SQL Server CDC Source V2

**Connector Class:** `SqlServerCdcSourceV2`

```json
{
  "connector.class": "SqlServerCdcSourceV2",
  "name": "cdc-pipeline-skill-sqlserver-connector",

  "kafka.api.key": "${file:/secrets.properties:KAFKA_API_KEY}",
  "kafka.api.secret": "${file:/secrets.properties:KAFKA_API_SECRET}",

  "database.hostname": "<sqlserver-host>",
  "database.port": "1433",
  "database.user": "<sqlserver-user>",
  "database.password": "<sqlserver-password>",
  "database.names": "<database-name>",
  "database.server.name": "<logical-server-name>",
  "topic.prefix": "<topic-prefix>",

  "table.include.list": "dbo.<table1>,dbo.<table2>",

  "output.data.format": "JSON_SR",
  "output.key.format": "JSON_SR",

  "snapshot.mode": "initial",

  "tombstones.on.delete": "true",

  "include.schema.changes": "false",

  "decimal.handling.mode": "string",
  "binary.handling.mode": "base64",

  "tasks.max": "1"
}
```

**SQL Server-specific parameters:**
- `database.names`: Comma-separated list of databases
- `table.include.list`: Format: `schema.table` (use `dbo` for default schema)

**Topic Naming:**
Topic pattern: `<topic.prefix>.<schema>.<table>`

Example: `sqlserver-cdc.dbo.orders`

**Documentation:**
https://docs.confluent.io/cloud/current/connectors/cc-microsoft-sql-server-cdc-source-v2-debezium/cc-microsoft-sql-server-cdc-source-v2-debezium.html

---

## Oracle XStream CDC Source

**Connector Class:** `OracleXStreamSource`

```json
{
  "connector.class": "OracleXStreamSource",
  "name": "cdc-pipeline-skill-oracle-connector",

  "kafka.api.key": "${file:/secrets.properties:KAFKA_API_KEY}",
  "kafka.api.secret": "${file:/secrets.properties:KAFKA_API_SECRET}",

  "database.hostname": "<oracle-host>",
  "database.port": "1521",
  "database.user": "<oracle-user>",
  "database.password": "<oracle-password>",
  "database.dbname": "<service-name-or-sid>",
  "database.service.name": "<service-name-or-sid>",
  "database.server.name": "<logical-server-name>",
  "topic.prefix": "<topic-prefix>",

  "database.connection.adapter": "xstream",
  "database.out.server.name": "dbz_outbound",
  "database.processor.licenses": "1",

  "table.include.list": "<schema>.<table1>,<schema>.<table2>",

  "output.data.format": "JSON_SR",
  "output.key.format": "JSON_SR",

  "snapshot.mode": "initial",

  "tombstones.on.delete": "true",

  "decimal.handling.mode": "string",
  "binary.handling.mode": "base64",
  "interval.handling.mode": "string",

  "tasks.max": "1"
}
```

**Oracle-specific parameters:**
- `database.dbname`: Oracle service name or SID
- `database.service.name`: **Required.** Oracle service name (often same as `database.dbname`)
- `database.processor.licenses`: **Required.** Number of Oracle processor licenses (use `"1"` for testing)
- `database.connection.adapter`: Use `xstream` for XStream
- `database.out.server.name`: XStream outbound server name (created in Oracle)
- `table.include.list`: Format: `SCHEMA.TABLE` (uppercase)
- `interval.handling.mode`: Set to `string` for INTERVAL columns (same as PostgreSQL)
- Oracle `NUMBER`/`FLOAT` without precision produces `VariableScaleDecimal` struct — `decimal.handling.mode: string` fixes this
- **`after.state.only` is NOT supported** by OracleXStreamSource — do not use this option

**Topic Naming:**
Topic pattern: `<topic.prefix>.<schema>.<table>`

Example: `oracle-cdc.HR.EMPLOYEES`

**Prerequisites:** Oracle XStream requires significant database-side setup before the connector can be deployed — GoldenGate replication must be enabled, ARCHIVELOG mode configured, supplemental logging set up, an XStream admin user created, and an XStream outbound server provisioned. See `references/database-prerequisites.md` "Oracle XStream CDC Prerequisites" for the complete step-by-step guide.

**Prerequisites validation:** https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-cdc-source/prereqs-validation.html

**Documentation:**
https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-cdc-source/cc-oracle-xstream-cdc-source.html

---

## DynamoDB CDC Source

**Connector Class:** `DynamoDbCdcSource`

```json
{
  "connector.class": "DynamoDbCdcSource",
  "name": "cdc-pipeline-skill-dynamodb-connector",

  "kafka.api.key": "${file:/secrets.properties:KAFKA_API_KEY}",
  "kafka.api.secret": "${file:/secrets.properties:KAFKA_API_SECRET}",

  "aws.access.key.id": "<aws-access-key>",
  "aws.secret.access.key": "<aws-secret-key>",

  "dynamodb.service.endpoint": "https://dynamodb.<aws-region>.amazonaws.com",
  "dynamodb.table.discovery.mode": "INCLUDELIST",
  "dynamodb.table.sync.mode": "SNAPSHOT_CDC",
  "dynamodb.table.includelist": "<table1>,<table2>",

  "output.data.format": "JSON_SR",

  "dynamodb.cdc.max.poll.records": "5000",
  "dynamodb.snapshot.max.poll.records": "1000",

  "tasks.max": "1"
}
```

**Key Parameters:**
- `aws.access.key.id` / `aws.secret.access.key`: IAM credentials with DynamoDB Streams **and** checkpointing table write permissions (see `references/database-prerequisites.md` for the complete IAM policy)
- `dynamodb.service.endpoint`: **Required.** Full DynamoDB service URL including region (e.g., `https://dynamodb.us-east-2.amazonaws.com`)
- `dynamodb.table.discovery.mode`: **Required.** Set to `INCLUDELIST` for explicit table names, or `TAG` for tag-based auto-discovery
- `dynamodb.table.sync.mode`: Snapshot behavior — `SNAPSHOT_CDC` (default, snapshot then stream), `SNAPSHOT` (one-time scan only), or `CDC` (streams only, no initial snapshot)
- `dynamodb.table.includelist`: **Required when discovery mode is INCLUDELIST.** Comma-separated list of DynamoDB table names (note: this is `dynamodb.table.includelist`, NOT `table.include.list` like SQL connectors)
- `dynamodb.cdc.max.poll.records`: Max records per DynamoDB Streams GetRecords call (each call also limited to 1 MB)
- `dynamodb.snapshot.max.poll.records`: Max records per DynamoDB Scan call during snapshot (each call also limited to 1 MB)

**tasks.max for DynamoDB:** A single DynamoDB table can only be processed by one task. `tasks.max` should equal the number of tables in `dynamodb.table.includelist`. Configuring more tasks than tables results in idle tasks. For multi-table setups, increase `tasks.max` to match — especially during snapshot to avoid the 24-hour DynamoDB Streams expiry window.

**Critical IAM note:** The CDC phase requires write permissions to create and manage a KCL-style checkpointing table in DynamoDB. Without `CreateTable`, `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem` permissions, the snapshot will succeed but CDC streaming will silently fail (no real-time changes captured). This is the most common cause of DynamoDB CDC appearing "stuck" after snapshot. See `references/database-prerequisites.md` for the complete IAM policy and `references/troubleshooting.md` "DynamoDB CDC: Snapshot Works but CDC Streaming Silently Fails" for diagnosis steps.

**Topic Naming:**
Topic pattern: `<kafka.topic>` (configurable, unlike other connectors)

Example: `cdc-pipeline-skill-dynamodb-users`

**Documentation:**
https://docs.confluent.io/cloud/current/connectors/cc-amazon-dynamodb-source.html

---

## Configuration Best Practices

### Data Type Serialization

Debezium's default serialization modes can produce raw bytes, lossy approximations, or unexpected types that break downstream consumers (Flink, ksqlDB, etc.). Always include these settings for maximum interoperability:

**All connectors:**
```json
{
  "decimal.handling.mode": "string",
  "binary.handling.mode": "base64"
}
```

| Setting | Default | Problem | Recommended | Affected Types |
|---|---|---|---|---|
| `decimal.handling.mode` | `precise` | DECIMAL/NUMERIC serialized as raw bytes | `string` | PG: DECIMAL, NUMERIC, MONEY. MySQL: DECIMAL, NUMERIC. SQL Server: DECIMAL, NUMERIC, MONEY, SMALLMONEY. Oracle: NUMBER(p,s), NUMBER, FLOAT |
| `binary.handling.mode` | `bytes` | Binary data breaks JSON serialization | `base64` | PG: BYTEA. MySQL: BINARY, VARBINARY, BLOB. SQL Server: BINARY, VARBINARY, IMAGE. Oracle: RAW, BLOB |
| `interval.handling.mode` | `numeric` | Intervals approximated as microseconds (lossy) | `string` | PG: INTERVAL. Oracle: INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND |
| `hstore.handling.mode` | `json` | HSTORE as JSON string, not native MAP | `map` | PG: HSTORE |

**Database-specific quirks:** MySQL `TINYINT(1)` → INT16 not BOOLEAN; MySQL `BIGINT UNSIGNED` overflows at 2^63; SQL Server `DATETIMEOFFSET` → STRING; Oracle `NUMBER` (no precision) → `VariableScaleDecimal` struct (fixed by `decimal.handling.mode = string`).

### Secrets Management

**Option 1: Use Confluent Secrets (Recommended)**
```json
{
  "database.password": "${file:/path/to/secrets.properties:DB_PASSWORD}",
  "kafka.api.key": "${file:/path/to/secrets.properties:KAFKA_API_KEY}"
}
```

**Option 2: Environment Variables**
```json
{
  "database.password": "${env:DB_PASSWORD}"
}
```

### Snapshot Modes

- `initial`: Snapshot existing data on first run, then stream changes (recommended for new connectors)
- `never`: Skip snapshot, only capture new changes (use when you don't need historical data)
- `schema_only`: Snapshot the schema but not the data, then stream changes (use for initial pipeline validation on large tables)
- `always`: Always snapshot on restart (use for testing, not production)

**Large tables (100M+ rows):** Use `schema_only` first to validate the pipeline end-to-end, then recreate with `initial`. The initial snapshot can lock the source DB, burst-produce to Kafka, and trigger SR rate limits — you don't want to wait hours to discover a config error.

### Other Settings (Already in Templates)

The connector templates above include correct defaults for these — only adjust if needed:
- **`tombstones.on.delete: true`** — Produces null-value record on delete; important for log compaction and downstream consumers
- **`include.schema.changes: false`** — Suppresses DDL noise in the pipeline
- **`heartbeat.interval.ms: 30000`** — Detects stalled connectors (PostgreSQL template only)
- **`tasks.max: 1`** — Ensures message ordering; only increase for high-throughput with careful testing

### Handling Topics Without Schema Registry

If a topic was produced with plain `JSON` (no SR serializer), no schema is registered in Schema Registry. This affects Flink auto-discovery and Tableflow, but there are several ways to handle it.

**Option 1: Register a JSON schema manually in Schema Registry (recommended)**

Register a JSON schema for the topic's subject in Schema Registry. Once registered, Flink will infer the schema and read the topic as if it were serialized with SR serializers — no re-ingestion needed.

For JSON topics, the schema can be **partial** — you don't have to define every field, just the ones you need. Flink will map the defined fields and ignore the rest.

Register via CLI:
```bash
confluent schema-registry schema create --subject "<topic-name>-value" --schema schema.json --type JSON --environment <env-id>
```

Or use the Confluent Cloud UI: Schema Registry → Subjects → Create Subject → paste the JSON Schema.

**Option 2: Infer a schema from existing messages**

Confluent Cloud can [infer a schema from messages](https://docs.confluent.io/cloud/current/sr/schemas-manage.html#infer-a-schema-from-messages) already on the topic. This auto-generates a schema from sample payloads and registers it in SR. After inference, Flink auto-discovers the topic normally.

**Option 3: Use Flink's raw BYTES inference and parse with JSON functions**

For topics with no schema at all, Flink infers a raw table:
```sql
-- Flink auto-infers schemaless topics as raw bytes
CREATE TABLE inferred_raw (`key` BYTES, `val` BYTES);

-- Switch to STRING to work with JSON payloads
ALTER TABLE inferred_raw MODIFY (`key` STRING, `val` STRING);

-- Then use JSON functions to extract fields
INSERT INTO target_table
SELECT
  JSON_VALUE(val, '$.id' RETURNING INT) AS id,
  JSON_VALUE(val, '$.name') AS name,
  JSON_VALUE(val, '$.email') AS email
FROM inferred_raw;
```

This is useful when you can't register a schema or need ad-hoc exploration, but it's more fragile than schema-based approaches.

**Option 4: Re-create the connector with an SR-backed format**

If you control the producer, change `output.data.format` from `JSON` to `JSON_SR`, `AVRO`, or `PROTOBUF`. Delete the old connector and topics, then recreate. This is the cleanest path for new CDC pipelines.

**Important format note for schema registration:**
- **JSON**: Partial schemas are fine — define only the fields you need
- **Avro**: Must define the complete schema matching the payload structure (binary format, partial schemas won't decode correctly)
- **Protobuf**: Must define the complete schema matching the payload structure (binary format, same as Avro)

**For new CDC pipelines**, always use `JSON_SR`, `AVRO`, or `PROTOBUF` — never plain `JSON`.

### Subject Name Strategies

The **subject name strategy** controls how Schema Registry subjects are named when schemas are registered. This directly affects whether Flink can auto-discover topics and whether Tableflow can work with them. There are three strategies:

#### TopicNameStrategy (Default)

**Subject pattern:** `<topic-name>-key` / `<topic-name>-value`

```
Topic: postgres-cdc.public.customers
  → Key subject:   postgres-cdc.public.customers-key
  → Value subject: postgres-cdc.public.customers-value

Topic: postgres-cdc.public.orders
  → Key subject:   postgres-cdc.public.orders-key
  → Value subject: postgres-cdc.public.orders-value
```

**Behavior:** One schema per topic. Every record on a given topic must conform to the same schema (with compatible evolution). This is the Debezium default — each captured table gets its own topic and its own schema subject.

| Aspect | Status |
|---|---|
| Flink auto-discovery | **Works** — one schema per topic, Flink maps it directly to a table |
| Tableflow | **Works** — single schema per topic maps cleanly to Iceberg/Delta columns |
| Schema evolution | Governed by subject-level compatibility (e.g., `BACKWARD`, `FULL_TRANSITIVE`) |
| Multi-table CDC | Each table on its own topic — no conflicts |

**This is the recommended strategy for CDC-to-Tableflow pipelines.**

#### RecordNameStrategy

**Subject pattern:** `<fully-qualified-record-name>-key` / `<fully-qualified-record-name>-value`

```
Topic: all-cdc-events  (multiple tables routed here)
  → Record "com.example.Customer" → Value subject: com.example.Customer-value
  → Record "com.example.Order"    → Value subject: com.example.Order-value
```

**Behavior:** The subject is derived from the record's fully qualified name (namespace + name), not the topic. Multiple different schemas can coexist on the same topic because each record type gets its own subject.

| Aspect | Status |
|---|---|
| Flink auto-discovery | **Does NOT work** — Flink expects one schema per topic; it cannot resolve multiple SR subjects for the same topic |
| Tableflow | **Does NOT work reliably** — mixed schemas on one topic produce inconsistent Iceberg/Delta files |
| Schema evolution | Each record type evolves independently (different subjects) |
| Use case | Shared topics where multiple event types are intentionally co-located |

**Configuration (self-managed connectors):**
```json
{
  "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.RecordNameStrategy"
}
```

**Note:** For Confluent Cloud fully-managed connectors, the subject name strategy may not be directly configurable. Check the specific connector documentation.

#### TopicRecordNameStrategy

**Subject pattern:** `<topic-name>-<fully-qualified-record-name>-key` / `<topic-name>-<fully-qualified-record-name>-value`

```
Topic: all-cdc-events
  → Record "com.example.Customer" → Value subject: all-cdc-events-com.example.Customer-value
  → Record "com.example.Order"    → Value subject: all-cdc-events-com.example.Order-value
```

**Behavior:** Combines the topic name and record name. Like `RecordNameStrategy`, it allows multiple schemas per topic, but subjects are scoped to both topic and record type. This means the same record type on different topics gets separate subjects (unlike `RecordNameStrategy` where they'd share one).

| Aspect | Status |
|---|---|
| Flink auto-discovery | **Does NOT work** — same limitation as `RecordNameStrategy`; Flink can't resolve compound subjects |
| Tableflow | **Does NOT work reliably** — same issue as `RecordNameStrategy` |
| Schema evolution | Each topic+record combination evolves independently |
| Use case | Multiple topics with overlapping record types that need independent evolution |

**Configuration (self-managed connectors):**
```json
{
  "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"
}
```

#### Strategy Comparison Summary

| | TopicNameStrategy | RecordNameStrategy | TopicRecordNameStrategy |
|---|---|---|---|
| Subject derived from | Topic name | Record name | Topic + Record name |
| Schemas per topic | One | Many | Many |
| Flink auto-discovery | Yes | No | No |
| Tableflow | Yes | No | No |
| Debezium default | Yes | No | No |
| CDC-to-Tableflow ready | **Yes** | Requires splitting | Requires splitting |

### Multi-Event Topics

A **multi-event topic** contains records with different schemas on the same Kafka topic. By default, Debezium creates one topic per captured table (`{topic.prefix}.{schema}.{table}`) using `TopicNameStrategy`, so each topic has a single event type. Multi-event topics occur when:

- Topic routing is used (e.g., Debezium's `transforms.route` SMT to merge multiple tables into one topic)
- An upstream producer writes different event types to the same topic
- `RecordNameStrategy` or `TopicRecordNameStrategy` is configured (see above)

**Handling multi-event topics for Tableflow:**

1. **Best approach — avoid them for CDC:** Use Debezium's default `TopicNameStrategy` with one-topic-per-table. This is the standard and recommended pattern for CDC-to-Tableflow.

2. **If multi-event topics already exist**, split them before the Flink/Tableflow stage:
   - Create a Flink SQL job per event type that filters and routes to separate target topics
   - Each target topic gets its own schema and can be used with Tableflow normally
   - For schemaless multi-event topics, use the raw BYTES approach (Option 3 above) with JSON functions to filter by event type:
     ```sql
     -- Route events by type to separate typed target tables
     INSERT INTO target_orders
     SELECT JSON_VALUE(val, '$.order_id' RETURNING INT), ...
     FROM inferred_raw
     WHERE JSON_VALUE(val, '$.event_type') = 'order';
     ```

3. **If migrating from `RecordNameStrategy` / `TopicRecordNameStrategy`**, the long-term fix is to re-route events to separate topics with `TopicNameStrategy`. Short-term, use Flink to split and re-serialize as described above.

**Recommendation:** Keep `TopicNameStrategy` (default) and one-topic-per-table for CDC pipelines targeting Tableflow.

---

## Validation Checklist

Before deploying a connector configuration:

- [ ] Database prerequisites met (WAL, binlog, CDC enabled, etc.)
- [ ] Database user has correct permissions
- [ ] Network connectivity verified
- [ ] Schema Registry endpoint is correct
- [ ] API keys are valid and have correct permissions
- [ ] Table names are correctly formatted (schema.table, case-sensitive for some DBs)
- [ ] Snapshot mode is appropriate for use case
- [ ] Tombstones enabled for delete handling
- [ ] Secrets are properly externalized (not hardcoded)

---

## Testing Connector Configuration

After creating a connector, verify it's working:

```bash
# Check connector status
confluent connect cluster describe <connector-id>

# Should show: "status": "RUNNING"

# View topics created
confluent kafka topic list | grep <connector-name>

# Consume sample messages
confluent kafka topic consume <topic-name> --from-beginning --max-messages 5

# Check connector tasks
confluent connect cluster describe <connector-id> --show-tasks
```

If connector fails, check logs:
```bash
confluent connect cluster describe <connector-id> --show-logs
```
