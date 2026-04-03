---
name: confluent-cloud-cdc-tableflow
description: Set up end-to-end Change Data Capture (CDC) pipelines on Confluent Cloud using Debezium source connectors, Flink for transformation, and Tableflow for data lake integration. This skill handles the complete workflow from database to Iceberg/Delta tables. Use this skill whenever users mention CDC, Debezium, Tableflow, streaming database changes, replication to data lakes, real-time data pipelines from databases, or want to stream changes from SQL Server, MySQL, PostgreSQL, Oracle, or DynamoDB to Iceberg or Delta Lake. Also trigger for phrases like "set up database replication", "stream database to data lake", "capture database changes", "sync database to Iceberg/Delta", or any mention of connecting databases to Confluent Cloud with Schema Registry.
---

# Confluent Cloud CDC to Tableflow Pipeline

Build production-ready Change Data Capture pipelines that stream database changes through Confluent Cloud to Iceberg or Delta Lake tables using Debezium, Flink, and Tableflow.

## Overview

This skill automates the setup of a complete CDC pipeline:

**Database → Debezium CDC Connector → Kafka + Schema Registry → Flink (decode & transform) → Tableflow → Iceberg/Delta Tables**

### Supported Databases (Fully-Managed Debezium Connectors Only)
- Microsoft SQL Server CDC Source V2
- MySQL CDC Source V2
- PostgreSQL CDC Source V2
- Oracle XStream CDC Source
- DynamoDB CDC Source

### Key Components
1. **Debezium CDC Source Connector**: Captures database changes as events
2. **Schema Registry**: Manages Avro/JSON/Protobuf schemas (default: JSON_SR)
3. **Confluent Cloud Flink**: Decodes Debezium envelopes and transforms data
4. **Tableflow**: Native Confluent Cloud feature that materializes Kafka topics as Iceberg or Delta tables

### Important Clarifications
- **Tableflow is NOT a connector.** It is a native topic-level feature enabled via the Tableflow API or Confluent Cloud UI.
- **Confluent Cloud Flink auto-discovers CDC tables.** You do NOT need to manually create source tables — topics with Schema Registry schemas are automatically available as Flink tables.
- **Managed connectors use `output.data.format`**, not `key.converter`/`value.converter` classes.

## Workflow Phases

### Phase 0: Tool Selection & MCP Server Validation (CRITICAL)

**Default: Use Confluent MCP Server.** The MCP server is the preferred method for all Confluent Cloud operations. Only fall back to the Confluent CLI (`confluent` command) and REST APIs if the MCP server is not installed or unavailable.

#### 0.1 Verify MCP Server Availability

Check if the Confluent MCP tools are available. Look for tools prefixed with `mcp__confluent__`. Key tools needed:
- `mcp__confluent__list-environments`
- `mcp__confluent__list-clusters`
- `mcp__confluent__create-connector` / `mcp__confluent__read-connector` / `mcp__confluent__delete-connector`
- `mcp__confluent__create-flink-statement` / `mcp__confluent__read-flink-statement` / `mcp__confluent__list-flink-statements`
- `mcp__confluent__create-tableflow-topic` / `mcp__confluent__list-tableflow-topics`
- `mcp__confluent__list-schemas` / `mcp__confluent__list-topics` / `mcp__confluent__consume-messages`
- `mcp__confluent__search-topics-by-name`

**If MCP is not available**, fall back to the Confluent CLI (`confluent` command) and REST APIs for all operations. The CLI fallback should mirror the same workflow phases but use CLI commands instead of MCP tool calls.

**CLI Fallback Examples:**
```bash
# Environment & cluster discovery
confluent environment list
confluent kafka cluster list --environment <env-id>

# Connector operations
confluent connect cluster create --config-file connector-config.json --cluster <cluster-id> --environment <env-id>
confluent connect cluster describe <connector-id>
confluent connect cluster list --cluster <cluster-id> --environment <env-id>

# Flink operations
confluent flink statement create <statement-name> --sql "<SQL>" --compute-pool <pool-id> --environment <env-id>
confluent flink statement describe <statement-name> --environment <env-id>

# Topic & schema operations
confluent kafka topic list --cluster <cluster-id> --environment <env-id>
confluent schema-registry subject list --environment <env-id>
```

#### 0.2 Read MCP Environment File for Defaults (CRITICAL)

**Before asking the user for environment/cluster details, read the MCP server's `.env` file to extract defaults.** The `.env` file path is specified in the project's `.mcp.json` (look for the `-e` flag in the `args` array). If not found there, check `~/.config/claude/mcp.json` for an `envFile` field.

Read the `.env` file and extract these defaults:

| .env Variable | Purpose | Use As Default For |
|---|---|---|
| `KAFKA_ENV_ID` | Environment ID | `environmentId` in all MCP calls |
| `KAFKA_CLUSTER_ID` | Kafka cluster ID | `clusterId` in all MCP calls |
| `BOOTSTRAP_SERVERS` | Kafka bootstrap endpoint | MCP cluster targeting |
| `KAFKA_API_KEY` / `KAFKA_API_SECRET` | Cluster-scoped Kafka API keys | Connector `kafka.api.key` / `kafka.api.secret` |
| `FLINK_COMPUTE_POOL_ID` | Flink compute pool | `computePoolId` in Flink statements |
| `FLINK_ENV_NAME` | Flink catalog name | `catalogName` in Flink statements |
| `FLINK_DATABASE_NAME` | Flink database name | `databaseName` in Flink statements |
| `FLINK_REST_ENDPOINT` | Flink API base URL | `baseUrl` for Flink MCP calls |
| `FLINK_ORG_ID` | Organization ID | `organizationId` in Flink MCP calls |
| `SCHEMA_REGISTRY_ENDPOINT` | Schema Registry URL | Verification only |

**If the `.env` file contains these values**, present them to the user and ask for confirmation before proceeding. For example:

> "I found the following targets in your MCP `.env` file:
> - **Environment:** `env-0ypxv6`
> - **Cluster:** `lkc-qo5k36` (bootstrap: `pkc-921jm.us-east-2.aws.confluent.cloud:9092`)
> - **Flink Compute Pool:** `lfcp-3v39xw` (catalog: `erick_cdc_tableflow_test`, database: `cluster_0`)
> - **Schema Registry:** `psrc-19262jq.us-east-2.aws.confluent.cloud`
>
> Should I use these for the pipeline, or would you like to target a different environment/cluster?"

If the user confirms, use these values throughout the pipeline. If they want different targets, ask them to specify.

**If the `.env` file is not found or missing key variables**, ask the user which environment and cluster to use, then fall back to discovering via `mcp__confluent__list-environments` and `mcp__confluent__list-clusters`.

#### 0.3 Verify MCP Cluster Targeting

The MCP server's `BOOTSTRAP_SERVERS` and `KAFKA_API_KEY` from the `.env` file determine which cluster it targets for Kafka operations (consume, list-topics, create-tableflow-topic).

**Quick verification:**
1. Run `mcp__confluent__list-topics` to confirm the MCP server is connected to the expected cluster
2. Run `mcp__confluent__list-schemas` to verify Schema Registry is accessible

Schema Registry is shared at the environment level across all clusters.

### Phase 1: Discovery & Validation

#### 1.1 Check Existing Setup

Use MCP to check what already exists:

```
mcp__confluent__list-connectors(environmentId, clusterId)  →  Existing CDC connectors
mcp__confluent__list-flink-statements(environmentId, computePoolId)  →  Existing Flink jobs
mcp__confluent__list-tableflow-topics(environmentId, clusterId)  →  Existing Tableflow topics
```

Ask the user:
- "Do you have any CDC connectors already running?"
- "Do you have a Flink compute pool you'd like to use, or should I create one?"
- "Is your database already configured for CDC?"

#### 1.2 Gather Required Information

**Database Configuration:**
- Database type (SQL Server, MySQL, PostgreSQL, Oracle, or DynamoDB)
- Connection details (hostname, port, database name)
- Credentials (username, password)
- Specific tables to capture (format: `schema.table`)

**Kafka API Keys:**
- The CDC connector needs Kafka API keys scoped to the target cluster
- If the MCP `.env` file contains `KAFKA_API_KEY` and `KAFKA_API_SECRET`, use those as defaults
- Only ask the user for Kafka API keys if they are not present in the `.env` file

**Tableflow Destination:**
- Target format: Iceberg or Delta Lake
- Storage: Managed (recommended, Confluent manages S3) or BYOB (user's own S3 bucket, requires Provider Integration ID)

**Naming Convention:**
- Default: `cdc-pipeline-skill-{database-type}-{YYYYMMDD}`
- Example: `cdc-pipeline-skill-postgres-20260323`

#### 1.3 Validate Database Prerequisites

Each database requires specific CDC setup. Read `references/database-prerequisites.md` for details:
- PostgreSQL: WAL level = logical, replication slots, publication
- MySQL: binlog format = ROW, GTID mode
- SQL Server: CDC enabled on database and tables, SQL Server Agent running
- Oracle: Archive log mode, supplemental logging, XStream
- DynamoDB: Streams enabled with NEW_AND_OLD_IMAGES

If the database isn't properly configured, guide the user through setup before proceeding.

### Phase 2: Planning

Generate the complete configuration plan and present it to the user for approval.

#### 2.1 Connector Configuration

Based on the database type, generate the connector configuration. See `references/connector-configs.md` for templates.

**Required fields for ALL CDC V2 connectors:**
- `connector.class`: The connector class (e.g., `PostgresCdcSourceV2`)
- `topic.prefix`: **REQUIRED** — Controls topic naming. Topics will be named `{topic.prefix}.{schema}.{table}`
- `kafka.api.key` / `kafka.api.secret`: Kafka API keys for the target cluster
- `output.data.format`: `JSON_SR` (default, recommended — uses JSON serializer with schema ID in header, safer for downstream consumers)
- `output.key.format`: `JSON_SR`
- `snapshot.mode`: `initial` (snapshot + stream), `never` (stream only)
- `tombstones.on.delete`: `true`
- `decimal.handling.mode`: `string` — **Always include for all connectors.** Without it, Debezium serializes DECIMAL/NUMERIC columns as raw bytes, producing garbled data in Flink and other downstream consumers. Affects PostgreSQL (DECIMAL, NUMERIC, MONEY), MySQL (DECIMAL, NUMERIC), SQL Server (DECIMAL, NUMERIC, MONEY, SMALLMONEY), and Oracle (NUMBER, FLOAT).
- `binary.handling.mode`: `base64` — Include if tables have binary columns (BYTEA, VARBINARY, BLOB, RAW). Default `bytes` breaks JSON serialization.
- `tasks.max`: `1` (recommended for CDC)

**Topic Naming Pattern:**
`{topic.prefix}.{schema}.{table}`
Example with `topic.prefix = "postgres-cdc"`: `postgres-cdc.public.customers`

#### 2.2 Flink SQL Statements

In Confluent Cloud Flink, the CDC source table is **auto-discovered** from the Kafka topic. You only need to:

1. **Create a target table** (for plain JSON_SR output to Tableflow):
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

2. **Create an INSERT statement** (continuous job to decode and transform):
```sql
INSERT INTO `target_customers`
SELECT
  `id`,
  `name`,
  `email`,
  TO_TIMESTAMP_LTZ(`created_at` / 1000, 3)
FROM `postgres-cdc.public.customers`;
```

**IMPORTANT Cloud Flink differences:**
- Do NOT specify `'connector'`, `'value.format'`, `'properties.bootstrap.servers'`, or Schema Registry URLs in CREATE TABLE — Cloud Flink handles all of this automatically
- Do NOT create source tables for CDC topics — they are auto-discovered
- Do NOT reference `after.*` fields or filter by `op` — Flink interprets CDC changelog semantics natively
- Use `TIMESTAMP_LTZ(3)` for Debezium timestamps, not `TIMESTAMP(3)`

**Debezium Type Conversions:**
| Debezium Type | Flink Source Type | Target Type | Conversion |
|---|---|---|---|
| `MicroTimestamp` | BIGINT (microseconds) | TIMESTAMP_LTZ(3) | `TO_TIMESTAMP_LTZ(col / 1000, 3)` |
| `Timestamp` | BIGINT (milliseconds) | TIMESTAMP_LTZ(3) | `TO_TIMESTAMP_LTZ(col, 3)` |
| `Date` | INT (days since epoch) | DATE | Direct or cast |
| `DECIMAL`/`NUMERIC` | STRING (with `decimal.handling.mode=string`) | DECIMAL or STRING | Direct; without `string` mode, arrives as BYTES which Flink cannot cast |
| `INTERVAL` (PG/Oracle) | STRING (with `interval.handling.mode=string`) | STRING | Direct; default `numeric` mode is lossy |
| Binary (BYTEA, BLOB, etc.) | STRING (with `binary.handling.mode=base64`) | STRING | Base64-encoded; default `bytes` breaks JSON |
| Numeric/String | Direct mapping | Same | No conversion needed |

For detailed patterns, see `references/flink-sql-patterns.md`.

#### 2.3 Tableflow Configuration

Tableflow is a **native topic-level feature**, not a connector. It is enabled per-topic.

**Storage Options:**
- **Managed** (recommended): Confluent manages the S3 storage. No credentials needed.
- **BYOB (Bring Your Own Bucket)**: User provides their S3 bucket. Requires a Provider Integration ID set up in Confluent Cloud (Settings → Provider Integrations).

**Table Formats:** Iceberg (recommended) or Delta Lake

#### 2.4 Present the Plan

Show the user:
1. Connector configuration (with sensitive fields masked)
2. Flink compute pool to use
3. Flink SQL statements (target table + INSERT)
4. Tableflow config (storage type, format)
5. Expected topic names

Wait for explicit confirmation before proceeding.

### Phase 3: Execution

Execute step-by-step using MCP tools, checking status after each component.

#### 3.1 Create CDC Source Connector

**Using MCP:**
```
mcp__confluent__create-connector(
  connectorName: "cdc-pipeline-skill-postgres-20260323-connector",
  environmentId: "<env-id>",
  clusterId: "<cluster-id>",
  connectorConfig: {
    "connector.class": "PostgresCdcSourceV2",
    "topic.prefix": "postgres-cdc",
    "database.hostname": "<host>",
    "database.port": "5432",
    "database.user": "<user>",
    "database.password": "<password>",
    "database.dbname": "<dbname>",
    "table.include.list": "public.customers",
    "kafka.api.key": "<KAFKA_API_KEY>",
    "kafka.api.secret": "<KAFKA_API_SECRET>",
    "output.data.format": "JSON_SR",
    "output.key.format": "JSON_SR",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_slot",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "decimal.handling.mode": "string",
    "heartbeat.interval.ms": "30000",
    "tasks.max": "1"
  }
)
```

**Verify connector provisioning:**
Managed connectors take **2-5 minutes** to provision. Poll status:
```
mcp__confluent__read-connector(connectorName, environmentId, clusterId)
```
- `tasks: []` → Still provisioning, wait and retry
- `tasks: [{...}]` → Provisioned, tasks assigned

**Verify data is flowing:**
```
mcp__confluent__list-schemas(subjectPrefix: "postgres-cdc")
```
When schemas appear (e.g., `postgres-cdc.public.customers-key`, `postgres-cdc.public.customers-value`), the connector is producing data.

If no schemas appear after 5 minutes with tasks assigned, check database connectivity and credentials. The MCP `read-connector` tool does NOT show error logs — use the Confluent Cloud UI for connector error details.

#### 3.2 Execute Flink SQL

**Step 1: Verify CDC table is auto-discovered:**
```
mcp__confluent__create-flink-statement(
  statementName: "show-tables-check",
  statement: "SHOW TABLES;",
  environmentId: "<env-id>",
  computePoolId: "<pool-id>",
  catalogName: "<environment-display-name>",
  databaseName: "<cluster-display-name>"
)
```
Then read results:
```
mcp__confluent__read-flink-statement(statementName: "show-tables-check", environmentId: "<env-id>")
```
Look for the CDC topic table (e.g., `postgres-cdc.public.customers`). If not present, the connector hasn't produced data yet — wait and retry.

**Step 2: Create target table:**
```
mcp__confluent__create-flink-statement(
  statementName: "cdc-create-target-customers",
  statement: "CREATE TABLE `target_customers` (...) WITH ('changelog.mode' = 'upsert');",
  environmentId, computePoolId, catalogName, databaseName
)
```

**Step 3: Create INSERT job:**
```
mcp__confluent__create-flink-statement(
  statementName: "cdc-decode-customers",
  statement: "INSERT INTO `target_customers` SELECT ... FROM `postgres-cdc.public.customers`;",
  environmentId, computePoolId, catalogName, databaseName
)
```

The INSERT creates a continuous Flink job. Verify it transitions to RUNNING (not FAILED):
```
mcp__confluent__read-flink-statement(statementName: "cdc-decode-customers", environmentId)
```

**Common INSERT failures:**
- "Table does not exist" → CDC source table not yet auto-discovered; wait for connector
- "Incompatible types for sink column" → Type mismatch; check Debezium type mappings above
- "Unsupported format" → Remove any explicit format properties from CREATE TABLE

**Advisory warnings (can be ignored):**
- "Primary key does not match upsert key" — Expected for CDC decode patterns
- "Highly state-intensive operators without TTL" — Advisory; set TTL if needed for production

#### 3.3 Enable Tableflow

**Using MCP:**
```
mcp__confluent__create-tableflow-topic(
  tableflowTopicConfig: {
    "display_name": "target_customers",
    "storage": { "kind": "Managed", "bucket_name": "managed", "provider_integration_id": "managed" },
    "table_formats": ["ICEBERG"],
    "config": { "record_failure_strategy": "SUSPEND", "retention_ms": "6048000000" }
  }
)
```

**KNOWN LIMITATION:** The MCP `create-tableflow-topic` tool does NOT accept `environmentId` or `clusterId` parameters. It defaults to the cluster configured in the MCP server's `BOOTSTRAP_SERVERS` (from the `.env` file). If the MCP server points to a different cluster than where the target topic exists, this will fail with "topic not found".

**Workarounds if MCP Tableflow creation fails:**
1. **Update the MCP `.env` file** to point to the correct cluster, reconnect with `/mcp`, then retry
2. **Use the Confluent Cloud UI**: Environment → Cluster → Topics → `target_customers` → Tableflow tab → Enable

**Verify Tableflow is enabled:**
```
mcp__confluent__list-tableflow-topics(environmentId, clusterId)
```
Status will transition from `PENDING` → `ACTIVE`.

### Phase 4: Verification & Troubleshooting

#### 4.1 Verify End-to-End Pipeline

**Check each component:**

| Check | MCP Tool | What to Look For |
|-------|----------|-----------------|
| Connector running | `read-connector` | `tasks` array is non-empty |
| Schemas registered | `list-schemas(subjectPrefix)` | Key and value schemas for CDC topic |
| CDC table in Flink | `create-flink-statement("SHOW TABLES")` | CDC topic appears as table |
| Flink job running | `read-flink-statement` | No error in response |
| Target topic has data | `consume-messages(topicNames)` | Messages appear (note: consumer starts at latest offset) |
| Tableflow enabled | `list-tableflow-topics` | Status is PENDING or ACTIVE |

**Consume from target topic to verify decoded data:**
```
mcp__confluent__consume-messages(
  topicNames: ["target_customers"],
  value: { "useSchemaRegistry": true },
  key: { "useSchemaRegistry": true },
  maxMessages: 5,
  timeoutMs: 15000
)
```

Note: The consumer starts at the latest offset. If the initial snapshot already completed, you may see 0 messages until a new database change occurs.

**Test real-time CDC by inserting a row in the source database:**
```sql
INSERT INTO public.customers (name, email, created_at)
VALUES ('Test User', 'test@example.com', NOW());
```

#### 4.2 Troubleshooting

For detailed troubleshooting, see `references/troubleshooting.md`.

**Quick reference for common issues:**

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Connector creation fails: "topic.prefix is required" | Missing required field | Add `"topic.prefix"` to connector config |
| Connector tasks stay empty | Still provisioning | Wait 2-5 minutes, retry |
| No schemas after 5 min | DB connectivity or credentials | Check host, port, user, password; verify DB CDC config |
| MySQL: "LOCK TABLES privilege required" | Managed MySQL lacks SUPER privilege | `GRANT LOCK TABLES ON db.* TO 'user'@'%';` — required for RDS, Aurora, Cloud SQL, Azure MySQL |
| SHOW TABLES missing CDC table | Connector not producing yet | Verify schemas exist first, then wait |
| CREATE TABLE: "Unsupported format" | Explicit format specs | Remove all `'value.format'`, `'connector'` properties |
| INSERT: "Incompatible types" | Debezium type mismatch | Use TIMESTAMP_LTZ(3) + TO_TIMESTAMP_LTZ conversion |
| Tableflow: "topic not found" | MCP cluster mismatch | Update MCP `.env` file or use Cloud UI |
| DECIMAL columns show garbled bytes | Missing `decimal.handling.mode` | Add `"decimal.handling.mode": "string"` to connector config; must fix at connector level, not Flink |
| INTERVAL columns show large integers | Missing `interval.handling.mode` | Add `"interval.handling.mode": "string"` for PG/Oracle; default is lossy microseconds |
| Binary columns break JSON | Missing `binary.handling.mode` | Add `"binary.handling.mode": "base64"` for BYTEA/VARBINARY/BLOB/RAW columns |
| Oracle NUMBER produces struct not scalar | Oracle NUMBER without precision | Add `"decimal.handling.mode": "string"` — default produces VariableScaleDecimal struct |
| Flink DEGRADED: "Schema ID not found" | Stale schema IDs on topics after schema deletion | Delete CDC source topics + hard-delete schemas + drop replication slot, then recreate connector fresh |
| consume-messages returns 0 | Consumer at latest offset | Insert a new row in DB, or MCP targets wrong cluster |

### Phase 5: Documentation

After successful setup, provide the user with:

1. **Pipeline Summary Table**: All component names, IDs, and statuses
2. **Topic Names**: Source CDC topic and target JSON_SR topic
3. **Monitoring**: Check connector, Flink job, and Tableflow status in Confluent Cloud UI
4. **Test Command**: SQL INSERT to verify real-time CDC
5. **MCP `.env` Config**: Note which `.env` file was used and the environment/cluster defaults extracted from it

## Important Notes

- **Tableflow is NOT a connector** — It's a native topic feature enabled via API or UI
- **Cloud Flink auto-discovers CDC tables** — No manual source table creation needed
- **Managed connectors use `output.data.format`** — Not converter classes
- **`topic.prefix` is REQUIRED** — Controls topic naming for all CDC V2 connectors
- **Schema Evolution**: Database schema changes require updating Flink target table and INSERT job
- **Scaling**: Increase Flink compute pool CFU for higher throughput
- **Cost**: CDC connectors, Flink CFUs, and Tableflow all incur costs
- **Tableflow works with any cluster type** — Basic, Standard, Dedicated, and Enterprise clusters all support Tableflow
- **MCP `.env` file is the source of truth** — Read the `.env` file referenced in `.mcp.json` for default environment, cluster, Flink, and Schema Registry config
- **MCP cluster targeting**: The MCP server targets the cluster specified in the `.env` file's `BOOTSTRAP_SERVERS`

## References

- Database Prerequisites: `references/database-prerequisites.md`
- Connector Configurations: `references/connector-configs.md`
- Flink SQL Patterns: `references/flink-sql-patterns.md`
- Troubleshooting Guide: `references/troubleshooting.md`
- Confluent Cloud Flink Docs: https://docs.confluent.io/cloud/current/flink/overview.html
- Tableflow Docs: https://docs.confluent.io/cloud/current/topics/tableflow/overview.html
- Debezium CDC Docs: https://debezium.io/documentation/
- Confluent MCP Server: https://github.com/confluentinc/mcp-confluent
