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

  "tasks.max": "1"
}
```

**REQUIRED field: `topic.prefix`** — Controls topic naming. All CDC V2 connectors require this. Topics will be named `{topic.prefix}.{schema}.{table}`.

**Schema Format Options:**
- `JSON_SR` (default, recommended — JSON with Schema Registry, uses schema ID in header which is safer for downstream consumers and won't break existing consumers)
- `AVRO` (binary format, requires Avro-compatible consumers)
- `PROTOBUF`

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

  "tasks.max": "1"
}
```

**Key Parameters:**
- `topic.prefix`: **REQUIRED** — Controls topic naming (e.g., `postgres-cdc`)
- `database.server.name`: Logical name for this database server
- `table.include.list`: Comma-separated list of tables (format: `schema.table`)
- `plugin.name`: Use `pgoutput` (native PostgreSQL logical replication)
- `publication.name`: Publication created in PostgreSQL
- `slot.name`: Replication slot name (must be unique)
- `snapshot.mode`:
  - `initial`: Snapshot existing data, then stream changes
  - `never`: Only stream changes (no initial snapshot)
  - `always`: Always snapshot on startup

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

  "tasks.max": "1"
}
```

**Key Parameters:**
- `database.server.id`: Unique numeric ID for this connector (5-10 digits)
- `topic.prefix`: **REQUIRED** — Controls topic naming
- `database.server.name`: Logical name for this database server
- `database.include.list`: Comma-separated list of databases
- `table.include.list`: Comma-separated list of tables (format: `database.table`)
- `gtid.source.includes`: GTID filter (use `.*` for all)
- `snapshot.mode`: Same options as PostgreSQL

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

  "tasks.max": "1"
}
```

**Key Parameters:**
- `topic.prefix`: **REQUIRED** — Controls topic naming
- `database.names`: Comma-separated list of databases
- `database.server.name`: Logical name for this database server
- `table.include.list`: Comma-separated list of tables (format: `schema.table`, use `dbo` for default schema)
- `snapshot.mode`: Same options as PostgreSQL

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
  "database.server.name": "<logical-server-name>",
  "topic.prefix": "<topic-prefix>",

  "database.connection.adapter": "xstream",
  "database.out.server.name": "dbz_outbound",

  "table.include.list": "<schema>.<table1>,<schema>.<table2>",

  "output.data.format": "JSON_SR",
  "output.key.format": "JSON_SR",

  "snapshot.mode": "initial",

  "tombstones.on.delete": "true",

  "tasks.max": "1"
}
```

**Key Parameters:**
- `database.dbname`: Oracle service name or SID
- `database.connection.adapter`: Use `xstream` for XStream
- `database.out.server.name`: XStream outbound server name (created in Oracle)
- `topic.prefix`: **REQUIRED** — Controls topic naming
- `table.include.list`: Comma-separated list of tables (format: `SCHEMA.TABLE` - uppercase)
- `snapshot.mode`: Same options as PostgreSQL

**Topic Naming:**
Topic pattern: `<topic.prefix>.<schema>.<table>`

Example: `oracle-cdc.HR.EMPLOYEES`

**Documentation:**
https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-source/cc-oracle-xstream-source.html

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
  "aws.dynamodb.region": "<aws-region>",

  "table.include.list": "<table1>,<table2>",

  "kafka.topic": "cdc-pipeline-skill-dynamodb-<table-name>",

  "output.data.format": "JSON_SR",
  "output.key.format": "JSON_SR",

  "tasks.max": "1"
}
```

**Key Parameters:**
- `aws.access.key.id` / `aws.secret.access.key`: IAM credentials with DynamoDB Streams permissions
- `aws.dynamodb.region`: AWS region where DynamoDB table is located
- `table.include.list`: Comma-separated list of DynamoDB table names
- `kafka.topic`: Topic name pattern (use table name variable)

**Topic Naming:**
Topic pattern: `<kafka.topic>` (configurable, unlike other connectors)

Example: `cdc-pipeline-skill-dynamodb-users`

**Documentation:**
https://docs.confluent.io/cloud/current/connectors/cc-amazon-dynamodb-source.html

---

## Configuration Best Practices

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
- `always`: Always snapshot on restart (use for testing, not production)

### Tombstones

Always enable tombstones for proper delete handling:
```json
{
  "tombstones.on.delete": "true"
}
```

This produces a null-value record when a row is deleted, which is important for downstream consumers.

### Schema Changes

For production, usually disable schema change events:
```json
{
  "include.schema.changes": "false"
}
```

Schema changes (DDL) are captured separately and can create noise in the pipeline.

### Heartbeat

Enable heartbeat to detect stalled connectors:
```json
{
  "heartbeat.interval.ms": "30000",
  "heartbeat.action.query": "INSERT INTO heartbeat (ts) VALUES (NOW())"
}
```

Heartbeat produces a special event every N milliseconds to show the connector is alive.

### Task Parallelism

Most CDC connectors work best with `tasks.max = 1` because:
- Single-task ensures message ordering per table
- Database transactions are sequential
- Parallel tasks can cause out-of-order events

Only increase for very high-throughput scenarios with careful testing.

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
