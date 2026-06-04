# Credential Templates by Environment

Credentials can live in either of two formats — pick whichever fits the skill:

- **`.env`** — flat, shell-style `KEY=value` variables. Best when the skill drives the CLI or shell scripts that read environment variables.
- **`credentials.yaml`** — structured/nested YAML. Best when the skill's runtime (Python, etc.) parses a config file, or when credentials are naturally grouped (per-component, per-environment).

Create the chosen file in the workspace with only the sections relevant to the skill being created, and ship the matching template (`.env.template` or `credentials.yaml.template`).

**Security (applies to both formats):** Never read, display, or log credential file contents. For `.env`, reference variables by name only and verify with `test -n "$VAR"`, never by reading values. For `credentials.yaml`, load it only inside the consuming process (a YAML parser at runtime) and never echo parsed values. Always add the credential file to `.gitignore`.

## Option A — `.env` format

### Base Kafka + Schema Registry

```bash
# Kafka cluster
BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
CLUSTER_API_KEY=<your-api-key>
CLUSTER_API_SECRET=<your-api-secret>

# Schema Registry
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-east-2.aws.confluent.cloud
SCHEMA_REGISTRY_API_KEY=<your-sr-key>
SCHEMA_REGISTRY_API_SECRET=<your-sr-secret>
```

### Additional for Confluent Cloud

```bash
# Cloud API (for resource management)
CONFLUENT_CLOUD_API_KEY=<cloud-api-key>
CONFLUENT_CLOUD_API_SECRET=<cloud-api-secret>
```

### Additional for Flink

```bash
# Flink compute pool
FLINK_COMPUTE_POOL_ID=<pool-id>
FLINK_API_KEY=<flink-key>
FLINK_API_SECRET=<flink-secret>
```

### Additional for Tableflow

```bash
# Tableflow connection
TABLEFLOW_CONNECTION_ID=<connection-id>
TABLEFLOW_API_KEY=<tableflow-key>
TABLEFLOW_API_SECRET=<tableflow-secret>
```

### Additional for Connectors

```bash
# Connector credentials
CONNECTOR_API_KEY=<connector-key>
CONNECTOR_API_SECRET=<connector-secret>

# Source database (for CDC)
DB_HOST=<database-host>
DB_PORT=<database-port>
DB_USER=<database-user>
DB_PASSWORD=<database-password>
DB_NAME=<database-name>
```

## Option B — YAML format (`credentials.yaml`)

The same credentials as nested YAML. Include only the sections the skill needs. Keys mirror the `.env` names so guidance and scripts can map between the two formats.

```yaml
# Kafka cluster
kafka:
  bootstrap_servers: pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
  cluster_api_key: <your-api-key>
  cluster_api_secret: <your-api-secret>

# Schema Registry
schema_registry:
  url: https://psrc-xxxxx.us-east-2.aws.confluent.cloud
  api_key: <your-sr-key>
  api_secret: <your-sr-secret>

# Confluent Cloud API (for resource management) — Confluent Cloud only
confluent_cloud:
  api_key: <cloud-api-key>
  api_secret: <cloud-api-secret>

# Flink (if using Flink)
flink:
  compute_pool_id: <pool-id>
  api_key: <flink-key>
  api_secret: <flink-secret>

# Tableflow (if using Tableflow)
tableflow:
  connection_id: <connection-id>
  api_key: <tableflow-key>
  api_secret: <tableflow-secret>

# Connectors (if using Connectors)
connector:
  api_key: <connector-key>
  api_secret: <connector-secret>
  # Source database (for CDC)
  database:
    host: <database-host>
    port: <database-port>
    user: <database-user>
    password: <database-password>
    name: <database-name>
```

## After creating the template

Tell the user (substitute the format you chose):
> "I've created a `.env.template` (or `credentials.yaml.template`) file in the workspace. Please copy it to `.env` (or `credentials.yaml`) and fill in your credentials. Let me know when you're ready."

Wait for confirmation before proceeding.
