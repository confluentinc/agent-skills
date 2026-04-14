# Confluent Cloud REST API Reference

Use these endpoints when neither the MCP server nor Confluent CLI is available. All management API calls use HTTP Basic Auth with a **Cloud API Key** (created at Settings > API Keys > Add Key > Cloud resource management).

```bash
# Set once for all commands
export CC_API_KEY="<cloud-api-key>"
export CC_API_SECRET="<cloud-api-secret>"
export CC_AUTH="-u ${CC_API_KEY}:${CC_API_SECRET}"
```

---

## Base URLs

| Service | Base URL |
|---|---|
| Management API | `https://api.confluent.cloud` |
| Kafka REST | `https://pkc-xxxxx.<region>.<cloud>.confluent.cloud:443` (from cluster `spec.http_endpoint`) |
| Schema Registry | `https://psrc-xxxxx.<region>.<cloud>.confluent.cloud` (from SR cluster `spec.http_endpoint`) |
| Flink SQL | `https://flink.<region>.<cloud>.confluent.cloud` (region-specific, lowercase cloud provider) |

---

## Environment & Cluster Discovery

```bash
# List environments
curl -s ${CC_AUTH} "https://api.confluent.cloud/org/v2/environments" | jq '.data[] | {id, display_name: .display_name}'

# List clusters in environment
curl -s ${CC_AUTH} "https://api.confluent.cloud/cmk/v2/clusters?environment=<env-id>" | jq '.data[] | {id, display_name: .spec.display_name, cloud: .spec.cloud, region: .spec.region}'

# Get Schema Registry cluster
curl -s ${CC_AUTH} "https://api.confluent.cloud/srcm/v2/clusters?environment=<env-id>" | jq '.data[0] | {id, endpoint: .spec.http_endpoint}'
```

---

## Connector Operations

```bash
# List connectors
curl -s ${CC_AUTH} "https://api.confluent.cloud/connect/v1/environments/<env-id>/clusters/<cluster-id>/connectors" | jq .

# Create connector (POST with full config JSON)
curl -s -X POST ${CC_AUTH} -H "Content-Type: application/json" \
  "https://api.confluent.cloud/connect/v1/environments/<env-id>/clusters/<cluster-id>/connectors" \
  -d '{"name": "<connector-name>", "config": { <connector-config> }}'

# Check connector status
curl -s ${CC_AUTH} \
  "https://api.confluent.cloud/connect/v1/environments/<env-id>/clusters/<cluster-id>/connectors/<connector-name>/status" | jq '.connector.state, .tasks[].state'

# Delete connector
curl -s -X DELETE ${CC_AUTH} \
  "https://api.confluent.cloud/connect/v1/environments/<env-id>/clusters/<cluster-id>/connectors/<connector-name>"
```

---

## Schema Registry Operations

```bash
# List subjects
curl -s ${CC_AUTH} "<sr-endpoint>/subjects" | jq .

# Check global compatibility
curl -s ${CC_AUTH} "<sr-endpoint>/config" | jq .

# Set per-subject compatibility
curl -s -X PUT ${CC_AUTH} -H "Content-Type: application/json" \
  "<sr-endpoint>/config/<subject-name>" \
  -d '{"compatibility": "FULL_TRANSITIVE"}'

# Register a schema (e.g., JSON schema for a schemaless topic)
curl -s -X POST ${CC_AUTH} -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  "<sr-endpoint>/subjects/<topic-name>-value/versions" \
  -d '{"schemaType": "JSON", "schema": "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}"}'
```

---

## Flink Operations

Flink SQL requires an **organization ID** and **principal ID** (user or service account). Get them:

```bash
# Get your user/principal ID
curl -s ${CC_AUTH} "https://api.confluent.cloud/iam/v2/users" | jq '.data[0].id'

# Org ID is in the resource_name of any environment response
curl -s ${CC_AUTH} "https://api.confluent.cloud/org/v2/environments" | jq '.metadata'
```

```bash
export FLINK_URL="https://flink.<region>.<cloud>.confluent.cloud"
export ORG_ID="<org-id>"
export PRINCIPAL_ID="<user-id-or-sa-id>"
```

```bash
# Create Flink compute pool
curl -s -X POST ${CC_AUTH} -H "Content-Type: application/json" \
  "https://api.confluent.cloud/fcpm/v2/compute-pools" \
  -d '{"spec": {"display_name": "<pool-name>", "cloud": "<CLOUD>", "region": "<region>", "max_cfu": 10, "environment": {"id": "<env-id>"}}}'

# Submit a Flink SQL statement
curl -s -X POST ${CC_AUTH} -H "Content-Type: application/json" \
  "${FLINK_URL}/sql/v1/organizations/${ORG_ID}/environments/<env-id>/statements" \
  -d '{
    "name": "<statement-name>",
    "spec": {
      "statement": "<SQL>",
      "compute_pool": {"id": "<pool-id>"},
      "principal": {"id": "'"${PRINCIPAL_ID}"'"},
      "properties": {
        "sql.current-catalog": "<environment-display-name>",
        "sql.current-database": "<cluster-display-name>"
      }
    }
  }'

# Read statement status/results
curl -s ${CC_AUTH} \
  "${FLINK_URL}/sql/v1/organizations/${ORG_ID}/environments/<env-id>/statements/<statement-name>" | jq '.status'

# Delete a Flink statement
curl -s -X DELETE ${CC_AUTH} \
  "${FLINK_URL}/sql/v1/organizations/${ORG_ID}/environments/<env-id>/statements/<statement-name>"
```

---

## Tableflow Operations

```bash
# Enable Tableflow (POST). Storage: {"kind": "Managed"} or {"kind": "BYOB", "bucket_name": "<bucket>", "provider_integration": {"id": "<cspi-id>"}}
curl -s -X POST ${CC_AUTH} -H "Content-Type: application/json" \
  "https://api.confluent.cloud/tableflow/v1/tableflow-topics" \
  -d '{"display_name": "<topic>", "topic_name": "<topic>", "environment": {"id": "<env-id>"}, "kafka_cluster": {"id": "<cluster-id>"}, "storage": {"kind": "Managed"}, "table_formats": ["ICEBERG"], "config": {"record_failure_strategy": "SUSPEND", "retention_ms": "6048000000"}}'

# List / Describe / Disable
curl -s ${CC_AUTH} "https://api.confluent.cloud/tableflow/v1/tableflow-topics?environment=<env-id>&spec.kafka_cluster=<cluster-id>" | jq '.data[] | {name: .display_name, status: .status.phase}'
curl -s ${CC_AUTH} "https://api.confluent.cloud/tableflow/v1/tableflow-topics/<id>" | jq .
curl -s -X DELETE ${CC_AUTH} "https://api.confluent.cloud/tableflow/v1/tableflow-topics/<id>"
```

---

## Topic Operations

```bash
# List topics (uses Kafka cluster API key, not Cloud API key)
curl -s -u "<kafka-api-key>:<kafka-api-secret>" \
  "<cluster-rest-endpoint>/kafka/v3/clusters/<cluster-id>/topics" | jq '.data[].topic_name'
```

---

## Notes

- **Cloud API Key** for management (connectors, Flink, Tableflow, SR). **Kafka API Key** for data-plane (topics, produce, consume).
- **Flink** requires org ID + principal ID (not needed for MCP/CLI). Base URL is region-specific with lowercase cloud provider.
- Full API reference: https://docs.confluent.io/cloud/current/api.html
