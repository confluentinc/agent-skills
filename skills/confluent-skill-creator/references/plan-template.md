# CRUD Operations Plan Template

Before running any test or performing any resource-modifying operation, create a plan and get user confirmation.

## Template

```
Plan for test case: <test-name>

Environment: <local/platform/cloud/warpstream>
Interaction method: <MCP tools / CLI / REST APIs / client libraries / combination>

Operations (in execution order):

1. <Operation description>
   Tool/Command: <exact MCP tool name, CLI command, or API endpoint>
   Example: `confluent kafka topic create orders --partitions 3`

2. <Operation description>
   Tool/Command: <exact tool/command>

...

Resources affected:
- Topics: <list with partitions and retention>
- Schemas: <list with format (JSON_SR | Avro | Protobuf)>
- Flink resources: <compute pool, SQL statements> (if applicable)
- Connectors: <connector names and types> (if applicable)

Test data:
- Produce N sample messages to <topic>
- Verify expected output (N messages in <output-topic>)

Cleanup (after verification):
- <cleanup operations, noting which are local-only vs all-environment>

Proceed with this plan? [yes/no]
```

**Wait for user confirmation.** Do not proceed without explicit approval.

## Topic Management Rules

- **Confluent Cloud**: Topics must be pre-created by the user. Check if they exist before testing. If missing, tell the user which topics need to be created.
- **Local Confluent**: Auto-create topics during testing. Include auto-creation in the plan.
- **Confluent Platform**: Ask user preference (pre-create or auto-create).
- **WarpStream**: Topics can be auto-created. Note that `replication.factor` is cosmetic (hard-coded to 3).

## Interaction Method Examples

**MCP tools** (use fully qualified names — `ServerName:tool_name` — the server name depends on the user's MCP config):
```
1. List existing topics
   Tool: confluent-local-oauth:list-topics (environment_id: env-xxx, cluster_id: lkc-xxx)
2. Create orders topic
   Tool: confluent-local-oauth:create-topics (topics: [{topic: "orders", numPartitions: 3}])
```

**CLI:**
```
1. Create orders topic
   Command: confluent kafka topic create orders --partitions 3 --cluster lkc-xxx
2. Register schema
   Command: confluent schema-registry schema create --subject orders-value --schema schema.json
```

**REST API:**
```
1. Create topic via Kafka REST
   Endpoint: POST /kafka/v3/clusters/{cluster_id}/topics
2. Register schema via SR API
   Endpoint: POST /subjects/orders-value/versions
```
