# MCP Tools Reference

Optional tools from the `schema-registry` MCP server for enhanced functionality.

## Core Tools (Always Available)

No prerequisites needed:
- **`Glob`** — Find build files, schema files, source code
- **`Grep`** — Detect Kafka dependencies, producers, consumers, serializers, risks
- **`Read`** — Read source files, data models, configs
- **`Write`** — Create schema files, Terraform configs, report

## MCP Tools (Optional)

Requires `schema-registry` MCP server installed and configured.

### schema_status

Get current schema project configuration.

**Call first** to understand project state:
```
schema_status(path: <project root>)
```

Returns info on:
- Existing `schema.yaml` configuration
- Registered schemas
- Environments configured
- Current schema registry connection

Use this to avoid duplicating work or conflicting with existing schema management.

### schema_infer

Generate schemas from sample JSON data files.

```
schema_infer(
  path: <path to sample data file>,
  format: json | avro | protobuf,
  name: <schema name based on topic>
)
```

Useful when:
- Sample data files exist (`.json`, `.ndjson`, test fixtures)
- No explicit data models in code
- Inferring schema from actual data is more accurate than code inspection

### schema_lint

Validate schemas and fix common issues.

```
schema_lint(
  path: <schema file or schemas/ directory>,
  fix: true
)
```

**Always fix warnings** — they prevent real problems during schema evolution:
- Missing `default` values on optional fields (breaks backward compatibility)
- Naming convention issues
- Missing field documentation

See [Schema Registry compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) for compatibility modes.

### schema_validate

Check compatibility against main branch or live Schema Registry.

```
schema_validate(
  path: <schema file>,
  against: main | live_sr
)
```

Verifies:
- Schema is valid for the chosen format
- Schema is backward/forward compatible with existing versions
- No breaking changes

### schema_init

Create `schema.yaml` project configuration.

```
schema_init(path: <project root or schemas/ directory>)
```

Generates:
```yaml
environments:
  dev:
    url: ${SCHEMA_REGISTRY_URL}
    api_key: ${SCHEMA_REGISTRY_API_KEY}
    api_secret: ${SCHEMA_REGISTRY_API_SECRET}

schemas:
  - path: schemas/avro/order-events-value.avsc
    subject: order-events-value
    type: AVRO
```

## Fallback Behavior

If MCP tools are not available:
1. **schema_status**: Check for `schema.yaml` file manually
2. **schema_infer**: Manually infer from JSON structure
3. **schema_lint**: Skip automated lint, add warning to report
4. **schema_validate**: Skip compatibility check, add warning to report
5. **schema_init**: Manually create `schema.yaml`

**Add to report when MCP tools unavailable:**
```markdown
⚠ **Schemas were not machine-validated.** Before registering:
- Install the `schema-registry` MCP server
- Run `schema_lint` and `schema_validate`
- Or manually validate using the Schema Registry REST API
```

## schema.yaml Format

```yaml
environments:
  dev:
    url: ${SCHEMA_REGISTRY_URL}
    api_key: ${SCHEMA_REGISTRY_API_KEY}
    api_secret: ${SCHEMA_REGISTRY_API_SECRET}
  
  prod:
    url: ${PROD_SCHEMA_REGISTRY_URL}
    api_key: ${PROD_SCHEMA_REGISTRY_API_KEY}
    api_secret: ${PROD_SCHEMA_REGISTRY_API_SECRET}

schemas:
  - path: schemas/avro/order-events-value.avsc
    subject: order-events-value
    type: AVRO
    
  - path: schemas/json/user-events-value.json
    subject: user-events-value
    type: JSON
    
  - path: schemas/proto/payment-events-value.proto
    subject: payment-events-value
    type: PROTOBUF
```

Environment variables are resolved at runtime. This allows the same `schema.yaml` to work across environments.
