# CC Flink — Formats and Serialization

## Supported `value.format` values (7 total)

| Format | Type | Notes |
|---|---|---|
| `avro-registry` | SR-backed Avro | Default if unspecified |
| `json-registry` | SR-backed JSON Schema | Use instead of `'json'` (which fails) |
| `proto-registry` | SR-backed Protobuf | |
| `raw` | Binary VARBINARY, no SR | |
| `avro-debezium-registry` | Debezium CDC + Avro | Requires `changelog.mode = retract` or `upsert` |
| `json-debezium-registry` | Debezium CDC + JSON Schema | |
| `proto-debezium-registry` | Debezium CDC + Protobuf | |

**NOT supported:** `json`, `avro`, `csv`, `string`, `canal-json`, `maxwell-json`, `debezium-json`

**Naming trap:** CC uses format-first naming (`json-debezium-registry`). OSS uses debezium-first (`debezium-json`).

## Schema-in-header (clean output)

```sql
CREATE TABLE clean_output (
  ...
) WITH (
  'value.format' = 'json-registry',
  'value.json-registry.id-encoding' = 'header',   -- schema ID in Kafka header
  'changelog.mode' = 'append'
);
```

| `id-encoding` | Behavior |
|---|---|
| `confluent` (default) | Magic byte + 4-byte schema ID prepended to payload |
| `header` | Schema ID in Kafka record header, payload is clean format |

Write-only option. Reads auto-resolve via header → prefix → fallback chain.

Use `header` when downstream consumers expect plain JSON/Avro/Protobuf without SR wire format.

## Reading schemaless input (no magic byte)

Register a JSON Schema in SR under `<topic>-value` subject. No magic byte needed in data:

```bash
confluent schema-registry schema create --subject my-topic-value --schema schema.json --type JSON
```

Flink auto-maps the topic to a typed table. Use `SELECT src.field` instead of `JSON_VALUE(CAST(val AS STRING), '$.field')`.

Caveat: registered schema types must match actual JSON types exactly. For polymorphic fields, omit from schema and use `JSON_VALUE` selectively.

## Consumer output format flags

| Topic encoding | `--value-format` flag |
|---|---|
| Raw bytes (no SR) | `string` (default) |
| JSON Schema (SR-backed, `id-encoding=confluent`) | `jsonschema` |
| JSON Schema (SR-backed, `id-encoding=header`) | `jsonschema` or `string` (both work) |
| Avro | `avro` |
| Protobuf | `protobuf` |

**Default `string` on SR-backed topic with `id-encoding=confluent`** = first 5 bytes are wire-format prefix → UTF-8 errors.

**With `id-encoding=header`:** `string` works but consumer prints interleaved `% Headers:` lines on stdout:
```
{"payload":{...}}
% Headers: [__value_schema_id="82aef084-..."]
```

**Always filter when piping:**
```bash
confluent kafka topic consume TOPIC \
  --cluster CLUSTER --from-beginning --print-key=false \
  --value-format jsonschema 2>/dev/null \
  | grep -v '^%' \
  | head -n "$EXPECTED_COUNT" > actual.jsonl
```

`grep -v '^%'` needed regardless of `--value-format` when `id-encoding=header` is active.
