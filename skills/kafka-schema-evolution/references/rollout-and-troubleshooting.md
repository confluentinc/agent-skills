# Rollout & troubleshooting

## Rollout order

Driven entirely by the compatibility mode.

### BACKWARD / BACKWARD_TRANSITIVE — consumers first

1. Register the new schema version (producers still emitting the old one).
   - Registering ahead of use is safe: it only adds a version, it doesn't change what
     producers write.
2. Deploy the new schema to **all consumer** groups. Verify: no deserialization errors, lag
   returns to baseline.
3. Deploy the new schema to **producers**. They now write records the updated consumers can
   already read.
4. Verify end to end on a canary partition/topic before full producer rollout.

### FORWARD / FORWARD_TRANSITIVE — producers first

1. Register the new schema version.
2. Deploy to **producers**. Old consumers keep reading new data (they ignore added fields /
   fall back to defaults for removed optional fields).
3. Deploy to **consumers** on their own schedule.

### FULL / FULL_TRANSITIVE — any order

Register, then roll producers and consumers independently. Still canary first.

### Universal checks after each stage

- Consumer group lag (`kafka-consumer-groups --describe`) returns to steady state.
- No `SerializationException` / `Error deserializing` in consumer logs.
- Dead-letter-queue / error-topic volume flat.
- Schema Registry `_schemas` topic: new version present, expected version count.

## Diagnosing a rejected registration or failed check

Run the compatibility call with `?verbose=true` (see `compatibility-modes.md`) to get the
specific messages. Common message → cause:

| Message fragment | Cause | Fix |
|---|---|---|
| `READER_FIELD_MISSING_DEFAULT_VALUE` | Added a field without a default; mode is BACKWARD/FULL | Add a default (Avro) / make it optional (JSON, Protobuf) |
| `WRITER_FIELD_MISSING_DEFAULT_VALUE` | Removed a field that had no default; mode is FORWARD/FULL | Keep the field, or add a default first then remove in a second step |
| `TYPE_MISMATCH` | Incompatible type change | Use a widening change, or a new field |
| `NAME_MISMATCH` | Renamed a field/record without alias (Avro) or changed package/namespace | Use `aliases` (Avro), keep the name, or new subject |
| `MISSING_ENUM_SYMBOLS` | Removed an enum symbol | Keep the symbol, or set an enum default |
| `is not backward compatible with an earlier schema` (no version number) | Non-transitive check against latest | Look at the *latest* version, not the one you think is current |
| `... with earlier schema version N` | Transitive check failed against a historical version | Either fix the change or accept you cannot stay transitive |
| HTTP 409 on register | Compatibility failure at registration time | Same as above — run the explicit check to see why |
| HTTP 422 `Invalid schema` | Malformed schema (bad Avro JSON, unresolved `$ref`, missing `import`) | Fix syntax; pass `references` for external types |
| HTTP 404 `Subject not found` on the compat check | Wrong subject name or subject-name strategy | Confirm `<topic>-value` vs RecordNameStrategy; list subjects |

### Runtime errors that look like evolution problems but aren't

| Symptom | Actual cause |
|---|---|
| `Unknown magic byte!` | Consumer using an SR deserializer on data that wasn't written with an SR serializer (or wrong topic / plain-JSON producer). Not a schema issue. |
| `Error retrieving Avro schema for id N` | Consumer can't reach Schema Registry, or is pointed at a *different* registry than the producer. |
| `Schema not found; error code 40403` | Schema ID in the message doesn't exist in the registry the consumer is querying (cross-cluster consume, registry restored from stale backup). |
| Consumer reads `null` / default for a field the producer set | Subject-name strategy mismatch → consumer resolved against the wrong schema. |
| Works in dev, fails in prod after "no schema change" | Subject-level compatibility config differs between environments. Compare `GET /config/{subject}`. |

## CI checks (catch it before deploy)

Wire a compatibility check into the pipeline that builds the producer:

- **Maven**: `io.confluent:kafka-schema-registry-maven-plugin`, goal `test-compatibility`,
  bound to `verify`. Point `schemaRegistryUrls` at the target registry and map each subject
  to its schema file.
- **Gradle**: the `com.github.imflog.kafka-schema-registry-gradle-plugin` `testSchemasTask`,
  or a `curl` to the compatibility endpoint in a CI step.
- **Registry-less / air-gapped CI**: keep the previous schema(s) checked into the repo and
  run the compatibility endpoint of a disposable local Schema Registry container against
  them, or use `yakov/avro`-style structural diff libraries as a first gate.
- Fail the build on `is_compatible: false`. Print the verbose messages.

Full plugin docs:
<https://docs.confluent.io/platform/current/schema-registry/develop/maven-plugin.md>

## Breaking changes — when compatibility genuinely can't hold

1. **New subject / new topic**: version the topic name (`orders.v2`), run both in parallel,
   migrate consumers, then retire `orders`. Cleanest, most operationally visible.
2. **Compatibility groups + Data Contracts migration rules** (Confluent Cloud, CP 7.4+):
   set the subject's config `compatibilityGroup` to a schema metadata property
   (e.g. `major_version`). Schemas in different groups are exempt from the mode check;
   `ruleSet.migrationRules` (JSONata transforms with `UPGRADE` / `DOWNGRADE`) let SR-aware
   consumers read across the break. Requires clients on a recent serde version. Docs:
   <https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.md>
3. **Dual-write / transform**: a stream processor (Flink, Streams) reads `orders` and writes a
   transformed `orders.v2`. Use when consumers can't all move at once.

Never: set the subject to `NONE`, register the breaking schema, set it back. The incompatible
version stays in history and transitive consumers / replays will hit it.
