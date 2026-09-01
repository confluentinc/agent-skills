# Compatibility modes

Schema Registry docs: <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.md>

## The seven modes

| Mode | Check: new schema vs… | Allowed changes | Upgrade first |
|---|---|---|---|
| `BACKWARD` (default) | latest registered version | delete fields, add **optional** fields | **Consumers** |
| `BACKWARD_TRANSITIVE` | **all** previous versions | delete fields, add optional fields | Consumers |
| `FORWARD` | latest registered version | add fields, delete **optional** fields | **Producers** |
| `FORWARD_TRANSITIVE` | all previous versions | add fields, delete optional fields | Producers |
| `FULL` | latest registered version | add **optional** fields, delete **optional** fields | Either order |
| `FULL_TRANSITIVE` | all previous versions | add optional fields, delete optional fields | Either order |
| `NONE` | nothing | anything | — (no guarantee) |

What counts as an **optional** field depends on the format:

- **Avro** — the field has a `default`. Avro substitutes the default when a value is absent,
  which is what makes an add or a remove safe. See `avro-rules.md`.
- **JSON Schema** — the field is **not listed in `required`**. A `default` is advisory only;
  Confluent's serializers do not inject it, so a `default` alone does **not** make a field
  optional. See `json-schema-rules.md`.
- **Protobuf** — every field is already optional on the wire (proto3 scalars carry an
  implicit default), so "add a field" is generally safe both ways; a removal still needs
  `reserved`. See `protobuf-rules.md`.

### What "backward" and "forward" actually mean

- **BACKWARD**: a consumer using the **new** schema can read data produced with the **old**
  schema. You deploy the new schema to consumers first, then producers start writing it.
- **FORWARD**: a consumer using the **old** schema can read data produced with the **new**
  schema. You deploy the new schema to producers first; consumers upgrade on their own schedule.
- **FULL**: both hold, so deploy order does not matter.
- `_TRANSITIVE`: the guarantee holds against every historical version, not just the latest —
  needed when consumers may replay a topic from the beginning.

## Read the current mode

Subject-level config overrides the global default. Always read the **effective** mode for the
subject before reasoning about a change.

REST (works on every platform):

```bash
# effective mode for a subject, falling back to global if no subject override
curl -s "$SR_URL/config/${SUBJECT}?defaultToGlobal=true" $SR_AUTH
# global default
curl -s "$SR_URL/config" $SR_AUTH
```

`$SR_AUTH` is `-u "$SR_KEY:$SR_SECRET"` on Confluent Cloud, a `-H "Authorization: ..."` header
on secured Confluent Platform, or empty on an unsecured community registry. See the matching
`references/<platform>.md` for how the URL and auth are formed — never echo the secret.

CLI equivalents are in each platform reference.

## Check before you register

Validate a candidate schema against a subject **without** creating a version:

```bash
curl -s -X POST "$SR_URL/compatibility/subjects/${SUBJECT}/versions/latest?verbose=true" \
  $SR_AUTH -H "Content-Type: application/json" \
  -d @<(jq -n --rawfile s new-schema.avsc '{schema:$s, schemaType:"AVRO"}')
# -> {"is_compatible": true}  or  {"is_compatible": false, "messages": ["..."]}
```

- Use `versions/latest` for non-transitive modes; the registry automatically checks all
  versions when the subject's mode is `_TRANSITIVE`.
- `schemaType` is `AVRO`, `JSON`, or `PROTOBUF`.
- `?verbose=true` returns the specific incompatibility messages — always use it when
  diagnosing.
- For schemas with references (e.g. a JSON Schema `$ref`, a Protobuf `import`), include the
  `references` array in the body.

## Set the mode (mutation — confirm first)

```bash
# subject scope (preferred)
curl -s -X PUT "$SR_URL/config/${SUBJECT}" $SR_AUTH \
  -H "Content-Type: application/json" -d '{"compatibility": "FULL_TRANSITIVE"}'

# global scope — affects every subject without an override; avoid for a single-subject fix
curl -s -X PUT "$SR_URL/config" $SR_AUTH \
  -H "Content-Type: application/json" -d '{"compatibility": "BACKWARD"}'

# remove a subject override, reverting it to the global default
curl -s -X DELETE "$SR_URL/config/${SUBJECT}" $SR_AUTH
```

Changing the mode **does not re-validate existing versions** — it only governs future
registrations. A subject can already contain versions that would fail its current mode.

## Notes

- **Compatibility groups / Data Contracts**: Confluent Cloud and Platform can gate
  compatibility by a metadata property (e.g. `application.major.version`) so that a breaking
  change is allowed across group boundaries but checked within a group. See
  `rollout-and-troubleshooting.md` § "Breaking changes".
- **Schema contexts** (Cloud, CP 7.4+): an independent namespace of subjects
  (`:.contextname:subject`). A change in one context has no bearing on another. Confirm which
  context the subject lives in if the user uses them.
- **Key vs value**: `<topic>-key` and `<topic>-value` are separate subjects with independent
  config. Key schema changes are usually far more constrained — partitioning and log
  compaction depend on serialized key bytes being stable.
