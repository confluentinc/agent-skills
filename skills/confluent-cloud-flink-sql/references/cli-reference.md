# Confluent Cloud Flink — CLI Reference

## Flag schema

All `flink statement` subcommands accept `--cloud`/`--region`, `--environment`, and `--context` — none of them are rejected on any subcommand. `flink statement create` requires `--sql`, `--compute-pool`, `--database`, and `--environment`; the other subcommands (`list`/`describe`/`stop`/`delete`) leave all of these optional, falling back to whatever `confluent environment use`/`confluent kafka cluster use` context is active.

| Subcommand | `--cloud`/`--region` | `--environment` | `--compute-pool` | `--database` | `--sql` | `--wait` |
|---|---|---|---|---|---|---|
| `flink statement create` | optional | ✅ required | ✅ required | ✅ required | ✅ required | optional (60s default) |
| `flink statement list` | optional | optional | — | — | — | — |
| `flink statement describe` | optional | optional | — | — | — | — |
| `flink statement stop` | optional | optional | — | — | — | — |
| `flink statement delete` | optional | optional | — | — | — | — |

This table covers `flink statement` only. Other `flink` subcommand groups diverge:
- `flink artifact create`/`list`/`describe`: `--cloud` and `--region` are REQUIRED, unlike `flink statement` where both are optional.
- `flink compute-pool`: no `--cloud` flag exists. `list` accepts `--region`; `describe` accepts only `--environment`.

Key rules:
- `create` requires `--environment`, `--compute-pool`, and `--database` in addition to `--sql` — omitting any of them fails to schedule the statement or risks running it against whichever pool/cluster happens to be active in context. `list`, `describe`, `stop`, and `delete` accept `--cloud`/`--region` AND `--environment` but don't require them — there's no per-subcommand rejection there. Pass whichever ones the active CLI context hasn't already resolved.
- `--sql-file` does NOT exist — read file: `--sql "$(cat file.sql)"`
- `--property` accepts comma-separated values in one flag (`--property "k1=v1,k2=v2"`); the CLI docs describe it as a string slice, so it may also work as a repeated flag — comma-separated is the tested form.

## Canonical commands

```bash
# Session verification
confluent organization list                    # verify control-plane session
confluent environment use env-XXX
confluent kafka cluster use lkc-XXX

# Flink shell
confluent flink shell --compute-pool <pool-id> --environment <env-id>

# Statement lifecycle
confluent flink statement create <name> \
  --sql "$(cat repro.sql)" \
  --compute-pool <pool-id> --database <cluster-id> --environment <env-id> [--wait]

confluent flink statement list --cloud <provider> --region <region>
confluent flink statement describe <name> --cloud <provider> --region <region>
confluent flink statement stop <name> --cloud <provider> --region <region>
confluent flink statement delete <name> --cloud <provider> --region <region>
confluent flink statement exception list <name> --cloud <provider> --region <region>

# Artifact management (UDFs)
confluent flink artifact create <name> --cloud <provider> --region <region> --artifact-file <jar>
confluent flink artifact list --cloud <provider> --region <region>
confluent flink artifact describe <artifact-id> --cloud <provider> --region <region>

# Kafka operations
confluent kafka topic consume <topic> --cluster <id> --from-beginning --value-format jsonschema 2>/dev/null | grep -v '^%'
confluent kafka topic produce <topic> --cluster <id> < input.jsonl

# Schema Registry
confluent schema-registry schema describe --subject <subject>
confluent schema-registry subject list
```

## Token expiration

CLI OAuth tokens expire periodically. Subprocess token caches can go stale independently of the interactive shell.

```bash
# Fix: re-login and persist credentials
confluent login --save
```

Observed: interactive `confluent organization list` succeeds while Maven subprocess fails with `expired token`. Re-login on the host fixes both.

## Carry-over offsets (stateless statement replacement)

Replace a running statement without reprocessing:

```bash
# Step 1: submit v2 with carry-over. NO --wait (v2 stays PENDING until v1 stops)
confluent flink statement create kf-stmt-v2 \
  --sql "$(cat new-filter.sql)" \
  --compute-pool POOL --database CLUSTER --environment ENV \
  --property "sql.tables.initial-offset-from=kf-stmt-v1"

# Step 2: STOP v1 (NOT delete — delete dangles the carry-over reference)
confluent flink statement stop kf-stmt-v1 --cloud aws --region eu-central-1

# Step 3: poll v2 until RUNNING (~25-30s)
for i in $(seq 1 60); do
  STATUS=$(confluent flink statement describe kf-stmt-v2 \
    --cloud aws --region eu-central-1 --output json \
    | jq -r '.status | if type == "object" then .phase else . end // "UNKNOWN"')
  [[ "$STATUS" == "RUNNING" ]] && break
  [[ "$STATUS" == "FAILED" ]] && { echo "v2 failed"; exit 1; }
  sleep 5
done
```

Constraints:
- Stateless statements only (no aggregates/windows/pattern matching/upsert sinks)
- Same org + environment + region for v1 and v2
- v1 must be STOPPED, not deleted
- Do NOT use `--wait` on carry-over creates

## Eventually-consistent state

After delete, `list` still shows resource for ~5-15s. Gate on side-effects, not existence:

```bash
# Gate on topic existence (DDL creates topics)
if confluent kafka topic list --cluster $CLUSTER --output json | grep -q '"name": "my-topic"'; then
  echo "topic exists; skip DDL"
fi
```

`kafka topic delete` and `schema-registry subject delete --permanent` are irreversible. Confirm with the user before running either — state which topic/subject and cluster/registry you're targeting — then wait for deletion propagation:

```bash
confluent kafka topic delete my-topic --cluster $CLUSTER --force
until ! confluent kafka topic list --cluster $CLUSTER --output json | grep -q '"name": "my-topic"'; do
  sleep 5
done
```

Always use `--output json` for programmatic checks — tabular output has leading whitespace that breaks grep anchors.

## Approximate timing (AWS eu-central-1, low load)

| Operation | Wall time |
|---|---|
| `kafka topic create` | ~5s |
| `kafka topic delete` (CLI returns) | ~5s (propagation ~10-15s) |
| `schema-registry subject delete --permanent` | ~2s |
| `flink statement create` DDL `--wait` | ~25-35s |
| `flink statement create` filter `--wait` | ~5-15s |
| `flink statement stop` | <1s; STOPPED ~5s later |
| `flink statement delete` | <1s |
| Carry-over PENDING → RUNNING | ~25-30s |
| `kafka topic produce` (10 records) | ~3s |
| `kafka topic consume` time-to-first | ~5-15s |

Any drop-then-create needs 30s buffer or explicit poll loop.

## Output format notes

`describe --output json` flattens status to plain string:
```json
{"name": "stmt", "status": "RUNNING", "status_detail": "..."}
```

NOT `{"status": {"phase": "RUNNING"}}`. Handle both forms defensively in parsers.
