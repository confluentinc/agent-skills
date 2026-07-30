---
name: confluent-cloud-flink-sql
description: "Write and debug Flink SQL that runs on Confluent Cloud, enforcing the CC-vs-Apache-Flink (OSS) dialect boundary. Use when the working directory is a Confluent Cloud Flink workspace, when a Flink SQL statement needs checking before it runs on a CC compute pool, or when the user mentions CC Flink, Confluent Cloud Flink SQL, the `confluent flink` CLI, a CFU compute pool, `CREATE CONNECTION`, or asks to check or debug Flink SQL whose runtime is Confluent Cloud. Also trigger when a Flink SQL question is posed and nothing establishes an Apache Flink OSS runtime. Do NOT trigger for: building or deploying Flink UDFs in Java (UDF/UDTF/PTF — use flink-udf); a full CDC pipeline from a database through Flink into Tableflow/Iceberg/Delta Lake (use confluent-cloud-cdc-tableflow); Kafka Streams topology work (use kafka-streams-programming); or Flink SQL confirmed to run on Apache Flink OSS, not Confluent Cloud."
metadata:
  version: "1.0.0"
compatibility: Requires the `confluent` CLI (logged in, `confluent login --save`) and access to a Confluent Cloud environment with a Flink compute pool for live verification.
---

# Confluent Cloud Flink SQL

Enforce the CC-Flink-vs-OSS-Flink dialect boundary and the CLI-driven verification loop for any Confluent Cloud Flink SQL work. Apache Flink OSS training data is a trap — CC rejects or silently mishandles a long list of otherwise-valid Flink SQL constructs.

## Non-negotiables

1. **CC Flink ≠ Apache Flink.** Verify every API, SQL construct, and runtime behavior against:
   - [Confluent Cloud Flink docs](https://docs.confluent.io/cloud/current/flink/)
   - [CC Flink SQL reference](https://docs.confluent.io/cloud/current/flink/reference/)
   - [Confluent Terraform provider](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)
   - Live `confluent flink shell` against the user's compute pool
2. **No mocks in verification.** Integration claims require real `confluent` CLI runs. Unit tests may mock; anything calling itself "end-to-end verification" may not.
3. **Docs first.** Decisions land in `docs/*.md` before code. Dialect traps go in `docs/flink-dialect-traps.md`.
4. **Secrets never in repo.** `CREATE CONNECTION` parameters are Terraform-injected, never hardcoded. Gitignore `.tfvars`, `.tfstate*`, `*.secret*` from day one.
5. **EXPLAIN before CREATE.** Always `EXPLAIN` a query before `statement create` — catches parse/type errors without consuming CFUs.

## Reference files

Load these on demand when the topic matches — do not read them all upfront:

| File | When to load |
|------|-------------|
| [references/dialect-traps.md](references/dialect-traps.md) | Before writing ANY Flink SQL — 32 CC-vs-OSS traps, single source of truth |
| [references/cli-reference.md](references/cli-reference.md) | Before running `confluent` CLI — flag schemas, carry-over recipe, timing, token expiry |
| [references/sql-patterns-cc.md](references/sql-patterns-cc.md) | When writing SQL — CC-validated patterns: windows, joins, dedup, MATCH_RECOGNIZE, JSON, External Tables |
| [references/formats-and-serialization.md](references/formats-and-serialization.md) | When configuring table formats — 7 supported formats, id-encoding, consume flags |
| [references/troubleshooting-cc.md](references/troubleshooting-cc.md) | When debugging errors — CC-specific error to cause to fix |
| [references/reserved-words.md](references/reserved-words.md) | When hitting parse errors — must-backquote identifiers |

## Red flags

Stop and consult `references/dialect-traps.md` if you catch yourself writing any of these:

- DataStream API (Java/Scala) — not supported on CC. Table API (Java/Python) IS supported
- `CREATE CATALOG ...` — catalog = CC environment, not creatable
- `SET 'execution.checkpointing.*'` — CC-managed, not settable
- `CREATE TABLE ... WITH ('connector' = 'kafka', ...)` — tables auto-map from topics
- `'value.format' = 'json'` — must be `'json-registry'` (or another SR-backed format)
- `WITH cte AS (...) INSERT INTO ...` — CC requires the CTE AFTER `INSERT INTO`
- `$rowtime AS alias` in a CTE — silently strips the time-attribute property
- `GROUP BY TUMBLE(ts, INTERVAL ...)` — must use the TVF form: `TUMBLE(TABLE t, DESCRIPTOR(ts), ...)`
- `LATERAL TABLE(UNNEST(...))` — parse error; use `CROSS JOIN UNNEST(...)`
- `PROCTIME()` — not supported; use External Tables/`KEY_SEARCH_AGG` or an upsert-kafka join
- `CREATE FUNCTION f AS '...'` without `USING JAR` — CC UDFs require an uploaded artifact
- Savepoints / `STOP WITH SAVEPOINT` — not exposed on CC
- `--sql-file` flag — doesn't exist; use `--sql "$(cat file.sql)"`
- `--cloud`/`--region` on `statement create` — rejected; use `--environment`

## Verification loop

Canonical validation loop for any CC Flink SQL claim:

0. **EXPLAIN** the query in `flink shell` — catches syntax and type errors for free.
1. Write a minimal reproducer in `repro/<phase>-<slug>.sql`.
2. Run: `confluent flink statement create <name> --sql "$(cat repro.sql)" --compute-pool <id> --database <cluster> --environment <env> --wait`
3. Observe. Consume downstream: `confluent kafka topic consume <topic> --cluster <id> --from-beginning --value-format <matching-format> 2>/dev/null | grep -v '^%'` — match `<matching-format>` to the sink's `value.format` (see [references/formats-and-serialization.md](references/formats-and-serialization.md); `jsonschema` for `json-registry`, `avro` for `avro-registry`, `protobuf` for `proto-registry`, `string` for `raw`)
4. Paste the command + output into `docs/VERIFICATION-<phase>.md`.

Escalation-required states (no silent workarounds):

- Statement `PENDING` > 60s → `confluent flink statement exception list <name> --cloud <provider> --region <region>`
- UDF deploy "jar not found" → `confluent flink artifact list`
- Schema mismatch → `DESCRIBE <table>`, diff against the producer schema
- Egress denied → check `CREATE CONNECTION` + `USING CONNECTIONS` clause

See [references/cli-reference.md](references/cli-reference.md) for full flag schemas and timing expectations.

## Anti-patterns

- Apache Flink docs or Stack Overflow answers tagged `apache-flink` cited as CC authority
- LLM memory of "Flink SQL syntax" used without CC verification
- Mocking the `confluent` CLI in anything claiming end-to-end verification
- `terraform apply -auto-approve` on the first run of a root module
- Committing `.tfvars`, `.tfstate*`, `.terraform/`, `*.secret*`
- Swallowing Flink statement exceptions — fail loud; read `statement exception list`
- Hardcoded secrets in `CREATE CONNECTION` or UDF source

## Tutorials

[developer.confluent.io/tutorials/#flink](https://developer.confluent.io/tutorials/#flink) mixes OSS and CC tutorials.

**Filter rule:** Only use tutorials that list "Confluent Cloud" in prerequisites or use `confluent flink shell`. Apply the dialect trap table to any SQL copied from a tutorial — many target OSS Flink or Kafka Streams and are not CC-compatible as-is.

## Source-of-truth hierarchy

1. `references/dialect-traps.md` (this skill) — canonical, consolidated
2. Per-project `CLAUDE.md` — references this skill, adds project-specific context
3. Per-project `docs/flink-dialect-traps.md` — append-only session log, periodically upstreamed into this skill

When a new trap is discovered: add it to `references/dialect-traps.md` first, then propagate.

## References

- [Confluent Cloud Flink docs](https://docs.confluent.io/cloud/current/flink/)
- [Confluent Cloud Flink SQL reference](https://docs.confluent.io/cloud/current/flink/reference/)
- [Confluent Terraform provider](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)
- [Confluent CLI reference — Flink](https://docs.confluent.io/confluent-cli/current/command-reference/flink/)
- [Tutorials (filter for CC only)](https://developer.confluent.io/tutorials/#flink)
