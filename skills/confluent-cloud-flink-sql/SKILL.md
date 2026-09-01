---
name: confluent-cloud-flink-sql
description: "Write and debug Flink SQL that runs on Confluent Cloud, enforcing the CC-vs-Apache-Flink (OSS) dialect boundary. Use when the working directory is a Confluent Cloud Flink workspace, when a Flink SQL statement needs checking before it runs on a CC compute pool, or when the user mentions CC Flink, Confluent Cloud Flink SQL, the `confluent flink` CLI, a CFU compute pool, `CREATE CONNECTION`, or asks to check or debug Flink SQL whose runtime is Confluent Cloud. Also trigger when a Flink SQL question is posed and nothing establishes an Apache Flink OSS runtime. Do NOT trigger for: building or deploying Flink UDFs in Java (UDF/UDTF/PTF — use flink-udf); a full CDC pipeline from a database through Flink into Tableflow/Iceberg/Delta Lake (use confluent-cloud-cdc-tableflow); Kafka Streams topology work (use kafka-streams-programming); or Flink SQL confirmed to run on Apache Flink OSS, not Confluent Cloud."
compatibility: Requires the `confluent` CLI (authenticated session) and an active Confluent Cloud compute pool — statement runs consume CFUs. Terraform is optional, needed only if managing `CREATE CONNECTION` credentials via the Confluent Terraform provider.
metadata:
  author: confluent
  version: "1.1.0"
  last_updated: "2026-09-01"
---

# Confluent Cloud Flink SQL

Enforce the CC-Flink-vs-OSS-Flink dialect boundary and the CLI-driven verification loop for any Confluent Cloud Flink SQL work. Apache Flink OSS training data is a trap — CC rejects or silently mishandles a long list of otherwise-valid Flink SQL constructs.

Scope note: this skill's reference material is built around the OSS-vs-CC dialect boundary — traps in constructs that exist in both dialects but behave differently. It does not yet catalog CC-only DDL that has no OSS counterpart (e.g. `CREATE MATERIALIZED TABLE`, `CREATE MODEL`/`AI_COMPLETE`, `CREATE AGENT`, `USE CATALOG`). For those, verify directly against the [CC Flink SQL reference](https://docs.confluent.io/cloud/current/flink/reference/overview.md) rather than expecting a trap entry here.

## Non-negotiables

1. **CC Flink ≠ Apache Flink.** Verify every API, SQL construct, and runtime behavior against:
   - [Confluent Cloud Flink docs](https://docs.confluent.io/cloud/current/flink/overview.md)
   - [CC Flink SQL reference](https://docs.confluent.io/cloud/current/flink/reference/overview.md)
   - [Confluent Terraform provider](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)
   - Live `confluent flink shell` against the user's compute pool
2. **No mocks in verification.** Integration claims require real `confluent` CLI runs. Unit tests may mock; anything calling itself "end-to-end verification" may not.
3. **Record decisions somewhere durable.** Ask the user where they want dialect traps and verification notes tracked (e.g. `docs/flink-dialect-traps.md` in their project) before writing any new file — don't assume a `docs/` layout.
4. **Secrets never in repo.** `CREATE CONNECTION` parameters are Terraform-injected, never hardcoded. Gitignore `.tfvars`, `.tfstate*`, `*.secret*` from day one.
5. **EXPLAIN before CREATE.** Always `EXPLAIN` a query before `statement create` — catches parse/type errors without consuming CFUs.
6. **Don't invent identifiers.** Use `<placeholder>` for any topic, table, statement, or resource name you haven't verified.

## Reference files

Load these on demand when the topic matches — do not read them all upfront:

| File | When to load |
|------|-------------|
| [references/dialect-traps.md](references/dialect-traps.md) | Before writing ANY Flink SQL — 22 CC-vs-OSS traps, single source of truth |
| [references/cli-reference.md](references/cli-reference.md) | Before running `confluent` CLI — flag schemas, carry-over recipe, timing, token expiry |
| [references/sql-patterns-cc.md](references/sql-patterns-cc.md) | When writing SQL — CC-validated patterns: windows, joins, dedup, MATCH_RECOGNIZE, JSON, External Tables |
| [references/formats-and-serialization.md](references/formats-and-serialization.md) | When configuring table formats — 7 supported formats, id-encoding, consume flags |
| [references/troubleshooting-cc.md](references/troubleshooting-cc.md) | When debugging errors — CC-specific error to cause to fix |
| [references/reserved-words.md](references/reserved-words.md) | When hitting parse errors — must-backquote identifiers |

## Red flags

Stop and consult `references/dialect-traps.md` if you catch yourself writing any of these:

- DataStream API (Java/Scala) — not supported on CC. Table API + PTF are GA in Java; Python Table API is Open Preview with no PTF yet
- `CREATE CATALOG ...` — catalog = CC environment, not creatable
- `SET 'execution.checkpointing.*'` — CC-managed, not settable
- `CREATE TABLE ... WITH ('connector' = 'kafka', ...)` — tables auto-map from topics
- `'value.format' = 'json'` — must be `'json-registry'` (or another SR-backed format)
- `WITH cte AS (...) INSERT INTO ...` — CC requires the CTE AFTER `INSERT INTO`
- `GROUP BY TUMBLE(ts, INTERVAL ...)` — must use the TVF form: `TUMBLE(TABLE t, DESCRIPTOR(ts), ...)`
- `LATERAL TABLE(UNNEST(...))` — parse error; use `CROSS JOIN UNNEST(...)`
- `PROCTIME()` — not supported; use External Tables/`KEY_SEARCH_AGG` or an event-time temporal join (avoid a regular join against an upsert-kafka topic — it retains the whole table in state)
- `CREATE FUNCTION f AS '...'` without `USING JAR` — CC UDFs require an uploaded artifact
- Savepoints / `STOP WITH SAVEPOINT` — not exposed on CC
- `--sql-file` flag — doesn't exist; use `--sql "$(cat file.sql)"`
- `DROP TABLE` — deletes the physical Kafka topic and its data on CC, not just metadata; confirm before running

## Optional HOL Guard gate for local coding-agent sessions

When the user explicitly asks to protect state-changing Confluent CLI work and the `confluent` command will be executed by a HOL Guard-supported local coding agent, use the published HOL Guard runtime instead of adding custom command-blocking logic to this skill. HOL Guard is an open-source security layer for AI agents and coding tools.

Do not infer a HOL Guard harness from the command being protected. In particular, `confluent` and `confluent-cli` are **not** harness names. Here, `<harness>` means a supported local coding agent detected by `hol-guard start`. Do not skip detection or invent a harness name.

1. **Keep Confluent controls authoritative.** HOL Guard does not replace Confluent Cloud authentication, RBAC, resource permissions, or the explicit confirmation gate below.
2. **Ask before changing local setup.** If HOL Guard is not installed and the user approves installation, use `pipx install hol-guard`, then `hol-guard init`. If the user declines, continue with this skill's existing EXPLAIN and confirmation gates; do not invent equivalent runtime enforcement.
3. **Detect the local coding-agent harness first.** Run `hol-guard start` and use only a supported harness it detects for the intended coding agent. If the intended coding agent is not supported, say that Guard cannot protect this workflow through a supported harness and continue with Confluent's native safety gates or stop, according to the user's request. Do not create a workaround wrapper.
4. **Wire and verify only with approval.** If the intended harness is supported and the user approves wiring it, use `hol-guard install <harness>`, then `hol-guard status`.
5. **Launch the mutation-bearing coding-agent session through Guard.** Use `hol-guard run <harness>`. The state-changing `confluent` command runs from that protected coding-agent session; it is not itself the harness. Still present the Confluent statement, target resources, and side effects and wait for explicit user confirmation before running step 3 of the verification loop.
6. **Do not bypass or recreate Guard.** If Guard blocks or holds an action for review, resolve that decision in Guard or stop. Do not implement a shell wrapper, denylist, policy engine, hook, or middleware that substitutes for HOL Guard.
7. **Keep the scope local.** Do not claim HOL Guard runs inside Confluent Cloud or intercepts server-side Confluent APIs.

## Verification loop

Canonical validation loop for any CC Flink SQL claim:

0. **EXPLAIN** the query in `flink shell` — catches syntax and type errors for free.
1. Write a minimal reproducer.
2. **Present the plan and wait for explicit user confirmation** before running anything that creates or modifies a real resource. State: the statement name and SQL, the compute pool/database/environment it targets, and any side effects (DDL creates a Kafka topic and Schema Registry subject; every run consumes CFUs). Do not proceed to step 3 without a go-ahead.
3. Run: `confluent flink statement create <name> --sql "$(cat repro.sql)" --compute-pool <id> --database <cluster> --environment <env> --wait`
4. Observe. Consume downstream: `confluent kafka topic consume <topic> --cluster <id> --from-beginning --value-format <matching-format> 2>/dev/null | grep -v '^%'` — match `<matching-format>` to the sink's `value.format` (see [references/formats-and-serialization.md](references/formats-and-serialization.md); `jsonschema` for `json-registry`, `avro` for `avro-registry`, `protobuf` for `proto-registry`, `string` for `raw`)
5. Record the command + output for later reference.

Escalation-required states (no silent workarounds):

- Statement `PENDING` > 60s → `confluent flink statement exception list <name> --cloud <provider> --region <region>`
- UDF deploy "jar not found" → `confluent flink artifact list --cloud <provider> --region <region>`
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
3. A per-project trap log, if the user wants one kept — ask where before creating it

When a new trap is discovered during a session, tell the user so they can decide whether to record it in their project's own notes. Do not edit this skill's own installed files (`references/dialect-traps.md` or elsewhere) — propose the change and let the user (or a separate PR to this skill's repo) apply it.

## References

- [Confluent Cloud Flink docs](https://docs.confluent.io/cloud/current/flink/overview.md)
- [Confluent Cloud Flink SQL reference](https://docs.confluent.io/cloud/current/flink/reference/overview.md)
- [Confluent Terraform provider](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)
- [Confluent CLI reference — Flink](https://docs.confluent.io/confluent-cli/current/command-reference/flink/index.md)
- [Tutorials (filter for CC only)](https://developer.confluent.io/tutorials/#flink)
