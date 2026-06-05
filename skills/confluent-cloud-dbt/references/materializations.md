# Materialization mechanics

How to pick a materialization, how each behaves in the dbt-confluent adapter, plus execution modes.

## Picking a materialization

**Choose:**

| User intent | Materialization | Note |
|---|---|---|
| Batch transformation that runs to completion | `table` | CTAS. Re-runs are no-ops (schema-drift gated). |
| Continuous streaming pipeline (a topic of derived events) | `streaming_table` | Two statements: a quick `CREATE TABLE` + a long-running `INSERT INTO ... SELECT`. |
| Connector-backed source table (e.g. Datagen for testing) | `streaming_source` | Model body is column DDL, not a SELECT. `connector` config required.[^connectors] |
| Read-only reference to an existing Kafka topic | **dbt `source`**, not a model | Topics auto-appear as Flink tables. |
| Lightweight virtual relation | `view` | Drop-and-recreate every run. |
| Inline CTE-style helper | `ephemeral` | Standard dbt behaviour. |

[^connectors]: Only the `faker` connector is exercised in the adapter's test suite today. Other connectors should work via `config(connector='...', with={...})` but aren't yet validated end-to-end against the adapter — verify before relying on them in production. Catalog: https://docs.confluent.io/cloud/current/connectors/index.html

**Don't:**

- **Don't reach for `incremental`** — compiler error. The user's mental model ("efficient updates without recompute") is the *default* in Flink streaming. Use `streaming_table`.
- **Don't reach for `materialized_view`** — compiler error. In Confluent Flink, `table` *is* a continuously-updated CTAS. Use `table`.
- **Don't reach for `snapshot`** — no `MERGE`/`UPDATE`. SCD2 needs Flink-native changelog/temporal-table patterns; out of scope for the adapter.
- **Don't assume `table` means "batch table" in the warehouse sense** — it's a CTAS that Flink keeps fresh in the background. Re-running dbt won't recompute; it SKIPs unless schema drifted or `--full-refresh` is passed.
- **Don't use `streaming_source` for read-only references to topics produced elsewhere.** Use a dbt `source` — every Kafka topic is automatically a Flink table.

## `table`
Single CTAS. The materialization:
1. Loads the cached relation. If it doesn't exist, jumps to step 4.
2. On `--full-refresh`: deletes the deterministic Flink statement, drops the relation. Continues to step 4.
3. Otherwise: runs `check_for_schema_drift` (unless `on_schema_drift='ignore'`). Returns SKIP if no drift, raises a `CompilationError` if drift detected.
4. Issues `CREATE TABLE {target} AS ({sql})` under the deterministic statement name.

There is no atomic swap (no `ALTER TABLE RENAME`). The default `execution_mode` (`streaming_query`) is fine for CTAS; the resulting Flink statement completes on its own and is short-lived.

## `view`
Always drops and recreates. Deletes any prior statement under the same deterministic name first, then `CREATE VIEW`. No drift gate (views are cheap to rebuild).

A bug in Confluent's `ALTER VIEW … RENAME` means view rename is implemented as `SHOW CREATE VIEW` → regex-rewrite → `CREATE VIEW` → `DROP VIEW`. Users won't typically hit this.

## `streaming_table`
Two statements:
1. **DDL** in `streaming_ddl` mode: `CREATE TABLE {target} <inferred-columns> [WITH (...)]`. Statement name gets a `-ddl` suffix. Completes quickly.
2. **INSERT** in `streaming_query` mode: `INSERT INTO {target} {sql}`. **Long-running** — this is the streaming Flink job. Statement name is the primary one.

The drift gate runs before either statement is issued. If drift is detected, neither runs and the materialization fails. On `--full-refresh`, both statement names are deleted before recreating.

The two-statement split exists because the long-running INSERT needs to be addressable separately for lifecycle management (delete on full-refresh, find by label, etc.).

## `streaming_source`
Single statement in `streaming_ddl` mode:

```
CREATE TABLE {target} ({column_definitions}) WITH ('connector' = '...', ...)
```

The column definitions come verbatim from the model body — **not from a SELECT**. `connector` is a mandatory config; missing it raises a clear compile error. Watermarks and PKs go inline in the column body.

## `ephemeral`
Standard dbt CTE inlining; no override.

## `incremental` / `materialized_view` / `snapshot`
Each raises `raise_compiler_error` with a clear message routing the user to the supported alternative. `incremental` says "use `streaming_table`"; `materialized_view` says "use `table` (CTAS in Confluent Flink is continuously updated)".

## `test` (custom)
Wraps the test SQL with a `coalesce(sum(failures), 0)` + `UNION ALL <fallback>` row. Reason: in Flink streaming mode, `count(*)` over an empty result set returns zero rows, not one row of value 0. dbt-core expects exactly one row with three columns; the wrapper guarantees that.

## `unit` (custom)
Doesn't use CTEs — Flink CTEs can't carry watermarks. Instead, for each fixture:
1. `CREATE TABLE <fixture_name> LIKE <tested_model> (EXCLUDING OPTIONS)` (the EXCLUDING OPTIONS part strips connector settings so the fixture table can be a sink for INSERT).
2. `INSERT INTO <fixture_name> <fixture_body>`.
3. Drop the fixture table after the test runs.

This means the **tested model must already exist** in the cluster. If it doesn't, the materialization raises:

> The original relation referenced in tests does not exist: `<db>.<schema>.<model_identifier>`

Workflow: `dbt run --select my_model && dbt test --select unit_my_model`.

## `seed`
Single batched `INSERT INTO ... VALUES (...), (...), ...`. There is no chunking; if `agate_table.rows | length > batch_size`, the materialization raises a compile error. For datasets that don't fit, route to a `streaming_source` (Datagen) or an external producer.

## Execution modes

The `confluent_sql.execution_mode.ExecutionMode` enum has four values:
- `streaming_query` — long-lived streaming SELECT/INSERT (dbt default)
- `streaming_ddl` — DDL inside a streaming context (used by `streaming_table` CREATE and `streaming_source` CREATE)
- `snapshot` — point-in-time bounded query
- `snapshot_ddl` — point-in-time DDL

**User guidance**: leave it on the default. The materializations pick the right mode internally. Per-node override (`config(execution_mode='snapshot')`) is supported but should only be used when the user explicitly needs a bounded snapshot query. The four-mode enum is being simplified upstream toward `streaming` / `snapshot`; don't surface its details in user-facing recommendations.
