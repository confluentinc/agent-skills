# Statement lifecycle, naming, and cursor behaviour

How the adapter names, creates, deletes, and reads Flink statements.

## Statement lifecycle and naming

Format: `{statement_name_prefix}{project_name}-{model_name}{suffix}`. Defaults:
- `statement_name_prefix` = `dbt-`
- `suffix` = `''` for primary statements, `-ddl` for the `streaming_table` DDL.

Sanitization (`naming.py`):
- Lowercased; characters not in `[a-z0-9-]` (underscores included) replaced with `-`.
- Leading hyphens stripped (must start with alphanumeric).
- If anything was replaced **or** the result exceeds 100 chars, append a 6-char MD5 hash suffix to disambiguate (so `my_model` and `my.model` don't collide).
- Empty or non-alphanumeric input raises `ValueError`.

On `--full-refresh`: the adapter deletes the existing statements (both `-ddl` and primary), polling for async deletion of RUNNING statements with exponential backoff up to 60s, then recreates them. Orphan statements (statement exists but table doesn't) are cleaned up on the next normal run.

If a delete poll exceeds 60s, the run fails with `DbtDatabaseError("Statement '...' still exists after Ns")` — re-runnable via `dbt retry`.

## Custom statement names

`config(statement_name='my-name')` overrides the deterministic name. The intended use is **adopting an existing Flink statement into a new dbt project** (so the running job can be managed by dbt without restart). This path is implemented but not yet thoroughly validated; don't recommend it as a primary feature unless the user explicitly asks for it.

## Hidden statements

Internal queries (drift checks, `INFORMATION_SCHEMA` reads, `list_relations`) pass `hidden=True` to the custom `statement` macro, which adds a `HIDDEN_LABEL` to the Flink statement. The Confluent Cloud UI filters hidden statements by default — users won't see drift-check temp-table statements in their statement list.

## Streaming-cursor fetch behaviour

The `confluent_sql` cursor distinguishes bounded vs unbounded statements (`cursor.statement.is_bounded`). For **bounded** statements (e.g. snapshot queries, `INFORMATION_SCHEMA` reads), `fetchall()` works as expected. For **unbounded** statements (a streaming SELECT against a streaming table), `fetchall()` is unsafe — the adapter falls back to `fetchmany(1000)` with a warning.

Implications:
- Catalog/metadata reads return complete results.
- `dbt show`, `dbt test`, and any `fetch=True` macro on a streaming model return at most a 1000-row partial snapshot. The snapshot is taken from the changelog compressor for non-append-only streams, which means you get the latest value per key (not all changelog operations).
- Tests should be designed against bounded sources where possible. See `testing.md` for mitigations.
