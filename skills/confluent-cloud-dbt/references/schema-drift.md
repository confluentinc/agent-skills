# Schema-drift detection rules

> ⚠️ The silent-failure mode below is the single most important thing in this skill. Read the "What's *not* detected" section carefully.

## What's detected
- **Column added** (drift error)
- **Column removed** (drift error)
- **Column renamed** (drift error — appears as add+remove)
- **Column data type changed** (drift error, e.g. `BIGINT` → `INT` or `DECIMAL(10,2)` → `DECIMAL(10,3)`)
- **WITH option value changed** (drift error)

## What's allowed (no drift)
- **Column reordering** — order doesn't matter for Kafka-backed tables.

## What's *not* detected (silent skip)
- **Query logic changes** that don't change column names/types: `WHERE` predicates, aggregation expressions, `CASE` rewrites, join conditions, etc. The drift check inspects `INFORMATION_SCHEMA`, not the originating SQL. **Tell the user to run `--full-refresh` to deploy logic-only changes.**
- **WITH options removed from config.** Connectors add default options automatically (e.g. faker adds `fields.*.expression`), and the adapter can't distinguish user-set from auto-generated. To remove a configured option, `--full-refresh`.

## How drift detection works
For `table` and `streaming_table`: a temporary table `__dbt_tmp_schema_check_<model>_<invocation_id>` is created from the model's SELECT (`CREATE TABLE ... AS SELECT ... WHERE FALSE`), its columns queried from `INFORMATION_SCHEMA.COLUMNS`, then dropped. For `streaming_source`: same temp-table approach but built from the column-definition body (without the connector). Comparison is against the existing table's columns from `INFORMATION_SCHEMA.COLUMNS`.

## `on_schema_drift` config
- `'fail'` (default) — raise CompilationError on drift.
- `'ignore'` — skip the drift check entirely; always SKIP if the table exists.
- Anything else — clear error: `Invalid value for on_schema_drift ('...'). Expected 'ignore' or 'fail'.`
