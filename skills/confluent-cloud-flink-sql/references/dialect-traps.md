# Confluent Cloud vs Apache Flink SQL — Dialect Traps

Single source of truth. Update here; per-project CLAUDE.md references this file.

Reviewed by a Flink PMC / CC-for-Apache-Flink SME via live reproducers on a real CC compute pool. Two originally-listed traps were refuted by that testing and have been removed (see history below); do not re-add them without a live repro.

## CC-vs-OSS dialect traps (22)

These behave differently between Apache Flink OSS and Confluent Cloud Flink SQL.

| # | Apache Flink SQL (OSS) | Confluent Cloud Flink SQL | Severity | Source |
|---|---|---|---|---|
| 1 | `CREATE CATALOG ...` | Catalog = env, database = cluster; not creatable | HIGH | CC UDF project |
| 2 | `SET 'execution.checkpointing.interval'` | CC manages checkpointing; not user-settable | HIGH | CC UDF project |
| 3 | `CREATE TABLE ... WITH ('connector' = 'kafka', ...)` | Tables auto-mapped from Kafka topics; manual DDL limited. Auto-mapped columns default to `key`/`val` BINARY unless a schema is registered under a matching TopicNameStrategy subject | HIGH | CC Flink project |
| 4 | `CREATE TEMPORARY VIEW v AS (...)` | `TEMPORARY` not supported — use persistent `CREATE VIEW` or a CTE (`WITH v AS (...)`) | MEDIUM | CC Flink project |
| 5 | `t.$rowtime` (qualified system column) | CC exposes `$rowtime` as a built-in event-time system column on auto-mapped tables. It may be referenced unqualified or qualified with a table alias (e.g. `o.$rowtime`) — both work (verified live) | LOW | CC Flink project |
| 6 | `ALTER TABLE t ADD WATERMARK ...` | Uses `MODIFY`, not `ADD`, for watermarks — and also for other one-time schema recipes: overriding key/value types on schemaless topics (`` ALTER TABLE t MODIFY (`key` STRING, `val` STRING) ``), setting schema context (`ALTER TABLE t SET ('value.format.schema-context' = ...)`), and adding metadata columns | MEDIUM | CC Flink project |
| 7 | `SOURCE_WATERMARK()` | Default watermark strategy on CC; emits every 200ms. No special event count threshold | LOW | CC docs |
| 8 | UDF `open(RuntimeContext)` with FS/socket access | CC sandbox — no filesystem, no sockets without `CONNECTION` + `USING CONNECTIONS` | HIGH | CC UDF project |
| 9 | `GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)` | `TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE)` (table-valued functions required) | HIGH | CC UDF project |
| 10 | `CREATE FUNCTION f AS '...'` | Must include `USING JAR 'confluent-artifact://<id>'` + `USING CONNECTIONS` for egress | HIGH | CC UDF project |
| 11 | `'value.format' = 'json'` | NOT supported. Use `'json-registry'` (or `avro-registry`, `proto-registry`, `raw`). CC supports 7 formats only. Rough OSS equivalents: `avro-registry` ↔ OSS `flink-avro`/`flink-sql-avro`, `json-registry` ↔ OSS `json`, `proto-registry` ↔ OSS `flink-protobuf` (OSS has ~no pure-SQL Protobuf support) | HIGH | CC Flink project |
| 12 | `debezium-json` (OSS naming) | CC uses `json-debezium-registry` (format-first naming: `<format>-debezium-registry`). CC also supports Debezium in Protobuf (`proto-debezium-registry`), which OSS does not offer as a built-in format | MEDIUM | CC Flink project |
| 13 | Savepoints, `STOP WITH SAVEPOINT` | Not exposed on CC; statement deletion = state loss; use `prevent_destroy` in TF | CRITICAL | CC Flink project |
| 14 | Savepoint-based statement upgrade | NOT available on CC — savepoints internal-only; cannot transfer state to different statement | CRITICAL | CC Flink project |
| 15 | DataStream API | Not supported on CC. The replacement for DataStream/`ProcessFunction` is a ProcessTableFunction (PTF) in the Table API, invocable from SQL. Table API + PTF are GA in **Java**; the Python Table API is **Open Preview** and has no PTF yet — don't claim Python parity | HIGH | CC docs |
| 16 | `PROCTIME()` function | Not supported on CC. Use External Tables + `KEY_SEARCH_AGG` (canonical) or an event-time temporal join (`FOR SYSTEM_TIME AS OF`). Do **not** emulate a lookup join with a regular join against an upsert-kafka topic — it keeps the whole reference table in state and is inefficient; CC also infers upsert/retract mode from the topic itself (compaction → upsert, Debezium schema → retract) | HIGH | CC docs / community |
| 17 | `SET 'sql.state-ttl' = '<ms>'` | Also supported on CC as a `SET` statement (default 0 ms = disabled). `--property sql.state-ttl=<ms>` on `flink statement create` is an *additional* path — useful for setting the value before the statement starts — not the only one | LOW | CLI experience |
| 18 | `--sql-file` flag on `flink statement create` | Does not exist. Read file, pass via `--sql "$(cat file.sql)"` | HIGH | CLI experience |
| 19 | `CREATE DATABASE` | Not supported on CC | MEDIUM | CC docs / community |
| 20 | Aggregate UDFs (UDAF), table aggregate functions | UDAF and table aggregates not supported. Scalar UDFs and table functions (UDTF, Java only) ARE supported | HIGH | CC docs |
| 21 | `CREATE TEMPORARY FUNCTION` | Not supported on CC | MEDIUM | CC docs / community |
| 22 | `DROP TABLE` / `CREATE TABLE` are pure metadata operations | `DROP TABLE` deletes the **physical Kafka topic and its data**. `CREATE TABLE` creates the **physical topic and its Schema Registry schemas**. Never run `DROP TABLE` without confirming the topic is disposable | CRITICAL | CC docs / community |

## General Flink SQL behavior (not CC-specific)

These are accurate but identical between Apache Flink OSS and Confluent Cloud — don't cite them as CC-vs-OSS dialect differences, but they're common footguns worth knowing about while writing CC Flink SQL.

| Behavior | Detail | Source |
|---|---|---|
| `WITH cte AS (...) INSERT INTO ...` | Flink requires `INSERT INTO ... WITH cte AS (...) SELECT ...` — the CTE comes AFTER `INSERT INTO`, in both OSS and CC | CC Flink project |
| `INSERT INTO ... VALUES` | Materializes as a bounded source in any Flink dialect; test use only, not production | CC UDF project |
| Multiple `OVER` windows in one query | Supported only if all window specs are identical in streaming mode, in both OSS and CC | CC docs |
| `MATCH_RECOGNIZE` with `PREV()`/`NEXT()` offsets | No physical offsets, flat columns only, no greedy quantifiers as last pattern — a Calcite-level restriction shared by OSS and CC | CC Flink project |
| `CAST('2024-01-01T00:00:00Z' AS TIMESTAMP)` | ISO-8601 fails in any Flink dialect — use `TO_TIMESTAMP(REPLACE(...))` | CC Flink project |
| `UNIX_TIMESTAMP(ts_column)` on TIMESTAMP | Only accepts a STRING argument in any Flink dialect — cast first | CC Flink project |
| `current_watermark()` on updating table | Not supported in any Flink dialect — non-deterministic function with update messages error | CC docs |
| `CURRENT_TIMESTAMP` in update-producing queries | Rejected as non-deterministic in any Flink dialect | CC docs / community |
| `LATERAL TABLE(UNNEST(...))` | Not valid Flink SQL syntax in *either* dialect — it always fails to parse. Use `CROSS JOIN UNNEST(...)` to explode arrays instead. Plain `LATERAL (subquery)` (no `TABLE`/`UNNEST` wrapper) works fine on CC | CC Flink project |

## Removed traps (refuted by live testing — do not re-add)

- ~~`t.$rowtime` must never be qualified~~ — refuted. Qualifying `$rowtime` with a table alias works fine on CC (see row 5 above for the corrected entry).
- ~~Aliasing `$rowtime AS x` in a CTE silently strips the time-attribute property~~ — refuted. `WITH t AS (SELECT $rowtime AS event_time, * FROM ...) SELECT ... FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(event_time), ...))` runs fine on a live CC compute pool; the time attribute survives the alias.
- ~~`SESSION_START(ts)` with one argument requires two arguments~~ — obsolete now that TVF-only windowing is required; the legacy scalar form doesn't come up in CC Flink SQL at all.

## Severity guide

- **CRITICAL**: Silent data loss or incorrect results with no error message
- **HIGH**: Hard error, but wasted time debugging non-obvious cause
- **MEDIUM**: Error message hints at fix, minor time cost
- **LOW**: Cosmetic, informational, or test-only impact
