# Confluent Cloud vs Apache Flink SQL — Dialect Traps

Single source of truth. Update here; per-project CLAUDE.md references this file.

## Trap table

| # | Apache Flink SQL (OSS) | Confluent Cloud Flink SQL | Severity | Source |
|---|---|---|---|---|
| 1 | `CREATE CATALOG ...` | Catalog = env, database = cluster; not creatable | HIGH | CC UDF project |
| 2 | `SET 'execution.checkpointing.interval'` | CC manages checkpointing; not user-settable | HIGH | CC UDF project |
| 3 | `CREATE TABLE ... WITH ('connector' = 'kafka', ...)` | Tables auto-mapped from Kafka topics; manual DDL limited | HIGH | CC Flink project |
| 4 | `WITH cte AS (...) INSERT INTO ...` | `INSERT INTO ... WITH cte AS (...)` — CTE comes AFTER INSERT INTO (empirical) | HIGH | CC Flink project |
| 5 | `CREATE TEMPORARY VIEW v AS (...)` | `TEMPORARY` not supported — use persistent `CREATE VIEW` or CTE (`WITH v AS (...)`) | MEDIUM | CC Flink project |
| 6 | `INSERT INTO ... VALUES` | Materializes; test use only, not production | LOW | CC UDF project |
| 7 | `t.$rowtime` (qualified system column) | Custom event-time columns with explicit watermarks ARE supported. For auto-mapped tables, use `$rowtime`; never qualify as `t.$rowtime` | MEDIUM | CC Flink project |
| 8 | `$rowtime AS alias` in CTE | Silently strips time-attribute property — downstream watermark breaks (empirical) | CRITICAL | CC Flink project |
| 9 | `ALTER TABLE t ADD WATERMARK ...` | Uses `MODIFY` not `ADD` for watermarks | MEDIUM | CC Flink project |
| 10 | `SOURCE_WATERMARK()` | Default watermark strategy on CC; emits every 200ms. No special event count threshold | LOW | CC docs |
| 11 | Multiple `OVER` windows in one query | Supported only if all window specs are identical in streaming mode | MEDIUM | CC docs |
| 12 | `MATCH_RECOGNIZE` with `PREV()`/`NEXT()` offsets | No physical offsets, flat columns only, no greedy quantifiers as last pattern | HIGH | CC Flink project |
| 13 | `CAST('2024-01-01T00:00:00Z' AS TIMESTAMP)` | ISO-8601 fails — use `TO_TIMESTAMP(REPLACE(...))` | MEDIUM | CC Flink project |
| 14 | `UNIX_TIMESTAMP(ts_column)` on TIMESTAMP | Only accepts STRING argument — cast first | MEDIUM | CC Flink project |
| 15 | `SESSION_START(ts)` with one arg | Requires two arguments | LOW | CC Flink project |
| 16 | `current_watermark()` on updating table | Not supported — non-deterministic function with update messages error | HIGH | CC docs |
| 17 | UDF `open(RuntimeContext)` with FS/socket access | CC sandbox — no filesystem, no sockets without `CONNECTION` + `USING CONNECTIONS` | HIGH | CC UDF project |
| 18 | `GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)` | `TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE)` (table-valued functions required) | HIGH | CC UDF project |
| 19 | `CREATE FUNCTION f AS '...'` | Must include `USING JAR 'confluent-artifact://<id>'` + `USING CONNECTIONS` for egress | HIGH | CC UDF project |
| 20 | `'value.format' = 'json'` | NOT supported. Use `'json-registry'` (or `avro-registry`, `proto-registry`, `raw`). CC supports 7 formats only | HIGH | CC Flink project |
| 21 | `debezium-json` (OSS naming) | CC uses `json-debezium-registry` (format-first naming) | MEDIUM | CC Flink project |
| 22 | Savepoints, `STOP WITH SAVEPOINT` | Not exposed on CC; statement deletion = state loss; use `prevent_destroy` in TF | CRITICAL | CC Flink project |
| 23 | Savepoint-based statement upgrade | NOT available on CC — savepoints internal-only; cannot transfer state to different statement | CRITICAL | CC Flink project |
| 24 | DataStream API | Not supported. Table API (Java/Python) IS supported; DataStream is not | HIGH | CC docs |
| 25 | `LATERAL TABLE(UNNEST(...))` | Parse error on CC. Use `CROSS JOIN UNNEST(...)` | HIGH | CC Flink project |
| 26 | `PROCTIME()` function | Not supported on CC. Use External Tables/KEY_SEARCH_AGG, upsert-kafka join, or event-time temporal join | HIGH | CC docs / community |
| 27 | `CURRENT_TIMESTAMP` in update-producing queries | Rejected as non-deterministic | MEDIUM | CC docs / community |
| 28 | `SET 'sql.state-ttl' = '...'` as separate statement | Not allowed. Pass via `--property sql.state-ttl=ms` | MEDIUM | CLI experience |
| 29 | `--sql-file` flag on `flink statement create` | Does not exist. Read file, pass via `--sql "$(cat file.sql)"` | HIGH | CLI experience |
| 30 | `CREATE DATABASE` | Not supported on CC | MEDIUM | CC docs / community |
| 31 | Aggregate UDFs (UDAF), table aggregate functions | UDAF and table aggregates not supported. Scalar UDFs and table functions (UDTF, Java only) ARE supported | HIGH | CC docs |
| 32 | `CREATE TEMPORARY FUNCTION` | Not supported on CC | MEDIUM | CC docs / community |

## How to update

1. Hit a new trap → add row here
2. Run `cc-flink-sql` skill in affected project → skill loads this file
3. Periodically upstream to per-project CLAUDE.md shareable block

## Severity guide

- **CRITICAL**: Silent data loss or incorrect results with no error message
- **HIGH**: Hard error, but wasted time debugging non-obvious cause
- **MEDIUM**: Error message hints at fix, minor time cost
- **LOW**: Cosmetic or test-only impact
