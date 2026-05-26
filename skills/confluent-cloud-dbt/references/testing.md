# Testing dbt models on streams

- **Data tests (`unique`, `not_null`, custom singular tests) on unbounded streaming tables are unreliable.** The cursor returns at most a partial snapshot (capped at 1000 rows for unbounded statements; warning logged). A `not_null` violation that hasn't yet appeared in the snapshot passes spuriously. Mitigations, in order of preference:
  1. Bound the source for testable models (`with={'number-of-rows': '...'}` on `streaming_source`).
  2. Use `config(limit=N)` on the test.
  3. Prefer **unit tests** with controlled fixtures.

  The `count(*)` over empty-result quirk is handled by a custom test materialization, so the test always returns exactly one row — but that row may reflect a partial snapshot. See `statement-lifecycle.md` → "Streaming-cursor fetch behaviour" for the underlying mechanism.

- **Unit tests** are supported via a custom materialization that creates real fixture tables (`CREATE TABLE LIKE original (EXCLUDING OPTIONS)` + `INSERT`) instead of CTEs (CTEs can't carry watermarks). **The tested model must already exist** in the cluster. Workflow: `dbt run --select my_model && dbt test --select unit_my_model`. If the original relation isn't present, the materialization raises a clear compile error.
