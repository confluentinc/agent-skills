# Flink SQL syntax (the things Claude gets mechanically wrong)

The high-level guardrails are in `SKILL.md` → "Top mistakes to avoid". This file has the worked examples and exact forms.

> Flink SQL reference: https://docs.confluent.io/cloud/current/flink/reference/queries/overview.html · [`CREATE TABLE`](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html)

## Identifiers and config vs SQL

- **Backticks for identifiers**, never double quotes: `` `order_id` ``, `` `my-cluster` ``. Backtick every identifier in model SQL — `SELECT`, `GROUP BY`, `ORDER BY`, `WHERE`, etc. — including columns emitted by TVFs (`` `window_start` ``, `` `window_end` ``). Don't rely on Flink's reserved-word list staying stable across versions; consistent quoting is cheap and future-proofs the SQL.
- **Do NOT backtick `name:` entries in `sources.yml` / `models.yml`** — those are plain dbt YAML identifiers; the adapter quotes them itself when generating SQL. Backticking them there makes dbt look for a literally backticked column name in `INFORMATION_SCHEMA`.
- **`WITH` table options come from config, not SQL.** Write `config(with={'changelog.mode': 'append'})`, not a literal `WITH (...)` clause in the model body.
- **`connector` is a config key**, not SQL: `config(connector='faker')`.

## Windowing — use the modern TVF syntax

For tumbling, hopping, and cumulative windows:

```sql
FROM TABLE(TUMBLE(TABLE <source>, DESCRIPTOR(<event_time_col>), INTERVAL '5' MINUTES))
GROUP BY window_start, window_end, ...
```

Don't use the legacy `GROUP BY tumble(<col>, ...)` form. The TVF emits `window_start`/`window_end` columns directly. Use `` `$rowtime` `` as `<event_time_col>` when reading an existing topic (see `streaming-semantics.md`). Full TVF reference: https://docs.confluent.io/cloud/current/flink/reference/queries/window-tvf.html

## `streaming_source` body is column DDL

No `CREATE TABLE` wrapper, no comma after the last entry:

```sql
{{ config(materialized='streaming_source', connector='faker',
          with={'rows-per-second': '1'}) }}
`order_id` BIGINT,
`price` DECIMAL(10, 2),
`order_time` TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
PRIMARY KEY (`order_id`) NOT ENFORCED
```

## Constraints

- **`PRIMARY KEY` syntax in Flink is `PRIMARY KEY (cols) NOT ENFORCED`** — column list *before* `NOT ENFORCED`. The adapter renders model-level constraints in the right order; column-level PKs in `models.yml` use `expression: "not enforced"`.
- **`NOT NULL` is a column constraint**, not part of `data_type`. In `models.yml`:

  ```yaml
  columns:
    - name: order_id
      data_type: bigint
      constraints:
        - type: not_null
  ```

  Putting `data_type: bigint NOT NULL` raises a clear compile-time error from the adapter.
