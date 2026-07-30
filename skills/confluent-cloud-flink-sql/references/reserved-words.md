# CC Flink SQL — Reserved Words

## Must-backquote list

These identifiers MUST be enclosed in backticks everywhere they appear — column definitions, ROW type aliases, SELECT lists, CREATE TABLE, aliases:

`timestamp`, `value`, `time`, `payload`, `name`, `key`, `offset`, `partition`, `row`, `table`, `order`, `group`, `select`, `from`, `where`, `having`, `join`, `on`, `as`, `set`, `type`, `data`, `start`, `end`, `interval`, `date`, `year`, `month`, `day`, `hour`, `minute`, `second`

## Examples

```sql
-- WORKS: backquoted everywhere
SELECT
    CAST(ROW(...) AS ROW<
        `containerId`     STRING,
        `timestamp`       STRING,
        `value`           STRING
    >) AS `payload`,
    JSON_VALUE(...) AS `time`
FROM input;

-- FAILS: "SQL parse failed. Encountered \"timestamp\" at line N, column M"
ROW<containerId STRING, timestamp STRING>
```

## Diagnostic hint

When `confluent flink statement describe` shows:
```
Status Detail: SQL parse failed. Encountered "timestamp" at line 36, column 9.
               Was expecting one of: <BACK_QUOTED_IDENTIFIER> ...
```

The `Was expecting one of: <BACK_QUOTED_IDENTIFIER>` means the token IS a reserved word — add backticks.

## Where backticks are NOT needed

- JSON path arguments inside `JSON_VALUE`/`JSON_QUERY` — these are string literals:
  ```sql
  JSON_VALUE(val, '$.timestamp')    -- fine, it's a string literal
  JSON_VALUE(val, '$.payload.value') -- fine
  ```

- System columns use `$` prefix:
  ```sql
  SELECT `$rowtime` FROM my_table;  -- backtick the whole thing including $
  ```

## Full reserved word list

Reference: https://docs.confluent.io/cloud/current/flink/reference/keywords.html
