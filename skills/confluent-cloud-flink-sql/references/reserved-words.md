# CC Flink SQL — Reserved Words

## Must-backquote list (reserved keywords)

These are reserved keywords per the [CC Flink SQL keyword reference](https://docs.confluent.io/cloud/current/flink/reference/keywords.md) and MUST be enclosed in backticks everywhere they appear as identifiers — column definitions, ROW type aliases, SELECT lists, CREATE TABLE, aliases:

`timestamp`, `value`, `time`, `offset`, `partition`, `row`, `table`, `order`, `group`, `select`, `from`, `where`, `having`, `join`, `on`, `as`, `set`, `start`, `end`, `interval`, `date`, `year`, `month`, `day`, `hour`, `minute`, `second`

## Commonly backquoted but NOT reserved

`payload`, `name`, `key`, `type`, and `data` are nonreserved on CC — they parse fine unquoted. They show up backquoted in examples throughout this skill purely as a defensive habit (backquoting a nonreserved word is always safe), not because CC rejects them bare.

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

Reference: https://docs.confluent.io/cloud/current/flink/reference/keywords.md
