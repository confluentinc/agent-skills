# CC Flink SQL Patterns (Verified for Confluent Cloud)

All patterns use table-valued window functions (TVFs), CC-supported syntax only.

## Table of contents

- [Window Aggregations](#window-aggregations) — tumbling, hopping, session, cumulating, chained
- [Joins](#joins) — interval, temporal, External Tables + `KEY_SEARCH_AGG`
- [Deduplication](#deduplication) — keep latest, keep first
- [Top-N](#top-n) — continuous, window
- [Pattern Detection (MATCH_RECOGNIZE)](#pattern-detection-match_recognize)
- [JSON Processing](#json-processing)
- [Statement Sets (multi-insert)](#statement-sets-multi-insert)
- [Late Data Routing](#late-data-routing)
- [Conditional Aggregation](#conditional-aggregation)
- [OVER Aggregation](#over-aggregation)
- [LAG/LEAD Window Functions](#laglead-window-functions)
- [CC System Columns](#cc-system-columns)

## Window Aggregations

### Tumbling window
```sql
SELECT
  window_start, window_end,
  vehicle_id,
  COUNT(*) as reading_count,
  AVG(speed_kmh) as avg_speed,
  MAX(fuel_level) as max_fuel
FROM TABLE(
  TUMBLE(TABLE telemetry_signals, DESCRIPTOR(recorded_at), INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end, vehicle_id;
```

### Hopping window
```sql
SELECT window_start, window_end, sensor_id, AVG(temperature) as avg_temp
FROM TABLE(
  HOP(TABLE sensor_readings, DESCRIPTOR(reading_time),
      INTERVAL '5' MINUTE, INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end, sensor_id;
```

### Session window
```sql
SELECT window_start, window_end, driver_id, COUNT(*) as trip_events
FROM TABLE(
  SESSION(TABLE vehicle_trips, DESCRIPTOR(gps_time), INTERVAL '15' MINUTE)
)
GROUP BY window_start, window_end, driver_id;
```

### Cumulating window (running totals)
```sql
SELECT window_start, window_end, SUM(revenue) as cumulative_revenue
FROM TABLE(
  CUMULATE(TABLE transactions, DESCRIPTOR(transaction_time),
           INTERVAL '1' HOUR, INTERVAL '1' DAY)
)
GROUP BY window_start, window_end;
```

### Chained windows (multi-level aggregation)
```sql
-- Step 1: 1-minute pre-aggregation (use CTE, not CREATE TEMPORARY VIEW on CC — TEMPORARY is unsupported; persistent CREATE VIEW works but a CTE avoids the extra object)
INSERT INTO hourly_stats
WITH minute_stats AS (
  SELECT window_start, window_end, sensor_id,
         AVG(temperature) as avg_temp, COUNT(*) as cnt
  FROM TABLE(TUMBLE(TABLE sensor_readings, DESCRIPTOR(reading_time), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, sensor_id
)
SELECT window_start, window_end, sensor_id,
       AVG(avg_temp) as avg_temp_1h, SUM(cnt) as total_readings
FROM TABLE(TUMBLE(TABLE minute_stats, DESCRIPTOR(window_end), INTERVAL '1' HOUR))
GROUP BY window_start, window_end, sensor_id;
```

## Joins

### Interval join (time-bounded)
```sql
SELECT a.alert_id, a.triggered_at, r.resolved_at,
       TIMESTAMPDIFF(MINUTE, a.triggered_at, r.resolved_at) as resolution_minutes
FROM alerts a, resolutions r
WHERE a.alert_id = r.alert_id
  AND r.resolved_at BETWEEN a.triggered_at AND a.triggered_at + INTERVAL '2' HOUR;
```

### Temporal join (event-time, versioned table)
```sql
SELECT o.order_id, o.amount, o.currency, r.rate, o.amount * r.rate as amount_usd
FROM orders o
JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time AS r
ON o.currency = r.currency;
```

> **Avoid regular joins against upsert-kafka topics as a lookup substitute.** CC has no `PROCTIME()`, but a plain `LEFT JOIN` against a compacted/upsert-kafka topic is a streaming join over an *updating* table — it retains the entire reference table in state and doesn't scale. CC also infers upsert/retract mode from the topic itself (compaction → upsert, Debezium schema → retract), so hand-rolling this pattern is doubly unnecessary. Use External Tables + `KEY_SEARCH_AGG`, or an event-time temporal join, instead — both are append-only and efficient.

### External Tables + KEY_SEARCH_AGG (canonical CC lookup)

For per-row lookups against external databases (Postgres, MySQL, SQL Server, Oracle, REST, MongoDB, Couchbase):

```sql
-- Step 1: CREATE CONNECTION (endpoint + credentials)
CREATE CONNECTION fleet_db_conn
  WITH (
    'type' = 'confluent_jdbc',         -- underscore in CONNECTION
    'endpoint' = '<jdbc_url>',
    'username' = '<user>',
    'password' = '<pass>',
    'environment' = '<ENV_ID>'
  );

-- Step 2: CREATE TABLE referencing connection
CREATE TABLE vehicle_registry (
    vin STRING,
    make STRING,
    model STRING,
    fleet_id STRING
) WITH (
    'connector' = 'confluent-jdbc',    -- hyphen in TABLE
    'confluent-jdbc.connection' = 'fleet_db_conn',
    'confluent-jdbc.table-name' = 'vehicles'
);

-- Step 3: KEY_SEARCH_AGG + CROSS JOIN UNNEST
SELECT s.signal_id, v.make, v.model, v.fleet_id
FROM telemetry_signals s,
LATERAL TABLE(KEY_SEARCH_AGG(vehicle_registry, DESCRIPTOR(vin), s.vehicle_id))
CROSS JOIN UNNEST(search_results) AS v(vin, make, model, fleet_id);
```

Naming gotcha: connection type = underscore (`confluent_jdbc`), table connector = hyphen (`confluent-jdbc`).

KEY_SEARCH_AGG tuning options (optional 4th argument):
```sql
LATERAL TABLE(KEY_SEARCH_AGG(
  customers_ext, DESCRIPTOR(customer_id), customer_id,
  MAP['async_enabled', 'true', 'client_timeout', '30',
      'max_parallelism', '10', 'retry_count', '3']
))
```

Limitations: single-column key only, output is array (always follow with UNNEST), no cache/TTL knobs.

## Deduplication

### Keep latest
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) AS rn
  FROM events
) WHERE rn = 1;
```

### Keep first
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time ASC) AS rn
  FROM events
) WHERE rn = 1;
```

## Top-N

### Continuous Top-N (emits updates)
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn
  FROM products
) WHERE rn <= 10;
```

### Window Top-N (final, non-updating)
```sql
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY total_sales DESC) AS rn
  FROM (
    SELECT window_start, window_end, supplier_id, SUM(sales) as total_sales
    FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '5' MINUTE))
    GROUP BY window_start, window_end, supplier_id
  )
) WHERE rn <= 3;
```

## Pattern Detection (MATCH_RECOGNIZE)

CC limitations: no `PREV()`/`NEXT()` physical offsets, flat columns only, no greedy quantifiers as last pattern variable, no UDFs inside MATCH_RECOGNIZE. Within `DEFINE`, navigate using the logical offset functions `LAST(variable.field, n)`/`FIRST(variable.field, n)` — documented pattern-navigation constructs, handling the first-row NULL case.

```sql
SELECT * FROM sensor_readings
MATCH_RECOGNIZE (
  PARTITION BY vehicle_id
  ORDER BY recorded_at
  MEASURES
    FIRST(N.recorded_at) AS escalation_start,
    LAST(N.recorded_at) AS escalation_end,
    COUNT(N.temperature_c) AS spike_count,
    MAX(N.temperature_c) AS peak_temp
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (N{3,} C)
  DEFINE
    N AS N.temperature_c > 90 AND (LAST(N.temperature_c, 1) IS NULL OR N.temperature_c > LAST(N.temperature_c, 1)),
    C AS C.temperature_c > 120
);
```

## JSON Processing

```sql
-- Extract nested fields
SELECT
  JSON_VALUE(CAST(val AS STRING), '$.customer.name') as customer_name,
  JSON_VALUE(CAST(val AS STRING), '$.items[0].product_id') as first_product,
  JSON_QUERY(CAST(val AS STRING), '$.items') as all_items
FROM raw_topic;

-- Explode JSON array (CC uses CROSS JOIN UNNEST, NOT LATERAL TABLE(UNNEST(...)))
SELECT order_id, signal_str
FROM input_topic
CROSS JOIN UNNEST(
  JSON_QUERY(CAST(val AS STRING), 'lax $.signals[*]' RETURNING ARRAY<STRING>)
) AS T(signal_str);
```

## Statement Sets (multi-insert)

```sql
BEGIN STATEMENT SET;

INSERT INTO critical_alerts
SELECT * FROM sensor_readings WHERE temperature_c > 100;

INSERT INTO routine_telemetry
SELECT * FROM sensor_readings WHERE temperature_c <= 100;

END;
```

## Late Data Routing

```sql
BEGIN STATEMENT SET;

INSERT INTO on_time_signals
SELECT * FROM telemetry_signals
WHERE recorded_at >= CURRENT_WATERMARK(recorded_at);

INSERT INTO delayed_signals
SELECT * FROM telemetry_signals
WHERE recorded_at < CURRENT_WATERMARK(recorded_at);

END;
```

Note: `CURRENT_WATERMARK()` not supported on updating tables.

## Conditional Aggregation

```sql
SELECT
  window_start,
  COUNT(*) as total_orders,
  COUNT(*) FILTER (WHERE status = 'completed') as completed,
  SUM(amount) FILTER (WHERE status = 'completed') as completed_revenue
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR))
GROUP BY window_start;
```

## OVER Aggregation

CC supports multiple `OVER` windows in one query only if all window specs are identical in streaming mode; otherwise stick to a single `OVER` window per query:

```sql
SELECT
  event_id, user_id,
  COUNT(*) OVER (
    PARTITION BY user_id ORDER BY event_time
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
  ) as events_last_hour
FROM events;
```

## LAG/LEAD Window Functions

```sql
SELECT product_id, event_time, price, prev_price,
  CASE
    WHEN price > prev_price THEN 'UP'
    WHEN price < prev_price THEN 'DOWN'
    ELSE 'FLAT'
  END AS trend
FROM (
  SELECT *, LAG(price) OVER (PARTITION BY product_id ORDER BY event_time) AS prev_price
  FROM price_updates
);
```

## CC System Columns

```sql
SELECT
  id,
  `$rowtime` as event_time,            -- Kafka record timestamp (TIMESTAMP_LTZ(3))
  `$headers`['correlation-id'] as cid  -- Kafka headers (MAP<STRING, BYTES>)
FROM my_topic;
```

`$rowtime` can be aliased inside a CTE (`SELECT $rowtime AS event_time, ...`) without losing the time-attribute property — verified on a live CC compute pool. It can also be qualified with a table alias (`o.$rowtime`).
