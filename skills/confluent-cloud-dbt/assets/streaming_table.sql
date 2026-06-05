-- After editing this query, you MUST run `dbt run --full-refresh` to deploy the change.
-- Schema-drift detection only checks columns, types, and WITH options — query logic
-- changes are not detected and will be silently skipped on a normal `dbt run`.
--
-- Per-customer order count and spend in 5-minute tumbling windows.
--
-- The DDL step requires column declarations — see the matching entry in models.yml.
-- Windowing uses the modern table-valued function form: TABLE(TUMBLE(...)).
-- Event time is `$rowtime`, the auto-attached watermarked timestamp on every
-- Confluent Cloud Flink table — no need for the source to declare its own.

{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}

select
  `window_start`,
  `window_end`,
  `customer_id`,
  count(*)      as `order_count`,
  sum(`price`)  as `total_spend`
from table(
  tumble(
    table {{ source('orders_raw', 'orders') }},
    descriptor(`$rowtime`),
    interval '5' minutes
  )
)
group by `window_start`, `window_end`, `customer_id`
