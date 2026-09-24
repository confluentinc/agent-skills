-- Batch-style CTAS. The Flink statement runs to completion (does NOT stay
-- running like a streaming_table). Re-running dbt is a no-op unless drift is
-- detected or --full-refresh is passed.
--
-- Note: this is functionally close to a "materialized view" in other warehouses.
-- Confluent Flink continuously updates the underlying table — there is no
-- separate `materialized_view` materialization in this adapter.
--
-- Column declarations in `models.yml` are OPTIONAL for `table` (types are
-- inferred from the SELECT). The matching entry in models.yml here is for
-- documentation only — you can omit it without breaking the build.

{{ config(materialized='table') }}

select
  `customer_id`,
  count(*)      as `lifetime_order_count`,
  sum(`price`)  as `lifetime_spend`
from {{ source('orders_raw', 'orders') }}
group by `customer_id`
