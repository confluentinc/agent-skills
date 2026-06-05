-- After editing this query, you MUST run `dbt run --full-refresh` to deploy the change.
-- Schema-drift detection only checks columns, types, and WITH options — query logic
-- changes are not detected and will be silently skipped on a normal `dbt run`.
--
-- Synthetic streaming source for testing and development. The adapter creates the
-- underlying table with the configured connector.
--
-- The model body is COLUMN DDL (not a SELECT). The `connector` config is mandatory.
-- Backticks are required for all identifiers.
--
-- For READ-ONLY references to topics that already exist in your cluster, do NOT use
-- this materialization — declare a dbt `source` instead (see the sources.yml example).

{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
      'rows-per-second': '5',
      'number-of-rows': '1000',     -- bound the source so dbt tests are deterministic
      'changelog.mode': 'append',
    }
) }}

`order_id` BIGINT,
`customer_id` BIGINT,
`price` DECIMAL(10, 2),
`order_time` TIMESTAMP(3),

WATERMARK FOR `order_time` AS `order_time` - INTERVAL '5' SECOND,
PRIMARY KEY (`order_id`) NOT ENFORCED
