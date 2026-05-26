# Known adapter behaviours (0.2.x bugfixes worth surfacing)

These are recent fixes that change the error a user might see — surface them proactively if a user reports the matching symptom.

- **HTTP read timeout on cold `INFORMATION_SCHEMA` lookups.** Before 0.2.1 the default `confluent-sql` HTTP timeout was 5s, which could surface the unified drift-catalog `UNION ALL` (`INFORMATION_SCHEMA.COLUMNS` + `TABLES` + `TABLE_OPTIONS`) as `read operation timed out` on the first run after a cold cluster. The default is now 60s. If you see this on an older pin, recommend upgrading.
- **DELETE-on-missing Flink statement under compute-pool-scoped FlinkDeveloper role.** Confluent returns `403` (not `404`) when the role can't see the statement. The adapter now **warns rather than errors** on a 403 from statement DELETE, treating it as "already gone." If a user is on an older version and `dbt run` fails with a 403 during cleanup, the fix is upgrading the adapter, not granting broader RBAC.
- **`409` name-conflict on CREATE after async teardown.** Statement DELETE returns synchronously but tear-down completes asynchronously, so a fast re-`CREATE` could collide. The adapter now retries `CREATE` on a `409`. Same guidance — upgrade if seen on older versions.
