# `profiles.yml`, SET statements, and API keys

Connection configuration, session-property caveats, and service-account guidance.

## `profiles.yml` essentials

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: confluent
      organization_id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
      environment_id: env-xxxxxx        # → exposed as dbt `database`
      dbname: my-kafka-cluster          # → exposed as dbt `schema`
      compute_pool_id: lfcp-xxxxx
      flink_api_key: xxx                # Flink-scoped API key, NOT a Kafka API key
      flink_api_secret: xxx
      cloud_provider: aws               # one of: cloud_provider+cloud_region OR endpoint
      cloud_region: us-west-2
      threads: 1
```

`environment_id` is internally aliased to `database`, and `dbname` to `schema`. You'll see those names everywhere else in dbt.

**Optional knobs:**
- `endpoint` — for private link / non-public regions, replaces `cloud_provider`+`cloud_region`.
- `execution_mode` — leave default (`streaming_query`). Don't override unless the user explicitly needs a snapshot query.
- `statement_name_prefix` — default `dbt-`. Final Flink statement name: `{prefix}{project}-{model}[-ddl]`.
- `statement_label` — default `dbt-confluent`. Consider setting it per-environment (e.g. `dbt-prod`, `dbt-staging`) if multiple environments share a compute pool, so cleanup/inspection by label can scope to one environment.

## SET statements

Confluent Flink SQL supports `SET 'key' = 'value';` to modify session properties (`sql.local-time-zone`, `sql.snapshot.mode`, `sql.state-ttl`, etc.). However:
- In Cloud Console workspaces, `SET` cannot be standalone — it must be submitted alongside another statement.
- The adapter submits each query as an independent Flink statement via the REST API. **A `SET` in a dbt `pre-hook` does not carry over to subsequent statements.**

Practical guidance for users:
- For per-table behaviour, use `config(with={...})`.
- For session-wide defaults (e.g. `sql.local-time-zone`), set them once in a Cloud Console workspace, not via dbt hooks.
- If a user truly needs to combine a `SET` with a dbt-issued statement, they'd have to embed the `SET` inside a custom macro that emits both — supported but advanced and not exercised in the adapter's test suite.

## Service accounts and API keys

Two API key flavours matter:
- **Flink API key** (used in `profiles.yml`) — authenticates Flink statement submission.
- **Kafka API key** — separate concern, used by Kafka clients and certain connectors. Not used by the adapter directly.

Bind the Flink API key to a **service account** (not a user account) for any environment where streaming statements are expected to outlive a person — i.e. any non-throwaway environment. User-bound keys are revoked when the user leaves the org and your INSERTs stop.

Creating a service account API key may require elevated RBAC roles (e.g. OrganizationAdmin granting management rights to whoever administers the SA). The exact roles change over time — direct users to the current docs rather than naming a specific role: https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/overview.html
