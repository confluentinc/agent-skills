# Workflow 0: existing dbt-confluent project

You're in a project that already has a working dbt-confluent setup. The user is doing ongoing work — adding or modifying models, checking the health of running statements, debugging failures, or asking about adapter behaviour.

Read the universal rules in `SKILL.md` first if you haven't already. They apply here as everywhere — including the **universal safety rule** (no autonomous state-changing commands).

## What's different from scaffolding

- **Don't scaffold or generate boilerplate.** The project structure already exists. Read what's there before proposing changes.
- **Use the existing project's conventions.** If models live in `models/staging/` and `models/marts/`, put new models there too. Don't reorganize without asking.
- **Read `dbt_project.yml`, `profiles.yml.example` (if present), and any existing `sources.yml` / `models.yml` files** before suggesting anything. The user's column-naming conventions, `with={...}` defaults, `statement_name` overrides, and `statement_label` are signals you should follow.

## Adding a new model

For any new model, follow `references/authoring-rules.md` — including the mandatory warning comment block at the top of every streaming model.

Reminder of the column-declaration rule:
- `streaming_table` → also generate the matching `models.yml` entry (column types + constraints — REQUIRED, not optional)
- `streaming_source` → only the `connector` config and column DDL body; no `models.yml` needed
- `table` → `models.yml` is optional; add it only for documentation / contracts

## Modifying an existing streaming model

This is the highest-stakes operation in workflow 0 because of the silent-failure mode in schema-drift detection.

- **If the change touches column names or types or `with={...}` options:** the next `dbt run` fails loudly with a drift error. The user knows what to do.
- **If the change touches only the SELECT logic** (`WHERE`, aggregations, `JOIN` conditions, `CASE` expressions, etc.): there will be NO drift error and `dbt run` will SKIP. The change won't deploy. **You must surface this to the user explicitly every time you suggest a logic-only edit.** Don't bury it; lead with it.

  Suggested deploy command (per universal safety rule, **don't run it autonomously**):

  ```bash
  dbt run --full-refresh --select <model>
  ```

## Health-checking live statements

The deterministic statement naming pattern is `{statement_name_prefix}{project_name}-{model_name}[-ddl]`. Default prefix: `dbt-`. Example: for a model `orders_per_5m` in project `streaming_demo`, the statement names are:
- `dbt-streaming_demo-orders_per_5m` — the long-running INSERT
- `dbt-streaming_demo-orders_per_5m-ddl` — the quick CREATE TABLE (usually completes and disappears from the active list)

Read the actual `statement_name_prefix` and `statement_label` from the project's `profiles.yml` / `profiles.yml.example` before suggesting commands — don't assume defaults.

Read-only commands you can suggest the user run:

```bash
# List all dbt-managed statements in this environment / compute pool
confluent flink statement list \
  --environment env-xxxxxx \
  --compute-pool lfcp-xxxxx \
  --label dbt-confluent  # or whatever statement_label is set in profiles.yml

# Detail on one specific statement
confluent flink statement describe dbt-<project>-<model> \
  --environment env-xxxxxx \
  --compute-pool lfcp-xxxxx

# Cluster-side topic check
confluent kafka topic list --cluster lkc-xxxxx
```

In the Cloud UI, equivalent: **Environment → Flink → Statements** (filter by label or name) and **Cluster → Topics**.

Common statement states the user might report:

| State | Meaning / action |
|---|---|
| `RUNNING` | Happy path for a streaming INSERT. |
| `COMPLETED` | Fine for snapshot/CTAS, problem for streaming (means the INSERT exited). Check upstream watermarks and source bounds. |
| `FAILED` | Click the statement in the UI → "Errors" tab. Common causes: schema mismatch with downstream consumers, watermark/event-time issues on upstream sources, compute pool exhausted, expired API key. |
| `STOPPED` | Manually stopped via the UI or `confluent flink statement stop`. (Note: `delete` removes the statement entirely — it does not produce STOPPED.) Recovery: `dbt run --full-refresh --select <model>` (state-changing — let the user run it). |
| `PENDING` / `STARTING` | Give it a minute; if it's been there >5 min, surface to the user. |

## Debugging a failed run

When the user reports a failure, before suggesting fixes:

1. Ask for the actual error output. It often differs between `dbt run`'s console output and the Flink statement's "Errors" panel in the UI — get both if the user has them.
2. Read the relevant model file and its `models.yml` entry.
3. Check the compiled SQL in `target/run/<...>.sql` if the user has run `dbt compile` or `dbt run` recently.

Common failure modes:

- **Schema-drift error after edit** → ask whether the user wants to recreate (full-refresh) or revert the edit.
- **`No watermark for column X`** → the source topic doesn't have `$rowtime` exposed (rare, but possible for non-Confluent-native sources) or the WATERMARK was on a column that doesn't exist. Check the source.
- **Compute pool exhausted** → suggest scaling the compute pool in the UI; not a code fix.
- **Connection error / 401 / 403** → likely API key expired or doesn't have RBAC for the cluster. Suggest verifying with `dbt debug` (read-only).

## Porting requests

If the user asks for help porting from another dbt backend (Snowflake/BigQuery/Postgres/etc.) or from a Terraform + shift-left-utils Flink setup, tell them those workflows aren't shipped yet and offer to step through it manually using the rules in `SKILL.md` plus the files in `references/`. Stay in workflow 0 for the rest of the session.
