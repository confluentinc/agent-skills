# Rules when generating model files and projects

Read this before creating or editing any model file or scaffolding a project. These are not suggestions — apply them as default behaviour without asking. The silent-failure mode rule 1 guards against is the ⚠️ callout in `SKILL.md` → "Top mistakes to avoid".

1. **Every `streaming_table` and `streaming_source` model file must start with this comment block, verbatim:**

   ```sql
   -- After editing this query, you MUST run `dbt run --full-refresh` to deploy the change.
   -- Schema-drift detection only checks columns, types, and WITH options — query logic
   -- changes are not detected and will be silently skipped on a normal `dbt run`.
   ```

   This is the only mechanism that surfaces the silent-failure mode to the user. Don't omit it; don't paraphrase it; don't move it to the bottom.

2. **Use the modern windowing TVF syntax** (`FROM TABLE(TUMBLE(TABLE <source>, DESCRIPTOR(<event_time_col>), INTERVAL '5' MINUTES))`), never the legacy `GROUP BY tumble(...)` form. The TVF emits `window_start`/`window_end` directly. Form + example in `sql-syntax.md`.

3. **Use `` `$rowtime` `` as the event-time column** when reading from an existing Kafka topic — it's auto-attached to every Confluent Cloud Flink table with a watermark from the Kafka record timestamp, so the user needn't declare one. Quote with backticks (the `$`). See `streaming-semantics.md`.

4. **Backtick every identifier in model SQL** (incl. TVF outputs like `` `window_start` ``). **Do not backtick `name:` entries in `sources.yml` / `models.yml`** — those are plain dbt YAML identifiers the adapter quotes itself. See `sql-syntax.md`.

5. **`models.yml` column declarations:**
   - **REQUIRED for `streaming_table` models.** The `CREATE TABLE` step needs the schema declared up front, before the INSERT runs. Without column declarations the DDL has no schema and the materialization fails. Always emit a `models.yml` entry alongside any `streaming_table` model.
   - **Optional for `table` models** — types are inferred from the SELECT. Add an entry only if the user wants enforced contracts or schema documentation.
   - **Not needed for `streaming_source` models** — the model body itself is the column DDL.
   - Use the constraints list (`constraints: [{type: not_null}, {type: primary_key, expression: "not enforced"}]`) for `NOT NULL` and `PRIMARY KEY` — never put them in `data_type:` (the adapter raises a clear error if you do).

6. **For new dbt projects** (full file-by-file steps in `workflow-scaffold.md`; copy-pasteable templates in `../assets/`), apply these non-obvious defaults:
   - `dbt_project.yml` sets `+materialized: streaming_table` as the project default (`table` is the exception, per-model via `{{ config(materialized='table') }}`) **and** `+schema: <kafka-cluster-name>` (the Kafka cluster the adapter writes to — use the cluster the user confirmed, don't invent one).
   - `requirements.txt` pins `dbt-confluent` to the **current published version** — verify against PyPI (`https://pypi.org/pypi/dbt-confluent/json`), don't guess.
   - Never commit a real `profiles.yml` — ship `profiles.yml.example` and `.gitignore` it (along with `target/`, `dbt_packages/`, `logs/`, `.env`). For production, recommend a **service-account-bound** Flink API key (user-bound keys are revoked when the user leaves and long-running INSERTs stop).

7. **If the user hasn't specified the source topic name, schema, or aggregation logic, ask once before scaffolding** — don't invent topic schemas. A single `AskUserQuestion` covering topic + key columns + aggregation type is the right level of friction.
