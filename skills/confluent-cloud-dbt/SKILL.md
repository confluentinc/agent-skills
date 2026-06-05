---
name: confluent-cloud-dbt
description: Authoring, scaffolding, or modifying dbt projects for the dbt-confluent adapter — Confluent Cloud's managed Apache Flink SQL service ONLY (not self-managed Flink, not other Flink runtimes). Triggers on existing dbt-confluent projects (`profile.type: confluent`, `dbt-confluent` in `pyproject.toml`/`requirements.txt`) and when the user wants to scaffold a new dbt-confluent project. Do NOT trigger for non-Confluent dbt adapters, plain Flink SQL outside dbt, or self-managed Flink clusters.
metadata:
  author: confluent
  version: 0.1.0
  last_updated: "2026-05-26"
compatibility:
  - dbt-confluent >= 0.2.0
  - Python >= 3.10 (adapter supports 3.10, 3.11, 3.12)
  - Confluent Cloud account with a Flink-enabled environment
---

# confluent-cloud-dbt

dbt adapter for Confluent Cloud's managed Apache Flink SQL service. About half of standard dbt assumptions don't hold here: no transactions, no `ALTER TABLE` (from dbt's point of view), no schema DDL, no `MERGE`, no atomic swaps, no incremental, no snapshots. Materializations are mostly drop-and-recreate gated by schema-drift detection, and streaming materializations submit **long-running Flink statements** — real cluster resources that the adapter lifecycle-manages.

If you're tempted to apply a Snowflake/BigQuery/Postgres pattern, stop and read this first. Deeper material (drift mechanics, statement lifecycle, profiles, streaming semantics, testing) lives in `references/`; read the relevant file on demand.

## First step: identify the workflow

Before reading models, proposing code, or asking specifics, identify which workflow the user is in. **This is the first thing you do when this skill loads.**

Detect from the working directory:

| Signal | Workflow |
|---|---|
| `dbt_project.yml` exists with profile pointing at `confluent` (existing dbt-confluent project) | **0** — ongoing work → read `workflows/0-existing-project.md` |
| Empty directory or only `.claude/` present | **1** — scaffold from scratch → read `workflows/1-scaffold.md` |
| User is asking a one-off question with no scaffolding intent (e.g. "what does `changelog.mode='upsert'` mean?") | **none** — answer using SKILL.md and the relevant `references/` file, skip the workflow files |

**Always confirm with the user via `AskUserQuestion`** even when detection is confident — pre-populate the detected option as the recommendation. Don't skip the confirmation; the user knows their context better than the heuristics, and a misroute wastes a lot of work.

Once a workflow is chosen, **read the corresponding `workflows/<name>.md` file via the Read tool and follow its instructions.** Each workflow has its own phases and stop-and-confirm gates that are not duplicated here.

If the user explicitly asks for help porting from another dbt backend or from a Terraform + shift-left-utils setup, tell them that workflow isn't shipped yet and offer to step through it manually using the rules in this file plus the files in `references/`.

## Universal safety rule

You do **not** autonomously execute commands that change the state of a Confluent Cloud environment, Kafka cluster, Flink statement set, or Terraform-managed infrastructure. You suggest these commands; the user runs them.

**OK to run autonomously (read-only — no state changes):**
- `dbt parse`, `dbt compile`, `dbt list`, `dbt debug`
- `confluent flink statement list`, `confluent flink statement describe`
- `confluent kafka topic list`, `confluent kafka cluster list`, `confluent environment list`
- `terraform plan`
- File reads, `git status`/`log`/`diff`, search tools.

**Suggest, never autonomously execute:**
- `dbt run`, `dbt run --full-refresh`, `dbt build`, `dbt seed`

**If the user explicitly asks you to run one of the suggested-only commands** ("go ahead and run `dbt run --full-refresh`"), surface what it will do (which models, expected statements created, expected topics created, ballpark resource impact) and ask for confirmation. After confirmation, proceed.

This rule is non-negotiable across all workflows.

## Top mistakes to avoid

In rough order of how often they bite. Read this before writing anything.

1. **Reaching for `incremental`.** Compiler error. The user's "incremental updates" instinct is satisfied by `streaming_table`, which Flink updates continuously by definition.
2. **Reaching for `materialized_view`.** Compiler error. Use `table` (CTAS in Confluent Flink is continuously updated) or `streaming_table` for explicit insert-driven semantics.
3. **Reaching for `snapshot` / SCD2.** No `MERGE`/`UPDATE` in Flink SQL. Direct the user to changelog streams / temporal tables instead.
4. **Putting `WITH (...)` clauses inline in the model SQL.** They go in `config(with={...})`. Same for `connector` on `streaming_source` — it's a config key, not SQL.
5. **Writing a `streaming_source` model body as `SELECT * FROM ...`.** It's a column-DDL body (column names + types + watermarks + PKs in backticks), not a SELECT. The `connector` config is mandatory.
6. **Using double quotes for identifiers.** Flink uses **backticks**. Always.
7. **Putting `NOT NULL` / `VIRTUAL` / `METADATA` in a column's `data_type` field in `models.yml`.** A custom validator raises a clear error. Constraints belong in the `constraints:` section.
8. **Suggesting `ALTER TABLE …` to fix anything.** All forms (rename, add/drop column, truncate) are unsupported from dbt's side. The answer is `--full-refresh`.
9. **Telling the user to just `dbt run` to redeploy a streaming pipeline after editing the SELECT.** Without `--full-refresh`, the materialization SKIPs (column names/types unchanged → no drift detected). See the schema-drift callout below — this is a silent failure.
10. **Treating `database` and `schema` as warehouse-style.** `database` = Confluent Environment (`env-xxxxxx`); `schema` = Kafka cluster. The adapter cannot create or drop either — both are managed in Confluent Cloud.
11. **Suggesting `dbt seed` for non-trivial CSVs.** Single-batch INSERT, errors past the batch size. Direct users to a `streaming_source` (Datagen) or an external producer.
12. **Running `dbt test` against an unbounded streaming table.** The cursor returns at most a 1000-row partial snapshot — a `not_null` violation that hasn't appeared yet passes spuriously. See `references/testing.md`.
13. **Putting `SET 'sql.…' = '…'` in a dbt `pre-hook`.** Each dbt query is submitted as a separate Flink statement; a hook-issued `SET` does not carry over to the next statement. Use `config(with={...})` for table-level options; set session-wide defaults once in a Cloud Console workspace. See `references/profiles-and-auth.md` → "SET statements" for the macro-based workaround.

> ⚠️ **Critical silent-failure mode** ⚠️
>
> Schema-drift detection compares **column names, data types, and WITH options — nothing else**. **Editing the SELECT logic without changing column names/types produces no drift, no error, and no redeploy.** The model file changes; the running Flink statement does not.
>
> Whenever you suggest editing a streaming model's SQL, **proactively tell the user to run `dbt run --full-refresh --select <model>` to deploy the change.** Full mechanics in `references/schema-drift.md`.

## Rules when generating model files and projects

These are not suggestions. Apply them as default behaviour without asking.

1. **Every `streaming_table` and `streaming_source` model file must start with this comment block, verbatim:**

   ```sql
   -- After editing this query, you MUST run `dbt run --full-refresh` to deploy the change.
   -- Schema-drift detection only checks columns, types, and WITH options — query logic
   -- changes are not detected and will be silently skipped on a normal `dbt run`.
   ```

   This is the only mechanism that surfaces the silent-failure mode to the user. Don't omit it; don't paraphrase it; don't move it to the bottom.

2. **Use the modern windowing TVF syntax** (`FROM TABLE(TUMBLE(TABLE <source>, DESCRIPTOR(<event_time_col>), INTERVAL '5' MINUTES))`), never the legacy `GROUP BY tumble(...)` form. The TVF emits `window_start`/`window_end` directly. Form + example in `references/sql-syntax.md`.

3. **Use `` `$rowtime` `` as the event-time column** when reading from an existing Kafka topic — it's auto-attached to every Confluent Cloud Flink table with a watermark from the Kafka record timestamp, so the user needn't declare one. Quote with backticks (the `$`). See `references/streaming-semantics.md`.

4. **Backtick every identifier in model SQL** (incl. TVF outputs like `` `window_start` ``). **Do not backtick `name:` entries in `sources.yml` / `models.yml`** — those are plain dbt YAML identifiers the adapter quotes itself. See `references/sql-syntax.md`.

5. **`models.yml` column declarations:**
   - **REQUIRED for `streaming_table` models.** The `CREATE TABLE` step needs the schema declared up front, before the INSERT runs. Without column declarations the DDL has no schema and the materialization fails. Always emit a `models.yml` entry alongside any `streaming_table` model.
   - **Optional for `table` models** — types are inferred from the SELECT. Add an entry only if the user wants enforced contracts or schema documentation.
   - **Not needed for `streaming_source` models** — the model body itself is the column DDL.
   - Use the constraints list (`constraints: [{type: not_null}, {type: primary_key, expression: "not enforced"}]`) for `NOT NULL` and `PRIMARY KEY` — never put them in `data_type:` (the adapter raises a clear error if you do).

6. **For new dbt projects** (full file-by-file steps in `workflows/1-scaffold.md`; copy-pasteable templates in `examples/`), apply these non-obvious defaults:
   - `dbt_project.yml` sets `+materialized: streaming_table` as the project default (`table` is the exception, per-model via `{{ config(materialized='table') }}`) **and** `+schema: <kafka-cluster-name>` (the Kafka cluster the adapter writes to — use the cluster the user confirmed, don't invent one).
   - `requirements.txt` pins `dbt-confluent` to the **current published version** — verify against PyPI (`https://pypi.org/pypi/dbt-confluent/json`), don't guess.
   - Never commit a real `profiles.yml` — ship `profiles.yml.example` and `.gitignore` it (along with `target/`, `dbt_packages/`, `logs/`, `.env`). For production, recommend a **service-account-bound** Flink API key (user-bound keys are revoked when the user leaves and long-running INSERTs stop).

7. **If the user hasn't specified the source topic name, schema, or aggregation logic, ask once before scaffolding** — don't invent topic schemas. A single `AskUserQuestion` covering topic + key columns + aggregation type is the right level of friction.

## Terminology mapping

Confluent docs, Flink docs, and dbt docs all use the same words to mean different things. Memorize this:

| dbt | Flink SQL | Confluent Cloud |
|---|---|---|
| `database` | Catalog | Environment (`env-xxxxxx`) |
| `schema` | Database | **Kafka cluster** |
| relation (table/view) | Table | Topic + key/value Schema |

**Implication for `+schema:` model configs:** the value must be the name of an existing Kafka cluster. The adapter cannot create or drop clusters; if the cluster doesn't exist the run fails with a clear `DbtDatabaseError`. There is no per-developer schema strategy here unless each developer has their own cluster.

## Materialization picker

**Choose:**

| User intent | Materialization | Note |
|---|---|---|
| Batch transformation that runs to completion | `table` | CTAS. Re-runs are no-ops (schema-drift gated). |
| Continuous streaming pipeline (a topic of derived events) | `streaming_table` | Two statements: a quick `CREATE TABLE` + a long-running `INSERT INTO ... SELECT`. |
| Connector-backed source table (e.g. Datagen for testing) | `streaming_source` | Model body is column DDL, not a SELECT. `connector` config required.[^connectors] |
| Read-only reference to an existing Kafka topic | **dbt `source`**, not a model | Topics auto-appear as Flink tables. |
| Lightweight virtual relation | `view` | Drop-and-recreate every run. |
| Inline CTE-style helper | `ephemeral` | Standard dbt behaviour. |

[^connectors]: Only the `faker` connector is exercised in the adapter's test suite today. Other connectors should work via `config(connector='...', with={...})` but aren't yet validated end-to-end against the adapter — verify before relying on them in production. Catalog: https://docs.confluent.io/cloud/current/connectors/index.html

**Don't:**

- **Don't reach for `incremental`** — compiler error. The user's mental model ("efficient updates without recompute") is the *default* in Flink streaming. Use `streaming_table`.
- **Don't reach for `materialized_view`** — compiler error. In Confluent Flink, `table` *is* a continuously-updated CTAS. Use `table`.
- **Don't reach for `snapshot`** — no `MERGE`/`UPDATE`. SCD2 needs Flink-native changelog/temporal-table patterns; out of scope for the adapter.
- **Don't assume `table` means "batch table" in the warehouse sense** — it's a CTAS that Flink keeps fresh in the background. Re-running dbt won't recompute; it SKIPs unless schema drifted or `--full-refresh` is passed.
- **Don't use `streaming_source` for read-only references to topics produced elsewhere.** Use a dbt `source` — every Kafka topic is automatically a Flink table.

## SQL syntax

The mechanical-error guardrails are in "Top mistakes to avoid" above (backticks not double quotes, `WITH`/`connector` go in `config()` not SQL, `streaming_source` body is column DDL). **For the exact forms and worked examples** — windowing TVF syntax, the `streaming_source` DDL body, `PRIMARY KEY ... NOT ENFORCED`, `NOT NULL` constraints in `models.yml`, and the don't-backtick-YAML caveat — read `references/sql-syntax.md` before writing model SQL.

## Sources from existing Kafka topics

Use a dbt `source` (not `streaming_source`) for read-only references to topics that already exist. Match the cluster as `schema` and the topic name as `identifier`; `{{ source('raw', 'pageviews') }}` then resolves as a Flink table with no adapter-side registration. Full decision rules and example in `references/sources.md`.

## Pointers

- `workflows/` — multi-phase workflows for specific user intents (read the one selected by the workflow router):
  - `0-existing-project.md` — ongoing work on an existing dbt-confluent project
  - `1-scaffold.md` — greenfield project setup
- `references/` — read the file matching the question:
  - `materializations.md` — per-materialization mechanics (`table`/`view`/`streaming_table`/`streaming_source`/`ephemeral`/`test`/`unit`/`seed`) + execution modes.
  - `schema-drift.md` — drift-detection rules and the silent-failure mode.
  - `sql-syntax.md` — exact Flink SQL forms: windowing TVF, `streaming_source` DDL body, constraints, identifier quoting.
  - `statement-lifecycle.md` — statement naming/lifecycle, custom statement names, hidden statements, streaming-cursor fetch behaviour.
  - `profiles-and-auth.md` — `profiles.yml` essentials, SET-statement caveats, service accounts / API keys.
  - `streaming-semantics.md` — `$rowtime`/changelog/watermark/PK/joins.
  - `sources.md` — sources-vs-`streaming_source` decision rules.
  - `testing.md` — testing dbt models on streams.
  - `adapter-behaviours.md` — 0.2.x bugfix notes worth surfacing.
  - `upstream-links.md` — upstream Confluent/Flink doc links.
- `examples/` — copy-pasteable starting points: `dbt_project.yml`, `profiles.yml.example`, `sources.yml`, `models.yml`, `streaming_source.sql`, `streaming_table.sql`, `table.sql`, `.gitignore`, `requirements.txt`.
