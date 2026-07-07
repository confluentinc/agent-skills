---
name: confluent-cloud-dbt
description: "Authoring, scaffolding, or modifying dbt projects for the dbt-confluent adapter — Confluent Cloud's managed Apache Flink SQL service ONLY (not self-managed Flink, not other Flink runtimes). Triggers on existing dbt-confluent projects (`profile.type: confluent`, `dbt-confluent` in `pyproject.toml`/`requirements.txt`) and when the user wants to scaffold a new dbt-confluent project. Do NOT trigger for non-Confluent dbt adapters, plain Flink SQL outside dbt, self-managed Flink clusters, or CDC/Tableflow pipelines (database → Iceberg/Delta) — that's the confluent-cloud-cdc-tableflow skill."
---

# confluent-cloud-dbt

dbt adapter for Confluent Cloud's managed Apache Flink SQL service. About half of standard dbt assumptions don't hold here: no transactions, no `ALTER TABLE` (from dbt's point of view), no schema DDL, no `MERGE`, no atomic swaps, no incremental, no snapshots. Materializations are mostly drop-and-recreate gated by schema-drift detection, and streaming materializations submit **long-running Flink statements** — real cluster resources that the adapter lifecycle-manages.

If you're tempted to apply a Snowflake/BigQuery/Postgres pattern, stop and read this first. Deeper material (drift mechanics, statement lifecycle, profiles, streaming semantics, testing) lives in `references/`; read the relevant file on demand.

## First step: identify the workflow

Before reading models, proposing code, or asking specifics, identify which workflow the user is in. **This is the first thing you do when this skill loads.**

Detect from the working directory:

| Signal | Workflow |
|---|---|
| Repo-local dbt-confluent markers: `dbt-confluent` in `requirements.txt`/`pyproject.toml`, and/or a `dbt_project.yml` alongside a checked-in `profiles.yml`(`.example`) with `type: confluent` (existing dbt-confluent project) | **0** — ongoing work → read `references/workflow-existing-project.md` |
| Empty directory or only `.claude/` present | **1** — scaffold from scratch → read `references/workflow-scaffold.md` |
| User is asking a one-off question with no scaffolding intent (e.g. "what does `changelog.mode='upsert'` mean?") | **none** — answer using SKILL.md and the relevant `references/` file, skip the workflow files |

**For workflows 0 and 1, confirm the detected workflow with the user via `AskUserQuestion`** before proceeding — pre-populate the detected option as the recommendation. Don't skip the confirmation; the user knows their context better than the heuristics, and a misroute wastes a lot of work. **Skip the confirmation for one-off questions (the `none` path)** — answer directly; making someone confirm a "workflow" just to get a definition is needless friction.

Once a workflow is chosen, **read the corresponding `references/workflow-*.md` file via the Read tool and follow its instructions.** Each workflow has its own phases and stop-and-confirm gates that are not duplicated here.

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

## Generating model files and projects

**Before creating or editing any model file, or scaffolding a project, read `references/authoring-rules.md`** and apply it as default behaviour without asking. It carries the non-negotiables: the mandatory verbatim `--full-refresh` warning block on every streaming model (the silent-failure guard — see the ⚠️ callout above), `models.yml` column declarations (REQUIRED for `streaming_table`), identifier/SQL rules, and project-scaffolding defaults.

## Terminology mapping

Confluent docs, Flink docs, and dbt docs all use the same words to mean different things. Memorize this:

| dbt | Flink SQL | Confluent Cloud |
|---|---|---|
| `database` | Catalog | Environment (`env-xxxxxx`) |
| `schema` | Database | **Kafka cluster** |
| relation (table/view) | Table | Topic + key/value Schema |

**Implication for `+schema:` model configs:** the value must be the name of an existing Kafka cluster. The adapter cannot create or drop clusters; if the cluster doesn't exist the run fails with a clear `DbtDatabaseError`. There is no per-developer schema strategy here unless each developer has their own cluster.

## Materialization picker

The default for a new model is `streaming_table`. **When choosing a materialization, read `references/materializations.md`** for the intent→materialization table, the unsupported list, and per-materialization mechanics. The high-frequency traps (`incremental`, `materialized_view`, `snapshot` all compile-error) are in "Top mistakes" above.

## SQL syntax

The mechanical-error guardrails are in "Top mistakes to avoid" above (backticks not double quotes, `WITH`/`connector` go in `config()` not SQL, `streaming_source` body is column DDL). **For the exact forms and worked examples** — windowing TVF syntax, the `streaming_source` DDL body, `PRIMARY KEY ... NOT ENFORCED`, `NOT NULL` constraints in `models.yml`, and the don't-backtick-YAML caveat — read `references/sql-syntax.md` before writing model SQL.

## Sources from existing Kafka topics

Use a dbt `source` (not `streaming_source`) for read-only references to topics that already exist. Match the cluster as `schema` and the topic name as `identifier`; `{{ source('raw', 'pageviews') }}` then resolves as a Flink table with no adapter-side registration. Full decision rules and example in `references/sources.md`.

## Pointers

- `references/` — read the file matching the task or question:
  - `references/workflow-existing-project.md` — multi-phase workflow for ongoing work on an existing dbt-confluent project (selected by the workflow router).
  - `references/workflow-scaffold.md` — multi-phase workflow for greenfield project setup (selected by the workflow router).
  - `references/authoring-rules.md` — non-negotiable rules for generating/editing model files and projects. Read before writing any model.
  - `references/materializations.md` — the materialization picker (intent→materialization + unsupported list) and per-materialization mechanics (`table`/`view`/`streaming_table`/`streaming_source`/`ephemeral`/`test`/`unit`/`seed`) + execution modes.
  - `references/schema-drift.md` — drift-detection rules and the silent-failure mode.
  - `references/sql-syntax.md` — exact Flink SQL forms: windowing TVF, `streaming_source` DDL body, constraints, identifier quoting.
  - `references/statement-lifecycle.md` — statement naming/lifecycle, custom statement names, hidden statements, streaming-cursor fetch behaviour.
  - `references/profiles-and-auth.md` — `profiles.yml` essentials, SET-statement caveats, service accounts / API keys.
  - `references/streaming-semantics.md` — `$rowtime`/changelog/watermark/PK/joins.
  - `references/sources.md` — sources-vs-`streaming_source` decision rules.
  - `references/testing.md` — testing dbt models on streams.
  - `references/adapter-behaviours.md` — version-aware quirks/fixes. Read when debugging a possibly-version-specific symptom: it carries a currency check (find the user's version → compare to latest → only surface quirks fixed in a *later* version) before suggesting an upgrade.
  - `references/upstream-links.md` — upstream Confluent/Flink doc links.
- `assets/` — copy-pasteable starting points: `assets/dbt_project.yml`, `assets/profiles.yml.example`, `assets/sources.yml`, `assets/models.yml`, `assets/streaming_source.sql`, `assets/streaming_table.sql`, `assets/table.sql`, `assets/.gitignore`, `assets/requirements.txt`.
