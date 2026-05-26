# Workflow 1: scaffold a new dbt-confluent project

You're creating a new dbt-confluent project from scratch. Per the **universal safety rule** in `SKILL.md`, you stop after file creation — you do **not** run `dbt run` or any other state-changing command. You suggest those commands and let the user run them.

The workflow has explicit phases with stop-and-confirm gates. **After each phase that ends with a question, do not proceed to the next phase until the user replies.**

## Phase 0: pre-flight check

Before generating any files, ask the user via `AskUserQuestion`:

```
Q: Do you already have all of these set up?
   - Confluent Cloud account
   - An environment
   - A Kafka cluster
   - A Flink compute pool
   - A Flink API key + secret (with access to the env)

   Options:
     - Yes, all of the above (skip pre-flight, go straight to scaffolding)
     - I'm missing some — walk me through getting them
     - Not sure — let's check
```

If the user picks "yes," skip to phase 1.

If "missing some" or "not sure," ask a follow-up to identify the gaps:

```
Q: Which of these do you need to set up?
   Multi-select:
     - Confluent Cloud account
     - Environment
     - Kafka cluster
     - Flink compute pool
     - Flink API key + secret
```

For each gap, surface the relevant Cloud Console path (don't just give a vague "create one"):

| Need | Where in Cloud Console |
|---|---|
| Account | https://confluent.cloud/signup |
| Environment | top-right env dropdown → "Add cloud environment" |
| Kafka cluster | inside the env → "Cluster" → "Create cluster" (Basic is fine for dev) |
| Compute pool | inside the env → "Flink" → "Compute pools" → "Create" |
| Flink API key + secret | top-right account menu → "API keys" → "Add key" → scope: Flink, environment: yours. **Recommend service-account-bound for prod** — long-running streaming statements outlive employees, and user-bound keys are revoked when the user leaves. |

Then ask one more question — the data source for the first model:

```
Q: What's the source data for your first model?
   Options:
     - Existing Kafka topic (I'll declare it as a dbt source)
     - Synthetic data first (generate a streaming_source with the faker connector)
     - Both — synthetic for now, swap in real source later
```

After phase 0: **STOP**. Wait for the user's choices before generating files.

## Phase 1: scaffold

Generate the project files per `SKILL.md` "Rules when generating model files and projects". Default project layout:

```
<project-name>/
├── .gitignore                       # excludes profiles.yml, target/, dbt_packages/, logs/, .env
├── dbt_project.yml                  # profile = <project-name>; +materialized: streaming_table default
├── profiles.yml.example             # placeholders + dev/prod targets; service-account note for prod
├── requirements.txt                 # pinned dbt-confluent (verify version against PyPI before pinning)
└── models/
    ├── sources.yml                 # source declaration (if user picked existing topic)
    ├── models.yml                  # column declarations for the streaming_table model (REQUIRED)
    └── <model-name>.sql             # streaming_table per the user's chosen aggregation
```

If the user picked "synthetic data," also generate `models/<topic>_synthetic.sql` as a `streaming_source` and reference it from the streaming_table model with `{{ ref('<topic>_synthetic') }}` instead of `{{ source(...) }}`.

If the user picked "both," generate both the synthetic source and the `sources.yml` declaration; pick one for the starter model and leave a comment in the other showing the alternative ref/source line.

Verify the published `dbt-confluent` version against PyPI (`https://pypi.org/pypi/dbt-confluent/json` via WebFetch) before writing `requirements.txt`. **Don't guess the version.**

After phase 1: present the file tree, then **STOP** and ask:

```
Q: Files look right? Any edits before I walk you through deploying?
```

## Phase 2: suggest next steps (do NOT execute)

Once the user confirms the files, present user-runnable commands as a numbered list. Be explicit about which are safe (read-only) vs which change cluster state:

```
1. Init the dbt project

     dbt init

2. Verify the connection (read-only — safe to run):

     dbt debug

3. Compile the project (no cluster state changes):

     dbt parse
     dbt compile

4. Deploy — when ready (STATE-CHANGING — submits long-running Flink statements
   that consume compute and create real Kafka topics):

     dbt run --full-refresh

5. Verify in Confluent Cloud UI (after step 4 completes):

   - Environment → Flink → Statements → look for `dbt-<project>-<model>`
     (and a `-ddl` suffix variant for streaming_table — DDL completes
     quickly, INSERT runs forever)
   - Statement status should be RUNNING for the INSERT
   - Cluster → Topics → your output topic should appear
   - Run a quick `SELECT * FROM <table> LIMIT 10` in a Cloud Console
     workspace to confirm data is flowing
```

If the user reports a failure at any step, switch to debugging mode (universal rules + the files in `references/`). Common phase-2 failures:

- `dbt debug` fails with auth error → API key expired or doesn't have RBAC for the env. Have user verify in the UI.
- `dbt run` fails with schema error on the source → the source topic doesn't have the schema the source declaration claims. Have user check the actual topic schema.
- `dbt run` fails with a Flink syntax error → check the compiled SQL in `target/run/<model>.sql` and adjust the model accordingly.

## Done

The scaffold is complete. Future sessions in this directory will detect workflow 0 (existing dbt-confluent project) and continue from there. If the user wants more help right now (add another model, debug, etc.), drop the workflow 1 context and switch to `workflows/0-existing-project.md`.

## What if the user wants you to actually run things?

Per the universal safety rule:

- **Read-only commands** (`dbt debug`, `dbt parse`, `dbt compile`) — you may run autonomously if needed.
- **State-changing commands** (`dbt run`, `dbt run --full-refresh`, `dbt seed`) — only if the user explicitly asks ("go ahead and run `dbt run --full-refresh`"). When they do:
  1. Surface what the command will do (which models, expected statements created, expected topics created, ballpark resource impact).
  2. Ask for confirmation.
  3. After confirmation, proceed.
