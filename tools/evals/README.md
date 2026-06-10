# Skill evals runner

`run_evals.py` scores every skill's `evals/evals.json` using
[pydantic-evals](https://pydantic.dev/docs/ai/evals/evals/) with a Claude
judge, and enforces the **90% per-skill pass-rate** that CLAUDE.md and the PR
template treat as the evals contract.

## What it does

For each `skills/<name>/evals/evals.json`:

1. Loads the skill's `SKILL.md` as the system prompt for a `pydantic-ai`
   `Agent`, and gives it a `read_skill_file` tool so it can lazy-load
   `references/*.md` on demand — mirroring how the skill is meant to run.
2. Runs each eval `prompt` through that agent (the pydantic-evals *task*).
   Evals that list `files` have those fixtures inlined into the prompt as
   context.
3. A custom `AssertionJudge` evaluator scores **each** `assertion` with a
   second Claude agent (`passed` + `reason`).
4. Computes the pass rate per skill and exits non-zero if any skill is below
   the threshold.

> **LLM-judge harness, not a sandbox.** It does not execute generated code.
> Assertions like "all unit tests pass with pytest" are judged from the
> generated code's correctness, not by running it.

## Run locally

```bash
export ANTHROPIC_API_KEY=sk-ant-...
pip install -r tools/evals/requirements.txt

python tools/evals/run_evals.py                              # all skills
python tools/evals/run_evals.py --skill kafka-streams-programming
python tools/evals/run_evals.py --threshold 0.9 --max-concurrency 4
```

Model selection (both default to `anthropic:claude-sonnet-4-6`):

```bash
export EVAL_AGENT_MODEL=anthropic:claude-opus-4-8   # model that runs the skill
export EVAL_JUDGE_MODEL=anthropic:claude-sonnet-4-6 # model that judges
```

Exit codes: `0` all skills pass · `1` one or more below threshold · `2` setup
error (e.g. missing `ANTHROPIC_API_KEY`).

## Weekly CI (Semaphore)

The weekly run is a standalone Semaphore pipeline, `.semaphore/evals.yml`,
triggered by a Semaphore **Schedule (Task)** — it is *not* part of the
per-push `.semaphore/semaphore.yml` and does not touch that ServiceBot-managed
file.

One-time setup:

1. **Create the API-key secret** (name must match `evals.yml`):

   ```bash
   sem create secret anthropic-api-key -e ANTHROPIC_API_KEY=sk-ant-...
   ```

   (Or add it in the Semaphore UI: Project → Settings → Secrets.)

2. **Register the weekly schedule** (Mondays 09:00 UTC — edit `at` to change):

   ```bash
   sem create -f .semaphore/evals-schedule.yml
   ```

   You can also create it in the UI: Project → Settings → Tasks → New Task,
   pointing at branch `main`, pipeline file `.semaphore/evals.yml`, cron
   `0 9 * * 1`.

3. **Verify / trigger a run on demand:**

   ```bash
   sem get schedules
   sem trigger weekly-skill-evals     # run now without waiting for the cron
   ```

A failing run (any skill below 90%) shows up as a failed pipeline in
Semaphore.
