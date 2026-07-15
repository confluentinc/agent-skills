# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository purpose

This repo packages a collection of **AI agent skills** for streaming/data-platform work on Confluent (Kafka producers/consumers, Kafka Streams, Schema Registry, Flink-based CDC to Tableflow). It is distributed as:

- A Claude Code plugin via `.claude-plugin/` (marketplace `confluent-agent-skills`, plugin `streaming-skills-plugin`).
- A Cursor plugin via `.cursor-plugin/`.
- The `npx skills add confluentinc/agent-skills` CLI for other agents.

The "code" in this repo is mostly markdown — skill prompts, reference material, and eval definitions. There is no application to build or run.

## Skill anatomy

Every skill lives under `skills/<skill-name>/` with this layout:

```
skills/<skill-name>/
├── SKILL.md          # Frontmatter (name, description) + body. Description gates triggering.
├── evals/evals.json  # Prompts + expectations used to score the skill
├── references/       # Lazy-loaded markdown the SKILL.md tells the agent to read on demand
└── scripts/          # (optional) shell helpers the skill can invoke
```

Architectural conventions to preserve when editing:

- **Lazy-load references.** SKILL.md bodies are deliberately short and route the agent to `references/<topic>.md` only when needed. Do **not** inline the contents of reference files into SKILL.md, and do not have SKILL.md instruct the agent to read all references upfront — see the explicit warning at the top of `skills/kafka-streams-programming/SKILL.md`. New reference files should be reachable from a decision/mode section in SKILL.md, not loaded preemptively.
- **The `description:` field is the trigger.** It must include positive trigger phrases *and* explicit "Do NOT trigger for…" exclusions where adjacent skills could fight over the same prompt (see the kafka-streams and CDC-tableflow descriptions for the pattern). When adding/renaming a skill, audit neighboring skills' descriptions for overlap.
- **Mode detection.** Larger skills (e.g. `kafka-streams-programming`) branch internally into Build / Architect / Debug modes. Keep that table-driven structure when extending — don't fork mode logic into separate skills.

## Evals are the contract

PRs must keep evals passing at the **90%+ threshold** (see `.github/pull_request_template.md`). Each `evals/evals.json` entry has:

- `prompt` — the user message to simulate
- `expected_output` — high-level success description
- `expectations[]` — concrete, individually-checkable assertions (these are what get scored)

When changing skill behavior, update `expectations[]` in the same PR. Expectations frequently encode hard-won correctness (e.g. "imports `StreamsUncaughtExceptionHandler` from `org.apache.kafka.streams.errors` — that nested class does not exist in KS 4.x") — treat them as regression tests, not aspirations.

`skills/kafka-schema-registry/evals/mock-repos/` holds fixture repos that evals point the skill at; keep fixtures and expectations in sync.

## Reviewing skill behavior

`tools/skill_review_dashboard.py` parses Claude session JSONL logs from `~/.claude/projects/` and serves a localhost dashboard of corrections, tokens, and user messages — used for triaging how a skill performed in a real session.

```
python tools/skill_review_dashboard.py --session-id <uuid> [--project-path /path/to/project]
```

Requires Python 3.10+. No other build tooling is needed for skill development.

## Makefile / service.yml

`Makefile` and `service.yml` are managed by Confluent's internal ServiceBot (cc-mk-include) and back the CI pipeline. The fenced `### BEGIN ... ### END ...` blocks are auto-regenerated nightly — do not hand-edit them. Local skill work does not require running `make`.

## Adding a new skill

Create `skills/<name>/` with `SKILL.md` (frontmatter — `name`, `description` with triggers + anti-triggers — and a body that routes to `references/*.md` at decision points), `evals/evals.json` with prompts and `expectations[]`, and any `scripts/`. The plugin manifests in `.claude-plugin/` and `.cursor-plugin/` point at `./skills/` so new skills are auto-discovered — no manifest edit needed. Add the skill to the table in `README.md`. The PR template requires an SME reviewer plus a DTX/DevRel reviewer for new skills.

## Versioning

There are two levels of version that must be kept in sync.

**Skill-level version** (`metadata.version` in `SKILL.md` frontmatter):

| Bump | When |
|------|------|
| PATCH (`x.y.Z`) | Non-behavioral: typos, wording clarifications, reference content corrections that don't change what the skill does or produces |
| MINOR (`x.Y.0`) | Additive: new modes, new reference files, new behavior branches, new eval cases for newly supported scenarios |
| MAJOR (`X.0.0`) | Breaking: trigger `description` overhaul that changes which prompts the skill handles, removed modes or features, significant behavioral shifts |

**Plugin-level version** (`.claude-plugin/plugin.json` and `.cursor-plugin/plugin.json`):

- Bump whenever any skill in the repo receives a MINOR or MAJOR version bump.
- Both files must be updated together and kept in sync with each other.
- A PR that only bumps individual skill PATCH versions does **not** require a plugin-level bump.
