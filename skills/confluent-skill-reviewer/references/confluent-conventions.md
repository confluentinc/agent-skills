# Confluent conventions — reference

Source of truth: `CLAUDE.md` and `.github/pull_request_template.md` at the repo root. This file is consulted during Phase B and Phase E; do not load it until a Confluent-specific finding is suspected.

## The three non-negotiables from `CLAUDE.md`

### 1. Lazy-load references (CLAUDE.md § Skill anatomy — "Lazy-load references" bullet)

> SKILL.md bodies are deliberately short and route the agent to `references/<topic>.md` only when needed. Do NOT inline the contents of reference files into SKILL.md, and do not have SKILL.md instruct the agent to read all references upfront.

How to detect inlining:
- A heading appears verbatim in both `SKILL.md` and `references/<topic>.md`.
- A long fenced code block (>20 lines) in SKILL.md is duplicated in a reference file.
- SKILL.md says "Read all reference files before answering" or similar — this is the explicit anti-pattern called out in `skills/kafka-streams-programming/SKILL.md:10-18`.

Severity: **Blocking**. Every activation pays the full context cost when references are inlined; the design intent is progressive disclosure.

### 2. Description field is the trigger (CLAUDE.md § Skill anatomy — "The `description:` field is the trigger" bullet)

> The `description:` field is the trigger. It must include positive trigger phrases *and* explicit "Do NOT trigger for…" exclusions where adjacent skills could fight over the same prompt.

How to detect:
- `description:` contains no `Do NOT trigger` (case-insensitive) clause.
- `description:` shares ≥2 noun keywords with another skill's description, and neither names the other in its anti-triggers (see `references/trigger-overlap.md` for the algorithm).

Severity: **Blocking** when collisions exist; **Warning** when description has positive triggers only but no neighbors overlap.

Canonical example — `kafka-streams-programming` description ends with:
> Do NOT trigger for Flink, connectors, CDC, or plain producer/consumer.

This explicitly hands off to `confluent-cloud-cdc-tableflow` (CDC) and `developing-kafka-python-client` (plain producer/consumer).

### 3. Mode detection table (CLAUDE.md § Skill anatomy — "Mode detection" bullet)

> Larger skills branch internally into Build / Architect / Debug modes. Keep that table-driven structure when extending — don't fork mode logic into separate skills.

How to detect:
- SKILL.md > 200 lines OR covers ≥2 distinct workflows.
- No `| User intent | Mode |` markdown table near the top.

Severity: **Warning**. The repo's convention is to keep related modes in one skill (see `skills/kafka-streams-programming/SKILL.md:26-34`).

## The evals contract (CLAUDE.md § Evals are the contract)

> PRs must keep evals passing at the **90%+ threshold**. `expectations[]` frequently encode hard-won correctness — treat them as regression tests, not aspirations.

This is checked in Phase D. The shape rules live in `references/evals-contract.md`.

Fixture sync (§ Evals are the contract — closing paragraph):
> `skills/kafka-schema-registry/evals/mock-repos/` holds fixture repos that evals point the skill at; keep fixtures and expectations in sync.

If `evals/evals.json` references a fixture path that doesn't exist on disk → **Blocking**.

## PR template gates (`.github/pull_request_template.md`)

Verify in PR-diff mode (Phase E):

| Checklist item | Wording in template | What to check |
|---|---|---|
| Docs updated | "If adding a new skill, I have reached out to reviewers to make sure the docs are updated to reflect the new skill." | If the PR adds a new `skills/<name>/`, the README.md skill table must have a corresponding row in the same PR. `git diff main...HEAD -- README.md`. |
| 90% evals | "Evals pass at 90%+ threshold" | Look for a score in PR comments, CI output, or the description. If absent, request the author paste it. |
| SME reviewer | "SME reviewer identified if adding a new skill" | `gh pr view --json reviewRequests,assignees` — needs a domain expert (Kafka Streams, Schema Registry, Flink CDC, Python client). |
| DTX/DevRel reviewer | "DTX/DevRel reviewer assigned" | Same call — needs a reviewer from `@confluentinc/dtx` or `@confluentinc/developer-advocates`. |

The first three apply only when a *new* skill is added. The DTX/DevRel reviewer is required on every PR.

## Mode-table preamble (style cue)

This repo's larger skills lead with an explicit lazy-load warning. Wording from `skills/kafka-streams-programming/SKILL.md:10-18`:

```markdown
## ⚠️ IMPORTANT: Lazy-Load References Only
**Do NOT read all reference files upfront. Read ONLY what you need, when you need it.**

- User asks "how do I X?" → Read `references/<topic>.md` § <section> only
- Most questions need 0–2 reference files total, not all 10

**Never read multiple files preemptively "just in case"**
```

Absence of this preamble in a multi-reference skill is a **Nit**, not Blocking — but worth recommending in the report.

## Provenance metadata

Skills authored in this repo carry attribution and freshness data in frontmatter:

```yaml
metadata:
  author: confluent
  version: "1.2"
  last_updated: "2026-05-12"
compatibility: Requires Python 3.9+, confluent-kafka, fastavro. Needs access to a Confluent environment (Cloud, Platform, local Docker, or WarpStream) for E2E testing.
```

- `metadata.author: confluent` — required on Confluent-authored skills.
- `metadata.version` — semver string. Bump on user-visible behavior change.
- `metadata.last_updated` — ISO date (YYYY-MM-DD). Useful for grading staleness.
- `compatibility` — free text ≤500 chars (per spec). Required when the skill assumes packages, CLI tools, or specific environment access. Skip on pure-prose advisory skills.

Severity: missing metadata fields → **Warning**. Missing `compatibility` on a skill that obviously needs env access → **Nit**.

## Platform scoping

Confluent skills serve four runtime targets: Confluent Cloud, Confluent Platform, Apache Kafka (OSS), WarpStream. The convention:

- **Platform-specific skill** — the platform name appears in `name` (e.g. `confluent-cloud-cdc-tableflow`, `warpstream-producer-optimization`). The body must scope every instruction to that platform. Mentioning all four platforms in a skill named `confluent-cloud-*` is **Blocking** — the user will be misled.
- **Cross-platform skill** — `name` has no platform token. The body must include a platform-detection step ("Which environment is this targeting?") near the top, and platform-divergent details live in dedicated reference files:
  - `references/confluent-cloud.md` — managed services, Cloud API keys
  - `references/confluent-platform.md` — self-managed security config
  - `references/apache-kafka.md` — OSS defaults
  - `references/warpstream.md` — object-storage architecture, agent config overrides

A cross-platform skill missing the platform-detection step **or** the per-platform reference files is **Warning** — the skill will give Cloud-shaped advice when the user is on WarpStream.

## Plan-before-execute (CRUD-capable skills)

Any skill whose workflow creates, modifies, or deletes Confluent resources — Kafka topics, schemas, connectors, Flink statements, RBAC bindings, Terraform state — must:

1. Present a numbered plan of every resource operation it will perform.
2. List the MCP tools, CLI commands, or API endpoints involved, in execution order.
3. **Wait for explicit user confirmation** before the first resource-modifying call.
4. Never execute resource-modifying operations silently.

Detection heuristic: grep the SKILL.md for resource-modifying verbs (`create`, `delete`, `register`, `deploy`, `apply`, `terraform`) paired with Confluent nouns (topic, schema, statement, connector). If matches exist and the SKILL.md has no "wait for confirmation" / "approve the plan" wording, **Blocking**.

Read-only skills (advisory, debugging, architecture review) are exempt.

## Credential handling

A skill that consumes credentials via `.env` must not read the file's contents — even to "verify it exists". The contract:

- Reference variables by name in scripts and prose: `$BOOTSTRAP_SERVERS`, `$CLUSTER_API_SECRET`.
- To check presence without disclosing the value: `test -n "$VAR"`.
- Never `cat .env`, `Read .env`, `head .env`, `grep .env`, or include `.env` in a `files: [...]` eval fixture.

Detection: grep the SKILL.md and `scripts/` for `cat .env`, `Read.*\.env`, `head.*\.env`, `grep.*\.env`. Any hit is **Blocking**. A skill that takes credentials but contains no guardrail language at all (no mention of `.env`, no `test -n`, no warning) is **Warning** — the next contributor will reach for `cat .env` and won't be stopped.

## Where Confluent diverges from the bare spec

| Spec says | Confluent adds |
|---|---|
| `description` must be ≤1024 chars | Description must also include `Do NOT trigger for…` when neighbors overlap |
| Body should be < ~5000 tokens | Larger skills must use a mode-table; don't fork into siblings |
| `evals/` is non-standard | This repo requires `evals/evals.json` and treats `expectations[]` as regression tests |
| No reviewer rules | SME + DTX/DevRel reviewer required per PR template |
| No README rule | New skills must be added to README.md's skill table in the same PR |
| `metadata` is optional | `metadata.author/version/last_updated` expected on repo-authored skills |
| Platform-agnostic | Platform name in `name:` if scoped; otherwise per-platform reference files |
| Silent execution allowed | CRUD-capable skills must plan-and-confirm before resource operations |
| No credential rules | Skills must never read `.env` contents; reference vars by name only |
