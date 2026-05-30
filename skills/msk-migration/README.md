# MSK Migration Skill

Assess and plan migrations from AWS MSK (Managed Streaming for Apache Kafka) to Confluent Cloud.

## Scope (V0.1 — MVP)

Covers **Assess and Plan** stages:

- **Assess** — runs a 16-row Red Flags audit on a KCP state file or manual profile, produces an Environment Summary grounded in state-file evidence, and scope-checks before the Plan stage.
- **Plan** — produces a 13-section Plan document with cluster type, sizing (P95 + peak), networking decision, auth approach, switchover approach, pre-migration workstream, risks, open questions, and next step. Tables follow standardized formats. Every number carries an inline citation to a state-file path or live doc URL.

Provision / Migrate / Switchover / Monitor stages are scoped for the next iteration following DTX review. The Plan stage emits structured decisions about networking, auth, switchover approach, and pre-migration steps that will feed those downstream stages when they ship. When users ask about downstream execution today, the skill redirects to docs.confluent.io and the Confluent account team rather than fabricating coverage.

## Validation

Validated via automated eval-driven iteration following the [agentskills.io spec](https://agentskills.io/skill-creation/evaluating-skills). The eval suite covers 14 scenarios across Assess and Plan, exercising both intake paths (KCP state file + manual `migration-profile.yaml`). All 14 evals pass with the skill loaded; the without-skill baseline runs at ~37% — a delta of ~+63pp.

Stopping criterion is dual-gate: the loop continues until both the mean pass rate **and** the minimum per-eval pass rate clear 90%. A high mean with one weak eval doesn't pass — that prevents shipping with hidden quality gaps.

## Triggers

Use this skill when the user signals intent to migrate from AWS MSK to Confluent Cloud. Examples:

- "scan my MSK environment"
- "plan my MSK to CC migration"
- "MSK to Confluent Cloud takeout"
- "kcp scan my MSK"
- "what cluster type should I use for moving off MSK?"
- "Cluster Linking from MSK"

Does NOT trigger on:

- Non-MSK Kafka sources (open-source Kafka, Aiven, Confluent Platform, Redpanda, Kinesis, Pulsar)
- Greenfield Confluent Cloud projects with no existing Kafka source
- General Kafka programming questions (producer/consumer code, Kafka Streams) unrelated to migration

## Architecture

Lazy-load dispatcher pattern. `SKILL.md` is a router with cross-stage decision tables and invariants — it loads on every invocation. Stage-specific procedures live in `references/` and load on demand when the user reaches that stage.

| File | Purpose |
|---|---|
| `SKILL.md` | Dispatcher with mode detection, intake routing, cross-stage decision tables (cluster type, auth, networking, switchover approach), and invariants |
| `references/assess.md` | Assess stage procedure: 16-row Red Flags audit, scan coverage, environment summary, handoff |
| `references/plan.md` | Plan stage procedure: 13-section template, sizing math, decision tables, citation conventions |
| `references/kcp-commands.md` | Reference for the KCP CLI surface — what each command does, what to look for in the output |
| `references/mcp-integration.md` | Reference for Local Confluent MCP — when to prefer MCP over CLI, write-tool guardrails |
| `assets/migration-profile.yaml` | Manual intake template for users who don't have KCP installed |
| `evals/evals.json` | 14 behavioral test scenarios with assertions |
| `evals/mock-environments/` | Fixture profiles for evals |

**Source-of-truth design.** Stable judgment lives in the skill (decision frameworks, trigger categories, stage handoffs, routing logic). Volatile product facts (cluster caps, version cutoffs, feature availability per cloud, command surfaces) are routed to live docs and fetched on demand. Hardcoded values are explicitly labeled and carry conditional framing in user-facing recommendations.

## Sources

All claims in the skill trace to:

- [docs.confluent.io](https://docs.confluent.io)
- [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp)
- General Kafka operational standards

No internal-only sources. No customer data. No information that requires Confluent network access to verify.

## Commands and tools the skill proposes

The skill never auto-executes commands without user approval. It proposes — the user runs. Commands referenced (with live verification before recommendation):

- `kcp` (open source) — Confluent's MSK migration tool. The skill recommends KCP as the primary scan path during Assess.
- Confluent CLI — for target-side operations the user runs themselves.
- Local Confluent MCP — optional. Used for target-side verification when connected.

## Future scope

V0.2 (post-DTX-review) will extend to:

- **Provision** — produce a Provisioning Runbook (Terraform from `kcp create-asset target-infra`, API key creation, networking validation, post-provision verification, rollback)
- **Migrate** — produce a Migration Runbook (Cluster Linking setup, schema migration order-of-operations, ACL/RBAC mapping, connector migration plan)
- **Switchover** — produce a Switchover Runbook (Zero-Cut command sequence or manual CL fallback, gateway config, client cutover order, rollback triggers)
- **Monitor** — produce a Monitoring Checklist (lag parity, throughput parity, error-rate watch, rollback window, decommissioning)

A sibling skill (`osk-migration`) is planned for open-source Kafka takeouts using the same architecture.
