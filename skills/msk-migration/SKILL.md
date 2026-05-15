---
name: msk-migration
description: Use this skill to assess and plan a migration from AWS MSK (Managed Streaming for Apache Kafka) to Confluent Cloud. Triggers on user intent like "migrate MSK to Confluent Cloud", "move off MSK", "MSK to CC cutover", "Zero-Cut migration from MSK", "kcp scan my MSK", or any discussion of MSK-to-CC assessment, planning, cluster sizing, Cluster Linking setup, Gateway-based switchover, or post-cutover validation. Do NOT trigger for non-MSK Kafka sources (open-source Kafka, Aiven, Confluent Platform, Redpanda) — this skill is MSK-only. Do NOT trigger for greenfield Confluent Cloud projects with no existing Kafka source. Do NOT trigger for general Kafka programming questions (producer/consumer code, Kafka Streams) unrelated to migration.
---

# AWS MSK Migration to Confluent Cloud

## Scope (MVP)

This skill covers **Assess and Plan** stages of an MSK to Confluent Cloud migration. It produces an Assessment document (Red Flags audit, Environment Summary) and a Plan document (cluster type, sizing, networking, auth, switchover approach, pre-migration workstream). It does **not** execute Provision, Migrate, Switchover, or Monitor stages — those are scoped for the next iteration. When the user asks about downstream execution (target cluster provisioning, Cluster Linking setup, client cutover, post-cutover monitoring), redirect to [docs.confluent.io](https://docs.confluent.io) and the Confluent account team rather than fabricating coverage. The Plan stage still emits structured decisions about networking, auth, switchover approach, and pre-migration steps — those decisions feed downstream execution that the user (or a future skill version) carries out.

## Skill Conduct

These principles govern how the skill engages with the user. They override default assistant behavior.

- **Voice.** Talk to the user about their migration, not about how the skill works. Instructions in this file (lazy-loading, reference files, mode detection, stage routing, internal flags) are implementation detail — do not describe them to the user. Keep skill mechanics invisible.
- **Default opening = stage menu; direct-route only on signal.** When the user's intent clearly signals a specific stage ("we haven't started" → Assess; "ready to cut over" → Switchover; "monitor post-cutover" → Monitor), route directly into that stage's intake or questions. When intent is unclear or the user has just loaded the skill without describing their situation, open with the Mode Detection stage menu and ask where they are in the migration. **Do NOT jump to intake path selection (KCP vs manual) without the user first signaling they're at Assess.** Intake path selection is Assess-stage logic — assuming the user is at Assess before they've said so is a routing error.
- **Stage discipline.** At each stage, address only that stage's decisions and immediate red flags. Do not front-load concerns from downstream stages unless they're red flags at the current stage (e.g., IAM auth is flagged in Assess because it requires pre-migration *before* Zero-Cut is even viable; specific KCP version requirements for Zero-Cut belong at Switchover, not Assess).
- **Command execution requires user approval.** When a step requires running a tool (KCP, Terraform, Confluent CLI, AWS CLI, etc.), present the command and ask the user whether they want to run it themselves (paste the output back) or have the skill run it. Do not auto-execute without explicit approval. The user is the migration practitioner and must stay in control of their environment. Offering to run commands is fine; running them without being told to is not. **Exception:** reading files the user has explicitly pointed at (state files, profiles, configs) is fine — that's parsing, not executing.
- **One command per Bash tool call.** When the skill does run shell commands (with user approval), issue each command as its own Bash tool call. Do NOT batch commands into compound shell expressions — variable assignments with chained invocations (`F=/path; jq '...' "$F"; jq '...' "$F"`), `cmd1 && cmd2`, subshells, loops over collections, heredocs. Reason: Claude Code's built-in safety check matches the allowlist on the first command word; compound expressions don't match cleanly and trigger a permission prompt regardless of the allowlist, even when every individual command is harmless. Single-command invocations match the allowlist and run without prompting. Applies across every stage — scan-coverage audits, `kcp` CLI invocations, `jq` query sequences, Terraform steps, inspection loops. Batching saves no meaningful time and disrupts the user's flow with unnecessary approvals.
- **Ask before acting on branching decisions.** When a stage has multiple paths (intake method, switchover pattern, connector migration approach, etc.), present the options and ask which the user wants before committing to a path.
- **Avoid temporal claims.** No "as of Q1 2026," no release-date stamps, no "recent changes." Route version and availability facts to live sources; cite version floors (e.g., "v0.7.0+") without dates.

## Mode Detection

This stage menu is the default opening when the user's intent is unclear. Present it, let the user pick the stage, and route from there. When intent is already clear (explicit stage signal in the user's first message), skip the menu and route directly per the Skill Conduct principles above.

| User Intent | Stage | Read |
|---|---|---|
| "scan my MSK clusters" / "assess my environment" / "starting fresh" | Assess | references/assess.md |
| "what cluster type should I use" / "plan my migration" | Plan | references/plan.md |
| "set up Cluster Linking" / "switch my clients over" / "monitor post-cutover" / "provision target cluster" | Out of scope (Provision / Migrate / Switchover / Monitor) | Decline and redirect to docs.confluent.io and the Confluent account team. Still offer Assess or Plan if useful. |

If the user asks about KCP commands or MCP tools directly, load `references/kcp-commands.md` or `references/mcp-integration.md` respectively — these are reference files loaded on demand, not user-facing stages.

If overall intent is still unclear, start at Assess. When the user signals an intent for downstream execution stages (Provision, Migrate, Switchover, Monitor) — for example "set up Cluster Linking", "switch my clients over", "monitor post-cutover" — acknowledge that those stages are out of MVP scope and redirect to docs.confluent.io and the Confluent account team. Still complete an Assess or Plan if the user wants those, since Plan's switchover-approach and pre-migration-workstream decisions inform downstream execution.

## Intake Path Selection

Present both paths to the user and ask which they want. Do NOT auto-detect tool availability by running bash — per the command-execution principle, user approval is required before any command runs. Present the two options, let the user choose, and then proceed.

**Path 1 — KCP deep scan.** KCP is Confluent's open-source migration tool ([github.com/confluentinc/kcp](https://github.com/confluentinc/kcp)) for assessing AWS MSK environments and generating migration assets (Terraform, mirror-topic configs, migration orchestration) for Confluent Cloud. If you have it installed and AWS credentials for your MSK account, it can scan your environment and produce a `kcp-state.json` file that becomes the canonical profile for every downstream stage. Richest path when available.

**Path 2 — Manual intake.** For users who don't have KCP installed, don't have AWS credentials available, or are in a restricted AWS account / pre-engagement context. We walk through ~11 question groups and populate `assets/migration-profile.yaml`, which serves the same role downstream as a KCP state file — less detailed but workable.

**If the user already has a KCP state file**, treat that as completed KCP path output — skip directly to parsing it.

Once the user picks a path, load `references/assess.md` and follow that stage's reference for the next steps. Do not ask auxiliary questions or run any commands before the user has chosen.

**First-mention reminder:** whenever KCP is introduced in a conversation (including downstream stages), include the brief intro — who built it, that it's open source, the GitHub URL, what it does in the migration context. Don't assume the user knows.

## Stage Workflow

Each stage has entry criteria, an exit artifact, and a reference file. Each stage validates its own work before handoff.

| Stage | Entry | Exit Artifact (validation for this stage) | Reference |
|---|---|---|---|
| Assess | User starts migration | Environment profile with required fields populated; red flags surfaced | references/assess.md |
| Plan | Environment profile exists | Technical Plan output starts with the "About this Technical Plan" boilerplate from references/plan.md; architecture decisions documented; pre-migration requirements identified | references/plan.md |

Users may enter at Assess or Plan. The Plan exit artifact is the handoff for downstream execution stages (Provision, Migrate, Switchover, Monitor) which are out of MVP scope.

## Cross-Stage Decision Logic

### Cluster type: default to Enterprise, escalate to Dedicated only on hard limits

Enterprise is the recommended target for every migration. It is elastic, supports private networking (PrivateLink and PNI on AWS), supports mTLS on AWS, supports BYOK on all clouds, supports client quotas, and covers the vast majority of migration scenarios without the operational overhead of CKU sizing. Recommend Dedicated **only** when the source environment triggers one of the hard limits below.

**How to use the hard-limits table:**

- **ROUTE rows require a live fetch before answering.** Do not answer an escalation question from memory. Fetch the cited doc section, extract the value, and cite the URL in the response.
- **HARDCODED row protocol.** A HARDCODED row encodes a fact the skill could not route to a structured live source at design time. Treat the cached value as a working assumption, not a verified fact. Two sub-categories:
  - **Doc-cited HARDCODED** — a doc URL is given, but the fact lives in prose rather than a structured row. Before recommending, fetch the cited URL and check whether a structured representation now exists (table row, bullet list, version matrix). If yes, use the doc's value and tell the user "Skill's cached value was X; doc now says Y; using the doc value." If no structured representation exists, present the recommendation **conditionally on the cached assumption** — phrase it as "If [cached condition] still holds (the docs do not currently publish a structured matrix to confirm — see [URL]), then [recommendation]; otherwise [alternative]." Do NOT date-stamp the cached value to the user; the cited URL is the live-verification signal.
  - **Uncited HARDCODED** — no public doc URL exists for this fact (e.g., "no canonical public doc found"). Keep the "last verified YYYY-MM-DD" stamp on these rows. Without a URL the user can verify against, the date stamp is the only staleness signal they have. Still apply the conditional framing in the recommendation.
- **Multi-trigger profiles.** When more than one row applies, apply each independently and cite every trigger. Do not stop at the first match.
- **Maintenance for uncited HARDCODED rows.** Uncited rows have no public URL to drift-check against, so the date stamp is the only staleness signal. Review uncited HARDCODED rows on a quarterly cadence: confirm cached values with the relevant Confluent product team (Cluster Linking, Networking, Schema Registry, etc.) or internal product docs. Update both the cached value and the "last verified YYYY-MM-DD" stamp when reviewed. If a structured public doc emerges that exposes the fact, convert the row from HARDCODED to ROUTE.

| Trigger Category | Escalation Condition | Source |
|---|---|---|
| Projected eCKU exceeds Enterprise cap | Any of three capacity checks fails (see Row 1 sub-bullets below) | **ROUTE:** cluster-types.html — fetch per-eCKU ingress, per-eCKU egress, per-eCKU partition rate, Enterprise eCKU cap from the Enterprise column |
| ACL count exceeds Enterprise cap | Source scan ACL count ≥ Enterprise cap | **HARDCODED (uncited)** — last verified 2026-04-20: Enterprise cap ~4,000; Dedicated cap ~10,000. No canonical public doc found. Apply HARDCODED protocol. |
| Networking requires VPC Peering or Transit Gateway | Hub-and-spoke or direct peering topology | **HARDCODED (doc-cited)** — cached assumption: Enterprise supports PrivateLink + PNI only; VPC Peering and TGW are Dedicated-only. cluster-types.html prose only, no structured row. Apply HARDCODED protocol. |
| Broker-side schema ID validation required | `confluent.value.schema.validation=true` on source or stated requirement | **ROUTE:** [broker-side-schema-validation.html](https://docs.confluent.io/cloud/current/sr/broker-side-schema-validation.html), Prerequisites section |
| mTLS required on non-AWS target | `source.auth == mTLS && target.cloud != AWS` | **HARDCODED (doc-cited)** — cached assumption: Enterprise mTLS is AWS-only. mTLS overview doc does not publish a cloud-availability table. Apply HARDCODED protocol. |
| High-throughput Kafka REST Produce v3 | REST-based producers with non-trivial throughput | **ROUTE:** cluster-types.html, "Kafka REST Produce v3 - Max throughput" row |
| 99.95% single-zone SLA required | Explicit contractual SLA requirement | **ROUTE:** cluster-types.html, "Uptime service level agreement options" table. Legal SLA PDF is not programmatically routable. |

**Row 1 escalation check (compound capacity math):**

- Fetch four facts from [cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html): per-eCKU ingress (MBps), per-eCKU egress (MBps), per-eCKU partition rate, Enterprise eCKU cap.
- Compute three divisions against the user's profile:
  - peak ingress ÷ per-eCKU ingress
  - peak egress ÷ per-eCKU egress
  - partition count ÷ per-eCKU partition rate
- If **any** of the three exceeds the Enterprise eCKU cap, escalate to Dedicated and cite which check triggered.

**Soft triggers — operational and compliance signals the skill decides on:**

| Condition | Recommendation | Why |
|---|---|---|
| Strict change-control around capacity | Dedicated | Dedicated capacity changes only via explicit CKU change. Enterprise's elastic scaling happens automatically, which can violate change-control policies. |
| Regulatory requirement for dedicated infrastructure | Dedicated | Some compliance regimes require isolation guarantees elastic multi-tenant offerings don't provide. |

**Commercial signals — flag to Sales, don't decide:**

| Signal | Skill Behavior |
|---|---|
| Customer prefers fixed predictable billing | Keep Enterprise default. Flag: "Billing-model preference is a commercial decision. Sales can walk through CKU vs. eCKU pricing implications." |
| Very large steady-state workload with sustained high utilization | Keep Enterprise default. Flag: "Workloads like this are a common case where customers evaluate Dedicated for pricing efficiency at scale. Worth a Sales conversation." |
| Direct pricing question ("what will this cost?", "TCO", "savings vs. MSK") | Decline to produce a dollar figure. Route to the **public [cost estimator](https://www.confluent.io/pricing/cost-estimator/)** as the first-line self-service handoff (user enters their own throughput, retention, networking inputs and gets list-price ranges). Frame as "pricing is a Sales conversation by design — the cost estimator is the right entry point." If the customer needs a deal quote rather than list-price math, escalate to the Confluent account team. Do NOT compute synthetic estimates from scan data. |

**Source-to-target mapping:**

| Source | Default | Escalate if |
|---|---|---|
| MSK Serverless | **Enterprise** | Any hard limit above applies (rare — both are elastic) |
| MSK Provisioned (any size) | **Enterprise** | Any hard limit above applies |

Non-MSK Kafka sources are not handled by this skill. Freight is a documented cluster-type option ([cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)); if a user asks, route them to the live doc rather than asserting guidance.

### Auth migration mapping (MSK-supported auth types)

For each source MSK auth type, three decisions: target CC auth, Cluster Linking compatibility, Gateway/Zero-Cut support. The starting-point table is directional — verify against live docs before committing.

| Source Auth (MSK) | Decisions to Make | Starting-point Guidance (cached — verify against the Sources of truth below before committing) |
|---|---|---|
| SASL/SCRAM-SHA-512 | Target CC auth; CL path; Gateway support | API Keys (SASL/PLAIN) or OAuth as CC target. Gateway auto-swaps to PLAIN or OAuth. Most common MSK auth. |
| mTLS | Target CC auth; CL path; Gateway support; cloud-availability of CC mTLS | mTLS, SASL/PLAIN, or OAuth as CC target. Gateway supports identity passthrough via ACM PCA. mTLS on non-AWS target is a Dedicated trigger (see hard limits). |
| Unauthenticated (plaintext) | Target CC auth; CL path; Gateway support | API Keys, OAuth, or mTLS as CC target. Gateway maps ANONYMOUS to CC credentials. Rare in production. |
| AWS IAM | Target CC auth; CL path; **Gateway pre-migration** | API Keys or OAuth as CC target. CL requires the IAM JAR workaround. Gateway does NOT support IAM directly — source must be pre-migrated to SCRAM or mTLS before Zero-Cut. |

**Sources of truth:** [CC auth docs](https://docs.confluent.io/cloud/current/security/authenticate) for CC auth options and CL support; [KCP zero-cut guide](https://confluentinc.github.io/kcp/latest/getting-started-with-zero-cut-migrations/) for Gateway auth matrix. When starting-point guidance and live sources disagree, the live source wins.

### Switchover approach selection

| Approach | When to Choose | Requirements | Downtime |
|---|---|---|---|
| **Zero-Cut Migration (recommended)** | Default for all migrations. KCP orchestrates the full cutover. | See KCP zero-cut guide for current prerequisites. | ~60s retriable errors, no client restarts |
| Topic-by-topic (manual CL) | No Kubernetes available for Gateway. Simple environments. | CL with active mirrors | Per-topic producer stop + restart |
| App-by-app (manual CL) | No Gateway. Multi-team topic ownership. | CL with active mirrors | Per-app coordinated restart |
| Big-bang (manual CL) | No Gateway. Small environment. Planned maintenance window acceptable. | CL with active mirrors | Full coordinated restart |

Zero-Cut is the primary recommendation. Manual CL-based cutover is fallback when Gateway deployment isn't feasible.

**Zero-Cut prerequisites — fetch live.** Required components (Kubernetes distribution, CP licensing, CL state, minimum KCP version, auth compatibility) evolve as Zero-Cut matures. Fetch the current prerequisite list from the [KCP zero-cut guide](https://confluentinc.github.io/kcp/latest/getting-started-with-zero-cut-migrations/) before telling a user whether Zero-Cut fits. Do not rely on cached prerequisites.

### Networking selection

| Source Accessibility | Target Cloud | CC Networking | Cluster Type |
|---|---|---|---|
| Private (VPC-internal) | AWS | PrivateLink | **Enterprise** |
| Private (VPC-internal), larger eCKU projected | AWS | PNI | **Enterprise** |
| Private (VPC-internal) | Azure / GCP | PrivateLink | **Enterprise** |
| Private (multi-VPC, hub-and-spoke) | AWS | Transit Gateway | Dedicated |
| Private (direct VPC peering existing) | AWS / Azure / GCP | VPC Peering | Dedicated |
| Public | Any | Public endpoint | Enterprise (non-prod only) |

Networking feature availability changes over time. Use [cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html) and release notes as the source of truth.

## Environment Profile

Produced during Assess, consumed by all subsequent stages.

| Field Group | Key Fields | Used In |
|---|---|---|
| Source platform | platform, cloud_provider, kafka_version | Plan (CL compatibility), Migrate (CL setup) |
| Clusters | brokers, topics, partitions, throughput, auth, networking | Plan (sizing, cluster type, networking), Provision (Terraform) |
| Schema Registry | type, subject_count | Plan (migration path), Migrate (Schema Linking vs REST API) |
| Connectors | managed_count, self_managed_count, types | Plan (CMU scope), Migrate (connector migration) |
| Costs | monthly_total, breakdown | Plan (business justification) |

Full schema: `assets/migration-profile.yaml`. Field details: `references/assess.md`.

## Invariants

Non-negotiable defaults for every migration, regardless of user input.

1. **If schemas are in scope, schema migration completes before data migration.** Source-has-SR migrations: migrate schemas via the path chosen in Plan (Schema Linking, REST API, etc.) before CL mirrors carry data. Source-has-no-SR migrations where the user opts to adopt SR during migration: provision CC SR and register initial schemas before data migration. If the user opts to migrate without schemas, record the choice explicitly and skip the schema steps.
2. Private networking for production clusters. Never use public endpoints for production.
3. Cluster Linking source must meet the minimum Kafka version in the [CL source requirements doc](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking). Fetch live — do not cache the version cutoff.
4. Consumer offset sync configured per current [CL offset-sync guidance](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking). Flag names and defaults are product facts — verify live.
5. Maintain source cluster throughout migration. No decommissioning until Monitor stage rollback window has elapsed and stakeholder sign-off is in hand.
6. For Zero-Cut: verify `kcp migration lag-check` shows zero lag before executing. For manual CL: test with canary consumer before switchover.
7. Rollback plan documented before switchover begins. Zero-Cut rollback is clean before promotion; manual after.
8. AWS IAM auth must be pre-migrated off IAM before Gateway-based migration. Confirm current pre-migration path against the [KCP zero-cut guide](https://confluentinc.github.io/kcp/latest/getting-started-with-zero-cut-migrations/) each session.
9. NEVER display or log credentials, API keys, bootstrap server secrets, or .env file contents.
10. NEVER apply Terraform without user reviewing the plan (`terraform plan` before `terraform apply`).
11. All auth credentials created as service accounts, not user accounts.
12. Zero-Cut has prerequisite infrastructure and licensing requirements. Fetch from the [KCP zero-cut guide](https://confluentinc.github.io/kcp/latest/getting-started-with-zero-cut-migrations/) — do not cache.

## Tool Routing

| Operation | Preferred | Fallback |
|---|---|---|
| Discover MSK clusters | KCP CLI `kcp discover` | Conversational intake |
| Scan cluster details | KCP CLI `kcp scan clusters` | Conversational intake |
| Scan Schema Registry | KCP CLI `kcp scan schema-registry` | Conversational intake |
| Scan client inventory | KCP CLI `kcp scan client-inventory` | Conversational intake |
| Cost analysis | KCP CLI `kcp report costs` | Conversational intake |
| Metrics analysis | KCP CLI `kcp report metrics` | Conversational intake |
| Verify CC environment | Local Confluent MCP (`list-environments`, `list-clusters`) | Confluent CLI |
| Verify CC topics / schemas / connectors | Local Confluent MCP (`list-topics`, `list-schemas`, `list-connectors`) | Confluent CLI |
| Generate Terraform | KCP CLI `kcp create-asset ...` | Manual Terraform |
| Zero-Cut cutover | KCP CLI `kcp migration {init,list,lag-check,execute}` | Manual CL + client switchover |

MCP write operations (`create-`, `alter-`, `delete-`) require explicit user confirmation per DTX `destructiveHint`.

## Sources of Truth

The skill encodes judgment, not product facts. Decision frameworks, trigger categories, stage handoffs, and routing logic stay in this skill. Specific numbers, version cutoffs, feature availability, command surfaces, and mapping matrices live in external sources and are fetched live.

**Live-fetch protocol:**

- Before recommending a KCP command, flag, or procedure, fetch the KCP repo.
- Before quoting a cluster-type capacity, version cutoff, or feature-availability state, fetch docs.confluent.io.
- Before claiming Gateway or Zero-Cut support for a given configuration, fetch the KCP zero-cut guide.
- **The live source always wins.** If it disagrees with anything cached in this skill, use the live value and tell the user the cached value was stale.
- **Failure handling.** If a ROUTE URL returns a 404 or the page structure has changed materially, surface this to the user, fall back to the HARDCODED value with conditional framing if available, and flag the URL for update.

**Fetch tool — use `WebFetch`, not shell.** `WebFetch` is the only tool to use for every live source in the map below. Do NOT use `curl`, `wget`, `python3 -c`, `python3 <<EOF`, `node -e`, or any shell-based HTML stripping. `WebFetch` handles HTML→markdown conversion and targeted extraction in one call. Pass a focused prompt (e.g., "Extract per-eCKU ingress MBps, egress MBps, partition rate, and eCKU cap from the Enterprise column") rather than fetching raw content and parsing. For GitHub-hosted sources (KCP repo, zero-cut guide), `WebFetch` against the rendered URL works; the `gh` CLI is also acceptable for KCP repo reads. No Confluent MCP tool exposes a `fetch-docs` capability — `WebFetch` is the only fetch path.

**Source map:**

| Topic | Live Source | Fetch When |
|---|---|---|
| KCP CLI commands, flags, subcommand behavior | [KCP Command Reference](https://confluentinc.github.io/kcp/latest/command-reference/) (structured CLI reference, generated from Cobra definitions) + [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) (README, repo context). Hub-page note: meta-refresh redirect; WebFetch may return redirect message — surface to user or navigate to current version per the failure-handling protocol. | Before recommending any command or flag |
| KCP state file schema | [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) (internal/types/state.go) | When parsing a state file |
| Zero-Cut prerequisites, flow, auth support | [KCP zero-cut guide](https://confluentinc.github.io/kcp/latest/getting-started-with-zero-cut-migrations/) | Switchover; any Gateway or Zero-Cut question |
| Gateway auth-swap scenarios (source-auth × target-auth matrix) | [KCP Gateway Switchover hub](https://confluentinc.github.io/kcp/latest/gateway-switchover/) — 8 documented scenarios (none/mTLS/SCRAM source × SASL-PLAIN/OAuth/mTLS target). Hub-page note: meta-refresh redirect; navigate from hub to specific scenario pages. | Auth target derivation in Plan stage; any question about specific source → target auth migration through Gateway |
| Cluster types, eCKU caps, per-eCKU throughput, partition rates, REST throughput, SLA | [cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html) | Plan; whenever capacity, throughput, or partitioning matters |
| Cluster Linking source requirements and compatibility | [cluster-linking docs](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking) | Any CL-related question or version compatibility check |
| CC authentication options and CL auth compatibility | [authenticate docs](https://docs.confluent.io/cloud/current/security/authenticate) | Auth mapping for each source auth type |
| mTLS availability by cloud | [mTLS overview](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/identity-providers/mtls/overview.html) and CC release notes | mTLS + Azure/GCP questions |
| Broker-side schema ID validation availability | [broker-side-schema-validation.html](https://docs.confluent.io/cloud/current/sr/broker-side-schema-validation.html) | When source uses schema ID validation |
| Schema Linking support matrix | [docs.confluent.io](https://docs.confluent.io) Schema Registry / Schema Linking pages | Schema migration path selection |
| Connector catalog (managed connector availability) | [connectors docs](https://docs.confluent.io/cloud/current/connectors) | Connector migration path decisions |
| CMU usage | [github.com/confluentinc/connect-migration-utility](https://github.com/confluentinc/connect-migration-utility) | Self-managed or MSK Connect connector migration |
| CC networking availability by cluster type | docs.confluent.io networking + cluster-types.html | Networking selection |
| Terraform provider resource schemas | [registry.terraform.io/providers/confluentinc/confluent](https://registry.terraform.io/providers/confluentinc/confluent) | Any Terraform generation or review |

Hardcoded values in this skill are labeled explicitly. The HARDCODED protocol in the hard-limits section applies: fetch the cited source each session, detect drift, prefer the live value if found.

## Reference File Index

One-line description of each reference file with when to read it:

- `references/assess.md` — Tool selection logic and red flag identification. Read when starting a migration.
- `references/plan.md` — Sizing, migration path selection for schemas and connectors, risk identification. Read after assessment.
- `references/kcp-commands.md` — When to use each KCP command and what to look for in the output. Read when user needs KCP guidance.
- `references/mcp-integration.md` — When to prefer MCP over CLI and how to interpret results. Read when MCP tools are available.
