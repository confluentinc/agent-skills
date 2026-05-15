# Plan

Plan turns the environment profile into architecture decisions: cluster type, networking, auth mapping, sizing, schema migration path, connector migration path, pre-migration requirements, and risks. High-level decision tables (cluster type, auth mapping, networking, switchover) live in SKILL.md. This file covers details and edge cases.

## Capacity Sizing Procedure

**Sizing is committed during Plan, not deferred.** Fetch the per-eCKU values live during the Plan conversation and compute eCKU counts with visible math. Do not leave the Technical Plan with "sizing TBD — requires live fetch" — that's deferral, not a plan. The only valid reasons to defer are (a) throughput data is missing from the state file (scan gap), or (b) the projected unit count exceeds the Enterprise eCKU cap and requires a Dedicated escalation conversation. Both get flagged explicitly, not left silent.

**Size to P95, not absolute peak.** Sizing to max observed throughput oversizes for workloads with rare outliers (seasonal spikes, annual Black Friday-style events). P95 captures sustained busy-period load while excluding once-a-year events. Enterprise is elastic — it auto-scales within its eCKU capacity — so spikes above P95 are absorbed by Enterprise elasticity; no need to provision for them.

1. Extract throughput time-series from KCP metrics (`kcp report metrics`) or the environment profile. If throughput data is missing, flag as a scan gap — recommend re-running `kcp report metrics` before committing sizing.
2. Compute **P95** of `BytesInPerSec` and `BytesOutPerSec` from the full `.metrics.results[].Values` array per cluster. Convert bytes/sec → MBps using 1 MB = 1,048,576 bytes (binary). Keep exact precision — do not pre-round.
3. Also record the **absolute peak** (max of the same Values array) for reference — report it in the Source Environment table alongside the P95 used for sizing so the user can see both.

**Manual-intake fallback when P95 is not provided.** Manual `migration-profile.yaml` profiles ask the user to provide P95 directly (`p95_ingress_mbps`, `p95_egress_mbps`) — preferred for accurate sizing. When P95 is provided, use it the same way you would CloudWatch-derived P95. **When only peak is provided** (the user fills `peak_ingress_mbps` / `peak_egress_mbps` and leaves P95 fields null), size on peak AND emit a **prominent overestimation flag** in the Technical Plan. The flag must appear in three places — not as a footnote:

- **At the top of the Sizing section** as a callout: *"⚠ Sizing computed on peak throughput, not P95. The recommended eCKU count below is **likely overestimated** vs. what P95 sizing would produce. Peak captures once-a-year spikes that Enterprise elasticity is designed to absorb; sizing for them inflates eCKU. To refine, provide P95 ingress/egress in MBps from CloudWatch `BytesInPerSec` / `BytesOutPerSec` over a representative 14-30 day window."*
- **As a row in Inputs & Default Assumptions** with `Sizing percentile = peak (P95 not provided — fallback)` and an Implication-of-Change note pointing to the overestimation.
- **As an Open Question** the user can close: "Provide P95 throughput to replace peak fallback in sizing."

Do NOT silently size on peak without surfacing the overestimation. The user needs to know the eCKU number is conservative-bound so they can decide whether to refine or accept.
4. **Spiky-workload flag.** If `peak > 2× P95` on any throughput metric, the workload is spiky. Surface as an Open Question: "Is this spike a steady-state or a seasonal event? P95 sizing handles sustained load + elasticity absorbs spikes. If you want to size for the absolute peak instead, say so and we'll re-run with max."
5. Fetch per-eCKU values live from [cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html):
   - per-eCKU ingress (MBps)
   - per-eCKU egress (MBps)
   - per-eCKU partition rate
   - Enterprise eCKU cap
   - (Also: PrivateLink eCKU cap, since it's lower than PNI)
6. Compute required units per cluster using P95 inputs at exact precision:
   - minimum eCKUs = max(p95_ingress_mbps ÷ per-eCKU-ingress, p95_egress_mbps ÷ per-eCKU-egress, user_partitions ÷ per-eCKU-partition-rate)
   - use **user partitions only** (internal topics don't migrate — they're recreated on the destination)
7. Apply headroom per the Inputs & Default Assumptions section (default 30%).
8. **CEIL (round up) the final eCKU count.** Can't provision a fractional eCKU. Rounding down would undersize. Always round up at the final step. Keep full precision in all intermediate steps.
9. Verify the projected unit count does not exceed the Enterprise eCKU cap. If it does, escalate to Dedicated per the hard-limits table in SKILL.md and state that escalation explicitly.
10. **Show the math in the Technical Plan.** Every cluster's recommended eCKU count should be traceable: P95 values, divisions, max selection, headroom applied, CEIL, final recommendation. No "~X eCKU, roughly" without the math behind it.
11. If projected eCKU exceeds the PrivateLink cap but fits under the PNI cap, recommend PNI networking for that cluster. This is a sizing-driven networking call, not an arbitrary preference.

**Tell the user P95 is the sizing basis.** In the Technical Plan, state explicitly that P95 is used (not peak) and note that Enterprise elasticity absorbs above-P95 spikes. If the user wants a different percentile (P99 for more conservative, or absolute peak for worst-case provisioning), capture that in Inputs & Default Assumptions and re-run sizing. The user should know what percentile drives their plan so they can challenge it.

**Required live fetches during Plan.** Before committing the Technical Plan, fetch these three sources live — do not rely on cached knowledge of caps, version floors, or Zero-Cut prerequisites. **Use `WebFetch` for all three — not `curl`, `wget`, `python3 -c`, or heredoc scripts that strip HTML. `WebFetch` handles HTML→markdown and targeted extraction in one call; pass a focused extraction prompt rather than fetching raw content and parsing.** See SKILL.md "Fetch tool" directive for the full rule.

1. **[cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)** — per-eCKU ingress, per-eCKU egress, per-eCKU partition rate, Enterprise eCKU cap (PNI and PrivateLink), cluster-type feature matrix. Without this fetch, the sizing math is guesswork.
2. **[Cluster Linking source requirements](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking)** — minimum source Kafka version, auth support matrix, supported source topologies (including MSK Express broker tier if present). Without this fetch, CL compatibility claims rest on stale general knowledge.
3. **[KCP zero-cut guide](https://confluentinc.github.io/kcp/latest/getting-started-with-zero-cut-migrations/)** — current Zero-Cut prerequisites (Kubernetes distribution, CP licensing, minimum KCP version, auth compatibility). Required if the Plan recommends Zero-Cut as the switchover approach (the default). Without this fetch, the Switchover Approach section is incomplete.

All three fetches should happen before writing the Technical Plan — not in a later iteration. Cite them inline per the Technical Plan Conventions (Style A).

**Use user topics/partitions for sizing math, not totals.** Internal topics (`__consumer_offsets`, Connect framework topics, Streams changelogs, MSK-managed topics) are recreated by Kafka, Connect, or Streams on the destination — they don't migrate and don't consume destination capacity the way user topics do. When computing partition counts for the Row 1 capacity check, use `.topics.summary.total_partitions` (user) from the state file, not `total_partitions + total_internal_partitions`. When reporting counts in the Technical Plan, follow the Assess convention: `user + internal = total` so both numbers are visible, but anchor sizing decisions to user.

**Run each `jq` query as its own Bash tool call.** Per the one-command-per-Bash-call principle in SKILL.md (Skill Conduct), issue each state-file query (throughput extraction, tiered storage peak, broker instance-type enumeration, Row 16 cost reconciliation) as a separate Bash invocation rather than batching into a compound shell command (variable assignment + chained `jq` calls). Applies to Plan-stage sizing math, tiered storage reads, and any multi-query sequence.

## Networking Edge Cases

- **Multi-VPC source.** Transit Gateway is the only option that avoids per-VPC peering — this escalates to Dedicated.
- **Cross-region migration.** Factor in data transfer costs and latency. Cluster Linking between regions is supported but adds per-GB transfer cost.
- **On-prem source.** Need VPN or Direct Connect to AWS, then PrivateLink/PNI from there. Treat as multi-hop networking.
- **Enterprise PNI on AWS.** Customer provides the networking substrate; Confluent delivers via ENIs. Higher throughput headroom than PrivateLink. Verify current ENI requirements against [networking docs](https://docs.confluent.io/cloud/current/networking/) live.
- **Enterprise PrivateLink on AWS.** Default private connectivity. eCKU cap applies; verify current cap per networking type against cluster-types.html.

## Networking Choice — sizing-driven by default

**Per-cluster networking is chosen by projected capacity, not by preference.** Default to PrivateLink unless the cluster's projected eCKU (including burst-to-peak elastic headroom) approaches the PrivateLink cap.

**Peak-burst eCKU formula (direct capacity — not ratio-based).** The elastic capacity the cluster needs at peak throughput:

```
peak_burst_eCKU = CEIL(max(peak_ingress ÷ per-eCKU-ingress, peak_egress ÷ per-eCKU-egress))
```

This is the raw eCKU count required to absorb absolute peak throughput. Use the absolute peak values from CloudWatch (`Cluster Aggregate - BytesInPerSec/BytesOutPerSec | max`), not P95.

**Do NOT compute peak-burst eCKU as `peak/P95 ratio × P95-sized eCKU`.** That double-counts the 30% headroom already baked into the P95 sizing and produces inflated numbers (e.g., 1.60× × 8 eCKU = 12.8 eCKU, vs. the correct 1,594 ÷ 180 = 8.86 eCKU). The ratio-based formulation is wrong.

**Decision rule per cluster — apply the 80% safety threshold:**

- **PrivateLink** when `peak_burst_eCKU < 80% of PrivateLink cap` (i.e., ≤ 7 eCKU with a 10 eCKU cap). Simpler to operate, lower overhead.
- **PNI** when `peak_burst_eCKU ≥ 80% of PrivateLink cap`. At 80%+ the cluster has no growth margin under the cap and is **fragile to throughput drift during bursts** — if peak edges up by even 10-20% post-migration, the cluster hits the ceiling. Use PNI's higher cap (32 eCKU) to preserve growth headroom.

Show both numbers in the Networking Decision justification: the peak-burst eCKU and its percentage of the PrivateLink cap. Example: "P95 sizing 8 eCKU; peak egress 1,594 MBps ÷ 180 MBps/eCKU = 8.86 eCKU peak-burst = 88% of 10 eCKU PrivateLink cap → **PNI** (only 1.14 eCKU headroom at peak, fragile)."

**Every networking choice in the Plan must carry a sizing-driven justification in the Networking Decision table.** If a cluster's projected eCKU is well under PrivateLink cap, the justification is "N eCKU against M cap, X% headroom — PrivateLink." Don't pad the reason.

**Operational-consistency exceptions are permitted but must be flagged explicitly.** If the user or the skill prefers a uniform networking substrate (e.g., all-PNI for fleet-wide operational consistency) even where sizing doesn't require it, that's a valid architectural preference — but treat it as an **override**, not a default:

- Make the sizing-driven recommendation per cluster first (per the table above).
- Then flag: "If the user prefers a single networking pattern for operational consistency, alternative is all-PNI. This is a preference, not a capacity requirement."
- Do NOT use "operational consistency" as the primary justification for a cluster whose projected eCKU doesn't warrant PNI. "Operational consistency" is valid as an override, not as a sizing reason.

This keeps the Plan's recommendations traceable to capacity math and separates preference decisions from sizing decisions so the user can evaluate both on their own merits.

## Schema Migration Path Selection

Path depends on source SR state. Version cutoffs (e.g., Schema Linking's CP version floor) and source-support are product facts — verify against the live [docs.confluent.io Schema Registry / Schema Linking docs](https://docs.confluent.io) before committing.

| Source SR | Path |
|---|---|
| Confluent SR (meets Schema Linking version floor) | Schema Linking |
| Confluent SR (below Schema Linking floor) | REST API export/import |
| AWS Glue | REST API export/import (verify Schema Linking source-support live) |
| Karapace | REST API export/import |
| None — adopt SR during migration | Provision CC SR, register initial schemas, then migrate data. For greenfield schema discovery in client code, cross-link to the `kafka-schema-registry` skill. |
| None — migrate without schemas | Skip SR steps. Record the explicit user choice. Proceed with data migration. |

## Connector Migration

For each source connector enumerated in Assess (or detected via state file at `aws_client_information.connectors[]`), the Plan looks up the CC managed connector catalog and classifies the migration path per-connector. Classification is at the individual-connector level, not the source-type level — MSK Connect and self-managed Connect determine which `kcp create-asset migrate-connectors {msk|self-managed}` sub-command emits the asset, not whether the connector goes to CC managed.

| Source Connector | CC Managed Equivalent | Path |
|---|---|---|
| `[name 1]` | [`cc-<name>.html`](https://docs.confluent.io/cloud/current/connectors/index.html) | CMU + `kcp create-asset migrate-connectors {msk|self-managed}` per source type. |
| `[name 2]` | Not available | Connect with your Confluent account team — options include custom connector, alternative target architecture, or staying on a self-managed Connect cluster (out of scope for full CC migration). |
| `[name 3]` | [`cc-<name>.html`](https://docs.confluent.io/cloud/current/connectors/index.html) (version or config differs materially) | CMU + `kcp create-asset migrate-connectors`. For substitution-specific guidance (e.g., Debezium 1.x → V2 config key differences), connect with your Confluent account team — these specifics are not published as a structured Confluent doc. |

Lookup source: [CC connector catalog](https://docs.confluent.io/cloud/current/connectors/index.html). Verify live before committing — managed connector availability evolves.

Operational concerns are not part of the Technical Plan. Connector-level operational planning belongs in the operational plan the customer's IT team builds on top.

## Risk Factors (flag to the user)

- High partition count (>10K) → longer mirror setup, more CL resources.
- Large ACL count (>100) → review generated RBAC bindings carefully.
- EOS/transactions in use → not supported on Cluster Linking.
- Kafka Streams apps → changelog topics should NOT be mirrored. Plan state-store rebuild time post-cutover.
- Multi-region clusters → each region may need a separate CC environment and CL setup.
- Cross-cloud target (source AWS, target Azure/GCP) → review networking and auth feature availability per cluster-types.html and mTLS overview doc live.

## Tiered Storage

**Tiered storage volume is the hidden migration cost.** Cluster Linking replicates what's on brokers + new writes. Historical data in tiered storage (S3-backed on MSK) does NOT transfer automatically. If consumers need historical data on CC, plan for bulk backfill (slow + expensive) or keep the source cluster accessible during the retention window.

Read peak tiered volume per cluster from the state file metrics — not from EBS provisioned capacity (which is a different thing):

- Correct source: `.regions[].clusters[].metrics.results[] | select(.Label == "Cluster Aggregate - TotalRemoteStorageUsage(GB)") | .Values | max`
- Wrong source: `BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.VolumeSize × NumberOfBrokerNodes` — that's provisioned capacity, not actual data volume. They can differ by a lot.

Also check the cluster's `StorageMode` field — clusters with `StorageMode: "TIERED"` use tiered storage; Express brokers don't have EBS tiered storage (handled internally).

Report the peak tiered volume per cluster in the Technical Plan. Include it in the risk conversation: "backing up X TB of tiered data would take Y days to transfer at Z MBps sustained CL throughput — decide if that cost is worth it vs. keeping source accessible."

## Open Questions Don't Block Plan Production

When the user says "write the Plan" (or equivalent — "produce the Technical Plan," "commit the Plan," "finalize Plan"), produce a **full Plan per the template below**. Not Assess output. Not a "Plan-Stage Preview." Not a hedged directional document.

This is a stage transition the skill has to get right even when the user has open questions:

- **Unresolved open questions do NOT defer, hedge, or postpone the Plan.** They go in the Open Questions section. The rest of the Plan commits to recommendations with visible working assumptions.
- **State working assumptions explicitly.** If the user hasn't confirmed which auth types clients use, the Plan picks an assumption (e.g., "assuming SCRAM-dominant based on broker config") and notes it. The user reacts to a concrete recommendation, which is more productive than answering questions in the abstract.
- **Use the literal `Working assumption:` label for every numbered Open Question — no exceptions.** Every item in the Open Questions section must have a corresponding `Working assumption:` line in the section that owns the decision the OQ rests on. The literal label is the audit handle — it lets the user scan the Plan and trace exactly which decisions rest on unconfirmed assumptions. Phrases like "Assuming X..." or "Default: Y..." do not substitute; use the literal `Working assumption:` prefix on the line. If the natural section is absent (e.g., no Tiered Storage section because the fixture omits it), put the label on the next-closest section that owns the underlying decision — Sizing for percentile / throughput OQs, Risks for not-in-profile or unknown-condition OQs, Pre-Migration Workstream for client-inventory / pre-cutover-work OQs. **A `Working assumption:` line inside the Open Questions entry itself does NOT satisfy the rule — the label must appear in the decision section that owns the recommendation.** **Self-check before delivering the Plan: count the items in Open Questions, then count the `Working assumption:` lines outside the Open Questions section. The two counts must match. If they don't, add the missing labels before delivering.**
- **Never output Assess content in place of the Plan.** If the user says "write the Plan," the deliverable is a Technical Plan following the template — even if the state file has gaps or the user has unanswered questions.

**The only two conditions that block Plan commitment:**

1. A cluster's projected sizing exceeds the Enterprise eCKU cap and the user must decide on Dedicated escalation before the networking/cluster-type rows can be filled. Even here, produce the rest of the Plan and surface the escalation decision as the single blocker.
2. The state file has scan gaps that make sizing impossible (no throughput data). Even here, produce every other section and explicitly flag sizing as blocked by the scan gap.

In all other cases, produce the full Plan. Capture gaps in Open Questions; document assumptions in-line next to the decision they support.

## Technical Plan Template — structure every plan the same way

Every Technical Plan output uses the same prologue, section ordering, and table formats. This makes Technical Plans comparable across migrations, prevents silent gaps (missing sizing hidden by section renaming), and gives internal reviewers and customers a consistent shape to expect.

**Output prologue — emit verbatim at the top of every Technical Plan output.**

The skill renders the following block verbatim as the prologue (before Section 1 Header). The title "Technical Plan" honestly signals scope: this artifact covers technical architecture and migration approach, not the customer's full operational plan.

````markdown
# Technical Plan

## About this Technical Plan

This Technical Plan is a **starting point** for your migration — Confluent
Cloud architecture and migration approach. It's the technical foundation
your IT team builds a full operational plan on top of, not the operational
plan itself. The Switchover Approach and Pre-Migration Workstream sections
describe the required technical work and patterns; the detailed runbook
(timing, approvals, ownership) is built by your IT team using these as
input.

**What this covers:** Target architecture (cluster type, sizing, networking,
auth), migration approach (switchover, schema, connectors), high-level
pre-migration workstream, and risks surfaced from the source assessment.

**What this does not cover:** Operational planning unique to your
organization, or commercial, legal, and contractual review.

**How to use it:** Share with your Confluent account team for operational
guidance specific to your environment. Validate live-fetched facts (cluster
capacities, Cluster Linking version floors, Zero-Cut prereqs) against
current docs.confluent.io before acting. Re-run as your source environment
or requirements change.
````

After the prologue, render the 13 required sections starting with Section 1 Header.

**Required sections — always present, in this order:**

1. **Header.** Source, target, state file path, drafted date. One-liner each.
2. **Inputs & Default Assumptions.** Parameters the Plan is built on. Surface them upfront so the user can challenge any default before reading recommendations that depend on them. Required rows: Sizing percentile, Headroom, SLA target, Target cloud/region, Schema posture. Add others as relevant. Format: 4-column table — Parameter | Value | Source | Implication of Change.
3. **Summary.** BLUF. Max 5 bullets covering: cluster type recommendation, networking recommendation, switchover approach, the one workstream most likely to drive the timeline, anything else urgent. The "one-page if you only read this" version.
4. **Source Environment.** Cluster table (standard columns below) + auth posture (server-side) + networking topology + VPC / region summary.
5. **Sizing.** Per-cluster eCKU math with citations. Either committed numbers with visible formulas, or explicit "deferred because [scan gap | Dedicated escalation conversation needed]" — never silently missing.
6. **Cluster Type Decision.** Enterprise vs Dedicated per cluster. If any cluster is Dedicated, cite which hard-limit row triggered the escalation.
7. **Networking Decision.** Per-cluster networking choice (PrivateLink, PNI, VPC Peering, TGW, public) with justification.
8. **Auth Approach.** Target CC auth per source auth type. If deferred, list the scenarios and name what closes the decision.
9. **Switchover Approach.** Pattern × mechanism (incremental + Gateway recommended; big-bang + Gateway when single window desired; big-bang + Manual CL fallback when Zero-Cut prereqs not met). Dual-write described for completeness only — no Confluent-specific tooling. Note that Incremental + Manual CL is not recommended (operationally heavy without the Gateway's atomic flip). Prerequisites fetched live at Switchover stage — don't cache them in Plan.
10. **Pre-Migration Workstream.** What has to happen before migration proper. Commonly: IAM→SCRAM auth migration, Kafka version upgrade, client inventory reconstruction. Include rough duration where known.
11. **Risks.** Table format (standardized below).
12. **Open Questions.** Numbered list with owner (User / Live fetch / etc.). These are the specific items that close before Provisioning.
13. **Next Step.** Single next action. Usually "confirm X, then move to Provision."

**Inputs & Default Assumptions — required rows and defaults:**

| Parameter | Default Value | Source | Implication of Change |
|---|---|---|---|
| Sizing percentile | **P95** of CloudWatch throughput | Default; user can override | Raising to P99 sizes for more outlier events; lowering sizes to typical load only. Absolute peak would size for once-a-year spikes. Enterprise elasticity absorbs above-P95 spikes. |
| Headroom | **30%** | Default; user can override | 50% adds ~1-2 eCKU on high-throughput clusters; use if meaningful YoY growth is expected. |
| SLA target | **99.9%** → 1 eCKU minimum | Default (user-confirmed if specified) | 99.99% raises the cluster minimum to 2 eCKU. Relevant for tiny clusters; no effect on larger clusters already above the floor. |
| Target cloud/region | **Same as source** | Match source unless user states otherwise | Cross-region adds CL transfer cost and latency. Cross-cloud may limit feature availability (mTLS on Azure/GCP, etc.) — verify live. |
| Schema posture | **Match source** (if SR exists, migrate; if schemaless, stay schemaless unless user opts into adoption) | Default; user decision | Adopting SR during migration adds a pre-data workstream but enables governance. Staying schemaless preserves velocity but defers adoption cost. |

Add other parameter rows when the Plan's analysis depends on a non-obvious default (e.g., "Consumer retention window for tiered storage backfill" when tiered volume is large). If the user has explicitly provided a value, update the Source column ("User-provided") so it's visible.

**Conditional sections — produce as discrete `##` headings when triggered.**

When a conditional section's trigger fires, produce a discrete `## <Section Name>` heading in the Technical Plan — not a bullet inside another section, not a column in a table, not a line in the Summary. The cluster table can carry a column for the same data (e.g., the Tiered (GB) column), and Summary / Risks / Pre-Migration entries can reference the section, but those references do NOT replace the section. The discrete heading is the structural commitment; the column and cross-references are supporting surfaces.

- **Tiered Storage.** Trigger: any cluster has `StorageMode: "TIERED"` (KCP state file) OR the manual profile records tiered usage OR the manual profile does not record StorageMode and tiered usage is unknown. When triggered, produce a `## Tiered Storage` heading with per-cluster peak volume cited from `metrics.results[] | "Cluster Aggregate - TotalRemoteStorageUsage(GB)" | max` (KCP) or the manual-profile equivalent (or an explicit "Profile does not record StorageMode — Open Question" note when unknown), plus a backfill-cost vs. keep-source-accessible discussion.
- **Schema Migration.** Trigger: source has Schema Registry OR user is considering adoption during migration. When triggered, produce a `## Schema Migration` heading. Omit (don't fold elsewhere) when continuing schemaless — record the opt-out in Open Questions instead.
- **Connector Migration.** Trigger: any connectors exist (MSK Connect managed OR self-managed Connect). When triggered, produce a `## Connector Migration` heading. Omit (don't fold elsewhere) when zero connectors — note the absence in the Summary.
- **Multi-VPC / Multi-Region considerations.** Trigger: source spans VPCs or regions, or MSK Multi-VPC Private Connectivity is detected. When triggered, produce a `## Multi-VPC / Multi-Region` heading.
- **Cluster Linking special considerations.** Trigger: non-standard CL constraint (version concerns, Express broker tier, cross-region, tiered-storage backfill complexity). When triggered, produce a `## Cluster Linking — Special Considerations` heading.

**The cluster-table column does not replace the section.** When `Tiered (GB)` shows non-`—` values for any cluster, the Tiered Storage section is required. Similarly, mentioning tiered facts in Summary, Pre-Migration, or Risks does not satisfy the trigger — those references can co-exist with the section but do not substitute for it.

**Standardized cluster table columns (always these 8):**

| Cluster | Brokers × Instance | Kafka | Topics (user+internal=total) | Partitions (user+internal=total) | P95 In / Out (MBps) | Peak In / Out (MBps) | Tiered (GB) |

P95 column drives sizing. Peak column is reference — shows absolute max observed, flags spiky workloads when `peak > 2× P95`. Tiered column shows peak remote storage in GB for tiered clusters, `—` for non-tiered (Express or non-tiered Provisioned).

**Standardized sizing table columns (always these 7):**

| Cluster | P95 In ÷ per-eCKU-in | P95 Out ÷ per-eCKU-out | User Partitions ÷ per-eCKU-partitions | Max | +Headroom | Verdict |

Forces the math to be visible. Uses **P95** values (from the CloudWatch time-series), not absolute peak. Final eCKU = CEIL(Max × (1 + headroom_fraction)). Verdict column = final eCKU + cluster type + networking decision (e.g., "14 eCKU Enterprise, PNI").

**Each cell must carry inline parenthetical citations.** Citations in a pre-table header line or a post-table "Math, traceable" paragraph do NOT satisfy the inline-citation rule from "Technical Plan Conventions — cite every number" — those are useful supplements but the table cells themselves must be verifiable in place. The per-eCKU divisors (24, 72, 1000) cite cluster-types.html inline; the user-supplied dividends (P95 ingress/egress in MBps, partition count) cite their profile field path inline.

Example cell — sized to fit, citations inline:

| `payments-prod` | 280 (`clusters[0].p95_ingress_mbps`) ÷ 24 ([cluster-types.html]) = 11.67 | 840 (`clusters[0].p95_egress_mbps`) ÷ 72 ([cluster-types.html]) = 11.67 | 3,568 (`clusters[0].user_partition_count`) ÷ 1,000 ([cluster-types.html]) = 3.57 | 11.67 | × 1.30 = 15.17 → CEIL = **16** | **16 eCKU Enterprise, PNI** |

Bracketed `[cluster-types.html]` is shorthand — render as a markdown link to `https://docs.confluent.io/cloud/current/clusters/cluster-types.html`. The first cell in a row may carry the link in full; subsequent cells in the same row may abbreviate to `[cluster-types.html]` if the row gets dense. The point is verifiability inline — the user must be able to trace each number from inside the table cell without scrolling to adjacent prose.

**Standardized risks table columns:**

| Risk | Severity | Mitigation |

Severity = High / Medium / Low / Unknown. Unknown is acceptable when the scan didn't capture the data needed to assess (e.g., "EOS/transactions in use? Unknown — confirm with owning teams").

**Naming conventions — don't vary these across Technical Plans:**

- "Source Environment" (not "Environment Profile" or "Peak throughput")
- "Sizing" (not "Capacity Sizing Procedure")
- "Cluster Type Decision"
- "Networking Decision"
- "Auth Approach"
- "Switchover Approach"
- "Pre-Migration Workstream" (not "Pre-Migration Plan" or "Pre-Migration Requirements")
- "Open Questions" (not "Open Decisions" or "Questions to Close")
- "Next Step"
- "Connector Migration" (not "Connectors" — renamed in B5; the conditional heading is `## Connector Migration`)

## Technical Plan Conventions — cite every number

**Every number in the Technical Plan must be traceable to its source.** The user should be able to open the state file or the cited doc and verify each number. Two annotation styles — pick one per Technical Plan and stay consistent:

**Style A — Inline parenthetical citations.** Each number gets a short parenthetical source tag at first use.
> Peak egress 1,594 MBps (`metrics.results[] | BytesOutPerSec | max`). Enterprise eCKU cap 32 ([cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html), "eCKU/CKU comparison" table).

**Style B — Data Sources appendix.** A section at the end of the Technical Plan listing each cited number with its state file path or doc URL. Numbers in the body reference the appendix by shorthand (e.g., a superscript or bracket reference).

Either style is acceptable; Style A is usually better for migration plans (shorter, easier to eyeball). Apply the style consistently.

**Citation paths by intake mode — cite the field path inline, not just the source filename.** Naming the source file in the metadata header is not sufficient. Each profile-sourced number must carry an inline parenthetical field-path citation at first use. The path style depends on the intake mode:

| Intake mode | Citation path style | Examples |
|---|---|---|
| KCP state file | `jq`-style path into the JSON | `regions[0].clusters[].aws_client_information.msk_cluster_config.Provisioned.BrokerNodeGroupInfo.InstanceType`; `regions[0].clusters[].kafka_admin_client_information.topics.summary.topics`; `regions[0].clusters[].metrics.results[] \| "Cluster Aggregate - BytesInPerSec" \| max` |
| Manual `migration-profile.yaml` | YAML key path from the top of the file | `clusters[0].broker_count`; `clusters[0].broker_instance_type`; `clusters[0].peak_ingress_mbps`; `clusters[0].peak_egress_mbps`; `clusters[0].user_partition_count`; `clusters[0].auth_types`; `schema_registry.type`; `schema_registry.subject_count`; `connectors.msk_connect_count`; `connectors.connector_types`; `source.kafka_version`; `source.region` |

The point of inline citation is verifiability — the user can open the profile (KCP JSON or manual YAML) and locate each number by the cited path. A single header-level reference to the profile filename does not satisfy this — every per-fact number gets its own field path.

**What must be cited:**

| Category | Must cite |
|---|---|
| Profile-sourced per-cluster facts (KCP state file OR manual `migration-profile.yaml`) | broker count, instance type, Kafka version, auth types, topic counts (user + internal), partition counts (user + internal), peak throughput (ingress, egress, messages), tiered storage peak volume, ACL count, networking topology, region |
| Profile-sourced environment-level facts | Schema Registry type and subject count, connector counts and types, monthly cost, migration drivers |
| Live-fetched product facts | per-eCKU caps (ingress, egress, partitions), Enterprise eCKU cap (PNI and PrivateLink), CL source-version floor, feature-availability facts (e.g., mTLS by cloud, broker-side SR validation) |
| Computed values | eCKU sizing (cite the inputs and the formula — if inputs are cited, the computation is traceable) |

**Example — full citation discipline on a sizing row, KCP state file source:**

> `dataplatformEvents`: peak egress 1,594 MBps (`metrics.results[] | BytesOutPerSec | max`) ÷ 180 MBps per eCKU ([cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html), Enterprise column) = 8.9 eCKU minimum, +50% headroom = **14 eCKU**. Below Enterprise cap of 32 (same doc); above PrivateLink cap of 10 → **PNI networking required**.

**Example — full citation discipline on a sizing row, manual `migration-profile.yaml` source:**

> `payments-prod`: peak egress 960 MBps (`clusters[0].peak_egress_mbps`) ÷ 180 MBps per eCKU ([cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html), Enterprise column) = 5.33 eCKU minimum, +30% headroom = **7 eCKU**. Below Enterprise cap of 32 (same doc); below PrivateLink cap of 10 → **PrivateLink**.

**Example — full citation discipline on a Source Environment cluster row, manual profile source:**

> `payments-prod`: 9 brokers × `kafka.m5.xlarge` (`clusters[0].broker_count`, `clusters[0].broker_instance_type`); Kafka 3.4.0 (`source.kafka_version`); 112 user + 8 internal = 120 total topics (`clusters[0].user_topic_count`, `clusters[0].internal_topic_count`); 3,568 user + 32 internal = 3,600 total partitions (`clusters[0].user_partition_count`, `clusters[0].internal_partition_count`); peak 320 MBps in / 960 MBps out (`clusters[0].peak_ingress_mbps`, `clusters[0].peak_egress_mbps`); auth: AWS IAM (`clusters[0].auth_types`); networking: single-VPC private (`clusters[0].network_accessibility`, `clusters[0].vpc_topology`); SR: AWS Glue, 94 subjects (`schema_registry.type`, `schema_registry.subject_count`); 3 MSK Connect managed connectors, types s3-sink + lambda-sink (`connectors.msk_connect_count`, `connectors.connector_types`).

If a number can't be cited (fabricated, inferred without evidence, or from training data), either remove it from the doc or explicitly flag it as an estimate: "Estimated at ~X — no profile field or live doc supports this number."

## Done Criteria (validate before moving to Provision)

- Cluster type chosen per the SKILL.md decision tree
- Networking approach chosen per source accessibility
- Auth mapping chosen for every source auth type
- Sizing calculated (CKUs/eCKUs with headroom) or deferred to eventsizer.io with the gap recorded
- Schema migration path chosen (Schema Linking, REST API, adopt-SR, or continue-schemaless — per the table above)
- Connector migration path chosen per source connector
- Pre-migration requirements identified and flagged (IAM → SCRAM conversion if Zero-Cut, Kafka version below current CL floor, etc.)
- Risks documented and acknowledged by the user
- Every number in the Technical Plan carries a citation (Style A inline or Style B appendix) per Technical Plan Conventions above

**Downstream platform scope.** Tableflow, Flink, and lakehouse integrations are out of scope for this skill. Customers can enable them post-migration. If a user asks about downstream pipelines, point them to the relevant docs and return focus to the migration path.

## Source of Truth

- Cluster type specs, capacity caps, networking feature availability, SLA — [cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html). Fetch live before recommending a cluster type or citing a cap.
- Cluster Linking source requirements, auth compatibility — [cluster-linking docs](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking). Fetch live.
- Schema Linking source matrix — [docs.confluent.io](https://docs.confluent.io) Schema Registry / Schema Linking pages. Fetch live before committing to Schema Linking as the path.
- Connector catalog — [connectors docs](https://docs.confluent.io/cloud/current/connectors). Fetch live to check managed-connector availability.
- Pricing and commercial quote are out of scope. Direct users to the public [cost estimator](https://www.confluent.io/pricing/cost-estimator/) as the Sales handoff.
