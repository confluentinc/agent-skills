# Plan

Plan turns the environment profile into architecture decisions: cluster type, networking, auth mapping, sizing, schema migration path, connector migration path, pre-migration requirements, and risks. High-level decision tables (cluster type, auth mapping, networking, switchover) live in SKILL.md. This file covers details and edge cases.

## Voice in Plan output

Plan artifacts go to the customer's account team, IT team, and decision-makers. They are user-facing artifacts, not internal development docs. Apply the SKILL.md Voice principle in every Plan output:

- **Do NOT use MVP-style framing** anywhere in the Plan — "MVP", "MVP scope", "next iteration", "future version", "scoped for later", "v0.1", "current skill release". These are skill-development internals and have no place in a customer-facing Technical Plan. This includes the Next Step section, footnotes, parentheticals, and any "this skill doesn't cover..." asides.
- **Do NOT enumerate out-of-scope downstream stages by name** in user-facing language. Phrases like "Provision, Migrate, Switchover, Monitor are outside this skill's scope" are roadmap leakage. When downstream execution needs to be referenced, name what the user does next ("engage your Confluent account team for Provision and Migrate execution") without framing it as a scope boundary of the skill.
- **State what is in scope; redirect on out-of-scope intent only when the user signals it.** The Plan is a Technical Plan covering architecture and migration approach. Operational planning, commercial review, legal review, and downstream execution belong with the customer's account team and IT team — already framed positively in the "About this Technical Plan" prologue. No additional "this skill doesn't do X" disclaimers needed.

## Capacity Sizing Procedure

**Sizing is committed during Plan, not deferred.** Fetch the per-eCKU values live during the Plan conversation and compute eCKU counts with visible math. Do not leave the Technical Plan with "sizing TBD — requires live fetch" — that's deferral, not a plan. The only valid reasons to defer are (a) throughput data is missing from the state file (scan gap), or (b) the projected unit count exceeds the Enterprise eCKU cap and requires a Dedicated escalation conversation. Both get flagged explicitly, not left silent.

**Size to P95, not absolute peak.** Sizing to max observed throughput oversizes for workloads with rare outliers (seasonal spikes, annual Black Friday-style events). P95 captures sustained busy-period load while excluding once-a-year events. Enterprise is elastic — it auto-scales within its eCKU capacity — so spikes above P95 are absorbed by Enterprise elasticity; no need to provision for them.

1. Extract throughput time-series from KCP metrics (`kcp report metrics`) or the environment profile. If throughput data is missing, flag as a scan gap — recommend re-running `kcp report metrics` before committing sizing. **If user partition count is missing because the deep scan did not complete** (e.g., `topics.details` empty while `msk_cluster_config` is populated — see the silent-scan-failure fingerprint in `references/assess.md`), treat it as a foundational gap: never assume or fabricate a partition count. The eCKU result is then a **throughput-only lower bound, NOT a committable sizing** — the partition term of the `max()` (step 6) is uncomputed, and it is the term that most often dominates for high-partition clusters, so the true eCKU can exceed the throughput floor by a large multiple. This is the opposite risk from the peak fallback below: peak OVER-estimates (safe to provision), an uncaptured partition dimension UNDER-estimates (dangerous to provision on). Because the downside is a customer provisioning far too little, surface it prominently in FOUR places — never a buried flag:

   - **Top-of-Sizing callout:** *"⚠ Sizing is NOT committable for provisioning. User partition counts were not captured (the deep scan did not complete), so the eCKU figures below are a throughput-only lower bound. The partition dimension — which often dominates for high-partition clusters — is uncomputed and could raise the count by a large factor. Do NOT provision on these numbers. Capture partition counts via re-scan (`kcp scan clusters` from inside the VPC) or manual intake before committing sizing."*
   - **Verdict column:** render as `≥ N eCKU (throughput-only lower bound — NOT committable; partition dimension uncaptured)`. Never render a clean `N eCKU Enterprise, PNI` that reads as a final recommendation.
   - **Summary bullet:** the cluster-type / sizing bullet states sizing is not committable pending partition capture — stated in the Summary itself, not only in a later section.
   - **Open Question AND a High-severity Risk row:** both carry it, with the "could understate the true requirement by a large factor" rationale so the reader understands the direction of the error.

   Throughput stays degradable per the peak fallback below; partition/scale does not.
2. Compute **P95** of `BytesInPerSec` and `BytesOutPerSec` from the full `.metrics.results[].Values` array per cluster. Convert bytes/sec → MBps using 1 MB = 1,048,576 bytes (binary). Keep exact precision — do not pre-round.
3. Also record the **absolute peak** (max of the same Values array) for reference — report it in the Source Environment table alongside the P95 used for sizing so the user can see both.

**Manual-intake fallback when P95 is not provided.** Manual `migration-profile.yaml` profiles ask the user to provide P95 directly (`p95_ingress_mbps`, `p95_egress_mbps`) — preferred for accurate sizing. When P95 is provided, use it the same way you would CloudWatch-derived P95. **When only peak is provided** (the user fills `peak_ingress_mbps` / `peak_egress_mbps` and leaves P95 fields null), size on peak AND emit a **prominent overestimation flag** in the Technical Plan. The flag must appear in three places — not as a footnote:

- **At the top of the Sizing section** as a callout: *"⚠ Sizing computed on peak throughput, not P95. The recommended eCKU count below is **likely overestimated** vs. what P95 sizing would produce. Peak captures once-a-year spikes that Enterprise elasticity is designed to absorb; sizing for them inflates eCKU. To refine, provide P95 ingress/egress in MBps from CloudWatch `BytesInPerSec` / `BytesOutPerSec` over a representative 14-30 day window."*
- **As a row in Inputs & Default Assumptions** with `Sizing percentile = peak (P95 not provided — fallback)` and an Implication-of-Change note pointing to the overestimation.
- **As an Open Question** the user can close: "Provide P95 throughput to replace peak fallback in sizing."

Do NOT silently size on peak without surfacing the overestimation. The user needs to know the eCKU number is conservative-bound so they can decide whether to refine or accept.
4. **Spiky-workload flag.** If `peak > 2× P95` on any throughput metric, the workload is spiky. Surface as an Open Question: "Is this spike a steady-state or a seasonal event? P95 sizing handles sustained load + elasticity absorbs spikes. If you want to size for the absolute peak instead, say so and we'll re-run with max."
5. Fetch per-eCKU values live from the `.md` variant of cluster-types: `https://docs.confluent.io/cloud/current/clusters/cluster-types.md` (per the SKILL.md "Fetch via the `.md` extension on docs.confluent.io URLs" directive — the `.html` page truncates the comparison table when WebFetched). The cited URL in the Plan artifact stays `.html` for user navigation: [cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html). Five values to extract from the Enterprise eCKU column of the "eCKU/CKU comparison" table:
   - per-eCKU ingress (MBps) — row label "Ingress (MBps)"
   - per-eCKU egress (MBps) — row label "Egress (MBps)"
   - per-eCKU partition rate — row label "Partitions (pre-replication)"
   - Enterprise eCKU cap (PNI) — from prose in scaling considerations / networking limits section
   - Enterprise eCKU cap (PrivateLink) — same section (lower than PNI; needed for the PrivateLink-vs-PNI sizing-driven networking check in step 11)

5a. **Verify the fetch before proceeding.** Even with `.md` URLs, a fetch can return truncated content, redirect to a different page, or extract a value from the wrong row. Sizing math committed against a wrong per-eCKU value produces a wrong eCKU verdict that downstream readers cannot detect from the Plan. Catch the failure here, not after the customer has read the Plan.

   - **Echo the five extracted values explicitly** in the Sizing section before doing any math. Render as a small `Sizing Inputs (live from cluster-types.md)` table — per-eCKU ingress, per-eCKU egress, per-eCKU partition rate, Enterprise eCKU cap (PNI), Enterprise eCKU cap (PrivateLink). For each value, cite the row label exactly as it appeared in the fetched markdown.
   - **Each value must be a concrete number.** "Partially obscured", "see doc", "TBD", "approximately X", "values returned truncated in fetch", "N/A" — all unacceptable. If the live fetch returned any of those for any of the five values, the fetch failed.
   - **Verify the row labels match expected structure.** Each value must be cited with a row label that names the dimension explicitly. Expected anchors:
     - Ingress value cited with "Ingress (MBps)" (case-insensitive, allow minor variations like "Max ingress" or "Ingress per eCKU")
     - Egress value cited with "Egress (MBps)" (same flexibility)
     - Partition rate cited with "Partitions (pre-replication)" or "Partition rate" or "Max partitions per eCKU"
     - PNI cap cited from prose containing "PNI" and "32" (or current value) together
     - PrivateLink cap cited from prose containing "PrivateLink" and "10" (or current value) together
   - If a value is cited from a row label that doesn't match these anchors — e.g., "Total client connections", "Connection attempts", "Compactable partitions" (a sub-limit, not the main partition rate), or "Kafka REST Produce v3" — the fetch grabbed the wrong row. Treat as failure.
   - **On fetch failure (any value missing, unparseable, non-numeric, or extracted from an unexpected row): STOP sizing.** Mark Sizing as blocked on cluster-types.md fetch failure. Surface as an Open Question: "Verify per-eCKU values manually from cluster-types.html before sizing can commit. Fetched values were: [list what was extracted, including the row label and failure mode]." Do not produce eCKU verdicts. Do not fall back to remembered values from training data. Wait for the user to confirm the values manually, then resume.

   This is row-label verification, not range checking. Per-eCKU numbers themselves drift as Confluent revs cluster generations; hardcoding a "plausibility range" in this skill creates maintenance debt and can falsely reject correct live values. The row label is the structural anchor — if the model cites the right row, the value is whatever the doc currently publishes; if the model cites the wrong row (or no row), the fetch failed regardless of how plausible the number looks.

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
2. **[Cluster Linking source requirements](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html)** (fetch the `index.md` variant — the migration floor lives in Footnote [1] under "Supported cluster types," not a prominent table) — the **migration-use-case** source version floor (Kafka 2.4.0+ / CP 5.4.0+, NOT the general Kafka 3.0+ floor — this skill is MSK-only and always operates in the migration-exception path), the `inter.broker.protocol=2.4` requirement, the auth support matrix, and supported source topologies (including MSK Express broker tier if present). Without this fetch, CL compatibility claims rest on stale general knowledge — and the wrong floor can produce false positives (rejecting eligible Kafka-2.4–2.9 sources) or false negatives (missing the IBP=2.4 check on Kafka-3.x sources with stale IBP).
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

## Networking Choice

For private AWS-to-AWS migrations, **PNI is the recommended default**. PNI charges only for cross-AZ traffic, while PrivateLink adds data processing fees and hourly endpoint fees. Actual customer savings depend on traffic profile and are best validated with your Confluent account team. PNI is AWS-only, supports Enterprise and Freight cluster types, and requires customer-side setup (51 ENIs + network interface permissions for CC) per https://docs.confluent.io/cloud/current/networking/aws-pni.html.

When `target_cloud` is AWS, the skill asks the customer: "Will your CC environment need to reach customer-side network resources — for example, managed connectors connecting to customer-hosted databases? PNI does not support egress, so this case adds an Egress PrivateLink Endpoint." Captured in `target_context.cc_egress_required`. This case ADDS an Egress PrivateLink Endpoint alongside PNI ingress — it does NOT flip the cluster off PNI. See the exception table below.

**Default on undetermined: `cc_egress_required = false`.** When the value is not provided by the customer, the Plan defaults to `false` and surfaces it as an Open Question. Reasoning: PNI is the skill's stated cost-preferred default; adding an Egress PrivateLink Endpoint adds materially to networking cost. The customer needs to actively confirm an egress requirement before the Plan adds the egress endpoint.

**Do NOT infer `cc_egress_required = true` from source-side signals.** Observing a self-managed Connect / Debezium pipeline on the source, or any other customer-side data integration, does NOT imply the customer will migrate that pipeline to CC managed connectors needing egress from CC. The Connect fleet may stay self-managed off-CC; the customer may keep customer-side connectors; the destination architecture is a customer decision the skill cannot make from source observation alone. Surface the question, default to false, do not assume the customer will move the pipeline.

The inference *"CDC pipeline on source → CC managed Debezium needs egress → cc_egress_required = true"* is plausible but premature — every step after the first is a customer decision, not a source-data fact. If the customer states they will migrate the Connect fleet to CC managed connectors against customer-hosted databases, the value flips to true and the cascade emits PrivateLink. Until they say so, default false stands.

### Networking exceptions on AWS

Two conditions flip the cluster off PNI entirely (PrivateLink becomes the ingress type). One condition keeps PNI ingress and adds an Egress PrivateLink Endpoint. The cluster stays PNI for ingress in the additive case.

| Condition | Effect | Why | Citation |
|---|---|---|---|
| Target cloud is Azure or GCP | Flip off PNI → PrivateLink ingress | PNI is AWS-only | https://docs.confluent.io/cloud/current/networking/overview.html |
| PNI gateway limit reached (≥2 PNI gateways already in CC environment) | Flip off PNI → PrivateLink ingress | Hard cap of 2 PNI gateways per CC environment; additional gateways require Confluent support contact | https://docs.confluent.io/cloud/current/networking/aws-pni.html |
| `target_context.cc_egress_required: true` | Keep PNI ingress; ADD an Egress PrivateLink Endpoint for outbound | PNI does not support egress; the Egress PrivateLink Endpoint is the documented additive substitute. It is environment-scoped and does not change the cluster's PNI ingress type. | https://docs.confluent.io/cloud/current/networking/aws-egress-privatelink-esku.html |
| Compliance scenarios, organizational policy on shared vs dedicated networking substrate, latency-sensitivity, or customer-managed-substrate posture | Defer | Not published as a structured Confluent doc | Connect with your Confluent account team |

### Level-of-effort consideration

PNI requires customer-side ENI setup (51 ENIs + network interface permissions for CC). See https://docs.confluent.io/cloud/current/networking/aws-pni.html for current specifics. Surface this in the Technical Plan as a top-level effort note alongside the networking recommendation — the customer's IT team owns the ENI provisioning step.

For non-private networking topologies (hub-and-spoke → Transit Gateway, direct VPC peering, public endpoints), see the SKILL.md networking selection table — those topologies still drive the cluster-type decision (e.g., TGW escalates to Dedicated).

**Every networking choice in the Technical Plan must carry a justification in the Networking Decision table.** For the AWS-to-AWS PNI default: state the cost reasoning (cross-AZ traffic only vs PrivateLink's data processing + hourly endpoint fees). For each exception case: cite which condition fired (`cc_egress_required` → adds an Egress PrivateLink Endpoint while keeping PNI ingress; gateway limit or non-AWS target → flips off PNI to PrivateLink ingress; or the account-team deferral case) and the supporting doc.

**Always emit the account-team deferral framing in the Networking Decision section, even when PNI is the recommendation and no exception fires.** The four-row exception list above is complete, so the Plan section must show all four — three citable conditions plus the account-team deferral case. State explicitly that compliance, organizational policy, latency-sensitivity, or customer-managed-substrate cases that fall outside the three citable conditions are not published as structured Confluent doc and defer to the Confluent account team. This is a non-conditional emit: it appears in every Plan's Networking Decision section so the user sees the full exception surface. Phrase as a short trailing line after the three citable conditions — e.g., "Compliance, organizational policy, latency-sensitivity, or customer-managed-substrate cases that fall outside the three citable conditions above are not published as structured Confluent doc — defer to the Confluent account team."

Do NOT compute customer-specific cost differential or quote a percentage savings figure. Actual savings are workload-dependent and validated with the account team. The cost reasoning is the standard PNI explanation, not a customer-specific number.

### Cluster Linking direction (MSK = destination-initiated only)

For an MSK source, Cluster Linking is **destination-initiated only** (`link.mode=DESTINATION`). There is no direction decision. `SOURCE` and `BIDIRECTIONAL` both require a cluster-link object on the source — a Confluent Server / Confluent Cloud capability. MSK is open-source Apache Kafka, which bidirectional explicitly excludes (https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/cluster-links-cc.html). Always emit destination-initiated.

What the cascade decides is not the direction but the reachability path — HOW Confluent Cloud reaches a private MSK source. Cascades from `target_networking`:

- **VPC peering, Transit Gateway, Public + MSK public** → the recommendation already provides the route. KCP generates the cluster-link resources once reachability exists. Do not ask the customer; emit derivation in the Plan.
- **PNI** → Reachability depends on whether the destination VPC has a direct route to the MSK VPC. **Ask the customer**: "Your recommended target networking is PNI. Does your PNI VPC have a network path to the MSK VPC via peering, TGW, or other direct route?" Capture answer in `target_context.cc_reaches_source`. Derive:
  - `cc_reaches_source: true` → direct route; destination-initiated; no egress endpoint needed.
  - `cc_reaches_source: false` → no direct route. Still destination-initiated, but an additive Egress PrivateLink Endpoint is required for CC to reach back to the source. The cluster stays PNI for ingress. Triggers the "Cluster Linking — Special Considerations" conditional section (see Conditional sections below).
  - `cc_reaches_source: unknown` → default to "assume a direct route" as a working assumption; flag as Open Question to close before Provision.

**KCP does not provision the networking.** PNI and the Egress PrivateLink Endpoint are set up per docs.confluent.io and the `confluentinc/cc-terraform-module-clusterlinking-outbound-private` module as a Provision-stage / account-team activity. KCP generates the cluster-link resources only once reachability exists.

**Emit derivation visible in the Plan.** Format the derivation as `target_networking={value}; {reachability derivation} → destination-initiated`, e.g., `target_networking=VPC peering provides CC → MSK route → destination-initiated` or `target_networking=PNI; customer indicated no PNI-VPC ↔ MSK-VPC route → destination-initiated + additive Egress PrivateLink Endpoint`. The reader should be able to trace which input drove the reachability call.

## Schema Migration Path Selection

Schema Linking uses a Schema Exporter on the source Schema Registry cluster that pushes schemas to the destination CC Schema Registry. The exporter is one-directional (source → destination) per https://docs.confluent.io/cloud/current/sr/schema-linking.html — the source SR must be able to reach CC SR. Path depends on source SR type, the Schema Linking version floor, and whether the source SR has outbound network access to reach CC Schema Registry endpoints.

**Source SR version floor.** Schema Linking requires source SR on Confluent Platform 7.0 and later. Cite https://docs.confluent.io/platform/current/schema-registry/schema-linking-cp.html Prerequisites section. State the floor explicitly in the Plan rather than punting to "verify live" — the floor is stable and publicly stated.

**Edition requirement.** Schema Linking is an Enterprise feature. Confluent Platform Community edition does not support it even at 7.0 and later. The Plan must state this explicitly when the source is Confluent SR — a customer on CP 7.0+ Community is in a different bucket than CP 7.0+ Enterprise.

**Non-Confluent SR.** If source SR is non-Confluent (Glue, Karapace, Apicurio), Schema Linking does not apply — see the path table below for the appropriate alternative.

**Customer confirmation.** The customer's platform team confirms source SR meets the version floor AND is on Enterprise edition before SL is committed. Surface as an Open Question in the Plan.

| Source SR | Path |
|---|---|
| Confluent Schema Registry (Cloud or self-managed Confluent Platform Enterprise) meeting the SL version floor | Schema Linking via Schema Exporter. Orchestrate with `kcp create-asset migrate-schemas`, which generates Terraform using the `confluent_schema_exporter` resource. Requires source SR to reach CC SR endpoints (see outbound-reachability question below). |
| Confluent Schema Registry — Community Confluent Platform, or below the SL version floor | Connect with your Confluent account team — migration path may require customer-owned setup beyond standard Schema Exporter. |
| AWS Glue Schema Registry | Direct registration into CC SR via Terraform. Orchestrate with `kcp create-asset migrate-schemas --glue-registry`, which generates `confluent_schema` resources (see [migrate-schemas docs](https://confluentinc.github.io/kcp/0.8.0/command-reference/create-asset/migrate-schemas/) and the [`confluent_schema` resource docs](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/schema)). Schema Linking does not apply. Per the resource docs, `version` and `schema_identifier` are server-assigned by CC SR (wire-format schema IDs change on registration); compatibility mode is set separately per subject; subject names are operator-controlled. |
| Other SR type (Karapace, Apicurio, etc.) | Connect with your Confluent account team for current migration tooling and patterns. |
| Existing schemas — recreate fresh in CC SR (clean break) | Provision CC SR; define schemas declaratively via the `confluent_schema` Terraform resource. Update producers and consumers to use the new schema IDs. Does not preserve historical schema IDs from source SR — apps must be coordinated to switch over. |
| No SR — adopt during migration | Provision CC SR; register initial schemas before data migration. Consider Schema ID in Kafka Headers (GA March 2026) for non-disruptive SR adoption on existing topics — see [Confluent Cloud serdes wire format docs](https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.html#wire-format). For greenfield schema discovery in client code, cross-link to the `kafka-schema-registry` skill. |
| No SR — skip schemas | Skip SR steps. Record the explicit user choice. Proceed with data migration. |

**Outbound-reachability cascade.** When source SR is Confluent Schema Registry (Cloud or CP Enterprise) meeting the SL floor, the skill asks the customer: "Does your source Schema Registry have outbound network access to reach CC Schema Registry endpoints (schema-registry.*.confluent.cloud)?" Capture answer in `target_context.source_sr_can_push_to_cc_sr`. Derive:

- `source_sr_can_push_to_cc_sr: true` → Schema Linking via Schema Exporter (Path 1 above).
- `source_sr_can_push_to_cc_sr: false` → Defer to Confluent account team for current migration tooling. Reasoning: Schema Linking's exporter is source-side, so the source SR must be able to push to CC SR; if not, customer-owned setup beyond standard Schema Exporter is required.
- `source_sr_can_push_to_cc_sr: unknown` → Schema Linking via Schema Exporter (default working assumption); flag as Open Question to verify outbound reachability before Migrate.

**Schema migration intent.** When the customer has existing schemas (any source SR type), the skill also asks: "Do you want to migrate your existing schemas to CC SR, recreate them fresh in CC SR (clean break), or skip schema management entirely?" Answer drives row selection — migrate intent uses source-type-specific rows; recreate intent uses the clean-break row regardless of source SR type; skip intent uses the no-SR skip row.

## Connector Migration

For each source connector enumerated in Assess (or detected via state file at `aws_client_information.connectors[]`), the Plan looks up the CC managed connector catalog and classifies the migration path per-connector. Classification is at the individual-connector level, not the source-type level — MSK Connect and self-managed Connect determine which `kcp create-asset migrate-connectors {msk|self-managed}` sub-command emits the asset, not whether the connector goes to CC managed.

**Destination Connect architecture is a customer decision; do not default it from source observation.** When self-managed Connect or MSK Connect is detected on the source AND the customer intake has not stated a destination Connect intent, the Plan must use uniform working-assumption language: *"Destination Connect architecture is not committed. The skill flags this as an Open Question. Do not default to either keep-self-managed or move-to-CC-managed without customer signal."* Do not state a working assumption about destination Connect architecture unless the customer intake explicitly indicates a direction. The "differ" classification in the connector path table is the appropriate landing zone when intent is unknown. This rule pairs with the `cc_egress_required` default-false rule above — both prevent the Plan from making customer-side architectural commitments the skill cannot make from source-data observation alone.

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
- **MirrorMaker 2 internal topics detected on source** (per Assess Row 15 — `<alias>.heartbeats.internal`, `<alias>.checkpoints.internal`, `mm2-offset-syncs.<alias>.internal`) → source is already in an active replication relationship. Surface as a Risks row and as a switchover sequencing input — the Plan's Switchover Approach section must reference MM2 detection because the existing replication affects cutover ordering (e.g., whether to drain MM2-replicated topics before CL takes over, whether the upstream MM2 source needs separate cutover coordination).

## Historical Data Handling

Cluster Linking replicates all historical data from source by default. Per https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/migrate-cc.html: "Sync all historical data and new data from the existing topics to the new mirror topics." Consumer offsets sync so consumers resume from their previous position. The customer-facing decision is whether downstream consumers actually need historical data on the target post-migration, captured in `target_context.consumer_history_requirement`.

| `consumer_history_requirement` | Path |
|---|---|
| `required` | CL backfills all history (default per https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/migrate-cc.html). Plan estimates backfill time = source data volume ÷ sustained CL throughput; surface as a Risks row when source has tiered storage (see callout below). |
| `not_required` | Connect with your Confluent account team — skipping history is not documented as a default CL flow and requires customer-side operational choices not addressable from public docs. |
| `mixed` | CL backfills all by default. For specific topics where backfill is undesirable, connect with the Confluent account team for per-topic configuration. |
| `unknown` | CL backfills (default); flag as Open Question to verify with downstream consumers before Migrate. |

**Tiered storage cost callout.** When the source has MSK tiered storage (`storage_mode: TIERED` in the migration profile, or detected from the state file via `StorageMode: "TIERED"`), the callout must name **three distinct customer-facing dimensions** so the reader sees each cost factor explicitly — do NOT conflate them into a single phrase like "material cost and time":

1. **S3 re-fetch (the mechanism):** the source has to pull historical data from S3, not from broker disk. Name "S3 re-fetch" (or "re-fetch from S3") explicitly so the reader understands where the cost comes from.
2. **Backfill time (the duration dimension):** the re-fetch takes time at sustained CL throughput. Name "backfill time" (or "time to backfill") as its own dimension — distinct from cost.
3. **Backfill cost (the dollar dimension):** the re-fetch incurs per-GB cost from S3. Name "backfill cost" (or "cost of backfill" / "re-fetch cost") as its own dimension — distinct from time.

Customers with real-time-only consumers may want to consider `consumer_history_requirement: not_required` to avoid all three costs — verify with your Confluent account team.

Read peak tiered volume per cluster from the state file metrics — not from EBS provisioned capacity (which is a different thing):

- Correct source: `.regions[].clusters[].metrics.results[] | select(.Label == "Cluster Aggregate - TotalRemoteStorageUsage(GB)") | .Values | max`
- Wrong source: `BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.VolumeSize × NumberOfBrokerNodes` — that's provisioned capacity, not actual data volume. They can differ by a lot.

Also check the cluster's `StorageMode` field — clusters with `StorageMode: "TIERED"` use tiered storage; Express brokers don't have EBS tiered storage (handled internally). Report the peak tiered volume per cluster in the Technical Plan and include it in the risk discussion: "backing up X TB of tiered data would take Y days to transfer at Z MBps sustained CL throughput — decide if that cost is worth it vs. keeping source accessible during the rollback window."

**Independent of decommissioning timing.** This decision is about whether to backfill history on CC, not about how long to keep MSK running. Invariant 5 (maintain source cluster through Monitor; decommission after rollback window) covers the decommissioning timeline separately. Don't conflate the two in the Plan output.

## Open Questions Don't Block Plan Production

When the user says "write the Plan" (or equivalent — "produce the Technical Plan," "commit the Plan," "finalize Plan"), produce a **full Plan per the template below**. Not Assess output. Not a "Plan-Stage Preview." Not a hedged directional document.

This is a stage transition the skill has to get right even when the user has open questions:

- **Unresolved open questions do NOT defer, hedge, or postpone the Plan.** They go in the Open Questions section. The rest of the Plan commits to recommendations with visible working assumptions.
- **State working assumptions explicitly.** If the user hasn't confirmed which auth types clients use, the Plan picks an assumption (e.g., "assuming SCRAM-dominant based on broker config") and notes it. The user reacts to a concrete recommendation, which is more productive than answering questions in the abstract.
- **Use the literal `Working assumption:` label for every numbered Open Question — no exceptions.** Every item in the Open Questions section must have a corresponding `Working assumption:` line in the section that owns the decision the OQ rests on. The literal label is the audit handle — it lets the user scan the Plan and trace exactly which decisions rest on unconfirmed assumptions. Phrases like "Assuming X..." or "Default: Y..." do not substitute; use the literal `Working assumption:` prefix on the line. If the natural section is absent (e.g., no Tiered Storage section because the fixture omits it), put the label on the next-closest section that owns the underlying decision — Sizing for percentile / throughput OQs, Risks for not-in-profile or unknown-condition OQs, Pre-Migration Workstream for client-inventory / pre-cutover-work OQs. **A `Working assumption:` line inside the Open Questions entry itself does NOT satisfy the rule — the label must appear in the decision section that owns the recommendation.** **Self-check before delivering the Plan: count the items in Open Questions, then count the `Working assumption:` lines outside the Open Questions section. The two counts must match. If they don't, add the missing labels before delivering.**
- **Never output Assess content in place of the Plan.** If the user says "write the Plan," the deliverable is a Technical Plan following the template — even if the state file has gaps or the user has unanswered questions.

**The conditions that block Plan commitment:**

1. A cluster's projected sizing exceeds the Enterprise eCKU cap and the user must decide on Dedicated escalation before the networking/cluster-type rows can be filled. Even here, produce the rest of the Plan and surface the escalation decision as the single blocker.
2. The state file has scan gaps that make sizing impossible (no throughput data at all). Even here, produce every other section and explicitly flag sizing as blocked by the scan gap.
3. A **foundational input** is missing — topics/partitions/scale, auth posture, or networking accessibility (private/public plus VPC topology). These hold the sections that depend on them: mark the dependent section "blocked — needs re-scan or manual intake" and ask the user to fill the gap (re-scan or manual intake). Do NOT fabricate a foundational value as a working assumption. Produce every section that does not depend on the missing input.

**Throughput is foundational-but-degradable, not a hard block on its own.** No throughput at all → ask (condition 2 above). Peak present but P95 missing → use the existing peak fallback with the overestimation flag; this does not block the Plan.

**Peripheral gaps never block.** EOS/transactions, Kafka Streams, connector detail, costs, client inventory, SR version, IBP, and the finer reachability route keep the working-assumption treatment — assume, label, capture an Open Question. The discriminator: if the assumption would invent a load-bearing recommendation (cluster type, sizing, networking, auth, switchover), it is foundational — ask; if it fills a peripheral unknown, assume and label.

In all other cases, produce the full Plan. Capture gaps in Open Questions; document assumptions in-line next to the decision they support.

## Technical Plan Template — structure every plan the same way

Every Technical Plan output uses the same prologue, section ordering, and table formats. This makes Technical Plans comparable across migrations, prevents silent gaps (missing sizing hidden by section renaming), and gives internal reviewers and customers a consistent shape to expect.

**Output prologue — emit verbatim at the top of every Technical Plan output.**

The skill renders the following block verbatim as the prologue (before the Header section). The title "Technical Plan" honestly signals scope: this artifact covers technical architecture and migration approach, not the customer's full operational plan.

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

After the prologue, render the required sections starting with the Header section.

**Section structure — every section header uses `## Section Name` format with no leading numbers.** The Plan has a canonical ordered list of section names — base sections always present plus conditional sections when triggered. Conditional sections slot into the order at their canonical position when their trigger fires; they are skipped (no heading emitted) when the trigger does not fire. Total section count varies with how many conditional triggers fire on the given migration. Do not renumber, do not insert a numbered heading anywhere, and do not reorder.

**Canonical section order (base + conditional, in render order):**

- **Header.** Source, target, state file path, drafted date. One-liner each.
- **Inputs & Default Assumptions.** Parameters the Plan is built on. Surface them upfront so the user can challenge any default before reading recommendations that depend on them. Required rows: Sizing percentile, Headroom, SLA target, Target cloud/region, Schema posture. Add others as relevant. Format: 4-column table — Parameter | Value | Source | Implication of Change.
- **Summary.** BLUF. Max 5 bullets covering: cluster type recommendation, networking recommendation, switchover approach, the one workstream most likely to drive the timeline, anything else urgent. The "one-page if you only read this" version.
- **Source Environment.** Cluster table (standard columns below) + auth posture (server-side) + networking topology + VPC / region summary. When the Assess output included a Topic-Level Readiness section (KCP state file with `topics.details[]`), restate the per-cluster bucket counts (Skip / Manual / Needs Config / Moves Cleanly) as a subsection here. Manual-bucket entries are itemized; Needs-config entries are summarized by reason.
- **Sizing.** Per-cluster eCKU math with citations. Either committed numbers with visible formulas, or explicit "deferred because [scan gap | Dedicated escalation conversation needed]" — never silently missing.
- **Cluster Type Decision.** Enterprise vs Dedicated per cluster, with audit format conditional on the verdict — see Cluster Type Decision audit rendering below.
- **Networking Decision.** Per-cluster networking choice (PrivateLink, PNI, VPC Peering, TGW, public) with justification. **When the source spans multiple clusters or multiple regions, name each cluster or region explicitly in the Networking Decision section** — either as table rows (one row per cluster with cluster name and region) or as inline references in the prose. The reader should see each source identifier (cluster name, region, or both) within Networking Decision itself, not only in Source Environment. Generic phrasing like "all three clusters" or "three target regions" is insufficient — the reader needs to know which cluster maps to which networking decision without chaining back to the cluster table. For single-cluster fixtures, the cluster name appears once in the prose.
- **Auth Approach.** Two-step: (a) source-side pre-migration requirements per source MSK auth type (IAM source requires pre-migration to SCRAM or mTLS before Zero-Cut per Invariant 8; other source auths have no pre-migration step). (b) Target CC auth method cascaded from `target_context.target_identity_model` per the SKILL.md target-options table — `oauth` → SASL/OAUTHBEARER, `api_keys` → SASL/PLAIN, `mtls` → SSL with client certs (verify live cluster-type × cloud support against cluster-types.html), `undecided` → default to API Keys + flag as Open Question, `other` → defer to Confluent account team. Emit derivation visible: customer's chosen `target_identity_model` and the resulting CC auth method. Keep the source-side pre-migration step clearly separated from the target-side identity choice in the output so readers don't conflate them. **The target-side block must render a compact 3-row summary table of all identity-model-to-mechanism mappings (OAuth → SASL/OAUTHBEARER, API Keys → SASL/PLAIN, mTLS → SSL with client certs) with the cascade-selected row highlighted as the recommendation.** The mTLS → "SSL with client certs" mapping is CC-specific terminology customers won't know without prompting; showing the full table once educates the reader about alternatives so they can revisit the choice space without round-tripping to docs. When the target cluster type doesn't support mTLS per the live-fetched matrix from cluster-types.html, name the specific matrix outcome (which cluster type fails the row) and cite the URL inline before proposing the SCRAM or API-keys fallback with a one-line rationale.
- **Switchover Approach.** Pattern × mechanism (incremental + Gateway recommended; big-bang + Gateway when single window desired; big-bang + Manual CL fallback when Zero-Cut prereqs not met). **The Pattern × Mechanism alternatives table must include an explicit `Incremental + Manual CL` row marked `Not recommended` with the rationale "operationally heavy without the Gateway's atomic flip." Do not silently omit the cell — the reader must see that the combination was evaluated and rejected.** **Consumer continuity** is handled by destination-initiated CL consumer offset sync plus the Gateway's atomic per-group flip (producers and consumers on the same route cut over together); on the manual CL fallback, migrate consumers before promoting mirror topics with `consumer.offset.sync.enable=true`. Do NOT offer bidirectional Cluster Linking — it is unavailable for an MSK source. A deliberate producers-first, consumers-much-later cutover with a long gap is not a documented MSK pattern; defer it to the Confluent account team. **The Dual-write row prose must include all three anchors so the reader understands WHY Dual-write is avoided, not just THAT it's avoided:** (a) operational-complexity language ("operationally complex", "double maintenance burden", or equivalent that names the burden of running two write pipelines in parallel); (b) dual-cost language ("running both clusters in parallel", "double infrastructure cost", or equivalent that names the cost of paying for two clusters simultaneously); (c) "no Confluent-specific tooling" (or "generic CL only"). Single-anchor framings like "Avoid — no Confluent-specific tooling" do NOT satisfy because they tell the customer the conclusion without the reasoning. Prerequisites fetched live at Switchover stage — don't cache them in Plan.
- **Historical Data Handling** (conditional — see triggers below).
- **Schema Migration** (conditional — see triggers below).
- **Connector Migration** (conditional — see triggers below).
- **Multi-VPC / Multi-Region** (conditional — see triggers below).
- **Cluster Linking — Special Considerations** (conditional — see triggers below).
- **Pre-Migration Workstream.** What has to happen before migration proper. Commonly: IAM→SCRAM auth migration, Kafka version upgrade, client inventory reconstruction. **Do NOT include duration estimates, rough timelines, or sequencing predictions** ("1-2 days", "several weeks", "Plan for X days of rollout", "longest-pole step", "lead time", "sequence first"). Runbook timing is an operational planning decision the customer's IT team owns — it depends on org-specific constraints, change windows, and team availability the skill cannot see. Per the D1 scope rule, the Plan stays architectural: what work is required and what triggers it, not when or how long. Owner column is fine (naming who does the work); a Duration column or any duration prose is not. **Topic-Level Readiness rollforward:** if Assess produced a Topic-Level Readiness section with Manual-bucket entries, each entry rolls into Pre-Migration Workstream as a discrete item (e.g., "Recreate `internal-metrics` at RF=3 on target," "Redesign Deny ACLs as Allow-only RBAC for `dlq-*` topics") so the workstream surface matches the topic-level reality. **Schema migration rollforward:** when source SR is non-Confluent (Glue, Karapace, Apicurio), the Schema Migration section feeds subject naming convention verification into Pre-Migration Workstream. KCP preserves source subject names on registration into CC SR. If source naming conventions diverge from the strategy CC producers and consumers will use to look up schemas (typically TopicNameStrategy with subjects named `<topic>-value` / `<topic>-key`), application teams align them before cutover. Owner: customer (application + data teams).
- **Risks.** Table format (standardized below).
- **Open Questions.** Numbered list with owner (User / Live fetch / etc.). These are the specific items that close before Provisioning.
- **Next Step.** Single next action. Usually "confirm X, then move to Provision."

**Self-check the section order before delivering the Plan.** Scan the rendered headings top-to-bottom: every base section must appear in the canonical order; every conditional section, if emitted, must appear in its canonical position. Pre-Migration Workstream must appear before Risks. The canonical sequence is the reader's load-bearing structure — out-of-order sections break downstream comparability across migrations.

**Cluster Type Decision audit rendering — conditional on verdict.** For each cluster in the Plan:

- **If the verdict is Dedicated:** render the full hard-limits row table with `Triggered` or `Not triggered` status on each of the 7 rows (eCKU cap / Kafka REST throughput, ACL count, networking topology, broker-side schema validation, mTLS x cluster type x cloud, REST throughput, single-zone SLA). At least one row is `Triggered`. Cite the live cap source on every `Triggered` row.
- **If the verdict is Enterprise:** emit a one-line audit summary listing each of the 7 triggers with its evaluated value. Example: *"All 7 hard-limits triggers evaluated and clear: eCKU cap (projected 4, cap 32), ACL count (12, cap 4,000), networking (PNI, no escalation), broker-side schema validation (none), mTLS (target type/cloud OK), REST throughput (n/a), single-zone SLA (multi-zone)."* Cite the live cap source for each.
- **Multi-cluster fixtures:** emit a verdict and audit (table or one-liner per the verdict) for each cluster individually.

The rendering rule is "show the work that mattered." For Dedicated escalations, the per-row table makes the trigger visible. For Enterprise verdicts, the one-liner confirms every trigger was evaluated without bloating the section with 7 rows of `Not triggered`.

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

- **Historical Data Handling.** Trigger: any cluster has `StorageMode: "TIERED"` (KCP state file) OR the manual profile records tiered usage OR the manual profile does not record StorageMode and tiered usage is unknown OR `target_context.consumer_history_requirement` is anything other than null/required-default. When triggered, produce a `## Historical Data Handling` heading containing the ternary cascade table (required / not_required / mixed / unknown) per the Historical Data Handling section above, plus per-cluster peak tiered volume cited from `metrics.results[] | "Cluster Aggregate - TotalRemoteStorageUsage(GB)" | max` (KCP) or the manual-profile equivalent (or an explicit "Profile does not record StorageMode — Open Question" note when unknown), plus the tiered-storage cost callout. Section name was previously "Tiered Storage" pre-B4 — the rename is intentional; tiered storage is now a callout within the broader history-handling decision.
- **Schema Migration.** Trigger: source has Schema Registry OR user is considering adoption during migration. When triggered, produce a `## Schema Migration` heading. Omit (don't fold elsewhere) when continuing schemaless — record the opt-out in Open Questions instead.
- **Connector Migration.** Trigger: any connectors exist (MSK Connect managed OR self-managed Connect). When triggered, produce a `## Connector Migration` heading. Omit (don't fold elsewhere) when zero connectors — note the absence in the Summary.
- **Multi-VPC / Multi-Region considerations.** Trigger: source spans VPCs or regions, or MSK Multi-VPC Private Connectivity is detected. When triggered, produce a `## Multi-VPC / Multi-Region` heading.
- **Cluster Linking special considerations.** Trigger: non-standard CL constraint (version concerns, Express broker tier, cross-region, tiered-storage backfill complexity) OR no direct route to a private MSK source (`target_context.cc_reaches_source: false`, requiring an Egress PrivateLink Endpoint) OR `cc_reaches_source == unknown` (per the Cluster Linking direction cascade above). When triggered, produce a `## Cluster Linking — Special Considerations` heading. When the no-direct-route case is the trigger, the section must: (a) state that an additive Egress PrivateLink Endpoint is required for CC to reach back to the private MSK source, that it is environment-scoped, and that the cluster stays PNI for ingress; (b) note the customer-side prerequisite (NLB / VPC Endpoint Service fronting the MSK brokers); (c) cite https://docs.confluent.io/cloud/current/networking/aws-egress-privatelink-esku.html for current prereqs; (d) note that KCP does not provision the networking — the egress endpoint is set up per docs.confluent.io and the `confluentinc/cc-terraform-module-clusterlinking-outbound-private` module before KCP generates the cluster-link resources; (e) confirm the link is destination-initiated.

**The cluster-table column does not replace the section.** When `Tiered (GB)` shows non-`—` values for any cluster, the Historical Data Handling section is required. Similarly, mentioning tiered facts in Summary, Pre-Migration, or Risks does not satisfy the trigger — those references can co-exist with the section but do not substitute for it.

**Standardized cluster table columns (always these 8):**

| Cluster | Brokers × Instance | Kafka | Topics (user+internal=total) | Partitions (user+internal=total) | P95 In / Out (MBps) | Peak In / Out (MBps) | Tiered (GB) |

P95 column drives sizing. Peak column is reference — shows absolute max observed, flags spiky workloads when `peak > 2× P95`. Tiered column shows peak remote storage in GB for tiered clusters, `—` for non-tiered (Express or non-tiered Provisioned).

**Standardized sizing table columns (always these 7):**

| Cluster | P95 In ÷ per-eCKU-in | P95 Out ÷ per-eCKU-out | User Partitions ÷ per-eCKU-partitions | Max | +Headroom | Verdict |

Forces the math to be visible. Uses **P95** values (from the CloudWatch time-series), not absolute peak. Final eCKU = CEIL(Max × (1 + headroom_fraction)). Verdict column = final eCKU + cluster type + networking decision (e.g., "14 eCKU Enterprise, PNI").

**Each cell must carry inline parenthetical citations.** Citations in a pre-table header line or a post-table "Math, traceable" paragraph do NOT satisfy the inline-citation rule from "Technical Plan Conventions — cite every number" — those are useful supplements but the table cells themselves must be verifiable in place. The per-eCKU divisors (24, 72, 1000) cite cluster-types.html inline; the user-supplied dividends (P95 ingress/egress in MBps, partition count) cite their profile field path inline.

**Cite with full URLs visible — never filename shorthand.** Per the SKILL.md "Full URLs in citation link text" rule, every doc citation must render the full URL in the visible text, not just behind a markdown href. **Do NOT abbreviate to `[cluster-types.html]` even when the row gets dense** — that pattern hides the URL from plaintext readers (Slack, print, non-rendering viewers). Acceptable in-cell citation forms: (1) inline with link text matching the href: `[https://docs.confluent.io/cloud/current/clusters/cluster-types.html](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)`; (2) bare URL: `https://docs.confluent.io/cloud/current/clusters/cluster-types.html` (most renderers auto-link). Dense rows should drop the citation entirely from later cells (the row-anchor cell carries it) rather than abbreviate to filename shorthand.

Example cell — sized to fit, citations inline (bare-URL form for compactness):

| `payments-prod` | 280 (`clusters[0].p95_ingress_mbps`) ÷ 24 (https://docs.confluent.io/cloud/current/clusters/cluster-types.html) = 11.67 | 840 (`clusters[0].p95_egress_mbps`) ÷ 72 = 11.67 | 3,568 (`clusters[0].user_partition_count`) ÷ 1,000 = 3.57 | 11.67 | × 1.30 = 15.17 → CEIL = **16** | **16 eCKU Enterprise, PNI** |

The first divisor cell carries the URL once for the row; later cells in the same row may omit the citation since the source is established. The point is verifiability inline — the user must be able to trace each number from inside the table cell, with the URL fully visible so plaintext readers can navigate to it.

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
> Peak egress 1,594 MBps (`metrics.results[] | BytesOutPerSec | max`). Enterprise eCKU cap 32 (https://docs.confluent.io/cloud/current/clusters/cluster-types.html, "eCKU/CKU comparison" table).

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

> `dataplatformEvents`: peak egress 1,594 MBps (`metrics.results[] | BytesOutPerSec | max`) ÷ 180 MBps per eCKU (https://docs.confluent.io/cloud/current/clusters/cluster-types.html, Enterprise column) = 8.9 eCKU minimum, +50% headroom = **14 eCKU**. Below Enterprise cap of 32 (same doc); above PrivateLink cap of 10 → **PNI networking required**.

**Example — full citation discipline on a sizing row, manual `migration-profile.yaml` source:**

> `payments-prod`: peak egress 960 MBps (`clusters[0].peak_egress_mbps`) ÷ 180 MBps per eCKU (https://docs.confluent.io/cloud/current/clusters/cluster-types.html, Enterprise column) = 5.33 eCKU minimum, +30% headroom = **7 eCKU**. Below Enterprise cap of 32 (same doc); below PrivateLink cap of 10 → **PrivateLink**.

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
- Pre-migration requirements identified and flagged (IAM → SCRAM conversion if Zero-Cut, Kafka version below the migration-use-case CL floor of 2.4.0+, `inter.broker.protocol` below 2.4 on the source, etc.)
- Risks documented and acknowledged by the user
- Every number in the Technical Plan carries a citation (Style A inline or Style B appendix) per Technical Plan Conventions above

**Downstream platform scope.** Tableflow, Flink, and lakehouse integrations are out of scope for this skill. Customers can enable them post-migration. If a user asks about downstream pipelines, point them to the relevant docs and return focus to the migration path.

## Source of Truth

- Cluster type specs, capacity caps, networking feature availability, SLA — https://docs.confluent.io/cloud/current/clusters/cluster-types.html. Fetch live before recommending a cluster type or citing a cap.
- Cluster Linking source requirements, auth compatibility — https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking. Fetch live.
- Schema Linking source matrix — https://docs.confluent.io Schema Registry / Schema Linking pages. Fetch live before committing to Schema Linking as the path.
- Connector catalog — https://docs.confluent.io/cloud/current/connectors. Fetch live to check managed-connector availability.
- Pricing and commercial quote are out of scope. Direct users to the public cost estimator at https://www.confluent.io/pricing/cost-estimator/ as the Sales handoff.
