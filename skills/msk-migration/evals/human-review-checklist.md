# Human-Review Checklist

This file is graded by a human reviewer during manual eval rounds. Items here are qualities the [agentskills.io spec](https://agentskills.io/skill-creation/evaluating-skills) places in human review rather than LLM-graded assertions:

> "Not everything needs an assertion. Some qualities — writing style, visual design, whether the output 'feels right' — are hard to decompose into pass/fail checks. These are better caught during human review. Reserve assertions for things that can be checked objectively."

Items in this file fall into two categories. **Transcript-behavioral items** check what the skill does during a run (asking before executing, walking in order, declining off-scope intent) — graded from `transcript.md` and `trace.md`. **Artifact-structural items** check qualities of the produced Plan or Assess that are too judgment-bound for a substring regex (adjacency, two-block separation, derivation visibility) — graded from the artifact itself. Both categories sit here because both are awkward to capture as pure LLM-graded assertions.

Pair this checklist with `evals.json` — `evals.json` covers substring-matchable artifact assertions; this file covers the remainder. Both should clear before declaring an iteration done.

## How to use

1. After an eval iteration completes, open each per-eval section below.
2. For each item, read the relevant source (transcript + trace for behavioral items, the Plan/Assess artifact for structural items) and mark pass / fail / note.
3. A fail here counts the same as a failed assertion in `evals.json`.
4. Copy this file into `iteration-N/` if you want to keep a per-iteration grading record.

### Two grading rules that apply to every item below

- **Items are artifact-checkable.** Every item should be answerable by reading the produced artifact (Plan or Assess) and transcript. If you find yourself asking "would a customer understand this?" stop — that's post-hoc reasoning, not artifact grading. Re-read the item as a presence/structure/proximity check and grade that.
- **Items test the final correct state.** No item asks you to confirm the skill avoided a prior bad framing. If an item reads as a negative ("X fails this item"), grade the positive form ("is the correct structure present?") instead.

### Global scan: D1 operational-guidance leakage

Apply this scan to every Plan output (Evals 2, 6, 16, 19, 20, 21, 22, 23, 24, 25, 26, 27). The Plan is a Technical Plan and stays architectural — pilot validation, testing strategy, validation gates, runbook timing, and operational rollout guidance belong in the customer's operational plan, not the Plan output.

- [ ] **Plan stays architectural.** No section of the Plan contains substrings: 'pilot rollout', 'pilot validation', 'testing strategy', 'validation gates', 'runbook timing', or operational rollout language. A closing note that operational concerns are out of Technical Plan scope is acceptable and expected; specific operational guidance is not.

## Eval 1 — kcp-fresh-start

- [ ] **Introduces KCP on first mention.** When KCP first comes up, the skill briefly explains what it is — Confluent's open-source migration tool, a link to `github.com/confluentinc/kcp`, and its purpose in the migration context. The skill does not assume the user knows what KCP is.
- [ ] **Presents the scan sequence before running anything.** The 6-step KCP scan sequence appears in the transcript before any command is invoked.
- [ ] **Does not auto-execute bash commands.** The skill does not run `kcp --version`, `kcp version`, or any bash command on its own. It either asks the user to run the command and share output, or explicitly asks whether the user wants the skill to run it.

## Eval 2 — plan-scram-baseline

- [ ] **Cluster Type Decision recommends Enterprise with no trigger named.** The profile (1 cluster, peak egress 540 MBps, 2400 partitions) is within Enterprise eCKU caps. Cluster Type Decision section names Enterprise as the target and contains no Row 1-N trigger reference or eCKU/partition cap-exceeded claim.
- [ ] **Auth Approach target matches the profile's source auth or names an explicit basis for divergence.** Profile lists `auth_types: [scram]` only. Recommended CC target auth is SCRAM-compatible (SCRAM-SHA-512, SCRAM-SHA-256, or API key) OR the section cites a source-grounded reason for an IAM/mTLS target (customer-driven preference or named CC constraint).

## Eval 3 — dedicated-large-fleet

- [ ] **Does not over-escalate dimensions that aren't triggered.** Recommendations do not pull along changes to auth, networking, SR, or other dimensions beyond what the profile's hard-limit triggers require. A Dedicated escalation for capacity does not produce unrelated recommendations.
- [ ] **Does not escalate without a trigger.** If another cluster in the same profile does not trigger any hard-limit row, the skill keeps it on Enterprise. Dedicated escalation is per-trigger, not a blanket recommendation.

## Eval 4 — intake-triage

- [ ] **Does not assume an intake path.** The skill does not dive into architecture recommendations, stage execution, or environment assumptions without first establishing what the user has.
- [ ] **Routes via stage menu OR presents intake options.** The opening either (a) presents the Mode Detection stage menu (Assess / Plan / out-of-scope) so the user can signal stage before intake-path selection, OR (b) offers KCP deep scan and Manual intake as the two intake options. The skill does not jump past both into architecture, sizing, or fabricated environment claims.
- [ ] **Asks what the user has.** The skill asks either where the user is in the migration (stage signal) or which intake path the user can produce, rather than recommending a path before knowing.
- [ ] **Does not fabricate environment details.** The skill does not invent cluster configurations, auth types, region, or scale assumptions to keep the conversation moving.

## Eval 5 — manual-no-kcp

- [ ] **Walks the 11 intake groups in order.** The skill proceeds through the intake groups (platform, cluster topology, scale, auth, networking, ACLs, Schema Registry, connectors, Kafka version, costs, migration drivers) in the documented order rather than jumping around based on what the user mentioned first.

## Eval 8 — out-of-scope-baremetal

- [ ] **Recognizes out-of-scope source.** The skill identifies that self-managed Kafka is outside the MSK-only scope.
- [ ] **Declines to run the workflow.** The skill does not proceed with the MSK migration workflow against a non-MSK source.
- [ ] **Does not map non-MSK concepts onto MSK primitives.** The skill does not ask for ARNs, CloudWatch access, Glue SR config, or other MSK-specific artifacts as if they applied to a bare-metal source.
- [ ] **Redirect targets are limited to real, documented resources.** When the skill points the user elsewhere, the named target is one of: docs.confluent.io, the Confluent account team / Sales, or another resource explicitly documented in `SKILL.md` or its references. No sibling skill name, internal tool, or partner program is named that isn't traceable to those sources.

## Eval 9 — mtls-azure-availability

- [ ] **Consults the feature-availability reference rather than asserting from general knowledge.** The skill routes the user to a live docs page for current feature availability, rather than asserting current availability state from training data.

## Eval 10 — pricing-scope

- [ ] **Frames pricing as a Sales handoff, not a skill limitation.** The explanation is that pricing is handled via Sales by design, not that the skill lacks the data.
- [ ] **Does not attempt to compute cost from scan data.** The skill does not combine scan outputs with pricing guesses to produce a synthetic estimate.

## Eval 11 — connector-decision

- [ ] **Surfaces the decision rather than picking blindly.** The skill treats managed-vs-self-managed as a decision the user needs to make, not committing to one without input.
- [ ] **Asks clarifying questions.** The skill asks whether a managed equivalent exists on Confluent Cloud for each connector type, whether source code is available for the custom plugins, and what throughput needs are.
- [ ] **Consults the connector reference.** The decision framework traces to the references rather than being improvised in the response.

## Eval 12 — assess-serverless

- [ ] **Does not fabricate throughput values for serverless.** The mock has `peak_ingress_mbps: null` and `peak_egress_mbps: null` — serverless returns limited metrics. The output does not invent throughput numbers and does not silently pass over the missing fields.

## Eval 14 — assess-iam-private-manual

- [ ] **Does not fabricate values for missing profile fields.** Every environment fact in the Environment Summary traces to a specific field path in the YAML profile. The skill does not invent values where data is missing.

## Eval 15 — hidden-connectors-manual

- [ ] **Recommends closing the connector gap before Plan commits connector paths.** The skill flags that follow-up data collection is needed before the Plan stage can commit a connector migration path — either re-running `kcp scan self-managed-connectors` (if the user later runs KCP) or asking the user to enumerate the Connect fleet manually.

## Eval 16 — full-plan-open-questions-manual

- [ ] **Sizing committed with math, not deferred.** Sizing produces concrete eCKU values per cluster with the computation visible. The skill does not defer with "TBD", "requires live fetch", or "decide at Provision". If a live fetch is needed for per-eCKU values, the skill either fetches or offers to.

## Eval 17 — assess-serverless-manual

- [ ] **Does not fabricate throughput values for serverless (manual intake).** The mock has `peak_ingress_mbps: null` and `peak_egress_mbps: null` — serverless returns limited metrics, and the profile's `known_gaps` entry documents this. The output does not invent throughput numbers and does not silently pass over the missing fields.

## Eval 18 — assess-hidden-cluster-manual

- [ ] **Recommends broadening discovery scope before Plan commits.** The Row 16 response recommends re-running `kcp discover` across all regions and all relevant AWS accounts (or, in manual intake, asking the user to enumerate any clusters not yet listed) before the Plan scope is committed.
- [ ] **Does not fabricate specs for the hidden cluster.** The skill does not invent broker count, topics, throughput, or other details for the unmatched cost entry. The cost signal confirms existence and gives a rough size cue (from instance type + spend), but specifics require enumeration of the hidden cluster.

## Eval 20 — switchover-pattern-mechanism

- [ ] **Three cells (Incremental + Gateway, Big-bang + Gateway, Big-bang + Manual CL) have distinct content in all four runbook columns.** Scan each column down: KCP-group, cutover-cadence, validation, and rollback columns each contain different text per row — no two rows share identical content in the same column.
- [ ] **Incremental + Gateway row carries a RECOMMENDED tag on the row itself.** Tag is in column 1 or 2 (or appended to the pattern label), not a separate sentence elsewhere in the section.
- [ ] **"Incremental + Manual CL not recommended" appears as its own paragraph, callout, or labeled row note.** Standalone block — not embedded inside another row's prose or hidden in a parenthetical.
- [ ] **Dual-write row prose contains all three substrings: operational-complexity language, dual-cost language, "no Confluent-specific tooling" (or "generic CL only").** All three present in the Dual-write row.

## Eval 21 — per-connector-classification

- [ ] **Each source connector has its own row with a distinct classification.** Fixture has s3-sink and custom-internal-validator. s3-sink row contains CMU + `kcp create-asset migrate-connectors` path. custom-internal-validator row contains the defer-to-account-team path. Two rows, two paths.
- [ ] **Substitution-nuance row, if present, names a specific substitution issue.** Substring anchor: "Debezium 1.x", "V2 config key", "config key difference", or equivalent named issue — not a generic "config differences" or "talk to Sales" phrase.

## Eval 22 — networking-pni-default

- [ ] **PNI recommendation and cost reasoning are adjacent in the Networking Decision section.** PNI recommendation sentence is immediately followed (within the same paragraph or the next paragraph) by cost reasoning containing both "cross-AZ" anchor AND one of "data processing" / "hourly endpoint" anchors.
- [ ] **Three citable PrivateLink exceptions and the deferral cases live in separate blocks.** Three citable exceptions (cc_egress_required, gateway limit, non-AWS target) each appear in their own bullet/row with the trigger labeled. Deferral cases (compliance, organizational policy, latency-sensitivity, substrate) are grouped in a distinct bullet/row or sub-section — not interleaved with the citable exceptions.

## Eval 23 — cl-direction-source-initiated

- [ ] **CL Direction section contains a visible derivation line.** Section contains a derivation of the form `target_networking={value} → reachability={value} → direction={value}` (or equivalent inline cascade format). Direction is derived, not configured — the cascade chain is on the page.
- [ ] **Source-initiated manual-setup callout contains all three anchors in one block.** Single paragraph/list/callout contains: (a) the manual-setup action ("establish the link manually" or "customer-owned setup"), (b) the doc citation `private-networking.html`, (c) the permanence note ("link.mode=SOURCE permanent" or "cannot be changed after the link is created"). All three within the same block — not scattered across separate paragraphs.

## Eval 24 — schema-linking-no-outbound

- [ ] **Schema Migration section opens with source-SR-type enumeration before the cascade.** First structural content of the section is the source SR type table or list (Confluent SR / Glue / other / clean-break / adopt / skip). Path selection and recommendations follow that enumeration, not precede it.
- [ ] **Plan names source-side outbound reachability as the SL gating constraint.** Substring anchor present in the cascade reasoning: "source-side outbound", "source SR can reach CC SR", "outbound reachability", or `source_sr_can_push_to_cc_sr`. Anchor appears in the cascade prose, not just a passing mention elsewhere.

## Eval 25 — auth-target-oauth

- [ ] **Three target identity models each paired with their specific CC SASL/SSL mechanism.** OAuth row/bullet names SASL/OAUTHBEARER. API Keys row/bullet names SASL/PLAIN. mTLS row/bullet names SSL with client certs. Three distinct mechanism strings, one per identity model.
- [ ] **Auth Approach has two visibly separate blocks: source-side pre-migration, then target-side identity choice.** Block 1 cites Invariant 8 and the IAM → SCRAM/mTLS source pre-migration path. Block 2 emits the OAUTHBEARER / PLAIN / SSL target cascade. Two blocks separated by a header, list boundary, or table boundary — not collapsed into a single paragraph.

## Eval 26 — mtls-azure-no-dedicated-escalation

- [ ] **"Enterprise supports mTLS on all clouds" claim co-located with cluster-types.html citation.** Cluster Type Decision or Auth Approach section contains the mTLS-all-clouds claim AND the cluster-types.html URL within the same sentence or adjacent bullets. Citation is inline next to the claim, not in a reference list at the bottom.
- [ ] **cluster-types.html URL appears within the section that contains the mTLS recommendation.** Co-located, not just cited once elsewhere in the Plan.

## Eval 27 — historical-data-handling-not-required

- [ ] **Tiered-storage callout names all three specifics: S3 re-fetch, backfill time, backfill cost.** Substring anchors all present: "S3 re-fetch" (or "re-fetch from S3"), "backfill time" (or "time to backfill"), and "backfill cost" (or "cost of backfill" / "re-fetch cost"). All three in the tiered-storage callout.
- [ ] **not_required defer-to-account-team row contains a one-line rationale.** Row prose contains substring matching "not a documented default" or "customer-specific setup required" or equivalent rationale anchor — not just "defer to account team" with no reason given.
- [ ] **Historical Data Handling and Invariant 5 / MSK-decommissioning timing appear in distinct headed blocks.** Consumer history decision and MSK decommissioning timing live in separate sections, sub-sections, or labeled paragraphs. Not collapsed into a single bullet.

## Eval 31 — topic-readiness-manual-opt-out

- [ ] **Opt-out note contains both data-shape anchors.** Note contains substring "manual profile" (or "profile schema") paired with "per-topic broker configs" (or "broker-side configs") AND contains either `topics.details[]` or "KCP state file" / "kcp scan clusters". Both anchors present — explaining the data-shape limitation, not a capability limitation.
- [ ] **Opt-out note enumerates at least two concrete next steps.** Step 1: re-run with KCP scan (substring anchor: "kcp scan" or "AWS credentials"). Step 2: capture configs manually for high-risk topics (substring anchor from this set: "tiered storage", "cleanup.policy", "RF" / "replication factor", or "unclean leader"). Both steps present.
