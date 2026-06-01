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

- [ ] **Walks the intake groups in a logical order without jumping around.** The skill proceeds through platform / cluster topology / scale / auth / networking / ACLs / Schema Registry / connectors / costs / migration drivers in a coherent sequence rather than jumping around based on what the user mentioned first. Kafka version may be asked alongside platform basics (Group 1) since it's platform-adjacent.

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

- [ ] **Pattern × Mechanism alternatives table renders all four required rows.** Incremental + Gateway, Big-bang + Gateway, Big-bang + Manual CL, and Dual-write each have their own row. Each row's rationale column contains substantively different content — no two rows share identical or near-identical rationale text.
- [ ] **Incremental + Gateway row carries a RECOMMENDED tag on the row itself.** Tag is in column 1 or 2 (or appended to the pattern label), not a separate sentence elsewhere in the section.
- [ ] **"Incremental + Manual CL not recommended" appears as its own paragraph, callout, or labeled row note.** Standalone block — not embedded inside another row's prose or hidden in a parenthetical.
- [ ] **Dual-write row prose contains all three substrings: operational-complexity language, dual-cost language, "no Confluent-specific tooling" (or "generic CL only").** All three present in the Dual-write row.

## Eval 21 — per-connector-classification

- [ ] **Each source connector has its own row with a distinct classification.** Fixture has s3-sink and custom-internal-validator. s3-sink row contains CMU + `kcp create-asset migrate-connectors` path. custom-internal-validator row contains the defer-to-account-team path. Two rows, two paths.
- [ ] **Substitution-nuance row, if present, names a specific substitution issue.** Substring anchor: "Debezium 1.x", "V2 config key", "config key difference", or equivalent named issue — not a generic "config differences" or "talk to Sales" phrase.

## Eval 22 — networking-pni-default

- [ ] **PNI recommendation and cost reasoning are adjacent in the Networking Decision section.** PNI recommendation sentence is immediately followed (within the same paragraph or the next paragraph) by cost reasoning containing both "cross-AZ" anchor AND one of "data processing" / "hourly endpoint" anchors.
- [ ] **The networking exceptions and the deferral cases live in separate blocks.** The two flip-to-PrivateLink conditions (PNI gateway limit, non-AWS target) and the additive-endpoint condition (CC egress required, or no direct route to the private source — both handled by an additive Egress PrivateLink Endpoint while the cluster stays PNI for ingress) each appear in their own labeled bullet/row. Deferral cases (compliance, organizational policy, latency-sensitivity, substrate) are grouped in a distinct bullet/row or sub-section — not interleaved with the citable exceptions.

## Eval 23 — cl-private-msk-egress-endpoint

- [ ] **Egress endpoint is framed as a reachability mechanism, not a cluster-networking-type change.** The Networking / Cluster Linking section presents the Egress PrivateLink Endpoint as an additive resource that lets Confluent Cloud reach back to the private MSK source for the destination-initiated replication pull, with the cluster staying on PNI for ingress. It is presented as additive, not as switching the cluster to PrivateLink ingress.
- [ ] **Direction reads as fixed for MSK, not a chosen option.** The section presents destination-initiated as a property of the MSK source, not as a direction the customer selects among DESTINATION / SOURCE / BIDIRECTIONAL.

## Eval 24 — schema-linking-no-outbound

- [ ] **Schema Migration section presents the selected path with cascade reasoning for the fixture's specific SR type.** EITHER (a) opens with the full 6-row source-SR-type enumeration (Confluent SR / Glue / Karapace / clean-break / adopt / skip) before the cascade, OR (b) presents the cascade-result for the fixture's specific source SR type with derivation prose that names the SR type AND names the selected path (Schema Linking via Schema Exporter, defer to account team, clean break, adopt, or skip). Per iter-19, the trimmed cascade form is acceptable when only one path applies to the fixture — showing 5 irrelevant rows is noise, not value.
- [ ] **Plan names source-side outbound reachability as the SL gating constraint.** Substring anchor present in the cascade reasoning: "source-side outbound", "source SR can reach CC SR", "outbound reachability", or `source_sr_can_push_to_cc_sr`. Anchor appears in the cascade prose, not just a passing mention elsewhere.

## Eval 25 — auth-target-oauth

- [ ] **Three target identity models each paired with their specific CC SASL/SSL mechanism.** OAuth row/bullet names SASL/OAUTHBEARER. API Keys row/bullet names SASL/PLAIN. mTLS row/bullet names SSL with client certs. Three distinct mechanism strings, one per identity model.
- [ ] **Auth Approach has two visibly separate blocks: source-side pre-migration, then target-side identity choice.** Block 1 cites Invariant 8 and the IAM → SCRAM/mTLS source pre-migration path. Block 2 emits the OAUTHBEARER / PLAIN / SSL target cascade. Two blocks separated by a header, list boundary, or table boundary — not collapsed into a single paragraph.

## Eval 26 — mtls-azure-matrix-fork

- [ ] **Plan names the specific matrix-row outcome inline with cluster-types.html citation.** Cluster Type Decision or Auth Approach section contains all four anchors in the same paragraph, list item, or table row: cluster-types.html URL + mTLS substring + cluster-type-name (Enterprise / Freight / Dedicated) + Azure-or-all-clouds qualifier. The matrix-row outcome must be named (e.g., "Enterprise mTLS is AWS-only", "Dedicated supports mTLS on all clouds", or equivalent that identifies which cluster-type-and-cloud combination fails the row).
- [ ] **Plan resolves the mTLS-on-Azure fork without recommending Enterprise + mTLS on Azure.** Either escalates Cluster Type Decision to Dedicated (with the mTLS row cited as the trigger) OR keeps Enterprise and routes target auth to SCRAM or API keys (with the matrix outcome named). Plan does NOT recommend the combination of Enterprise cluster type + mTLS auth + Azure target in a single recommendation block — that combination is a setup-time blocker.

## Eval 27 — historical-data-handling-not-required

- [ ] **Tiered-storage callout names all three specifics as distinct dimensions.** Within the callout: (a) "re-fetch" + "S3" co-occur (any phrasing — "re-fetch from S3", "S3 re-fetch", "re-fetch all of it from S3" all match the data-source-from-S3 anchor); (b) "backfill time" or "time to backfill" or equivalent dimension-named substring (must be distinct from cost); (c) "backfill cost" or "cost of backfill" or "re-fetch cost". Three distinct dimensions named — conflated phrases like "material cost and time" do not satisfy because they collapse two dimensions into one.
- [ ] **not_required defer-to-account-team row contains a one-line rationale.** Row prose contains substring matching "not a documented default" or "customer-specific setup required" or equivalent rationale anchor — not just "defer to account team" with no reason given.
- [ ] **Historical Data Handling and Invariant 5 / MSK-decommissioning timing appear in distinct headed blocks.** Consumer history decision and MSK decommissioning timing live in separate sections, sub-sections, or labeled paragraphs. Not collapsed into a single bullet.

## Eval 31 — topic-readiness-manual-opt-out

- [ ] **Opt-out note contains both data-shape anchors.** Note contains substring "manual profile" (or "profile schema") paired with "per-topic broker configs" (or "broker-side configs") AND contains either `topics.details[]` or "KCP state file" / "kcp scan clusters". Both anchors present — explaining the data-shape limitation, not a capability limitation.
- [ ] **Opt-out note enumerates at least two concrete next steps.** Step 1: re-run with KCP scan (substring anchor: "kcp scan" or "AWS credentials"). Step 2: capture configs manually for high-risk topics (substring anchor from this set: "tiered storage", "cleanup.policy", "RF" / "replication factor", or "unclean leader"). Both steps present.

## Foundational-Inputs gate — scan-failure fallback (local behavior check)

Run locally against a **discover-only state file** — one where `kcp discover` populated `msk_cluster_config`, networking, auth, and metrics, but `kcp scan clusters` did not populate the Kafka-protocol layer (`kafka_admin_client_information` empty/null). Test input: `state-file-coverage-staging/mock-environments/msk-acme-statefile/state-discover-only.json` (local only — not shipped in the PR). Drive the skill with that state-file path and a "write the Plan" or "assess this" prompt. This is the Ahmed scenario: a private MSK where the deep scan couldn't reach the brokers and KCP still reported success.

- [ ] **Identifies the deep scan did not complete.** The output states the topic / partition / ACL data is missing because the deep scan did not run or could not reach the brokers (private-network reachability) — not that the cluster has zero topics. It treats the absence of topic data in the state file as authoritative over any KCP "success" signal.
- [ ] **Holds the foundational sections instead of fabricating.** Sizing and any topic / partition / scale-dependent section carries a "blocked — needs re-scan or manual intake" marker rather than invented topic counts, partition counts, or scale figures.
- [ ] **Routes to a real next action.** The output offers re-running the scan from inside the VPC (bastion / VPN / Direct Connect) OR manual intake to capture the missing layer, as a concrete next step.
- [ ] **Reuses the surviving discover data.** The output uses the networking accessibility and source auth already present in `msk_cluster_config` (and throughput from metrics) rather than asking the user to re-supply them. Only the missing Kafka-protocol fields are requested.
- [ ] **Peripheral gaps still do not block (cross-check eval 29).** The gate fires only on the foundational inputs (topics / partitions / scale, auth posture, networking accessibility). Peripheral gaps continue to get working-assumption + Open Question treatment, not a block.
- [ ] **Sizing reads as non-committable, not a clean eCKU verdict.** If the run reaches a Plan with partition counts missing: the Sizing section carries a prominent top-of-section callout that the eCKU is a throughput-only lower bound that must NOT be provisioned on; the Verdict cells read as `≥ N eCKU (lower bound — not committable)` rather than a final `N eCKU Enterprise, PNI`; and the Summary carries the caveat (not buried in a later section). A reader skimming the Summary or the verdict cannot mistake the floor for a provisioning recommendation.
