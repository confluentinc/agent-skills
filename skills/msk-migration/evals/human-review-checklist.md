# Human-Review Checklist

This file is graded by a human reviewer during manual eval rounds. Items here are qualities the [agentskills.io spec](https://agentskills.io/skill-creation/evaluating-skills) places in human review rather than LLM-graded assertions:

> "Not everything needs an assertion. Some qualities — writing style, visual design, whether the output 'feels right' — are hard to decompose into pass/fail checks. These are better caught during human review. Reserve assertions for things that can be checked objectively."

The items in this file are conversational/behavioral checks: what the skill does during a run (asking before executing, walking in order, declining off-scope intent) versus what appears in the produced artifact. The reviewer answers each item by reading the run's transcript and trace, not the produced files alone.

Pair this checklist with `evals.json` — `evals.json` covers artifact-grounded assertions; this file covers the behavioral remainder. Both should clear before declaring an iteration done.

## How to use

1. After an eval iteration completes, open each per-eval section below.
2. For each item, read the run's `transcript.md` (and `trace.md` for tool-call evidence) and mark pass / fail / note.
3. Items are mirrored from the original behavioral assertions so the reviewer carries forward the same intent; a fail here counts the same as a failed assertion in `evals.json`.
4. Copy this file into `iteration-N/` if you want to keep a per-iteration grading record.

## Eval 1 — kcp-fresh-start

- [ ] **Introduces KCP on first mention.** When KCP first comes up, the skill briefly explains what it is — Confluent's open-source migration tool, a link to `github.com/confluentinc/kcp`, and its purpose in the migration context. The skill does not assume the user knows what KCP is.
- [ ] **Presents the scan sequence before running anything.** The 6-step KCP scan sequence appears in the transcript before any command is invoked.
- [ ] **Does not auto-execute bash commands.** The skill does not run `kcp --version`, `kcp version`, or any bash command on its own. It either asks the user to run the command and share output, or explicitly asks whether the user wants the skill to run it.

## Eval 2 — plan-scram-baseline

- [ ] **Cluster Type Decision does not claim a hard-limit trigger fired.** The profile has 1 cluster, peak egress 540 MBps, 2400 partitions — within Enterprise eCKU caps per the live docs. The Cluster Type Decision section should not claim a Row 1-N trigger or eCKU/partition cap was exceeded. Inventing a trigger to escalate to Dedicated, or naming a trigger that doesn't apply, fails this item.
- [ ] **Auth Approach does not recommend IAM or mTLS target without source-side basis.** The profile lists `auth_types: [scram]` only. The Auth Approach should not recommend an IAM or mTLS target without a stated, source-grounded reason (e.g., a customer-driven preference or a Confluent Cloud constraint named in the profile). Recommending IAM or mTLS as the primary target with no basis fails this item.

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
- [ ] **Does not invent capabilities when redirecting.** The skill does not fabricate a sibling skill, internal tool, partner program, or any other capability not documented in `SKILL.md` or its references. Naming a non-existent skill family or pretending an unwritten OSK-migration skill exists fails this item.

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

- [ ] **Three pattern × mechanism cells produce materially different runbook content.** The Incremental + Gateway, Big-bang + Gateway, and Big-bang + Manual CL cells each emit distinct content in the Plan: KCP group definition (multiple groups vs. single group vs. N/A), cutover cadence (per-group vs. single window vs. coordinated stop-and-restart), validation pattern, and rollback scope (per-group vs. all-or-nothing). Three labels for similar behavior fails this item.
- [ ] **Lands as a Confluent best-practice recommendation, not a neutral menu.** The Switchover Approach reads as a recommendation (Incremental + Gateway as the default), not as a neutral menu of equivalent options. The reader should come away knowing what Confluent recommends and why, not weighing four roughly-equal choices.
- [ ] **"Incremental + Manual CL not recommended" warning is clearly visible.** The note about Incremental + Manual CL being operationally heavy without the Gateway's atomic flip appears clearly enough that a customer reading the Plan would not pursue that path. A buried one-line footnote fails this item.
- [ ] **Dual-write framing discourages customers from picking it without strong reason.** The Dual-write row makes clear the operational complexity, dual cost, and absence of Confluent-specific tooling. A customer reading the Plan should not interpret Dual-write as a peer option to the other three patterns.

## Eval 21 — per-connector-classification

- [ ] **Per-connector table reads as actionable.** The per-connector table presents a clear path per connector (CMU + kcp create-asset for available, defer to account team for not-available). The table does not blur into a generic "use CMU for all MSK Connect connectors" recommendation that ignores per-connector classification.
- [ ] **Substitution-nuance handoff to account team lands specifically.** When a connector requires substitution-specific guidance (e.g., Debezium 1.x → V2 config key differences), the handoff to the Confluent account team is concrete — citing the specific substitution issue if known — rather than a generic "talk to Sales" punt. The third row of the per-connector table should differentiate substitution-nuance cases from straight CC-equivalent or no-equivalent cases.

## Eval 22 — networking-pni-default

- [ ] **PNI-default framing reads as Confluent best practice, not an arbitrary flip.** The Plan presents PNI as the recommended default for AWS-to-AWS private migrations, with the cost reasoning (cross-AZ traffic vs data processing + hourly endpoint fees) cited as the rationale. A reader should understand WHY PNI is preferred, not just that the skill prefers it.
- [ ] **Three citable PrivateLink exceptions cleanly distinguished from deferral cases.** The Plan distinguishes the three publicly cited exceptions (cc_egress_required, gateway limit reached, non-AWS target) from the deferral-to-account-team cases (compliance, organizational policy, latency-sensitivity, substrate constraints). A reader can tell which path applies to their situation without rereading paragraphs.

## Eval 23 — cl-direction-source-initiated

- [ ] **CL Direction section conveys reachability as the determinant.** The section helps the reader understand that direction is determined by network reachability (whether CC can reach MSK from the chosen target networking VPC), not by customer preference or convenience. Direction reads as a derived outcome of a cascade, not as a configuration choice the customer makes independently.
- [ ] **Manual-setup callout for source-initiated is actionable, not a buried handoff.** When the Plan emits source-initiated CL direction, the customer-owned manual-setup step is clearly stated with a path to the right doc (private-networking.html). A customer reading the Plan should understand: (1) what they need to do (establish the link manually before KCP migration commands resume), (2) where to look (private-networking.html), and (3) what's permanent (link.mode=SOURCE cannot be changed after the link is created).
