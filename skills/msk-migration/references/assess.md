# Assess

The goal of Assess is to produce an **environment profile** — enough data about the source MSK environment to make architecture decisions in Plan. Done when cluster count, Kafka version, auth type, networking accessibility, and topic/partition scale are captured, with any red flags surfaced.

## Ambiguities do not block the Assess output

When a state file is provided, produce the **full Assess output** (Scan Coverage + Environment Summary + Red Flags checklist + Additional Observations + handoff prompt) in one response. Do not stop mid-assessment to ask clarifying questions. Ambiguities and gaps belong in the output itself, not as blockers before it:

- **Ambiguous scan coverage** (e.g., `.schema_registries` empty — could mean no SR or scan didn't run) → report the Scan Coverage row as "Empty / ambiguous" and proceed. Capture the question in Open Questions at the end.
- **Unknown red flags** (e.g., EOS / Kafka Streams usage not detectable from state file) → report the row as "Unknown (needs user confirmation)" and proceed.
- **Null-vs-empty ambiguity** (e.g., connector fields `null` rather than `[]`) → report the row as "Unknown (scan gap)" and proceed.

The user gets a complete picture first, then decides which ambiguities to close. Don't interrupt the flow with mid-stream questions — that forces the user to answer piecemeal before they've seen the full picture.

**Only two exceptions block Assess:** (1) the state file is malformed or unreadable, or (2) the user hasn't yet provided a state file or picked a path. In either case, ask — but every other ambiguity goes into the output and is resolved via Open Questions.

## State File Audit — run first when a KCP state file is provided

Before producing any environment summary, run this scan-coverage audit. This forces every Assess session to read the state file the same way and prevents missing data that's present (e.g., skipping region-level fields like `costs` because you went straight to per-cluster details). Do not skim; run the queries.

**Audit checklist.** For each KCP scan, check whether it ran by testing the corresponding state file path. Use `jq` for each query (per the parsing directive below). Do not use inline Python.

| Scan | jq path to test | If populated | If empty / missing |
|---|---|---|---|
| `kcp discover` | `.regions[].clusters[] \| .name` | Clusters enumerated — proceed | State file effectively empty; reject and request a re-scan starting at `kcp discover` |
| `kcp scan clusters` | `.regions[].clusters[] \| .kafka_admin_client_information.topics.details \| length` | Topics, partitions, ACLs, auth, Kafka version captured per cluster | Gap — topic/ACL/auth data missing, needs re-scan before Plan |
| `kcp scan schema-registry` | `.schema_registries \| length` | SR inventory captured | Either no SR exists in the environment OR scan wasn't run — clarify with the user. Don't assume schemaless. |
| `kcp scan client-inventory` | `.regions[].clusters[] \| .discovered_clients \| length` | Producers/consumers discovered | Gap — needed for Switchover planning. Acknowledge the gap, recommend re-running with broker-log access (usually S3) before Switchover. |
| `kcp report costs` | `.regions[] \| .costs.results \| length` | AWS cost breakdown available per region | Gap — business-case data missing. Optional for migration mechanics but surface to the user. |
| `kcp report metrics` | `.regions[].clusters[] \| .metrics.results \| length` | CloudWatch throughput and storage metrics available | Gap — Plan sizing will be estimated rather than measured. Flag. |

**Always report audit results to the user before summarizing the environment.** Format the report as a "Scan Coverage" section showing which scans ran, which didn't, and what the missing ones would add. This surfaces gaps the user needs to close before Plan.

**Data grounding — inline parenthetical citations are required on every Environment Summary fact.** Every numeric or categorical claim in the Environment Summary (cluster count, brokers, instance type, Kafka version, topic/partition counts, throughput, auth, networking, SR, connectors, costs, drivers) carries an inline parenthetical citation to its source path at first use. Citing the field path only in the separate Scan Coverage table or in the Red Flags evidence column does not satisfy this — the user needs to verify each fact in place without cross-referencing another table.

Citation path style depends on intake mode:

- **KCP state file:** `jq`-style path into the JSON (e.g., `regions[0].clusters[].aws_client_information.msk_cluster_config.Provisioned.BrokerNodeGroupInfo.InstanceType`).
- **Manual `migration-profile.yaml`:** YAML key path from the top of the file (e.g., `clusters[0].broker_count`, `source.kafka_version`, `schema_registry.subject_count`).

When a field is missing, say so explicitly — don't fabricate. The same inline-citation rule applies in Plan (`references/plan.md` "Plan Doc Conventions — cite every number"); this paragraph applies the same discipline upstream in Assess so the Environment Summary the user sees is verifiable in place.

**CloudWatch metric labels — always use Cluster Aggregate, not Broker Aggregate.** The state file's `.metrics.results[]` array contains metrics with different labels including both `"Cluster Aggregate - *"` and `"Broker Aggregate - *"` variants. Cluster Aggregate gives the cluster-wide value (what you want for sizing and capacity decisions). Broker Aggregate gives per-broker values (which don't usefully sum to a cluster total). Always filter on `Cluster Aggregate` unless you have a specific reason to look at per-broker behavior.

Canonical metric labels to use:

- `"Cluster Aggregate - BytesInPerSec"` — cluster-wide ingress throughput
- `"Cluster Aggregate - BytesOutPerSec"` — cluster-wide egress throughput
- `"Cluster Aggregate - TotalRemoteStorageUsage(GB)"` — cluster-wide tiered storage volume
- `"Cluster Aggregate - PartitionCount"` — cluster-wide partition count
- `"Cluster Aggregate - MessagesInPerSec"` — cluster-wide message rate
- `"Cluster Aggregate - ClientConnectionCount"` — cluster-wide connection count

Convert bytes/sec values to MBps using 1 MB = 1,048,576 bytes (binary).

**Topic and partition count convention.** Always report topic and partition counts as `user + internal = total` when the split is available (e.g., "1,124 user + 53 internal = 1,177 total topics; 4,058 user + 53 internal = 4,111 total partitions"). Never report only one of the three without labeling which it is. Internal topics don't migrate — they're recreated by Kafka, Connect, or Streams on the destination — but knowing both counts is useful for environment understanding.

Source fields per intake type:
- **KCP state file:** `.topics.summary.topics` (user), `.topics.summary.internal_topics` (internal), `.topics.summary.total_partitions` (user), `.topics.summary.total_internal_partitions` (internal). Always available.
- **Manual `migration-profile.yaml`:** `user_topic_count` / `internal_topic_count` (topics) and `user_partition_count` / `internal_partition_count` (partitions). These are optional fields. When populated, apply the convention. When `null`, fall back to the aggregate `topic_count` / `partition_count_pre_replication` and label them as totals — e.g., "1,177 total topics (user/internal split not captured in manual profile)". Do NOT fabricate a split. Note the gap in `known_gaps` so a follow-up scan can fill it later.

## Intake Path Selection

Pick the richest path available.

### KCP deep scan

**First mention of KCP in a conversation requires an introduction.** Don't assume the user knows what KCP is. Briefly explain: KCP is Confluent's open-source migration tool ([github.com/confluentinc/kcp](https://github.com/confluentinc/kcp)) for assessing AWS MSK environments and generating migration assets for Confluent Cloud.

Use when the KCP CLI is installed (`kcp --version`) and the user has AWS credentials for the source account.

**Trust KCP; don't front-load questions the scan will answer.** When the user chooses KCP, walk them through the commands in sequence. Ask each command's specific preconditions right before running that command, not upfront as generic intake. The only upfront prereq checks are:

1. KCP installed (`kcp --version` or `kcp version`)
2. AWS credentials available for the source account (read access to MSK, EC2, CloudWatch, Cost Explorer)

Do NOT ask about regions, auth types, Schema Registry type, cluster count, networking topology, topic counts, or any other environment detail before scanning — the scan reveals these. Network reachability (private MSK requires broker line-of-sight for the deep cluster scan) is a precondition for `kcp scan clusters`; raise it at that step, not as pre-scan intake. The point of the KCP path is to let the tool discover the environment, not to have the user describe it manually before running it.

**Required sequence to produce a data-rich state file.** Run in order. Each command appends to the same state file (typically `kcp-state.json`). Verify exact flags against `kcp <cmd> --help` or the [repo](https://github.com/confluentinc/kcp) — flags change between versions.

| # | Command | Essential? | What It Adds to the State File |
|---|---|---|---|
| 1 | `kcp discover` | **Required** | Cluster ARNs, metadata per region |
| 2 | `kcp scan clusters` | **Required** | Topics, partitions, ACLs, auth, Kafka version, broker configs per cluster. Requires Kafka auth credentials (typically in `cluster-credentials.yaml`). |
| 3 | `kcp scan schema-registry` | **Required if any SR exists** | Subjects, versions, compatibility. Supports Confluent SR and AWS Glue (via `--sr-type=glue`). |
| 4 | `kcp scan client-inventory` | **Strongly recommended** | Producers and consumers discovered from broker logs. Requires log access (typically S3). Skip only if logs aren't reachable. |
| 5 | `kcp report metrics` | **Required for sizing** | CloudWatch peak throughput, storage utilization per cluster. Feeds Plan-stage sizing. |
| 6 | `kcp report costs` | Optional (business case) | AWS Cost Explorer breakdown. Nice for a business case; not required for migration mechanics. |

**Minimum for Plan decisions:** steps 1, 2, 3 (if SR exists), and 5.
**Minimum for confident Switchover:** add step 4.
**Full business case:** all six.

**Present the full 6-step sequence first, before any prereq check or scan step.** When the user picks the KCP path, the skill's next response must show the sequence table above so the user sees the complete map of what's ahead. Then invite them to start — don't jump straight to `kcp version` checks or run any command. Only after the user is ready do you begin step 1.

Once the user is ready, walk through the sequence one step at a time. For each scan command: state what's about to happen, ask only the precondition that command needs, present the command, and ask the user whether they want to run it themselves (paste output back) or have the skill run it. Per the command-execution principle in SKILL.md, user approval is required before the skill runs anything. Concretely:

- Before step 1 (`kcp discover`): ask which AWS region(s) to sweep. If the user doesn't know, note that `kcp discover` can scan multiple or all regions.
- Before step 2 (`kcp scan clusters`): ask about Kafka auth credentials per cluster. Help the user build `cluster-credentials.yaml` if needed. **Also confirm network reachability** — `kcp scan clusters` needs broker-protocol access to each cluster. If MSK is private, the scan host must be inside the VPC, VPN-connected, or reachable via Direct Connect. `kcp discover` works from anywhere (AWS APIs); `kcp scan clusters` needs Kafka-protocol reachability.
- Before step 3 (`kcp scan schema-registry`): the cluster scan output will indicate whether SR is in use. If yes, confirm SR type (Confluent or Glue) — usually visible from the config or the user can confirm.
- Before step 4 (`kcp scan client-inventory`): ask if broker logs are accessible (typically S3). If not, skip and record the gap.
- Steps 5 and 6: no preconditions — just run.

Output: `kcp-state.json` — the canonical environment artifact that feeds Plan, Provision, Migrate, and asset-generation stages.

**Parsing the state file.** Use `jq` for structured queries and the Read tool for full-file inspection. Do NOT use inline Python (`python3 <<EOF ... EOF`) or Node (`node -e ...`) to parse state files. Reasons: (1) KCP state files contain many optional fields that may be null, and Python scripts crash on `len()`/iteration against null without defensive coding every script would need; (2) inline interpreters are slow and noisy in Claude Code; (3) `jq` handles null fields cleanly and is the right tool for JSON query. When running jq through the user (per command-execution principle in SKILL.md), offer the exact jq expression and ask if they want to run it or have the skill run it.

**Run each `jq` query as its own Bash tool call.** Per the one-command-per-Bash-call principle in SKILL.md (Skill Conduct), issue each audit query as a separate Bash invocation rather than batching into a compound shell command (variable assignment + chained `jq` calls). Applies to the 6-scan coverage audit and any multi-query sequence in Assess.

**Fetching docs.** Same rule applies to live docs: use `WebFetch`, not `curl | python3` or heredoc scripts that strip HTML. `WebFetch` handles HTML→markdown and targeted extraction in one call. See SKILL.md "Fetch tool" directive for the full rule.

### Manual intake

Use when KCP is not available (restricted AWS account, pre-engagement intake, customer doesn't have credentials to run KCP).

- Copy `assets/migration-profile.yaml` to the user's working directory.
- Walk the user through the 11 intake question groups below, filling the YAML as you go.
- Output: populated `migration-profile.yaml`. Functionally equivalent to a KCP state file for downstream stages, but less detailed.

## Conversational Intake Questions (11 groups)

Walk these in order during manual intake. Record answers in the environment profile.

1. **Platform.** MSK Provisioned or MSK Serverless? AWS region(s)? Single-cluster or multi-cluster?
2. **Cluster topology.** Broker count, instance type (Provisioned), broker AZ distribution. Cross-region replication in place?
3. **Scale.** Peak ingress MBps, peak egress MBps, topic count, partition count (pre-replication), replication factor.
4. **Auth.** SASL/SCRAM, mTLS, AWS IAM, Unauthenticated — which is in use? Mixed within the cluster or per-cluster?
5. **Networking.** Private (VPC-internal only) or public? If private, VPC topology: single-VPC, hub-and-spoke, direct VPC peering, Transit Gateway?
6. **ACLs.** Rough ACL count. Kafka ACLs, MSK IAM policies, or both?
7. **Schema Registry.** Confluent SR (which version?), AWS Glue, Karapace, or none? Approximate subject count.
8. **Connectors.** MSK Connect managed connectors? Self-managed Connect clusters? Connector types (which source/sink systems)?
9. **Kafka version.** Source Kafka version. Cluster Linking requires a minimum — verify against the [CL source requirements doc](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking) live.
10. **Costs.** Rough monthly MSK spend (can pull via KCP `report costs` in Path B).
11. **Migration drivers.** Why migrate? What's the timeline? Any hard cutover constraint (peak season, compliance deadline)?

If the user can't answer a question, flag it as a known gap. Don't fabricate.

## Red Flags — required checklist plus judgment-based additions

Red flags are **structural migration concerns** — things that change the migration plan by blocking a path, adding pre-migration work, forcing cluster type escalation, or requiring special handling at Switchover. They are NOT the same as environment observations (scale, characteristics, topology facts) which belong in the Environment Summary.

**Distinguishing red flag vs. environment observation:**

| Is it a red flag? | Examples |
|---|---|
| Yes — structural migration concern | IAM auth blocks Zero-Cut; Kafka version below CL floor; tiered storage won't transfer via CL; EOS not supported on CL |
| No — environment observation (cover in Summary) | Private networking; cluster size; fan-out ratio; broker instance type; topic count distribution |

Assess output contains two finding sections: **Red Flags** (required checklist) and **Additional Observations** (judgment-based findings).

### Red Flags — required checklist, walk in order

Every Assess output walks these 16 rows **in order**, numbered 1–16, reporting one of three statuses for each:

- **Triggered** — condition is present; include specific evidence (which cluster, what value).
- **Not triggered** — condition is absent; include the row with "Not triggered" so the user sees it was checked.
- **Unknown** — scan gap prevents determination; include with "Unknown (scan gap)" and note what's needed to close.

Do NOT reorder, merge, rename, or skip rows. Additional color about a triggered item goes in the evidence, not as a new row.

**Null vs. empty for admin-scan fields — disambiguate using `topics.details`.** In the state file, `acls`, `connectors` (from MSK Connect side under `aws_client_information`), and `self_managed_connectors` are slices that KCP initializes as `nil` and appends to. On a successful scan that found zero entries, nothing is appended and the nil slice marshals to `null`. `null` does not, by itself, mean the scan didn't run.

Disambiguate per cluster using `.kafka_admin_client_information.topics.details` as the signal:

- **Topics populated AND cluster is PROVISIONED** → the admin scan succeeded. Apply per-row logic:
  - **Row 9 (ACLs):** `acls: null` means **zero ACLs found**. Row 9 applies if IAM is also enabled.
  - **Row 6 (MSK Connect):** `connectors: null` means zero MSK Connect connectors found. Row 6 = Not triggered.
  - **Row 7 (self-managed Connect):** `self_managed_connectors: null` is NOT decisive on its own. KCP's self-managed Connect scanner only matches the literal topic name `connect-configs`, so prefixed worker fleets (`connect-configs-cdc`, `connect-configs-events`, etc.) are invisible to it. Row 7's status is governed by the Row 15 cross-reference, not by this field. **If Row 15 identifies a Connect framework triad (naked `connect-offsets`/`connect-status`/`connect-configs` OR a prefixed triad), Row 7 = Triggered.** If Row 15 finds no triad, Row 7 = Not triggered. Do NOT report Row 7 as "Unknown (scan gap)" when topics are populated — the scan ran; the scanner is just blind to prefixed fleets.
- **Topics populated AND cluster is SERVERLESS** → KCP intentionally skips ACL scanning on serverless. `acls: null` is expected. Note as a KCP limitation, not a scan gap. **Authz migration still applies when IAM is enabled.** Serverless authorization lives in IAM policies, not Kafka ACLs — but the IAM-policy → CC RBAC translation is the same migration step. When Row 3 is Triggered on a Serverless cluster, surface `kcp create-asset migrate-acls iam` as the authz migration path in the Row 9 evidence (or in the commands-the-skill-would-propose section), independent of Row 9 itself being N/A. Do not defer this to "Plan-stage decision" without naming the command.
- **Topics null or empty** → the cluster admin scan didn't run successfully (cluster not in credentials file, or scan errored mid-flight). All downstream admin fields are scan gaps, not confirmed zeros. Rows 6, 7, 9 report **Unknown (scan gap)**.

`discovered_clients` is a separate case — it's populated by `kcp scan client-inventory`, a different command. `null` there reliably means that scan didn't run (Row 10).

**Manual-intake default for absent fields.** When a row's check names a KCP state file field that has no equivalent in the manual `migration-profile.yaml` schema, the absence of evidence in the manual profile defaults to **Not triggered** — report it as such with a note like "not indicated in manual profile." Do NOT report Unknown (scan gap) for fields the manual schema simply doesn't carry. Only treat as Unknown when the user explicitly says they don't know whether the condition applies (e.g., "I'm not sure if tiered storage is on"), or when a row has explicit "Unknown — confirm with owning teams" guidance (Rows 13, 14). Schema silence ≠ scan failure ≠ user uncertainty.

| # | Red Flag | Check | Response when triggered |
|---|---|---|---|
| 1 | Schemaless source (no SR) | `.schema_registries` empty or missing | Ask in Plan: adopt SR during migration or continue schemaless. Record the decision. |
| 2 | Kafka version below CL minimum | `.regions[].clusters[].aws_client_information.msk_cluster_config.Provisioned.CurrentBrokerSoftwareInfo.KafkaVersion` below current CL floor (verify live) | CL incompatible. Upgrade source or alternative path. |
| 3 | IAM auth enabled on any cluster | `.aws_client_information.msk_cluster_config.Provisioned.ClientAuthentication.Sasl.Iam.Enabled == true` (Provisioned) OR `.Serverless.ClientAuthentication.Sasl.Iam.Enabled == true` (Serverless) | Pre-migrate IAM clients to SCRAM or mTLS before Zero-Cut. Confirm current path against the [KCP zero-cut guide](https://github.com/confluentinc/kcp/blob/main/docs/assets/getting-started-with-zero-cut-migrations.md). **Also surface `kcp create-asset migrate-acls iam` as the authz migration path** — IAM-policy → CC RBAC translation applies whether ACLs were scanned (Provisioned) or intentionally skipped (Serverless). |
| 4 | AWS Glue Schema Registry in use | `.schema_registries[].type == "glue"` or user reports Glue | Schema Linking may not apply. Verify live. Default path is REST API export/import. |
| 5 | Partition count >10,000 on any cluster | `.topics.summary.total_partitions` (user) > 10,000 | High complexity. Plan for phased migration. |
| 6 | MSK Connect managed connectors present | `.aws_client_information.connectors \| length > 0` | Check CC managed-connector availability before assuming CMU can migrate each. |
| 7 | Self-managed Connect clusters present | Primary signal: `.kafka_admin_client_information.self_managed_connectors \| length > 0`. **Cross-reference with Row 15:** if Row 15 identifies a Kafka Connect framework triad (matching `connect-offsets`, `connect-status`, `connect-configs` under a common prefix like `-cdc` or `-events`), Row 7 **triggers** even when `self_managed_connectors` is null or empty. Reason: KCP's self-managed Connect scanner (`scanSelfManagedConnectors` in `internal/services/kafka/kafka_service.go`) looks for a topic named literally `connect-configs` and returns empty when it's not present — so prefixed worker-fleet triads (e.g., `connect-configs-cdc`, `connect-configs-events`) are NOT detected even when they're clearly active. A null/empty scan result does not rule out self-managed Connect when Row 15 has topic evidence. | Check CC connector availability and CMU support for self-managed Connect. When Row 7 triggers via Row 15 cross-reference, cite the specific triads found (e.g., "`connect-configs-cdc` + `connect-offsets-cdc` + `connect-status-cdc` → self-managed Connect fleet active, not visible to KCP scanner"). **Recommend closing the connector scan gap before Plan commits connector paths.** Name the specific follow-up: re-run `kcp scan self-managed-connectors` (or, if KCP can't reach the worker fleets, dump configs from each Connect cluster's REST API) so Plan has the actual connector inventory — types, source/sink systems, throughput — before picking migration paths. State this explicitly in the Assess output's commands-the-skill-would-propose section, not only as an Open Question to ask owners. |
| 8 | Multi-region source | `.regions \| length > 1` | Each region may need a separate CC environment and CL setup. |
| 9 | Zero Kafka ACLs with IAM enabled | Topics populated AND cluster is PROVISIONED AND (`acls == null` OR `acls \| length == 0`) AND IAM enabled. See null-vs-empty guidance above. | Triggered: scan confirmed zero ACLs with IAM on — authz is in IAM policies, not Kafka ACLs. `kcp create-asset migrate-acls iam` is the migration path. Report **Unknown (scan gap)** only when topics are also null/empty (whole admin scan didn't run). |
| 10 | Client inventory gap | `.discovered_clients \| length == 0` on any cluster | Not a blocker for Plan; blocker for confident Switchover. Recommend re-running `kcp scan client-inventory` with broker-log (S3) access before cutover. |
| 11 | MSK Express broker tier in use | InstanceType matches `express.*` | Newer MSK tier. Verify CL compatibility with Express brokers live against [CL source requirements](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking). |
| 12 | Tiered storage in use | **KCP state file:** `.msk_cluster_config.Provisioned.StorageMode == "TIERED"` on any cluster. **Manual `migration-profile.yaml`:** `clusters[].storage_mode == "TIERED"` on any cluster. If `storage_mode` is null/absent on every cluster, report **Not triggered** with the note "tiered storage not indicated in manual profile" — do NOT report Unknown (scan gap). The schema's null is the absence of evidence; only treat as Unknown when the user explicitly says they don't know whether tiered storage is in use. When triggered, also surface `peak_tiered_storage_gb` per cluster (if populated) — Plan's Tiered Storage section uses this to size the backfill discussion. | Historical tiered data does not transfer via CL. Decide at Plan: keep source accessible during retention window or bulk backfill. |
| 13 | EOS / Kafka transactions in use | Not directly in state file | Mark as "Unknown — confirm with owning teams." CL does not support EOS/transactions. |
| 14 | Kafka Streams apps consuming from source | Internal topics matching `*-changelog` or `*-repartition` are a signal | Mark "Unknown — confirm." If present, changelog topics must NOT be mirrored; state stores rebuild post-cutover. |
| 15 | Connect / tooling activity inferred from internal topic names | Scan `.regions[].clusters[].kafka_admin_client_information.topics.details[].name` for **any** topic-name pattern that suggests connector, replication, or tooling activity beyond the connector scan. See signal categories below. | If matches found, list each topic (or pattern + count) with cluster, infer the tool when possible, and flag as Plan/Migrate scope expansion. Connect workers and tooling often run outside the scanned MSK perimeter — the connector scan can show zero while actual connector activity is clearly present in internal topics. |
| 16 | Cost-to-inventory reconciliation (hidden / undiscovered clusters) | **KCP state file:** Extract broker instance types from AWS Cost Explorer usage-type strings in `.regions[].costs.results[].Groups[].Keys[]` (patterns like `Kafka.m7g.*`, `Express.m7g.*`, `Kafka.t3.*`). Compare the set of instance types appearing in cost data against the set of instance types in the discovered cluster inventory (`.regions[].clusters[].aws_client_information.msk_cluster_config.Provisioned.BrokerNodeGroupInfo.InstanceType`). **Manual `migration-profile.yaml`:** Compare `cost_line_items[].usage_type` strings against the `broker_instance_type` values on `clusters[]`. If `cost_line_items` is empty/absent, report **Not triggered** with the note "cost data not provided in profile" — not a scan gap, but a known absence the user can fill in later. | Triggered if any cost-data instance type does not correspond to a discovered cluster AND the spend is material — judge materiality relative to the discovered-cluster cost baseline (a line item on par with or larger than one of the scanned clusters is material; a line item that persists across most months of the cost window is material even if individually small). Cite the usage-type string and annual/monthly spend as evidence. Likely indicates a cluster that `kcp discover` did not enumerate (different region, different AWS account, or excluded by scan parameters). Recommend re-running `kcp discover` across all regions and all relevant accounts before committing Plan scope. Single-month line items that don't recur may be a decommissioned or resized cluster — note and deprioritize. |

**Signal categories for Row 15 (not an exhaustive list — recognize the pattern, don't just match names):**

- **Non-standard `__`-prefixed topics.** `__consumer_offsets`, `__transaction_state`, `__amazon_msk_canary` are Kafka/MSK internals and expected. Anything else starting with `__` is likely connector or tooling output.
- **Heartbeat topics.** Names containing `heartbeat` or `heartbeats` (with or without `__` prefix). Common signal of a source connector that emits heartbeats for lag monitoring.
- **Kafka Connect framework triads.** The presence of topics named `connect-offsets`, `connect-status`, `connect-configs` (or any three-topic set with those suffixes under a common prefix) indicates a Kafka Connect cluster using this Kafka as its coordination backbone.
- **Streams artifacts.** Topics ending in `-changelog`, `-repartition`, `-changelog-repartition`, or matching `<app-id>-<store>-changelog` patterns indicate Kafka Streams applications.
- **MirrorMaker 2 artifacts.** Topics prefixed with `mm2-`, or with cluster-alias prefixes (`<alias>.<topic>`), or named `heartbeats`, `mm2-offset-syncs`, `mm2-status`, etc. indicate active replication.
- **Tool-specific internal topics.** Anything obviously tooling-scoped — Cruise Control artifacts, observability/metric-collection tooling, custom pipeline framework internals. If the name looks like it belongs to infrastructure rather than an application, treat as tooling signal.
- **Suspiciously symmetric topic sets.** Three topics sharing a prefix with different suffixes (e.g., `<prefix>-offsets`, `<prefix>-status`, `<prefix>-configs`) are usually framework output — Connect, MirrorMaker, custom pipeline tooling.

**Known common patterns (illustration only — DO NOT treat as exhaustive):** `__debezium-*`, `__mongodb-*`, `__KafkaCruiseControl*`, `connect-*`, `mm2-*`, `*-changelog`, `*-repartition`. Real customer environments will have patterns not listed here (Elasticsearch, Snowflake, JDBC, Salesforce, custom plugins, internal naming conventions). Match the **signal category**, not a fixed string list.

**Required regex patterns when scanning topic names.** Use these anchored patterns — loose matching produces false positives on domain names that happen to contain similar strings:

- **MirrorMaker 2:** `\.checkpoints\.internal$`, `^mm2-`, `mm2-offset-syncs`, `mm2-status`, `mm2-heartbeats`. The `<alias>.checkpoints.internal` suffix is the clearest MM2 signature — it's produced by active replication and easy to miss if your MM2 regex is too narrow (e.g., only matching `^mm2-` misses `<alias>.checkpoints.internal`).
- **Kafka Streams (anchored suffixes only):** `-changelog$`, `-repartition$`, `-changelog-repartition$`. Use the end-of-name anchor. Loose matching like `contains("changelog")` produces false positives on domain names (e.g., `pubsub-blockstreamer-*` is a block-streaming domain name, not a Streams state store).
- **Kafka Connect framework triad:** look for all three of `connect-offsets`, `connect-status`, `connect-configs` under a common prefix (naked or `<prefix>-offsets`/`<prefix>-status`/`<prefix>-configs`). The full triad together is the signal — a single `connect-offsets` topic alone is weaker evidence.
- **Source connector heartbeats:** topics containing `heartbeat` or `heartbeats` (with or without `__` prefix) — `__debezium-heartbeat.*`, `__mongodb_heartbeats`, and similar patterns per source-connector convention.

Anchored suffix matching and full-triad matching reduce false positives. When in doubt, cite the specific topic names that match so the user can eyeball whether they're real framework output or coincidentally-named domain topics.

**What to do with findings:** list the discovered patterns with evidence (cluster, topic names, counts) and, where possible, infer what the source tool is. If a pattern doesn't match a recognized tool, still flag it — "unknown tooling activity observed on X topics" is better than silent omission.

### Additional Observations (judgment-based, with guardrails)

The 16-row Red Flags checklist is a **floor, not a ceiling**. The skill won't catch every novel migration concern on its own — real migrations surface patterns we haven't cataloged (new MSK features, regulatory constraints, unusual client behaviors, enterprise-specific configurations). Include additional findings in an "Additional Observations" section when they qualify.

**A finding belongs in Additional Observations only if ALL of these hold:**

1. It's a **structural migration concern** (blocks a path, adds pre-migration work, forces cluster type escalation, or requires special handling at Switchover). If it's just an environment characteristic, put it in Environment Summary, not here.
2. It **traces to a specific state file field or scan observation** — not a hunch, not general MSK knowledge. Cite the field path or the observed pattern.
3. It's **not already captured by a Red Flags row**. If it's a more specific version of an existing row (e.g., "Express broker tier with 1.3M msg/sec" is still Row 11, Express in use — add the msg/sec as evidence, not a new row).
4. It would **change the Plan if confirmed** — e.g., force a different cluster type, require a pre-migration workstream, constrain the switchover pattern.

Format Additional Observations as a short list below the Red Flags checklist, each with: finding, state file evidence, implication for Plan.

**Additional Observations is where the Red Flags checklist grows from.** If the same observation shows up across multiple migrations, it should become a new Red Flags row. Surface that to the user when you notice a pattern: "This has come up before — consider adding to the Red Flags checklist."

### What NOT to include as red flags

- Private networking. That's a networking characteristic, not a red flag — it's the expected case for production MSK. Cover in Environment Summary.
- "Cluster X is the load-bearing cluster." Observation, not a red flag. Cover in Notable Shape if relevant.
- Normal fan-out ratios (egress > ingress). Not a red flag on its own. Plan stage handles cluster type via the hard-limits table.
- Generic "high scale" observations without a specific threshold trigger.
- Any concern you can't tie to a state file field or observed pattern.

**Handoff framing — use this consistent phrasing at the end of Assess output:**

Once the checklist is produced and the Environment Summary is populated, end Assess with:

> **Ready to move to Plan?** Open questions above can carry forward — they don't block Plan production (Plan commits with working assumptions). Say "write the Plan" to proceed, or answer specific open questions first to tighten the assumptions Plan will make.

Do not invent alternate phrasings ("What would you like to do?" / "Where do you want to go from here?" / etc.). The consistent prompt tells the user exactly what to say next.

## What "done" looks like for this stage

An environment profile exists — as a KCP state file, MCP-captured data, or a populated `migration-profile.yaml`. At minimum:

- Cluster count, Kafka version, auth type (per cluster), networking accessibility, topic/partition scale
- Schema Registry state (type, subject count, or confirmed schemaless)
- Connector inventory (count, types, managed vs self-managed)
- Peak throughput (ingress/egress MBps)
- Red flags surfaced and acknowledged

Missing fields are recorded as known gaps, not fabricated.

## Source of Truth

- KCP CLI commands and flags — [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp). Do not quote flags from memory; fetch live before recommending a command.
- Cluster Linking source compatibility — [cluster-linking docs](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking). Version cutoffs change; verify live.
