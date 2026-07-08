# KCP Commands

Curated index of KCP commands used during Assess and Plan, with judgment notes on when to use each and what to look for in the output.

**This file is a curated index, not a full reference.** It covers the commands the skill recommends most often, with judgment guidance the user can't get from `kcp --help` alone (e.g., "0 clusters returned → check AWS credentials and region selection"). For anything not covered here — full flag references, command behavior in specific KCP versions, edge cases, new commands shipped after the skill's last update — route the user to the canonical KCP repo:

- **Full CLI reference:** [KCP Command Reference](https://confluentinc.github.io/kcp/latest/command-reference/) (the structured, generated CLI reference — primary full-reference link) alongside [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) — README, repo context, and `kcp --help` / `kcp <subcommand> --help`.
- **Live verification.** Before recommending a specific command or flag, fetch the relevant page on the docs site or repo (or run `kcp --help` if KCP is installed). KCP releases frequently and flag surfaces change between versions. The judgment guidance below is directional — if the live source differs, the live source wins.

### How to fetch the docs site correctly

The KCP docs site uses a `/latest/` alias so links never go stale across releases. **`/latest/` is a client-side redirect, not a content page.** Fetching it returns only a stub like `Redirecting to ../../0.8.4/command-reference/`. That stub is not an error — it is how you resolve the current published version.

Two-step fetch:

1. Fetch the `/latest/` URL (e.g. `https://confluentinc.github.io/kcp/latest/command-reference/`). Read the version number out of the `Redirecting to ...` target (e.g. `0.8.4`).
2. Re-fetch the version-pinned URL (e.g. `https://confluentinc.github.io/kcp/0.8.4/command-reference/`) to get the actual content.

Always start from a `/latest/` link so you pick up the newest docs, then resolve to the version-pinned path for content. Never hardcode a version in a recommendation.

**Use the real page slugs below — do not guess conventional names.** The site does not have a `/getting-started/` page; the slugs are specific.

| Section | Path (append to `.../<version>/`) |
|---|---|
| Home / install | `/` |
| Source Compatibility | `source-compatibility/` |
| Command Reference (index) | `command-reference/` |
| Per-command reference | `command-reference/<command>/` (e.g. `command-reference/scan/`, `command-reference/migration/`) |
| Apache Kafka config | `apache-kafka-configuration/` |
| Getting Started (Zero-Cut) | `getting-started-with-zero-cut-migrations/` |
| Gateway Switchover | `gateway-switchover/` |

## When to route to the KCP repo

Route the user to [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) when:

- They ask about a command not listed in the index below.
- They ask for full flag references on a command (the index covers judgment, not exhaustive flags).
- They ask about behavior that varies by KCP version (the live repo is the source of truth for current version).
- They ask about a new feature shipped after this file was last updated.
- They ask about edge cases (the file covers common cases; edge cases live in the docs).

In each case, name the relevant doc page or `kcp <subcommand> --help` invocation directly rather than guessing.

## Command Selection by Migration Stage

| Stage | Command | What You're Looking For |
|---|---|---|
| Assess | `kcp discover` | MSK cluster count, regions, cluster types (Provisioned/Serverless) |
| Assess | `kcp scan clusters` | Topic/partition counts, ACLs, auth types, Kafka version |
| Assess | `kcp scan schema-registry` | Subject count, SR type, compatibility. Supports Confluent SR and AWS Glue (`--sr-type=glue`). |
| Assess | `kcp scan client-inventory` | Active producers/consumers (from broker logs). Optional — most migrations can skip it; helps with client-level detail. |
| Assess | `kcp scan self-managed-connectors` | Self-managed Connect clusters that use this Kafka as their coordination backbone (looks for the `connect-configs` topic). |
| Assess | `kcp report costs` | Monthly MSK spend breakdown |
| Assess | `kcp report metrics` | Peak throughput for sizing, storage utilization |
| Provision | `kcp create-asset target-infra` | Terraform for CC environment + cluster |
| Migrate | `kcp create-asset migration-infra` | Cluster Linking + migration resources |
| Migrate | `kcp create-asset migrate-topics` | Mirror topic Terraform |
| Migrate | `kcp create-asset migrate-schemas` | Schema migration config. Supports Glue via `--glue-registry`. |
| Migrate | `kcp create-asset migrate-acls kafka` | Kafka ACLs → CC RBAC binding Terraform |
| Migrate | `kcp create-asset migrate-acls iam` | MSK IAM policies → CC RBAC binding Terraform. Use this when source cluster uses AWS IAM auth (Provisioned or Serverless). |
| Migrate | `kcp create-asset migrate-connectors` | CMU input configs |
| Switchover | `kcp migration init` | Validate setup, form migration groups, write migration state |
| Switchover | `kcp migration list` | Show migration groups, topics, and clients per group |
| Switchover | `kcp migration lag-check` | Real-time per-topic replication lag |
| Switchover | `kcp migration execute` | Full cutover: fence → promote at zero lag → switch routing → restore traffic |

## Cluster Credentials File (`cluster-credentials.yaml`)

`kcp scan clusters` needs Kafka auth credentials per cluster to run the admin scan. Format and location are documented in the KCP repo — fetch [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) before walking the user through filling it out. Common setup pattern:

- One entry per cluster ARN in scope.
- Auth section per cluster matching the source's auth (SCRAM, mTLS, IAM, unauthenticated).
- File should not be committed; treat like any other credentials file.

If the user is missing credentials for a cluster, the admin scan returns empty for that cluster — Row 9 reports Unknown (scan gap) rather than confirming zero ACLs. Help the user gather credentials before re-running the scan.

## Key Things to Watch in KCP Output

- `kcp discover`: 0 clusters → check AWS credentials and region selection.
- `kcp scan clusters`: 0 topics → two causes. (1) **Private-network unreachability** — on a private cluster with no broker line-of-sight, KCP silently skips the cluster and still reports success. If `msk_cluster_config` is populated (discover succeeded via AWS APIs) but `topics.details` is empty, this is the reachability case, NOT a zero-topic cluster: re-run `kcp scan clusters` from inside the VPC (VPN / Direct Connect / a host in the MSK VPC), or fall back to manual intake for the topic/partition/ACL layer. (2) **Wrong Kafka auth credentials** — check `cluster-credentials.yaml`. Either way, topics/scale is foundational — do not proceed to Plan on an assumed scale.
- `kcp scan schema-registry --sr-type=glue`: requires Glue API permissions.
- `kcp scan self-managed-connectors`: matches on the literal topic name `connect-configs`. Prefixed worker fleets (e.g., `connect-configs-cdc`) are invisible to it — cross-reference Row 15 (internal topic name patterns) for triads under common prefixes that indicate a Connect fleet the scanner missed.
- `kcp report metrics`: MSK Serverless returns limited metrics (throughput only, no per-broker).
- `kcp create-asset`: always review generated Terraform before applying (Invariant #10).
- `kcp create-asset migrate-acls iam`: only applies when source uses AWS IAM auth. Translates IAM policies into CC RBAC bindings. Useful even when Kafka ACLs are absent (e.g., Serverless or IAM-only Provisioned) — IAM is the authz source there.
- `kcp migration init`: errors if IAM auth is detected — this is the **Gateway** path, which does not accept IAM. For the default plain-Cluster-Linking path, an IAM source is handled per the auth mapping (CL-only SCRAM listener or KCP jump cluster) and does not require this command.
- `kcp migration lag-check`: wait for sustained zero lag, not momentary.
- `kcp migration execute`: runs 4 phases (pre-flight, block, promote, switch). Resumable if interrupted.
