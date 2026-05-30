# KCP Commands

Curated index of KCP commands used during Assess and Plan, with judgment notes on when to use each and what to look for in the output.

**This file is a curated index, not a full reference.** It covers the commands the skill recommends most often, with judgment guidance the user can't get from `kcp --help` alone (e.g., "0 clusters returned → check AWS credentials and region selection"). For anything not covered here — full flag references, command behavior in specific KCP versions, edge cases, new commands shipped after the skill's last update — route the user to the canonical KCP repo:

- **Full CLI reference:** [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) — README, `docs/`, and `kcp --help` / `kcp <subcommand> --help`.
- **Live verification.** Before recommending a specific command or flag, fetch the relevant page under [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) (or run `kcp --help` if KCP is installed). KCP releases frequently and flag surfaces change between versions. The judgment guidance below is directional — if the live repo differs, the live repo wins.

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
| Assess | `kcp scan client-inventory` | Active producers/consumers (from broker logs) |
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
- `kcp migration init`: errors if IAM auth is detected on source — must pre-migrate off IAM before proceeding.
- `kcp migration lag-check`: wait for sustained zero lag, not momentary.
- `kcp migration execute`: runs 4 phases (pre-flight, block, promote, switch). Resumable if interrupted.
