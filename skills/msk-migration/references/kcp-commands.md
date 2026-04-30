# KCP Commands

Judgment on when to use each KCP command and what to look for in the output. Not a man page — see [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) for full flag reference.

## Command Selection by Migration Stage

| Stage | Command | What You're Looking For |
|---|---|---|
| Assess | `kcp discover` | MSK cluster count, regions, cluster types (Provisioned/Serverless) |
| Assess | `kcp scan clusters` | Topic/partition counts, ACLs, auth types, Kafka version |
| Assess | `kcp scan schema-registry` | Subject count, SR type, compatibility. Supports Confluent SR and AWS Glue (`--sr-type=glue`). |
| Assess | `kcp scan client-inventory` | Active producers/consumers (from broker logs) |
| Assess | `kcp report costs` | Monthly MSK spend breakdown |
| Assess | `kcp report metrics` | Peak throughput for sizing, storage utilization |
| Provision | `kcp create-asset target-infra` | Terraform for CC environment + cluster |
| Migrate | `kcp create-asset migration-infra` | Cluster Linking + migration resources |
| Migrate | `kcp create-asset migrate-topics` | Mirror topic Terraform |
| Migrate | `kcp create-asset migrate-schemas` | Schema migration config. Supports Glue via `--glue-registry`. |
| Migrate | `kcp create-asset migrate-acls` | RBAC binding Terraform |
| Migrate | `kcp create-asset migrate-connectors` | CMU input configs |
| Switchover | `kcp migration init` | Validate setup, form migration groups, write migration state |
| Switchover | `kcp migration list` | Show migration groups, topics, and clients per group |
| Switchover | `kcp migration lag-check` | Real-time per-topic replication lag |
| Switchover | `kcp migration execute` | Full cutover: fence → promote at zero lag → switch routing → restore traffic |

## Key Things to Watch in KCP Output

- `kcp discover`: 0 clusters → check AWS credentials and region selection.
- `kcp scan clusters`: 0 topics → Kafka auth credentials may be wrong. Check `cluster-credentials.yaml`.
- `kcp scan schema-registry --sr-type=glue`: requires Glue API permissions.
- `kcp report metrics`: MSK Serverless returns limited metrics (throughput only, no per-broker).
- `kcp create-asset`: always review generated Terraform before applying (Invariant #10).
- `kcp migration init`: errors if IAM auth is detected on source — must pre-migrate off IAM before proceeding.
- `kcp migration lag-check`: wait for sustained zero lag, not momentary.
- `kcp migration execute`: runs 4 phases (pre-flight, block, promote, switch). Resumable if interrupted.

## Live Verification

KCP releases frequently and flag surfaces change between versions. Before recommending a specific command or flag, the skill verifies against the live repo: fetch the relevant page under [github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) or run `kcp --help` / `kcp <subcommand> --help` if KCP is installed. The command guidance above is directional — if the live repo differs, the live repo wins.

## Full CLI Reference

[github.com/confluentinc/kcp](https://github.com/confluentinc/kcp) — README, docs/, and `kcp --help` for current flags.
