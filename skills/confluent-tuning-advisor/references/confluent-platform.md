# Confluent Platform Tuning Notes

Self-managed Confluent Platform exposes everything Confluent Cloud hides: broker-level configs,
rack awareness, JVM/GC tuning, and OS-level network/disk settings. That's more power and more
ways to get it wrong — every broker-level change in
[references/tuning-parameters.md](tuning-parameters.md) applies here, none of it is off-limits,
but changes to shared broker/cluster config affect every topic on the cluster.

## Ask about the environment before recommending broker-level changes

Broker-level and OS-level changes need context this skill can't assume:

- **Broker count and current `broker.rack` assignment** — without rack awareness configured,
  `replication.factor=3` can silently place all 3 replicas in the same failure domain (same rack,
  same AZ, same power feed). Ask whether `broker.rack` is set before treating RF 3 as a real
  availability guarantee.
- **Security configuration** (SASL, mTLS, Kerberos) — adds a fixed per-connection/per-request
  latency and CPU cost that varies by mechanism. Ask which mechanism is in use before attributing
  a latency gap to Kafka config; mTLS and Kerberos typically cost more than SASL/PLAIN over TLS.
- **JVM version and current heap/GC flags** — G1GC with a tuned `MaxGCPauseMillis` is the usual
  starting point for latency-sensitive brokers; a large heap with default GC settings is one of
  the reasons for p99 latency spikes that look like a Kafka problem but are a GC-pause problem.
- **Disk type and OS page cache headroom** — Kafka throughput leans heavily on the OS page cache;
  a broker with insufficient free RAM relative to the active log segment size will show up as a
  throughput ceiling no client-side or topic-level config fixes.

## Broker-level changes are cluster-wide

Unlike topic config, `num.network.threads`, `num.io.threads`, `replica.lag.time.max.ms`, and
`unclean.leader.election.enable` (when set at the broker/cluster default level) apply to every
topic the broker serves. Always call this out explicitly in the plan (SKILL.md Step 5) — a
change made to satisfy one workload's target can affect another workload on
the same cluster.

For a disruptive broker-level change (e.g. one requiring a restart to take effect), apply and
verify one broker at a time rather than a fleet-wide rolling change in a single step — confirm
cluster health (under-replicated partition count back to zero) between brokers before continuing.

## Rack awareness

If `broker.rack` isn't set and the workload has an availability target that depends on surviving
an AZ/rack loss, this is the first thing to fix — before any `min.insync.replicas` tuning matters.
Setting it does **not** redistribute existing replicas automatically. New topic assignments and
later replica redistributions honor the rack constraint; existing topics require an explicit
replica reassignment to gain rack-aware placement. Treat that reassignment as a separate,
plan-worthy operation because it moves data and consumes replication bandwidth.

## Hardware, OS, and JVM baseline (check before blaming Kafka config)

A large share of "Kafka is slow" problems are hardware, OS, or JVM problems that no client- or
topic-level knob will fix. Use the current [Confluent Platform deployment guidance](https://docs.confluent.io/platform/current/kafka/deployment.md)
for hardware sizing, memory and page-cache allocation, supported Java versions, and GC settings.

- **Memory:** preserve OS page-cache headroom; a throughput ceiling that no client or topic setting
  moves is often a page-cache-starved broker.
- **CPU:** account for TLS and compression costs, and avoid capacity that cannot sustain the target
  load.
- **GC:** correlate pauses with request latency before tuning Kafka. When p99 is high but p50 is
  healthy, GC is a leading diagnostic candidate. Treat any JVM flags as workload- and
  version-specific starting points from the current deployment and compatibility documentation.
- **Storage:** multiple data disks (`log.dirs`) or RAID 10 (RAID 1/10 preferred, RAID 5 not
  recommended); XFS or ext4; do not share Kafka disks with the OS or app logs; avoid file-based NAS.
- **Network and OS:** verify bandwidth, inter-broker latency, file descriptors, memory-map limits,
  and other prerequisites against the current [deployment guidance](https://docs.confluent.io/platform/current/kafka/deployment.md)
  and [system requirements](https://docs.confluent.io/platform/current/installation/system-requirements.md).
  For dissimilar cross-region links, evaluate
  [Multi-Region Clusters](https://docs.confluent.io/platform/current/multi-dc-deployments/multi-region.md).

Confirm these are sane before recommending broker config changes — say so plainly when the real
fix is hardware/OS/JVM rather than a Kafka knob.

## Multi-datacenter durability / DR (self-managed)

For region/DC loss, flag the architecture choice separately from topic configuration:

- **Active-passive** (one-way replication to a standby cluster) or **active-active** (two-way).
  Put this choice to the user explicitly as its own decision, named in both directions, and keep it
  separate from the replication tooling and from the stretched-cluster option below. Do not collapse
  the topology question into a single Cluster-Linking-versus-stretched-cluster comparison.
- Tooling: **Cluster Linking** is the standard — it replicates topics byte-for-byte and preserves
  consumer offsets, with no separate connect cluster to run. It is the default cross-cluster DR
  option on Confluent Platform. Use the Platform
  [Cluster Linking guide](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/index.md)
  for replication, failover, and RPO/RTO behavior, and the
  [multi-datacenter deployment guide](https://docs.confluent.io/platform/current/multi-dc-deployments/overview.md)
  to choose the surrounding architecture.
- A single stretched **Multi-Region** cluster with rack/observer placement is another model for near-zero RPO when
  network latency between sites allows it.

## Platform operation references

Use these platform-specific operations in the recommendation's "How to apply" block:

- **REST / Admin API**: use
  [Change Kafka Configurations Without Restart](https://docs.confluent.io/platform/current/kafka/dynamic-config.md)
  to determine which broker and cluster settings are dynamic, and the
  [REST Proxy v3 API reference](https://docs.confluent.io/platform/current/kafka-rest/api.md#rest-proxy-v3)
  for the supported configuration operations and current request formats.
- **CLI**: use the current [Confluent CLI documentation](https://docs.confluent.io/confluent-cli/current/overview.md)
  for topic-config operations supported by the installed version.
- **Config files**: a `server.properties` change requires a broker restart to take effect for most
  settings — recommend it as a restart, not a live update, sequenced one broker at a time with a
  cluster-health check between brokers.

## Reference docs

- [Confluent Platform broker configuration reference](https://docs.confluent.io/platform/current/installation/configuration/broker-configs.md)
- [Rack awareness](https://docs.confluent.io/platform/current/kafka/post-deployment.md#balancing-replicas-across-racks)
- [Confluent Platform topic configuration reference](https://docs.confluent.io/platform/current/installation/configuration/topic-configs.md)
- [Confluent Platform security overview](https://docs.confluent.io/platform/current/security/index.md)
- [Supported versions and interoperability](https://docs.confluent.io/platform/current/installation/versions-interoperability.md)
- [System requirements](https://docs.confluent.io/platform/current/installation/system-requirements.md)
- [Running Kafka in production (hardware, memory, CPU, storage)](https://docs.confluent.io/platform/current/kafka/deployment.md)
- [Best practices for production deployments](https://docs.confluent.io/platform/current/kafka/post-deployment.md)
- [Change Kafka configurations without restart](https://docs.confluent.io/platform/current/kafka/dynamic-config.md)
- [Cluster Linking for Confluent Platform](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/index.md)
- [Deploy Confluent Platform in a multi-datacenter environment](https://docs.confluent.io/platform/current/multi-dc-deployments/overview.md)
- [Monitoring metrics — see references/monitoring.md](monitoring.md)
