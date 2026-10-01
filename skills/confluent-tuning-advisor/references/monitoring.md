# Monitoring and Metrics for Tuning

Tuning without measurement is guessing. Read this file when the workflow reaches Step 7 (benchmark
and verify) or whenever a user asks "how do I know if this change helped / what should I watch."
For every change, define the **primary outcome** it targets, capture a **before/after baseline**, and
collect **diagnostic** metrics that explain the result plus **guardrail** metrics that catch
regressions. Don't infer causality from one metric or a single short sample, and don't expect one
metric to fill all three roles.

A useful tuning record includes client and broker versions, workload shape, message size,
compression, partition count, replication factor, `acks`, traffic rate, consumer processing time,
test duration, and the exact config delta.

Metric names below are JMX MBeans on **Confluent Platform** brokers and clients. On **Confluent
Cloud** the broker fleet is managed and these broker MBeans are not exposed — use the Cloud
**Metrics API** / the Confluent Cloud console for the server-side metrics they make available, and
the **client** JMX metrics (producer/consumer sections below) for the parts you run. Do not assume
every broker MBean has a direct Metrics API equivalent; discover the current metric descriptors.
See the [Cloud metrics source of truth](confluent-cloud.md#cloud-metrics-source-of-truth) for the
authoritative discovery, metrics-reference, and query documentation.

**If the target is Confluent Cloud, do not copy any broker MBean name from the sections below into
the recommendation.** Those sections exist for Platform targets. For Cloud, take server-side metric
names only from live descriptor discovery, and use this file solely for the client-metrics sections
(producer, consumer, and rebalance), which apply on both platforms.

## Baseline health checks (verify before comparing runs)

Confluent's minimum broker-health set. In tuning, treat these as gate checks: if any is unhealthy
during a run, the comparison is invalid — don't attribute results until it's clean.

| Metric | MBean | Healthy value | Meaning |
|---|---|---|---|
| `ActiveControllerCount` | `kafka.controller:type=KafkaController,name=ActiveControllerCount` | sum across brokers **= 1** | ≠1 means no controller or split brain |
| `OfflinePartitionsCount` | `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | **0** | partitions with no leader — not readable or writable |
| `UncleanLeaderElectionsPerSec` | `kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec` | **0** | a nonzero value signals an unclean election and potential loss of acknowledged writes |

Exclude planned maintenance, restarts, scrape gaps, and metric resets from the comparison window.

## By priority — what to watch when tuning each axis

### Replication, availability & durability
Availability and durability are distinct tuning objectives, but their signals overlap — read each metric by what it primarily describes:

- **Partition availability** — can partitions elect leaders and serve reads/writes?
  - `OfflinePartitionsCount` (`kafka.controller:type=KafkaController,name=OfflinePartitionsCount`) `> 0`, `ActiveControllerCount ≠ 1`, `LeaderElectionRateAndTimeMs` (`kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs`) nonzero on broker failures.
- **Replica health** — is redundancy holding, and is fetcher lag disk/network/CPU-bound?
  - `UnderReplicatedPartitions` (`kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`) — nonzero means reduced replica redundancy and recovery headroom (not necessarily unavailability; depends on leader, ISR, `acks`, `min.insync.replicas`). **Don't compare runs with materially different replication health.**
  - `IsrShrinksPerSec` / `IsrExpandsPerSec` (`kafka.server:type=ReplicaManager,...`) — steady-state **0**; churn means replicas keep dropping out of ISR.
  - `MaxLag` (`kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica`) rising = replication can't keep up (consider `num.replica.fetchers`).
- **Write safety** — can `acks=all` writes still commit, and did an election risk loss?
  - `UnderMinIsrPartitionCount` (`kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount`) — partitions below `min.insync.replicas`; `acks=all` writes to them fail.
  - `AtMinIsr` / `InSyncReplicasCount` (`kafka.cluster:type=Partition,topic={t},partition={p},name=...`) — a partition sitting at min ISR has no headroom.
  - `UncleanLeaderElectionsPerSec` (should be **0**) — a nonzero value risks loss of acknowledged writes; treat as a data-safety event.
  - Producer `record-error-rate` (client) — rate of records whose send result returned an error (not only previously-acknowledged writes). Guardrail: classify the underlying error (timeout, retriable, auth) before attributing any change to tuning.
- **Client-visible impact** — did the condition reach applications? See Latency, Throughput, and the consumer client section: `records-lag-max` rising means the group is falling behind; `failed-rebalance-rate-per-hour` / `rebalance-rate-per-hour` spikes are the usual cause of the "whole group stalls on deploy" symptom that `cooperative-sticky` fixes.

Tuning for availability: emphasize OfflinePartitionsCount, aggregate ActiveControllerCount, abnormal leader-election activity, UnderMinIsrPartitionCount, producer errors, and consumer progress/lag.

Tuning for durability: emphasize UnderReplicatedPartitions, ISR shrink/expand activity, replica MaxLag, AtMinIsr, UnderMinIsrPartitionCount, and UncleanLeaderElectionsPerSec.

These categories overlap. UnderReplicatedPartitions, ISR churn, replica lag, AtMinIsr, UnderMinIsrPartitionCount, and unclean elections are shared guardrails; interpret them according to whether the tuning goal is continued service or preservation of replica redundancy and acknowledged data.

### Latency
- Request-latency breakdown, `kafka.network:type=RequestMetrics,name={X},request={Produce|FetchConsumer|FetchFollower}`:
  `TotalTimeMs` = `RequestQueueTimeMs` + `LocalTimeMs` + `RemoteTimeMs` + `ResponseQueueTimeMs` + `ResponseSendTimeMs`. Note `TotalTimeMs` is **broker-side** request time, not producer-to-consumer end-to-end latency; compare it against client `request-latency-avg` to isolate client/network time outside the broker. Use the largest component to choose the next check:
  - High `RequestQueueTimeMs`: correlate `RequestQueueSize`, request-handler idleness, and CPU before changing `num.io.threads`; a full or growing queue can also mean request concurrency exceeds processing capacity.
  - High `LocalTimeMs`: investigate local storage and page-cache pressure; correlate `LogFlushRateAndTimeMs` and host disk latency/IO wait.
  - High `RemoteTimeMs`: for produce with `acks=all`, investigate follower/ISR and inter-broker network delay. For an idle or caught-up consumer/follower fetch, a value near the configured fetch wait can be normal rather than network slowness.
  - High `ResponseQueueTimeMs`: correlate network-processor idleness and host network pressure before changing `num.network.threads`.
  - High `ResponseSendTimeMs`: investigate socket backpressure and network-path pressure (NIC saturation, slow clients); don't attribute it solely to disk-to-network transfer.
- `NetworkProcessorAvgIdlePercent` (`kafka.network:type=SocketServer,...`) — low values indicate
  network-thread saturation. Establish the alert from the workload baseline and current Confluent
  monitoring guidance rather than treating one threshold as universal.
- `RequestHandlerAvgIdlePercent` (`kafka.server:type=KafkaRequestHandlerPool,...`) — 0 = saturated I/O threads (the signal to consider `num.io.threads`).
- Producer `request-latency-avg` / `record-queue-time-avg`, consumer `fetch-latency-avg`.
- If p99 is high but p50 is fine, correlate with GC pauses before touching Kafka config — see [references/confluent-platform.md](confluent-platform.md).

### Throughput
- `BytesInPerSec` / `BytesOutPerSec` / `MessagesInPerSec` (`kafka.server:type=BrokerTopicMetrics,name=...,topic={t}`; omit topic for cluster-wide).
- `RequestsPerSec` (`kafka.network:type=RequestMetrics,name=RequestsPerSec,request={Produce|FetchConsumer|FetchFollower}`) and `RequestQueueSize` (`kafka.network:type=RequestChannel,name=RequestQueueSize`) — evaluate request rate alongside byte throughput. Many small requests tend to consume CPU and queue capacity; high MB/s tends to pressure disk and network. When request rate is high but MB/s is modest, inspect producer `batch-size-avg` and `records-per-request-avg`; if batches are small, recommend client batching as the first lever before adding broker threads.
- `RequestHandlerAvgIdlePercent` / `NetworkProcessorAvgIdlePercent` near 0 = thread-pool pressure. Sustained low idle **with** growing queues or rising request latency points to the broker as the bottleneck; correlate request rate, queue size, CPU, disk/network before scaling threads, brokers, or partitions. (Contrast with the small-batch/high-request-rate case above, where client batching is the first lever.)
- Compare `PartitionCount`, `LeaderCount`, byte rates, request rates, and host utilization across
  brokers before calling the cluster capacity-bound. A single hot broker can indicate a failed
  peer, skewed leadership, or skewed replica placement rather than insufficient total capacity.
  On Confluent Platform, consider
  [Self-Balancing Clusters](https://docs.confluent.io/platform/current/clusters/sbc/index.md)
  for continuous imbalance detection and supported reassignment; otherwise recommend an
  administrator-reviewed preferred-leader election or replica reassignment and monitor ISR health
  while data moves.
- Producer `batch-size-avg`, `records-per-request-avg`, `compression-rate-avg`,
  `buffer-available-bytes` (near zero means the producer is backpressured; consult the current
  client-metrics reference for version availability).
- Consumer `fetch-rate`, `records-consumed-rate`, `fetch-size-avg`.

### Failure, throttle, and backpressure signals
- Broker: `ErrorsPerSec` (by request/error code), `FailedProduceRequestsPerSec`.
- Producer backpressure beyond `buffer-available-bytes`: `record-queue-time-avg`, `max.block.ms`
  timeouts, retry/error rates. A low buffer alone doesn't say *why* — slow brokers, quota
  throttling, weak batching, oversized records, or intentional burst.
- Throttling: producer/consumer `*-throttle-time-avg`. **Nonzero throttle is a diagnostic clue,
  not proof the change failed** — compare against throughput, latency, lag, and the intended quota.

### Host, KRaft, and application signals
- **Host & storage (Platform):** disk free/inode, read-write latency & queue depth, network
  drops/retransmits, CPU steal/run-queue, JVM heap-after-GC & pause duration, FD usage. Explain
  outliers with these; don't apply universal thresholds without a baseline.
- **KRaft (version-qualified):** controller state/leader, metadata commit/apply latency,
  event/record queue depth, broker heartbeat timeouts. Keep separate from ZooKeeper-era metrics.
- **Application/data path:** producer-timestamp-to-consumption latency, per-poll processing
  duration, deserialization/schema errors, DLQ rate. A fast broker can still hide downstream problems.

## Client metrics (apply on both Confluent Platform and Confluent Cloud)

You run the clients, so client-side metrics apply on both platforms — **but the MBean names below
are Java-client JMX.** For librdkafka-based clients (Python, Go, C/C++, .NET) use the native
statistics interface (`statistics.interval.ms` / stats callback) or a supported exporter, and map
equivalent concepts rather than copying MBean names. Verify names, units, and availability for the
specific client and version.

**Producer** — `kafka.producer:type=producer-metrics,client-id={id}`:
`record-error-rate`, `record-retry-rate`, `request-latency-avg`/`-max`, `batch-size-avg`,
`records-per-request-avg`, `compression-rate-avg`, `buffer-available-bytes`, `record-queue-time-avg`.

**Consumer** — `kafka.consumer:type=consumer-fetch-manager-metrics,client-id={id}`:
`records-lag-max` (max offset lag across partitions assigned to **one consumer instance** —
aggregate across members for a group view, and retain the per-partition max for diagnosis),
`records-lag`, `fetch-latency-avg`/`-max`, `fetch-rate`, `records-consumed-rate`,
`fetch-throttle-time-avg` (nonzero means a quota is throttling you — relevant on Confluent Cloud).
Pair offset lag with **time-based lag / event age** — offset lag has no fixed meaning across topics
with different produce rates.

**Consumer rebalance** — `kafka.consumer:type=consumer-coordinator-metrics,client-id={id}`:
`rebalance-latency-avg`/`-max`, `rebalance-rate-per-hour`, `failed-rebalance-rate-per-hour`,
`last-rebalance-seconds-ago`, `commit-latency-avg`, plus `heartbeat-response-time-max`, `join-rate`,
and commit error rate to separate processing slowness from group instability.

## Before/after experiment procedure

1. Record the exact config delta and the effective client/broker values.
2. Warm up until throughput, cache, assignment, and replication stabilize.
3. Capture baseline and treatment under identical traffic, message size, partitions, and failure conditions.
4. Compare p50/p95/p99/p99.9 (where tail matters) plus throughput, error rate, lag age, resource use.
5. Check guardrails: ISR health, producer errors/retries, throttling, rebalances, memory, disk, network.
6. Attribute only when the primary outcome moves consistently **and** diagnostics support a plausible mechanism.
7. Record result, side effects, and rollback condition.

## Reference docs

- [Kafka monitoring / metrics reference (Confluent Platform)](https://docs.confluent.io/platform/current/kafka/monitoring.md)
- [Self-Balancing Clusters](https://docs.confluent.io/platform/current/clusters/sbc/index.md)
- [Broker metrics](https://docs.confluent.io/platform/current/kafka/broker-metrics.md)
- [Log & network request metrics](https://docs.confluent.io/platform/current/kafka/log-network-metrics.md)
- [Producer metrics](https://docs.confluent.io/platform/current/kafka/producer-metrics.md) ·
  [Consumer metrics](https://docs.confluent.io/platform/current/kafka/consumer-metrics.md)
- [Monitor Confluent Cloud clients](https://docs.confluent.io/cloud/current/client-apps/monitoring.md)
- [Confluent Cloud Metrics API](https://docs.confluent.io/cloud/current/monitoring/metrics-api.md)
- [Confluent Cloud Metrics Reference](https://api.telemetry.confluent.cloud/docs/descriptors/)
