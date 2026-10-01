# Tuning Parameter Matrix

One row per setting, one column per priority. "Recommended" values are starting points for a
typical workload — reconcile against the user's quantified target and actual cluster size before
finalizing the plan (see SKILL.md Step 4).

> **Client defaults vary by version and implementation.** Consult the current client configuration
> reference and phrase recommendations as a diff from the *actual* current value (Step 3 baseline),
> not from a remembered default.

> **Config-key names are the Java (`kafka-clients`) names.** The recommended *values* generally
> transfer to the librdkafka family — the Python (`confluent-kafka`), Go, C/C++, and .NET clients —
> but names, units, defaults, and partitioning differ, so a value is never safe to copy blind:
> - **Aliases:** `linger.ms` is `queue.buffering.max.ms` in librdkafka. librdkafka also has *two
>   separate* batch limits — `batch.size` (bytes) and `batch.num.messages` (record count) — not a
>   single alias of Java's `batch.size`, so state which unit a recommendation targets.
> - **Defaults differ, and by version:** as of current Java and librdkafka client versions, Java
>   `linger.ms` is `0` before Kafka 4.0 and `5` from 4.0 on, versus `5` in librdkafka; `enable.idempotence`
>   defaults to `true` in Java but `false` in librdkafka; `max.in.flight.requests.per.connection` defaults
>   to `5` in Java but `1000000` in librdkafka. Confirm the installed client's current default before
>   relying on it — the *baseline you diff from* is client- and version-specific even when the
>   recommended value is not.
> - **Partitioner differs:** Java hashes keys with murmur2 while librdkafka's default
>   (`consistent_random`) uses CRC32, so the *same key can land on a different partition* across
>   clients. Align the partitioner (e.g. librdkafka `partitioner=murmur2_random`) before mixing Java
>   and librdkafka producers on one keyed topic.
>
> Recommend the Java key with its value, note the librdkafka alias, and confirm the actual client
> library and version before giving a copy-paste config. This skill does not cover Kafka Streams,
> Flink, or Kafka Connect clients (see SKILL.md).

## Producer configuration

| Setting | Latency | Throughput | Availability | Durability | Why |
|---|---|---|---|---|---|
| `acks` | `1` (lower latency; leader responds before replicas — incompatible with `enable.idempotence=true`, and can lose acknowledged writes on leader failure); `all` only when durability must be preserved | `1` (Confluent's throughput recommendation); `all` only when durability must be preserved | `all` for durability of acked data; note `acks=all` blocks writes when the ISR falls below `min.insync.replicas`, so pure *write* availability favors `1` (or, as an explicit availability-over-durability exception, lowering `min.insync.replicas`) | `all` | `all` waits for every in-sync replica (a latency/throughput cost); `1` only waits for the leader and is Confluent's latency/throughput recommendation. `acks=all` does not require idempotence; the dependency runs the other way: `enable.idempotence=true` requires `acks=all`. `acks=1` plus `enable.idempotence=true` is never a valid combination — how a specific client resolves the conflict (reject outright vs. silently disable idempotence) is client- and version-specific, so validate the actual behavior per client rather than assuming one; either way, never present `acks=1` plus idempotence as a valid config. |
| `enable.idempotence` | keep default `true` (its sequence-number overhead is small; the `acks=all` it requires is the real *latency* cost — see `acks`); set `false` only if you have deliberately chosen `acks=1` | keep default `true` (its sequence-number overhead is small; the `acks=all` it requires is the real *throughput* cost — see `acks`); set `false` only if you have deliberately chosen `acks=1` | default (`true`) | `true` (required for idempotent producer *delivery*; full exactly-once *also* needs transactions + correct offset handling) | Prevents duplicate writes on retry and preserves ordering with supported in-flight settings. Its sequence-number overhead is small, but the `acks=all`/retry behavior it requires can materially affect latency and throughput (that cost lives in the `acks` row). It is incompatible with `acks=1`: disable it only when you have deliberately chosen `acks=1` and accepted duplicates/reordering. Idempotence gives idempotent *delivery* within a producer session, not end-to-end exactly-once (that needs transactions). |
| `compression.type` | `none` (spare CPU cycles); `lz4` if some compression is acceptable | `zstd` (best ratio) or `lz4` (best CPU/latency balance) | n/a | n/a | Compression trades CPU and a small per-batch latency cost for less network/disk I/O — a latency/throughput lever, not an availability or durability one. `gzip` has the best ratio but the worst CPU cost — avoid it for latency-sensitive paths. |
| `linger.ms` | `0` (but see note) | `10`–`100` | n/a | n/a | Batching improves throughput and reduces per-message overhead. Linger adds *up to* `linger.ms` of latency for a not-yet-full batch — a batch that fills first is sent immediately. It is a latency/throughput lever only. The default is version-dependent (Java `0` before Kafka 4.0, `5` from 4.0 on; librdkafka `5`), so diff from the observed baseline, not a remembered default. **Latency nuance:** at *low per-partition produce rate*, a small `linger.ms` (`5`–`10`) can sometimes lower p99 by reducing request volume — treat that as a hypothesis to benchmark, not a general rule. |
| `batch.size` | client default | `100000`–`200000` | n/a | n/a | Larger batches amortize request overhead; only helps if `linger.ms` or produce rate actually fills them. Do not raise it for latency without benchmark evidence. |
| `buffer.memory` | default | increase (e.g. `67108864`+) under sustained high-rate produce | default — a larger buffer only *delays* blocking (see `max.block.ms`); it is not a cluster-availability lever | default | Sizes the producer's send buffer against message size × active partitions × batch/linger/in-flight. When it is exhausted, `send()` blocks for up to `max.block.ms`; a larger buffer absorbs bursts but only postpones blocking under sustained backpressure and raises memory/OOM risk. Set it to an explicit memory budget, not as an availability fix. |
| `max.in.flight.requests.per.connection` | `5` with idempotence, or `1` only if idempotence is unavailable and strict ordering on retry matters | same | same | same | With `enable.idempotence=true`, up to 5 in-flight requests preserve ordering safely; without it, `5` risks reordering on retry. |
| `delivery.timeout.ms` / `retries` | lower timeout, fail fast | default/higher | higher (ride out transient issues) | set from the record's business-validity window, not simply "higher" | Set `delivery.timeout.ms` from how long the record stays valid and your recovery objective; a durability- or availability-first workload should retry through transient broker unavailability rather than surface an error. Control retry behavior via `delivery.timeout.ms` (kept consistent with `request.timeout.ms`), not by tuning `retries` alone — and note that retries cannot make an unsafe `acks` setting durable. |

## Consumer configuration

| Setting | Latency | Throughput | Availability | Durability | Why |
|---|---|---|---|---|---|
| `fetch.min.bytes` | `1` (default) | increase (Confluent recommends ~`100000`) | default | default | Higher values make the broker wait for more data before responding — better throughput, worse tail latency for sparse topics. |
| `fetch.max.wait.ms` | low (`50`–`100`) | default (`500`) | default | default | Caps how long the broker waits to satisfy `fetch.min.bytes`; the latency/throughput lever comes as a pair with the setting above. `500` is the default, so the throughput move is raising `fetch.min.bytes`, not this. |
| `max.partition.fetch.bytes` / `fetch.max.bytes` | default | raise alongside `fetch.min.bytes` so a larger response can actually be returned | default | default | These cap the fetch response per partition and per request; left at default they can bound the effect of a higher `fetch.min.bytes`. The first record batch is returned even if it exceeds these limits, so a consumer never stalls on an oversized message. |
| `max.poll.records` | default or lower | increase (e.g. `1000`+) | reduce if per-poll processing is slow (see note below) | default | Larger batches per poll reduce per-call overhead but increase the time between polls; if processing exceeds `max.poll.interval.ms` the member is removed and the group rebalances — a poll-interval eviction, *not* a heartbeat/`session.timeout.ms` one. |
| `isolation.level` | `read_committed` only if the consumer must hide aborted/open transactional records; else `read_uncommitted` | same | same | `read_committed` when reading a transactional topic whose aborted records must not be seen | `read_committed` skips aborted/open transactional records (a small latency cost) and is required *only* when the consumer must not read them; a consumer that accepts those visibility semantics can use `read_uncommitted`. It is a transactional-visibility/correctness setting — it does not itself make data durable. |
| `session.timeout.ms` / `heartbeat.interval.ms` | default | default | *increase* the session timeout to ride out network blips and avoid spurious rebalances (Confluent's default availability guidance; `session.timeout.ms` default `45000`, `heartbeat.interval.ms` default `3000` and must stay ≤ ⅓ of the session timeout); *lower* it only to detect a genuinely *dead* consumer faster, never so low it causes false-positive rebalances | default | These detect a *dead* consumer via missed heartbeats (a background thread), so raising them rides out blips while lowering them speeds dead-consumer recovery — they do **not** govern slow processing (that is `max.poll.interval.ms`; see note). Under the classic protocol these are client-controlled; the newer consumer group protocol (KIP-848) makes them server-side, so this row may not apply. |
| `partition.assignment.strategy` | n/a | n/a | `cooperative-sticky` | n/a | Cooperative rebalancing makes a rebalance incremental — only the partitions that actually move pause, instead of a stop-the-world pause across the whole group — so it reduces rebalance *impact*, not frequency. Every group member must support it, so migrate via a compatible rolling upgrade; the newer consumer group protocol (KIP-848) moves assignment server-side, where this client setting no longer applies. |

**Consumer availability — separate the two failure signals.** Heartbeats run on a background thread
governed by `session.timeout.ms`/`heartbeat.interval.ms` and detect a *dead* consumer (crashed,
network-partitioned, or process-paused). A *slow* consumer is different: it exceeds
`max.poll.interval.ms` — the maximum time allowed between `poll()` calls — and is removed *even while
heartbeats continue*. The fixes are therefore different, and raising `session.timeout.ms` does not
address slow processing:

- **Slow processing (poll-interval eviction):** reduce `max.poll.records`, raise
  `max.poll.interval.ms`, or move processing off the poll thread while preserving offset correctness.
- **Dead consumer / network blips (heartbeat detection):** tune `session.timeout.ms` and
  `heartbeat.interval.ms` (lower to detect faster, raise to tolerate blips).

Exact behavior depends on the consumer protocol and client implementation. Also prefer static group
membership (`group.instance.id`) to avoid unnecessary reassignment during brief, expected restarts —
a static member still triggers a rebalance if it stays away past `session.timeout.ms`.

## Topic / replication configuration

| Setting | Latency | Throughput | Availability | Durability | Why |
|---|---|---|---|---|---|
| `replication.factor` (topic creation/reassignment) | `3` (recommended minimum; Confluent Cloud default — the Apache Kafka broker `default.replication.factor` is `1`) | `3` | `3` (minimum for surviving one broker/AZ loss — only if replicas are placed across independent failure domains and the cluster has capacity to run after the loss; see `broker.rack`) | `3` | See [Replication factor is a placement operation](#replication-factor-is-a-placement-operation). Below 3, you cannot both tolerate one failure and keep `min.insync.replicas` at 2. This is rarely worth lowering even for "throughput" — it's a replication-traffic cost, not a client-facing one. |
| `min.insync.replicas` | n/a (only enforced with `acks=all`, and even then the producer waits for the full ISR — not a latency lever) | `2` (with `acks=all` and RF 3) | `1`–`2` — lowering it keeps writes enabled with fewer in-sync replicas as long as a writable leader remains; it does not make leaders survive more failures, and it widens the loss window | `2` (with RF 3 and `acks=all`) — never `1` | With RF 3, `min.insync.replicas=2` is the standard balance: tolerates one broker down while still requiring 2 durable copies before ack. Dropping to 1 trades away durability for availability during a double failure. |
| `unclean.leader.election.enable` | n/a | n/a | `true` only if the workload can tolerate silent data loss to stay writable during an all-replicas-down scenario | `false` (the only durable choice — enable `true` solely as the explicit availability trade-off in the column at left) | This is the sharpest availability/durability trade-off in Kafka: `true` lets an out-of-sync replica become leader (cluster stays up, but recent acknowledged writes on the lost leader can vanish). Default to `false` unless the user explicitly accepts the loss risk. |
| `cleanup.policy` | n/a | n/a | n/a | `compact` for keyed latest-state semantics; `delete` (or `compact,delete`) otherwise | Compaction retains the latest value per key as the log cleaner runs (asynchronously); tombstones are still removed after `delete.retention.ms`, and `compact,delete` is a valid combined policy. It is a retention/read-model choice for "latest state," not a substitute for replication, `acks`, or backups. |
| `num.partitions` | more partitions = more parallel consumers = can lower end-to-end latency under load, but each partition adds replication/metadata overhead | more partitions = more producer/consumer parallelism, up to the point of diminishing returns (broker file-handle/replication overhead) | fewer partitions = faster leader election/failover per partition during a broker loss | n/a | Partition count is a throughput/parallelism lever first; treat "just add partitions" requests skeptically once past what the workload's actual concurrency needs. |
| `retention.ms` / `retention.bytes` | n/a | n/a | n/a | set generously for replay/recovery needs | Longer retention widens the *recoverability/replay* window (time to notice and replay a bad write); it does not by itself improve durability, which comes from replication, placement, ISR policy, and `acks`. It also raises storage cost and interacts with `cleanup.policy` and tiered storage. |

### Replication factor is a placement operation

Replication factor is not a topic-config key/value change. For a new topic,
set it in the topic-creation request. For an existing topic on Confluent Platform, use an explicit
replica reassignment plan, confirm sufficient broker capacity and replication bandwidth, and monitor
the reassignment and ISR health until it completes. A reassignment moves data and can temporarily
increase network and disk load. Do not delete and recreate an existing topic solely to change RF;
that unnecessarily disrupts clients and risks data loss. On Confluent Cloud, follow the current Cloud documentation for the
cluster tier's supported replication-factor behavior; do not present broker placement or RF as a
user-managed broker setting.

## Broker / cluster configuration (Confluent Platform only)

Confluent Cloud manages all of these — see
[references/confluent-cloud.md](confluent-cloud.md) for what Cloud exposes instead (quotas,
cluster tier, multi-zone placement).

| Setting | Latency | Throughput | Availability | Durability | Why |
|---|---|---|---|---|---|
| `num.network.threads` | increase if the network-processor pool is saturated — many connections or high request rate | increase | default | default | Network-processor threads read requests off sockets and write responses back; raise them for connection/network-I/O concurrency, not for per-request CPU. Over-provisioning on an already-idle broker does nothing. |
| `num.io.threads` | increase if request handling is CPU/disk-bound | increase | default | default | Request-handler threads do the actual request processing and disk I/O — this is the pool to raise when handling, not the network, is the bottleneck. Over-provisioning on an already-idle broker does nothing. |
| `socket.send.buffer.bytes` / `socket.receive.buffer.bytes` | default | increase on high-bandwidth-delay-product (cross-region) links | default | default | OS-level TCP buffer sizing matters most when brokers and clients are far apart on the network. |
| `num.replica.fetchers` | increase if replication is a latency bottleneck | increase if replication is a throughput bottleneck | improves ISR catch-up speed after a broker rejoins | improves | More fetcher threads let a recovering replica catch up to the ISR faster, shrinking the window where `min.insync.replicas` is at risk. |
| `replica.lag.time.max.ms` | n/a | n/a | higher = more tolerant of transient slow replicas (avoids unnecessary ISR shrink, keeping `min.insync.replicas` satisfiable) | lower = stricter about what counts as "in sync" (shrinks the ISR sooner, so `acks=all` writes fail sooner when replicas lag) | Controls when a lagging follower is dropped from the ISR based on fetch progress — it does not itself make acked records more durable. Raising it keeps slow followers in the ISR longer (availability); lowering it tightens ISR membership. Keep `replica.fetch.wait.max.ms` below this value to avoid needless ISR shrink. |
| `broker.rack` | n/a | n/a | set to the physical AZ/rack — lets Kafka spread replicas across failure domains | set alongside `min.insync.replicas` for real fault-domain durability | Without rack awareness, all 3 replicas of a partition can land in the same AZ, silently defeating `replication.factor=3` as an availability guarantee. |
| JVM heap / GC (`-Xmx`, G1GC settings) | start from the supported-platform JVM baseline and tune only from GC logs and allocation metrics (G1; treat `MaxGCPauseMillis` as a tuning dial, not a fixed target) | larger heap tolerable | n/a | n/a | See [references/confluent-platform.md](confluent-platform.md) for the p99/GC diagnostic and baseline. |

## Cross-cutting notes

- **Latency vs. throughput is the most direct trade-off**: batching (`linger.ms`, `batch.size`,
  `fetch.min.bytes`/`fetch.max.wait.ms`) is throughput's main lever and latency's main cost.
- **Availability vs. durability is the second axis**: `min.insync.replicas` and
  `unclean.leader.election.enable` are the sharpest levers, but `acks`, replication-factor
  *placement*, idempotence, retries/`delivery.timeout.ms`, transactions, and retention/recovery
  design all participate — always state which way the recommendation leans and why.
- **Treat "optimize all four at once" skeptically:** these axes usually trade off against one
  another, so improving one often costs another unless you add resources (more hardware, more
  brokers/partitions, better placement). This is a rule of thumb, not a hard invariant — say so, and
  make each recommendation conditional on the stated goal and the measured baseline.

## Partition sizing

Partition count is the primary throughput/parallelism lever, but it is not free. Size it, don't
guess it.

- **Lower bound (throughput):** you need at least `max(t/p, t/c)` partitions, where `t` = target
  throughput, `p` = throughput you measure on a *single partition* from one producer, `c` =
  throughput one consumer can sustain on a single partition. `c` is application-dependent — measure
  it, don't assume it. This is a floor, not an answer: add headroom for growth, key skew (hot
  partitions), consumer-processing variance, and failure/rebalance scenarios.
- **Consumer parallelism is capped by partition count:** Kafka gives each partition to exactly one
  consumer in a group, so the number of actively consuming members in one group is bounded by the
  partition count. Too few partitions serialize consumers regardless of client tuning.
- **Upper bounds:** supported and practical partition ceilings depend on the Confluent Platform or
  Cloud version, cluster type, metadata mode, and workload. Check the current platform limits and
  partition-sizing documentation rather than relying on a fixed ceiling.
- **Costs of *more* partitions:** longer leader-election time on an unclean broker failure, more
  replication and recovery overhead, more open files (each segment carries several log/index files —
  validate broker open-file usage for your version and retention profile), and more client memory
  (producer memory scales with active partitions × `batch.size`, max record size, in-flight requests,
  and compression — size it from those, don't assume a fixed per-partition figure). p99 latency tends
  to rise as partitions-per-broker grows (more replication and metadata work), but more partitions
  can also *lower* application latency through parallelism — benchmark the net effect for your
  workload and bottleneck rather than assuming a fixed relationship.
- **Ordering caveat:** with a consistent partitioner and a stable partition count, messages with the
  same key route to the same partition and are ordered within it. Ordering is *per-partition* — not a
  cross-producer or cross-language guarantee: Java (murmur2) and librdkafka (CRC32 by default) hash
  keys differently, so align the partitioner before mixing clients (see the intro). Changing
  partition count later re-maps keys and breaks that ordering — so over-provision partitions up front
  for anticipated growth rather than repartitioning a keyed topic in place.

## Benchmarking and service goals

The core method behind this whole skill (from Confluent's *Optimizing Your Apache Kafka
Deployment*): **define the service goal first, tune toward it, then benchmark to prove it moved** —
there is no one-size-fits-all config, it depends on hardware, data profile, and enabled features.

- Measure **end-to-end latency** with an event timestamp carried in the record — Java `send()` is
  asynchronous and `poll()` returns a batch, so neither call marks completion. Define the endpoint
  explicitly (consumer receipt, deserialization, or processing complete) and report **p50/p95/p99**,
  not just the average — tail latency is what SLAs are written against.
- Measure throughput as sustained MB/s and msgs/s during the produce phase.
- Real trade-offs are measurable, not just theoretical. Use the linked Confluent benchmark as an
  illustration, but present the administrator's own before/after results rather than carrying its
  hardware-specific figures into a recommendation.
- A before/after benchmark (e.g. `kafka-producer-perf-test` / `kafka-consumer-perf-test`, or the
  administrator's own load-test harness) produces and consumes records — a write — so it is run by
  the **administrator**, never by this advisory-only skill; see SKILL.md Step 7.

## Reference docs

- [Confluent Platform producer configuration reference](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.md)
- [Confluent Platform consumer configuration reference](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.md)
- [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- [Optimizing Your Apache Kafka Deployment (white paper / blog)](https://www.confluent.io/blog/optimizing-apache-kafka-deployment/)
- [Tail Latency at Scale / Configure Kafka to Minimize Latency](https://www.confluent.io/blog/configure-kafka-to-minimize-latency/)
- [Optimize and Tune Confluent Cloud Clients — per-goal value tables](https://docs.confluent.io/cloud/current/client-apps/optimizing/overview.md)
  ([durability](https://docs.confluent.io/cloud/current/client-apps/optimizing/durability.md) ·
  [throughput](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.md) ·
  [latency](https://docs.confluent.io/cloud/current/client-apps/optimizing/latency.md) ·
  [availability](https://docs.confluent.io/cloud/current/client-apps/optimizing/availability.md))
- [How to Choose the Number of Topics/Partitions in a Kafka Cluster](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/)
