# Confluent Cloud Tuning Notes

Confluent Cloud manages the broker fleet — there is no `server.properties`, no JVM tuning, no
`broker.rack` to set by hand. Tuning happens through **cluster tier choice**, **topic-level
config**, **client-side config**, and **quotas**, not broker internals.

## Cluster tier sets the availability ceiling first

Before touching any config knob, identify the cluster type and consult the current
[Confluent Cloud cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.md)
documentation for its availability model, SLA, networking options, replication behavior, and
capacity units. If the target exceeds the tier's documented availability or placement guarantees,
recommend an appropriate tier or architecture change; topic configuration cannot compensate for a
cluster-level limitation.

## What you can and cannot set

Use the current [Confluent Cloud topic configuration](https://docs.confluent.io/cloud/current/topics/manage.md)
reference to determine which topic settings are editable and their supported ranges. Treat
replication and placement behavior as cluster-type capabilities, not user-managed broker settings.

**Not settable — fully managed by Confluent**: `unclean.leader.election.enable` (Cloud keeps this
`false`), `num.network.threads`/`num.io.threads`, `replica.lag.time.max.ms`, `broker.rack`,
JVM/GC settings. If a recommendation from
[references/tuning-parameters.md](tuning-parameters.md) names one of these, note that Cloud
already applies the durability-safe default and move on — don't present it as an action item.

**Client-side config** (producer/consumer `acks`, `compression.type`, `linger.ms`,
`fetch.min.bytes`, etc.) works exactly as documented in
[references/tuning-parameters.md](tuning-parameters.md) — Cloud doesn't change client-side
tuning semantics. Note `compression.type` on Cloud is a **producer** setting only — it is not a
settable topic config on Confluent Cloud, so recommend it on the client, never as a topic override.
Confluent's per-goal value tables for Cloud clients are the authoritative source — see
[Optimize and Tune Confluent Cloud Clients](https://docs.confluent.io/cloud/current/client-apps/optimizing/overview.md).

## Capacity limits replace some broker-level throughput levers

Cloud throughput ceilings come from the cluster type's documented capacity model rather than
tunable broker thread pools. Check ingress/egress, request-rate, partition, and capacity headroom
before assuming a client-config change will reach the target. Use the current cluster-types and
[service quotas](https://docs.confluent.io/cloud/current/quotas/service-quotas.md) documentation,
plus a read-only cluster-description operation from the installed CLI or configured MCP server. If
client tuning is already effective, recommend the documented scaling or tier change that provides
the required capacity and networking characteristics.
Kafka REST topic metadata does not identify the Cloud cluster type or its capacity entitlement; if
control-plane metadata is unavailable, ask for the cluster type rather than inferring Basic,
Standard, Enterprise, or Dedicated behavior.

Cloud brokers are managed infrastructure. Do not instruct the administrator to stop, restart, or
kill a broker to validate failure tolerance, and do not present broker fault injection as a
customer-run test. Validate the documented cluster-type guarantee, effective topic and producer
configuration, available Cloud metrics, and application behavior instead.

## Cloud metrics source of truth

Confluent Cloud does not expose the broker JMX MBeans listed in
[references/monitoring.md](monitoring.md). Use the current
[Confluent Cloud Metrics API guide](https://docs.confluent.io/cloud/current/monitoring/metrics-api.md)
for authentication, descriptor discovery, query behavior, and export integrations. Before naming
or querying a server-side metric, discover the available resource and metric descriptors; do not
infer a Cloud metric name from a Platform MBean.
In particular, names such as `UnderReplicatedPartitions` and
`UncleanLeaderElectionsPerSec` are Platform JMX names, not assumed Cloud Metrics API names. Use
them for Cloud only if live descriptor discovery explicitly returns those exact names. If Metrics
API credentials or descriptors are unavailable, say server-side metric validation is pending and
use documented client or application signals that are actually accessible. In that case, do not
list candidate server-side metric names at all; write only this exact line, once, and continue
with accessible validation:

    Server-side metric validation: pending descriptor access

**Pending is the exception, not the default posture.** Do not assume descriptors are unavailable.
Attempt discovery first against the live Metrics Reference or the descriptors endpoint, and name the
metrics it confirms. Use the pending wording only when the user has stated they lack Metrics API
access, or discovery was genuinely attempted and failed. When a user asks which metrics prove a
change worked, replying with the pending line alone is not an acceptable answer — name the confirmed
Cloud metrics, or state plainly what the user must do to obtain them.

Use the live [Metrics Reference](https://api.telemetry.confluent.cloud/docs/descriptors/) as the
source of truth for each metric's current name, resource type, labels, data type, aggregation
semantics, exportability, product or cluster-type applicability, and deprecation status. Use
[Confluent's Metrics API query examples](https://docs.confluent.io/cloud/current/monitoring/metrics-api-examples.md)
for current request shapes. If a required broker signal has no documented Cloud metric, say it is
not exposed and use an available Cloud metric, client JMX metric, or application-level SLI instead.

**Descriptor access does not imply query access.** A resource-scoped Kafka API key can read the
descriptor endpoints while the query endpoint rejects the same credentials with `401 Invalid
credentials`; querying requires a Cloud API key carrying the appropriate metrics role. Confirm the
query path resolves before promising a server-side metric as validation, and otherwise fall back to
the pending wording above.

Cloud exposes a far smaller server-side surface than Platform JMX. Treat the following as search
starting points for descriptor discovery under resource type `kafka` — confirm each name, its
labels, and its lifecycle status against the live descriptors before naming it in a recommendation:

| Priority | Look for |
|---|---|
| Latency | `producer_latency_avg_milliseconds`, `request_count` |
| Throughput | `received_bytes`, `sent_bytes`, `received_records`, `sent_records`, `cluster_load_percent` |
| Availability | `consumer_lag_offsets`, `max_pending_rebalance_time_milliseconds`, `client_limit_milliseconds` (quota throttling) |
| Durability / DR | `cluster_link_mirror_topic_offset_lag` (mirror lag is the current RPO), `cluster_link_mirror_transition_in_error` |
| Partition sizing / skew | `hot_partition_ingress`, `hot_partition_egress`, `partition_count` |

The Platform baseline MBeans have **no** Cloud equivalent — `ActiveControllerCount`,
`OfflinePartitionsCount`, `UncleanLeaderElectionsPerSec`, and `UnderReplicatedPartitions` are not
exposed, and neither is the broker request-latency breakdown nor the thread-idle metrics. When a
target depends on those signals, say they are unavailable on Cloud and validate with client metrics
and application SLIs instead.

That list is for your own reasoning only. **Do not reproduce those MBean names in the
recommendation's validation block** — listing them there, even to explain that they are
unavailable, still names server-side metrics and breaks the rule above. In the validation block say
only that Confluent Cloud does not expose Platform broker JMX, without enumerating the MBeans.

Kafka Admin REST lists resources and configurations; it is not a metrics API. Do not invent REST
metrics, fields, or endpoints. Use live Metrics API descriptor discovery for server-side signals,
and use Kafka REST only for metadata and configuration operations documented by its API reference.

Producer `acks`, `enable.idempotence`, and timeout values are client configuration, not standard
JMX metrics. Inspect them in the application's effective client/deployment configuration or other
version-supported configuration introspection; do not claim ordinary producer JMX metrics expose
those values. Client JMX remains useful for outcomes such as `record-error-rate`, retries, request
latency, and queue time.

## Cloud operation references

Use these platform-specific references in the recommendation's "How to apply" block:

- **CLI**: use the current [Confluent CLI documentation](https://docs.confluent.io/confluent-cli/current/overview.md)
  for the installed version's topic-config operation.
- **MCP tools**: for discovery and for straightforward topic-config updates when a Confluent MCP
  server is configured — fully qualified tool names (`<server-name>:update-topic-config`, etc.);
  the exact tool name depends on the administrator's MCP setup.
- **REST / Cloud API**: use the current [Confluent Cloud Kafka REST API and Cloud API
  documentation](https://docs.confluent.io/cloud/current/api.md) for the supported topic-config
  and cluster-level operations (tier, capacity, and CKU settings).

## Multi-region availability/durability options

For durability or availability targets beyond what a single multi-zone cluster provides:
- **Cluster Linking** — continuously mirror topics (data + consumer offsets) to a second
  cluster in another region/cloud for disaster recovery.

These are architecture decisions, not config-value changes — flag them as a separate
recommendation (with cost/complexity trade-offs) rather than folding them into a topic-config
plan.

**Cluster Linking DR — the honest RPO/RTO story.** If a user asks for region-level DR, set
expectations correctly rather than implying zero-loss failover:

- Replication is **asynchronous** — the producer is acked by the source before the link mirrors
  the record, so a region loss drops the in-flight (un-mirrored) window. **RPO is non-zero** and
  equals the current mirror lag (visible via the Metrics API / CLI). True zero RPO needs synchronous
  replication, which Cluster Linking is not.
- **RTO depends on your failover tooling**, not on Kafka — it's how fast you can re-point clients.
  Clients must re-bootstrap to the DR cluster (new bootstrap servers + credentials) and be
  restarted; externalize those in service discovery / a secrets manager rather than hardcoding.
- Consumer-offset synchronization can cause a **small number of duplicate reads** on failover —
  design consumers to be idempotent and consult the current failover documentation for defaults
  and supported tuning ranges.
  Failover clamps the resumed offset to the lower of the last-synced offset and the real log end,
  so you never skip data and never wait on phantom messages.
- Check the current failover documentation for client and group-protocol support restrictions.

## Reference docs

- [Confluent Cloud cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.md)
- [Confluent Cloud topic configuration](https://docs.confluent.io/cloud/current/topics/manage.md)
- [Confluent Cloud service quotas](https://docs.confluent.io/cloud/current/quotas/service-quotas.md)
- [Confluent Cloud Metrics API](https://docs.confluent.io/cloud/current/monitoring/metrics-api.md)
- [Metrics API metrics reference](https://api.telemetry.confluent.cloud/docs/descriptors/)
- [Metrics API query examples](https://docs.confluent.io/cloud/current/monitoring/metrics-api-examples.md)
- [Cluster Linking](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.md)
- [Cluster Linking for failover and disaster recovery](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-failover.md)
- [Optimize and tune Confluent Cloud clients](https://docs.confluent.io/cloud/current/client-apps/optimizing/overview.md)
