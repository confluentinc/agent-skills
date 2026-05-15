---
name: inlined-refs
description: Generate a Kafka consumer group lag dashboard. Use when the user asks to monitor lag, build a dashboard for consumer lag, or wire up Prometheus exporters for Kafka. Do NOT trigger for producer metrics, broker JMX, or Streams-specific monitoring.
---

# inlined-refs — consumer lag dashboarding

## Step 1 — pick the exporter

We support two exporters. Read the comparison below before recommending.

## Exporter comparison (from references/exporters.md)

### kafka-exporter (Danielqsj)

Pros: lightweight, single binary, exposes per-group lag directly.
Cons: stateful (consumes `__consumer_offsets` internally), can lag on large clusters.

Best for: clusters < 100 brokers, < 1000 consumer groups.

Config snippet:
```
kafka-exporter \
  --kafka.server=localhost:9092 \
  --kafka.version=3.5.0 \
  --web.listen-address=:9308
```

### Confluent Control Center

Pros: official, covers full Confluent Platform metrics.
Cons: paid licensing, heavier footprint.

Best for: enterprise Confluent Platform users.

## Step 2 — Grafana dashboard

Use Grafana dashboard 7589 for kafka-exporter. Filter by consumer group, plot `kafka_consumergroup_lag` over a 5-minute window.

## Step 3 — alerts

Set lag > 10,000 messages for 5 minutes as the default alert. Adjust per topic SLA.
