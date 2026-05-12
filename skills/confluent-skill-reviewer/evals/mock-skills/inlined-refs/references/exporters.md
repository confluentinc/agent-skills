# Exporter comparison

## kafka-exporter (Danielqsj)

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

## Confluent Control Center

Pros: official, covers full Confluent Platform metrics.
Cons: paid licensing, heavier footprint.

Best for: enterprise Confluent Platform users.
