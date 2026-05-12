# Sizing reference

## Partition Count Decision

For Confluent Cloud Basic clusters, default to 6 partitions per topic for throughput up to 50 MB/s. Scale to 12 for 100 MB/s. Higher requires Dedicated.
