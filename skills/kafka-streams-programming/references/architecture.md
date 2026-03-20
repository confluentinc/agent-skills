# Kafka Streams Architecture

How Kafka Streams works internally. Read this when explaining KS to users, sizing applications, or diagnosing issues.

**Docs:** [Architecture guide](https://docs.confluent.io/platform/current/streams/architecture.html)

## Core Concepts

Kafka Streams is a **client library** — it runs inside your JVM process, not on a separate cluster. Each instance connects to Kafka as a consumer, processes records, and produces results back to Kafka.

### Threading Model

```
KafkaStreams instance
├── StreamThread-1 (a Java thread)
│   ├── Consumer (polls from assigned partitions)
│   ├── Task 0_0 (partition 0 of subtopology 0)
│   │   ├── Processor topology
│   │   └── State stores (RocksDB)
│   ├── Task 0_1 (partition 1 of subtopology 0)
│   └── Producer (writes to output/changelog topics)
├── StreamThread-2
│   └── ...
├── GlobalStreamThread (if GlobalKTable is used)
│   └── Reads ALL partitions of global topics
└── StateRestoreThread
    └── Replays changelogs to rebuild state stores
```

- **StreamThread:** Each thread runs an event loop: poll → process → commit. One thread handles multiple tasks.
- **Task:** The unit of parallelism. One task per input partition per subtopology. A task owns its state stores.
- **Subtopology:** An independent processing graph with no shared state. KS creates separate subtopologies for disconnected parts of the topology.
- **GlobalStreamThread:** Separate thread for GlobalKTable — reads every partition, builds a local copy of the entire table.

### How Partitions Map to Tasks

```
Input topic: orders (6 partitions)
Topology: orders → groupByKey → aggregate → output

Tasks created: 6 (one per partition)
If 2 instances with 1 thread each: 3 tasks per instance
If 1 instance with 3 threads: 2 tasks per thread
```

**Max parallelism = number of input partitions.** More instances/threads than partitions means idle capacity.

### State Stores

Each stateful task has its own state store instance, backed by RocksDB (default). State is:
- **Partitioned:** Task 0_0 only has state for partition 0's keys
- **Persistent:** Survives restarts via local RocksDB files
- **Recoverable:** Backed by changelog topics in Kafka — can be rebuilt from scratch
- **Off-heap:** RocksDB memory (block cache, write buffers) is NOT controlled by JVM heap settings

### Lifecycle States

```
CREATED → REBALANCING → RUNNING → PENDING_SHUTDOWN → NOT_RUNNING
                ↑           │
                └───────────┘  (rebalance triggered)

Error paths:
RUNNING → PENDING_ERROR → ERROR
```

- **CREATED:** Instance created but not started
- **REBALANCING:** Consumer group is rebalancing task assignments
- **RUNNING:** Processing records normally
- **PENDING_SHUTDOWN:** `close()` called, draining
- **NOT_RUNNING:** Clean shutdown complete
- **ERROR:** Unrecoverable error (check UncaughtExceptionHandler)

### Commit and Flush

The commit cycle (controlled by `commit.interval.ms`):
1. Flush state store changes to RocksDB
2. Flush producer buffers (send pending output records)
3. Commit consumer offsets

For at-least-once: commit is asynchronous, default 30s interval.
For exactly-once: commit is transactional (atomic offset + output + changelog), default 100ms.

**The cache (`statestore.cache.max.bytes`):** Sits in front of RocksDB. Deduplicates updates to the same key within a commit interval. A 50MB cache with 30s commits means downstream consumers see fewer intermediate updates — only the latest value for each key per commit cycle.

### Changelog Topics

Every state store has a changelog topic that records all mutations:
- **Non-windowed:** `cleanup.policy=compact` (latest value per key retained forever)
- **Windowed:** `cleanup.policy=compact,delete` (expired window segments are deleted)
- **Naming:** `<application.id>-<store-name>-changelog`

Changelog topics enable recovery: if a task moves to a new instance, the state store is rebuilt by replaying the changelog. Standby replicas continuously replay the changelog to stay warm.

### Repartition Topics

When `selectKey()`, `groupBy()`, or `map()` changes the key, KS automatically creates a repartition topic:
- Records are written to the repartition topic with the new key
- A new subtopology reads from the repartition topic
- **Retention: infinite** — DO NOT set retention on repartition topics (causes data loss)
- **Naming:** `<application.id>-<operator-name>-repartition`

### How GlobalKTable Differs from KTable

| Aspect | KTable | GlobalKTable |
|--------|--------|-------------|
| Data distribution | Partitioned across instances | Replicated to ALL instances |
| Co-partitioning required for joins | Yes | No |
| Join key flexibility | Must match KTable key | Any field (via KeyValueMapper) |
| Memory cost | Proportional to assigned partitions | Full table on every instance |
| Changelog | Has its own changelog topic | No changelog — reads source topic directly |
| Recovery | Replays changelog | Replays source topic from beginning |
| Update propagation | Via consumer group rebalancing | Each instance reads independently |

**Use GlobalKTable when:** Lookup table is small (< few GB), join key isn't the record key, or co-partitioning is impractical.

## Sizing Guidelines

### Parallelism Model

The basic unit of parallelism is a **stream task**, which consumes from one partition. Number of tasks = max partitions across input topics per sub-topology.

```
total_threads = instances × num.stream.threads
total_threads ≤ input_partitions  (more = idle capacity, act as hot standbys only)
```

For multi-sub-topology applications, each sub-topology generates its own tasks. True maximum parallelism may exceed the partition count of the initial input topic. Use `topology.describe()` to understand the actual topology.

**Sizing rule:**
- Stateless: more threads per instance (match CPU cores), fewer instances
- Stateful: fewer threads per instance, size for RocksDB memory. Prefer fewer instances with more resources over many small instances — state restoration after scaling is expensive.

### Memory (stateful apps) — canonical reference

Other files cross-reference this section for the RocksDB memory formula.

```
JVM heap: application objects + serde buffers + cache
  - statestore.cache.max.bytes is on-heap (divided among threads)
  - Typical: 512MB - 2GB heap

RocksDB off-heap: per state store instance
  - block_cache(50MB) + write_buffers(16MB × 3) = 98MB per instance
  - Non-windowed: store_instances = partitions_per_instance × stores
  - Windowed: store_instances = partitions_per_instance × stores × 3 (segments)
  - Typical: 1-10 GB off-heap

Container total = heap + off-heap + OS overhead (~256MB)
Set MaxRAMPercentage=75 to leave room for RocksDB
```

**Worked example (windowed aggregation):**
- 40 partitions, 1 windowed store, 4 instances → 10 partitions/instance
- Windowed segments: 10 × 1 × 3 = 30 store instances per app
- RocksDB memory: 30 × 98MB = ~2.9 GB off-heap
- Container: 2GB heap + 2.9GB RocksDB + 256MB OS = ~5.2 GB minimum

**BoundedMemoryRocksDBConfig:** For apps with many stores, share a single block cache across all RocksDB instances to limit total off-heap. See `config-baseline.md` § Performance Tuning.

### Disk (stateful apps)

```
RocksDB disk = state_size × 3 (SST files + WAL + compaction overhead)
Standby replicas double the disk requirement
Use fast local storage (SSD), never NFS/EBS
Monitor disk usage with alerts at 70%
```

The 3x factor accounts for: active SST files, write-ahead log, and temporary files created during compaction. Windowed stores create multiple segments per partition, further increasing disk needs.

**Always use persistent volumes (PVCs) in Kubernetes for stateful apps.** Ephemeral storage forces full state restoration on every restart, which can take hours for large state stores.

### Network and Broker Sizing

```
Stateless broker overhead:  ~1x input volume (read + write output)
Stateful broker overhead:   ~1.5-2x input volume (+ changelog writes)
EOS broker overhead:        ~2x input volume (+ transaction markers)
```

Changelog topics are replicated by `replication.factor` (default 3). Each state store creates one changelog topic. Repartition topics add additional broker load approximately matching the original topic being remapped.

### Scale Out vs. Scale Up

**Scale out (add instances):** Best for network-bound or memory-bound apps. New instances get tasks after rebalance.

**Scale up (add threads/resources):** Best for CPU-bound apps. Must explicitly increase `num.stream.threads` and adjust JVM heap / RocksDB memory caps — simply adding hardware is not enough.
