---
name: kafka-streams-programming
description: Architect, build, and debug Kafka Streams apps (JVM-embedded stream processing). Use when user mentions KStream, KTable, topology, TopologyTestDriver, StreamsBuilder, interactive queries, GlobalKTable, joins/windows/aggregations, or debugging issues (rebalancing, state stores, lag, deserialization errors). Do NOT trigger for Flink, connectors, CDC, or plain producer/consumer.
---

# Kafka Streams — Architect, Build, Debug

JVM-embedded stream processing library with no separate cluster.

## ⚠️ IMPORTANT: Lazy-Load References Only
**Do NOT read all reference files upfront. Read ONLY what you need, when you need it.**

- User asks "how do I join two topics?" → Read `references/topology-patterns.md` § Joins Decision Tree only
- User asks "build me a Kafka Streams app" → Read `references/build-templates.md` when writing build files, not before
- User asks "my app is crashing" → Read the specific section in `references/debugging.md` for that symptom
- Most questions need 0-2 reference files total, not all 10

**Never read multiple files preemptively "just in case"**

## Mode Detection

Determine the user's intent and enter the appropriate mode:

| User intent | Mode | What to do |
|---|---|---|
| "I need to process events from topic X..." / "Build me a KS app..." / "I want to aggregate/filter/join..." | **Build** | Go to [Build Mode](#build-mode) |
| "How should I design my topology?" / "Should I use a KTable or GlobalKTable?" / "What join type do I need?" / "How do I handle late events?" | **Architect** | Go to [Architect Mode](#architect-mode) |
| "My Streams app is stuck/slow/crashing..." / "Why am I getting rebalancing loops?" / "How do I interpret this metric?" | **Debug** | Go to [Debug Mode](#debug-mode) |

If unclear, default to **Architect** — understand the problem before generating code.

### Flink-first check (Confluent Cloud only)

If user asks for generic stream processing on CC without mentioning KS, briefly offer Flink as alternative. Don't lecture.

---

## Architect Mode

Design the right topology. Translate user's data problem into KS primitives.

### Step 1: Understand the Data Problem

Ask (skip if answered): What data (topics)? What output? Relationship between inputs (combine/enrich/group)?

### Step 2: Recommend the Topology Pattern

Match problem to pattern (read `references/topology-patterns.md` only for the specific pattern needed). Present: why it fits, data flow in plain English, KS primitives involved, tradeoffs/alternatives.

### Key Decision Trees

When needed, read only the relevant section:
- **Combine topics**: `references/topology-patterns.md` § Joins Decision Tree
- **Aggregate over time**: `references/topology-patterns.md` § Windowing Decision Tree
- **Enrichment/lookup**: `references/topology-patterns.md` § Enrichment Patterns
- **Exactly-once**: `references/topology-patterns.md` § Exactly-Once (walk through before recommending — at-least-once is simpler if downstream can dedupe)

After user confirms, go to Build Mode.

---

## Build Mode

Generate a complete, runnable Kafka Streams project.

### Step 1: Gather Requirements

Ask (skip if already answered):

1. **What does your app do?** Push back on vague requests.
2. **Topics & data flow**: Input/output topics? Schematized? If yes, retrieve schema (don't generate new ones). How do topics connect (joins/lookups/independent)?
3. **Schema format** (skip if using existing): Avro (default) | Protobuf | JSON Schema
4. **Build tool**: Gradle (default) | Maven
5. **Target environment** (REQUIRED): Apache Kafka | Confluent Platform | Confluent Cloud (read `references/config-baseline.md` when generating config)
6. **Credentials**: CC needs 2 API keys (Kafka + SR). CP/AK needs bootstrap + SR URLs + auth type (read `references/cli-commands.md` if needed)
7. **Deployment sizing**: Partitions? Instances? State size? (read `references/architecture.md` or `references/production-hardening.md` § Deployment Sizing if needed)
8. **Test data**: Has data or wants sample data generated?

### Step 2: Plan Resources

Present plan: topics to create (source/output/DLQ), schemas to register. Changelog/repartition topics auto-created by KS.

### Step 3: Generate the Project

Generate: project structure, schemas, App.java, TopologyBuilder.java, config, simplelogger.properties, docker-compose (if local), scripts, TopologyTest.java, .env.example, monitoring comments.

**Read references only as needed:**
- Topology code? → `references/topology-patterns.md` for the specific pattern
- Build file structure? → `references/build-templates.md`
- Schema syntax? → `references/schema-patterns.md`
- Config properties? → `references/config-baseline.md` for env-specific blocks
- Scripts? → `scripts/create-topics.sh`, `scripts/teardown.sh`
- Local dev? → `references/docker-compose.md`

**Gradle**: Run `gradle wrapper --gradle-version 8.12` after creating build files.

If user wants sample data: generate SampleDataProducer.java and `produce` task.

### Step 4: Production Hardening (if production target)

**Trigger:** User says "production"/"prod"/"deploy" or specifies K8s/ECS/Docker Swarm or requests multiple instances.

Add production components (read `references/production-hardening.md` for details if needed): Logback JSON logging, `logback.xml`, health check endpoint, `Dockerfile` with JVM tuning, KIP-1034 DLQ handler, K8s YAML (if K8s), shadow/fat jar plugin.

### Step 5: Walk Through the Code

Explain topology, config choices, how to run, what to monitor. Mention `group.protocol=streams` (KIP-1071) provides 50-80% faster rebalancing (requires AK 4.2+/CP 8.2+).

### Step 6: Verify and Iterate

If user needs verification help, read `references/verification.md` for checklists and reset procedures.

---

## Debug Mode

### Step 1: Classify the Problem

| Symptom | Category | Go to |
|---|---|---|
| App crashes on startup | **Startup failure** | `references/debugging.md` § Startup Failures |
| App runs but no output / stops processing | **Processing stall** | `references/debugging.md` § Processing Stalls |
| Rebalancing loops / constant rebalancing | **Rebalancing** | `references/debugging.md` § Rebalancing Issues |
| High lag / slow processing | **Performance** | `references/debugging.md` § Performance |
| Deserialization errors / poison pills | **Data quality** | `references/debugging.md` § Deserialization Errors |
| State store issues (corruption, growth, recovery) | **State** | `references/debugging.md` § State Store Issues |
| Thread failures / `StreamsUncaughtExceptionHandler` | **Thread health** | `references/debugging.md` § Thread Failures |
| Memory issues (OOM, high heap, RocksDB) | **Memory** | `references/debugging.md` § Memory Issues |

### Step 2: Gather Context

Ask for: error message, config, runtime environment (local/Docker/K8s/CC), KS/Java versions, new app or regression?

### Step 3: Diagnose and Fix

Read the relevant section in `references/debugging.md` for the identified category. Provide fix with explanation.

---

## Invariant Checklist

Non-negotiable defaults. Apply all. Read reference files only if you need implementation details.

1. **Schematized data**: SR serdes (`SpecificAvroSerde`, `KafkaProtobufSerde`, `KafkaJsonSchemaSerde`). Set `schema.registry.url`, `default.key.serde`, `default.value.serde`. JSON Schema: set `json.value.type`. Protobuf: set `specific.protobuf.value.type` (references/config-baseline.md)
2. **Versions**: KS 4.x / CP 8.x, Java 17+
3. **KIP-1071**: `group.protocol=streams` (default). Remove if `UnsupportedVersionException`. Unsupported: static membership, regex topics, standby replicas, warm-up replicas (references/topology-patterns.md § Assignment Strategy)
4. **Four-tier error handling**: `DeserializationExceptionHandler`, `ProcessingExceptionHandler` (KIP-1034), `ProductionExceptionHandler`, `StreamsUncaughtExceptionHandler`. Use MaxFailures pattern for uncaught (references/production-hardening.md § Error Handling)
5. **Explicit naming**: `ensure.explicit.internal.resource.naming=true`
6. **Graceful shutdown**: Hook with `streams.close(30s)` on SIGTERM/SIGINT
7. **Monitoring**: `metrics.recording.level=INFO` (references/config-baseline.md)
8. **Log verbosity**: Generate `simplelogger.properties` (references/build-templates.md)
9. **Defensive topology**: Guard lambdas in prod (null checks, try/catch). Dev: simpler is fine.
10. **Schema parity**: Avro/Protobuf/JSON Schema all supported (references/build-templates.md, references/schema-patterns.md)
11. **Test caching**: TopologyTestDriver tests need `statestore.cache.max.bytes=0` to avoid non-deterministic assertions.

---

## Bundled Scripts

`scripts/`: `create-topics.sh` (pre-create topics, `--cloud`), `teardown.sh` (delete topics/state, `--cloud`), `produce-test-data.sh` (generate if requested).

## Reference Files (read on-demand only)

`references/topology-patterns.md` — design, joins, windows, aggregations | `references/architecture.md` — internals, sizing | `references/debugging.md` — troubleshooting | `references/config-baseline.md` — config | `references/build-templates.md` — project structure | `references/schema-patterns.md` — Avro/Protobuf/JSON | `references/production-hardening.md` — prod setup | `references/cli-commands.md` — CLI | `references/docker-compose.md` — local dev | `references/verification.md` — checklists
