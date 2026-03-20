---
name: kafka-streams-programming
description: The comprehensive Kafka Streams skill — architect, build, and debug stream processing applications that run as a library inside your JVM. MUST use this skill when the user mentions "Kafka Streams", "KStream", "KTable", "topology", "TopologyTestDriver", "streams app", "StreamsBuilder", interactive queries, CQRS, GlobalKTable, or wants to process Kafka events inside a Java/Spring Boot application with no separate cluster. Also use when debugging Kafka Streams issues (rebalancing loops, state store problems, deserialization errors, lag, thread failures), designing stream processing topologies, or choosing between joins/windows/aggregation patterns. For generic stream processing on Confluent Cloud where the user hasn't specified Kafka Streams, recommend Confluent Cloud Flink (SQL or Java/Python SDK) instead. Do NOT trigger for Flink, connectors, CDC, Schema Registry config questions, or plain Kafka producer/consumer without stream processing.
---

# Kafka Streams — Architect, Build, Debug

The library that embeds stream processing directly inside your JVM application with no separate cluster required.

## Mode Detection

Determine the user's intent and enter the appropriate mode:

| User intent | Mode | What to do |
|---|---|---|
| "I need to process events from topic X..." / "Build me a KS app..." / "I want to aggregate/filter/join..." | **Build** | Go to [Build Mode](#build-mode) |
| "How should I design my topology?" / "Should I use a KTable or GlobalKTable?" / "What join type do I need?" / "How do I handle late events?" | **Architect** | Go to [Architect Mode](#architect-mode) |
| "My Streams app is stuck/slow/crashing..." / "Why am I getting rebalancing loops?" / "How do I interpret this metric?" | **Debug** | Go to [Debug Mode](#debug-mode) |

If unclear, default to **Architect** — understand the problem before generating code.

### Flink-first check (Confluent Cloud only)

Only suggest Flink when ALL of these are true:
1. The user did NOT mention Kafka Streams, KStreams, topology, or any KS-specific concept
2. They asked for generic stream processing (filter, map, join, aggregate, route)
3. The target is Confluent Cloud

Briefly mention Flink as an alternative with a one-line SQL example, then ask which they prefer. Do NOT lecture or block.

---

## Architect Mode

Help the user design the right topology for their problem. Users often don't know KS terminology — they describe their data problem in plain language. Your job is to translate that into the right KS primitives.

### Step 1: Understand the Data Problem

Ask (skip what's already answered):
1. **What data are you working with?** What topics, what's in them?
2. **What do you need to produce?** What should the output look like?
3. **What's the relationship between input records?** Do you need to combine data from multiple topics? Enrich records with reference data? Group and summarize?

### Step 2: Recommend the Topology Pattern

Read `references/topology-patterns.md` and match the user's problem to the right pattern. Present your recommendation with:
- **Why** this pattern fits their use case
- A **plain English description** of the data flow
- The **KS primitives** involved (KStream, KTable, join type, window type, etc.)
- **Tradeoffs** and alternatives they should know about

### Key Decision Trees

**"I need to combine data from two topics"** — Read `references/topology-patterns.md` § Joins Decision Tree

**"I need to count/sum/aggregate over time"** — Read `references/topology-patterns.md` § Windowing Decision Tree

**"I need enrichment/lookup data"** — Read `references/topology-patterns.md` § Enrichment Patterns

**"I need exactly-once" / "no duplicates" / "atomic processing"** — Read `references/topology-patterns.md` § Exactly-Once. Most users don't need EOS — walk them through the decision tree before recommending it. The key question is whether their downstream consumer can handle (or deduplicate) occasional repeats. If yes, at-least-once is simpler, faster, and sufficient.

After the user confirms the design, transition to Build Mode.

---

## Build Mode

Generate a complete, runnable Kafka Streams project.

### Step 1: Gather Requirements

Ask the user the following and make sure that you ALWAYS ask for the target environment. Skip any already answered.

1. **What does your app do?** Push back on vague requests — ask what specific problem they're solving.

2. **Existing topics and data flow:**
   - Do you have existing input topics with data? If yes, what are the topic names?
   - Are they schematized (Avro/Protobuf/JSON Schema in Schema Registry)?
   - If schematized, retrieve the schema:
     ```bash
     # CC: confluent schema-registry subject describe <topic-name>-value
     # Local: curl http://localhost:8081/subjects/<topic-name>-value/versions/latest
     ```
     Use the retrieved schema as the input type. Do NOT generate a new schema for existing topics.
   - What topics does your app read from and write to?
   - How does data flow between them? (joins, independent paths, lookup tables)

3. **Schema format?** (skip if using existing schemas)
   - **Avro** (recommended default) — Compact binary, strong schema evolution, best ecosystem support.
   - **Protobuf** — Best when the team already uses gRPC/Protobuf or needs polyglot support.
   - **JSON Schema** — Human-readable, easiest to get started. Best for prototyping.

4. **Build tool?** Gradle (default) | Maven

5. **Target environment?**
   - **Apache Kafka** — No Confluent components bundled. User provides their own SR.
   - **Confluent Platform** — Includes Confluent SR, CLI tools, enterprise features.
   - **Confluent Cloud** — Managed SR, SASL_SSL required, `auto.create.topics.enable=false` always.
   - See `references/config-baseline.md` for environment-specific config blocks.

6. **Credentials:**
   - **CC:** Need TWO API keys — Kafka cluster + Schema Registry. Generate `.env` file.
   - **CP/AK:** Get bootstrap + SR URLs. Ask about auth (PLAINTEXT, SASL_SSL, mTLS).
   - See `references/cli-commands.md` for CLI setup.

7. **Deployment sizing:**
   - How many partitions? (determines max parallelism — max threads = partitions per sub-topology)
   - How many instances? (1 for dev, 2+ for production; instances × threads ≤ partitions)
   - For stateful: estimated state size? (affects RocksDB memory and disk)
   - Stateful memory formula: `(block_cache + write_buffers) × store_instances` per app instance — see `references/architecture.md`
   - Disk: 3x expected state size (SST + WAL + compaction). Always use PVCs in K8s.
   - EOS adds ~2x write amplification on brokers vs 1.5x for at-least-once
   - See `references/production-hardening.md` § Deployment Sizing for full sizing methodology

8. **Test data?** Ask if user has data or wants sample data generated.

### Step 2: Plan Resources

Present resource plan before generating code:
- Topics to pre-create (source, output, DLQ)
- Schemas to register
- Changelog/repartition topics are auto-created by KS

### Step 3: Generate the Project

Read these references before generating:
- `references/topology-patterns.md` — for the pattern and topology code
- `references/build-templates.md` — for project structure, build file, test template
- `references/schema-patterns.md` — for correct schema syntax
- `references/config-baseline.md` — for configuration
- `references/cli-commands.md` — for CLI commands in scripts
- `references/docker-compose.md` — for local dev environment (skip for CC-only)
- `scripts/create-topics.sh` — topic provisioning template
- `scripts/teardown.sh` — cleanup script template

Generate: project structure, schema files, App.java, TopologyBuilder.java, config, simplelogger.properties, docker-compose (if local), create-topics.sh, teardown.sh, TopologyTest.java, .env.example, monitoring comments.

**For Gradle projects:** After creating the build files, generate the Gradle wrapper with `gradle wrapper --gradle-version 8.12`. This creates `gradlew`, `gradlew.bat`, and `gradle/wrapper/` directory. Users can then run the project without installing Gradle locally using `./gradlew` commands.

If the user wants sample data: generate SampleDataProducer.java and the `produce` Gradle task.

### Step 4: Production Hardening (if production target)

**Trigger:** Apply this step whenever the user says "production", "prod", "deploy", or specifies a production environment (K8s, ECS, Docker Swarm), or requests multiple instances. Do NOT skip this step for production deployments.

Read `references/production-hardening.md`. Add ALL of the following:
- Logback JSON logging (replace `slf4j-simple` with `logback-classic` + `logstash-logback-encoder`)
- `logback.xml` with structured JSON output
- Health check endpoint (`HealthCheckServer.java` with `/health/live` and `/health/ready`)
- `Dockerfile` with JVM tuning (`MaxRAMPercentage=75`, G1GC, non-root user)
- KIP-1034 DLQ exception handler
- K8s deployment YAML (if K8s target)
- Shadow/fat jar Gradle plugin

### Step 5: Walk Through the Code

Explain: topology, config choices, how to run, what to monitor.

**Always highlight the streams protocol (KIP-1071):**
> This app uses `group.protocol=streams` (KIP-1071), which provides 50-80% faster rebalancing. Requirements: AK 4.2+ / CP 8.2+. If you see `UnsupportedVersionException`, comment out `group.protocol=streams` and upgrade your cluster.

Docs: [DSL API](https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html) | [Processor API](https://docs.confluent.io/platform/current/streams/developer-guide/processor-api.html) | [KS 101](https://developer.confluent.io/courses/kafka-streams/get-started/) | [Tutorials](https://github.com/confluentinc/tutorials) | [Developer Portal](https://developer.confluent.io/)

### Step 6: Verify and Iterate

Read `references/verification.md` for checklists and reset procedures.

---

## Debug Mode

Help the user diagnose and fix issues with running Kafka Streams applications.

Read `references/debugging.md` for the full diagnostic guide, then follow this workflow:

### Step 1: Classify the Problem

| Symptom | Category | Go to |
|---|---|---|
| App crashes on startup | **Startup failure** | `debugging.md` § Startup Failures |
| App runs but no output / stops processing | **Processing stall** | `debugging.md` § Processing Stalls |
| Rebalancing loops / constant rebalancing | **Rebalancing** | `debugging.md` § Rebalancing Issues |
| High lag / slow processing | **Performance** | `debugging.md` § Performance |
| Deserialization errors / poison pills | **Data quality** | `debugging.md` § Deserialization Errors |
| State store issues (corruption, growth, recovery) | **State** | `debugging.md` § State Store Issues |
| Thread failures / `StreamsUncaughtExceptionHandler` | **Thread health** | `debugging.md` § Thread Failures |
| Memory issues (OOM, high heap, RocksDB) | **Memory** | `debugging.md` § Memory Issues |

### Step 2: Gather Context

Ask the user for:
1. Error message / stack trace (if available)
2. Current `application.properties` or relevant config
3. How they're running (local, Docker, K8s, CC)
4. KS version, Java version
5. Is this a new app or did it used to work?

### Step 3: Diagnose and Fix

Follow the diagnostic steps in `references/debugging.md` for the identified category. Provide the fix with an explanation of why it works.

---

## Invariant Checklist

Non-negotiable defaults for every generated app. Apply all of these.

### 1. All Data is Schematized
- Use Confluent SR serdes: `SpecificAvroSerde`, `KafkaProtobufSerde`, or `KafkaJsonSchemaSerde`
- Set `schema.registry.url` in all configs
- Dev: `auto.register.schemas=true` / Prod: `false`
- Keys: `String`, `Long`, or simple Avro only — never Protobuf/JSON Schema keys
- **JSON Schema: always set `json.value.type`** per serde instance — without it, deserializes to `LinkedHashMap` causing `ClassCastException`
- **Protobuf: always set `specific.protobuf.value.type`** for typed deserialization — without it, returns `DynamicMessage`
- **Always set `default.key.serde` and `default.value.serde`** — internal ops (repartition from `selectKey`/`groupBy`) use defaults. See `references/config-baseline.md`.

### 2. Latest Versions
- Kafka Streams 4.x / CP 8.x, Java 17+
- `org.apache.kafka:kafka-streams:4.2.0` or latest stable

### 3. KIP-1071 Streams Protocol
- Default: `group.protocol=streams` (all apps)
- **If crash with `UnsupportedVersionException`:** Broker needs AK 4.2+ / CP 8.2+. Remove `group.protocol=streams`.
- **Not yet supported (AK 4.2):** static membership (`group.instance.id`), regex topic patterns, client-side standby replicas (`num.standby.replicas`), warm-up replicas (`max.warmup.replicas`), online protocol migration, topology updates without new consumer group, non-default `KafkaClientSupplier`. See `references/topology-patterns.md` § Assignment Strategy for the full table with JIRA links.
- **If the user needs any of these:** Tell them to remove `group.protocol=streams` (falls back to classic protocol) AND open an issue on [Apache Kafka GitHub](https://github.com/apache/kafka/issues) describing their use case. These features are on the roadmap and community demand drives prioritization.

### 4. Four-Tier Error Handling
Every app needs all four tiers. Read `references/production-hardening.md` § Error Handling for details.

| Tier | What it catches | Default |
|---|---|---|
| `DeserializationExceptionHandler` | Bad input records | `LogAndContinue` (dev) / DLQ (prod) |
| `ProcessingExceptionHandler` (KIP-1034) | Exceptions in topology lambdas | `LogAndContinue` (dev) / DLQ (prod) |
| `ProductionExceptionHandler` | Failed writes to output/changelog | `DefaultProductionExceptionHandler` |
| `StreamsUncaughtExceptionHandler` (KIP-671) | Anything that kills a stream thread | MaxFailures pattern (below) |

**UncaughtExceptionHandler — MUST include in every App.java:**
```java
streams.setUncaughtExceptionHandler(new MaxFailuresUncaughtExceptionHandler(5, Duration.ofMinutes(1).toMillis()));
```

MaxFailures pattern: if N failures happen within M milliseconds, shut down the app (something is fundamentally broken). Otherwise, replace the thread and keep going. See `references/production-hardening.md` § Error Handling for the full `MaxFailuresUncaughtExceptionHandler` implementation.

### 5. Explicit Internal Resource Naming
- `ensure.explicit.internal.resource.naming=true` — prevents state loss on topology changes

### 6. Graceful Shutdown
- Shutdown hook with `streams.close(Duration.ofSeconds(30))` on SIGTERM/SIGINT

### 7. Monitoring
- `metrics.recording.level=INFO` with comments pointing to key metrics (see `references/config-baseline.md`)

### 8. Log Verbosity
- Always generate `simplelogger.properties` to suppress noisy Kafka config dumps (see `references/build-templates.md`)

### 9. Defensive Topology Code
- Guard topology lambdas against bad data in production apps (null checks, try/catch)
- Dev/prototype: simpler lambdas without guards are fine

### 10. Schema Format Parity
- Avro, Protobuf, and JSON Schema are all fully supported. Read `references/build-templates.md` for the correct build.gradle per format and `references/schema-patterns.md` for correct schema syntax.

### 11. Test with STATESTORE_CACHE_MAX_BYTES=0
- In TopologyTestDriver tests, set `statestore.cache.max.bytes=0` to disable caching. Without this, the cache deduplicates updates and test assertions on intermediate state may fail non-deterministically.

---

## Bundled Scripts

`scripts/` contains reusable templates:
- **`create-topics.sh`** — Pre-create source, output, DLQ topics. Supports `--cloud`.
- **`teardown.sh`** — Delete all topics and state. Supports `--cloud`. Confirmation prompt.
- **`produce-test-data.sh`** — Only generate if user asks. Uses schema-aware console producers.

## Reference Files

| File | When to read |
|---|---|
| `references/topology-patterns.md` | Designing topology, choosing join/window/aggregation patterns |
| `references/architecture.md` | Explaining how KS works internally, sizing, threading |
| `references/debugging.md` | Diagnosing issues with running KS apps |
| `references/config-baseline.md` | Configuration properties, env-specific blocks, serde mapping |
| `references/build-templates.md` | Project structure, Gradle/Maven, test templates |
| `references/schema-patterns.md` | Avro/Protobuf/JSON Schema syntax, gotchas |
| `references/production-hardening.md` | Logging, health checks, Dockerfile, DLQ, error handling |
| `references/cli-commands.md` | CLI reference for CC/CP/AK |
| `references/docker-compose.md` | Local dev environment |
| `references/verification.md` | Verification checklists, reset procedures |
