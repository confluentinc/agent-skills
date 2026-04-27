---
name: kafka-streams-programming
description: Architect, build, and debug Kafka Streams apps (JVM-embedded stream processing). Use when user mentions KStream, KTable, topology, TopologyTestDriver, StreamsBuilder, interactive queries, GlobalKTable, joins/windows/aggregations, or debugging issues (rebalancing, state stores, lag, deserialization errors). Do NOT trigger for Flink, connectors, CDC, or plain producer/consumer.
---

# Kafka Streams — Architect, Build, Debug

JVM-embedded stream processing library with no separate cluster.

## ⚠️ IMPORTANT: Lazy-Load References Only
**Do NOT read all reference files upfront. Read ONLY what you need, when you need it.**

- User asks "how do I join two topics?" → Read `topology-patterns.md` § Joins Decision Tree only
- User asks "build me a Kafka Streams app" → Read `build-templates.md` when writing build files, not before
- User asks "my app is crashing" → Read the specific section in `debugging.md` for that symptom
- Most questions need 0-2 reference files total, not all 10

**Never read multiple files preemptively "just in case"**

## Always Confirm Target Environment First

Before answering in any mode (Architect, Build, Debug), confirm the target environment if the user hasn't stated it: **Apache Kafka | Confluent Platform | Confluent Cloud**. Versions/auth shape every recommendation — KIP-1071 support, SASL config, ACL model, transactional-id expiry, CLI tool names all branch on this. Skip the question only if the user already named the environment.

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

Confirm target environment first (see preamble). Then ask (skip if answered): What data (topics)? What output? Relationship between inputs (combine/enrich/group)?

### Step 2: Recommend the Topology Pattern

Match problem to pattern (read `topology-patterns.md` only for the specific pattern needed). Present: why it fits, data flow in plain English, KS primitives involved, tradeoffs/alternatives.

### Key Decision Trees

When needed, read only the relevant section:
- **Combine topics**: `topology-patterns.md` § Joins Decision Tree
- **Aggregate over time**: `topology-patterns.md` § Windowing Decision Tree
- **Enrichment/lookup**: `topology-patterns.md` § Enrichment Patterns
- **Exactly-once**: `topology-patterns.md` § Exactly-Once (walk through before recommending — at-least-once is simpler if downstream can dedupe)

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
5. **Target environment** (REQUIRED): Apache Kafka | Confluent Platform | Confluent Cloud (read `config-baseline.md` when generating config)
6. **Credentials**: CC needs 2 API keys (Kafka + SR). CP/AK needs bootstrap + SR URLs + auth type (read `cli-commands.md` if needed)
7. **Deployment sizing**: Partitions? Instances? State size? (read `architecture.md` or `production-hardening.md` § Deployment Sizing if needed)
8. **Test data**: Has data or wants sample data generated?

### Step 2: Plan Resources

Present plan: topics to create (source/output/DLQ), schemas to register. Changelog/repartition topics auto-created by KS. **If the user says input topics already exist, omit them from `create-topics.sh` — the script should only create new topics (typically output + DLQ).**

### Step 3: Generate the Project

Generate: project structure, schemas, App.java, TopologyBuilder.java, config, simplelogger.properties, docker-compose (if local), scripts, TopologyTest.java, .env.example, monitoring comments.

**Read references only as needed:**
- Topology code? → `topology-patterns.md` for the specific pattern
- Build file structure? → `build-templates.md`
- Schema syntax? → `schema-patterns.md`
- Config properties? → `config-baseline.md` for env-specific blocks
- Scripts? → `scripts/create-topics.sh`, `scripts/teardown.sh`
- Local dev? → `docker-compose.md`

**Gradle**: Run `gradle wrapper --gradle-version 8.12` after creating build files.

If user wants sample data: generate SampleDataProducer.java and `produce` task.

### Step 4: Production Hardening (if production target)

**Trigger:** User says "production"/"prod"/"deploy" or specifies K8s/ECS/Docker Swarm or requests multiple instances.

Add production components (read `production-hardening.md` for details if needed): Logback JSON logging, `logback.xml`, health check endpoint, `Dockerfile` with JVM tuning, KIP-1034 DLQ handler, K8s YAML (if K8s), shadow/fat jar plugin.

### Step 5: Walk Through the Code

Explain topology, config choices, how to run, what to monitor. Mention `group.protocol=streams` (KIP-1071) provides 50-80% faster rebalancing (requires AK 4.2+/CP 8.2+).

### Step 6: Run the App Before Handing Off

**You must actually start the app against a real broker and observe it reach `RUNNING` before declaring the task done.** Generated code that compiles and passes `TopologyTestDriver` tests can still fail at startup — version-mismatch `NoClassDefFoundError`s, silent logger fallbacks, missing runtime deps, and import-path errors all slip past `compile` + `test` and only surface against a real broker / Schema Registry. A green build is not a working app.

Branch on the target environment chosen in Step 1:

**Local (Apache Kafka or Confluent Platform via the generated `docker-compose.yml`):**
1. `docker compose up -d` and wait for Kafka + SR to be healthy (`docker compose ps`, or curl SR `/subjects`)
2. `./create-topics.sh`
3. Start the app **in the background** (`./gradlew run` or `mvn exec:java`) so you can read its logs while it runs
4. Tail the log and confirm `State transition from REBALANCING to RUNNING` within ~30s. If you don't see it, read the actual stack trace, diagnose via `references/debugging.md` § Startup Failures, fix, restart, re-verify
5. If the user wanted sample data: produce a few records and confirm output appears on the destination topic
6. Stop the app and `docker compose down` (or leave running if the user wants to keep iterating — ask)

**Confluent Cloud:**
You usually cannot run end-to-end yourself because the cluster + SR API keys are the user's. Do the most you can without them, then hand off the rest:
1. If `.env` has real CC creds (the user has set up a real `.env`): run the app locally pointed at CC (`./gradlew run` auto-loads `.env`) and follow steps 3–5 above. Don't skip just because it's CC — if you have creds, run it.
2. If creds are placeholders or not provided: do **not** fabricate a successful run. Instead:
   - Run `./gradlew build` (compile + unit tests) and report the result
   - List the exact commands the user must run to verify (`./create-topics.sh --cloud`, `./gradlew run`, the consume command from `references/verification.md` § Confluent Cloud) and what success looks like (`State transition from REBALANCING to RUNNING`, records on the output topic)
   - Tell the user explicitly: "I couldn't run this against your CC cluster because I don't have your API keys — please run the steps above and paste any errors back."

In the handoff, state plainly which of the above you did. If you ran it and saw `RUNNING`, say so. If you only compiled, say only that. Don't imply a runtime verification you didn't perform.

For CC consume commands, schema-aware producers, and reset procedures, read `references/verification.md`.

---

## Debug Mode

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

Confirm target environment first (see preamble) — most debug paths branch on it. Then ask for: error message, config, KS/Java versions, new app or regression?

### Step 3: Diagnose and Fix

Read the relevant section in `debugging.md` for the identified category. Provide fix with explanation.

---

## Invariant Checklist

Non-negotiable defaults. Apply all. Read reference files only if you need implementation details.

1. **Schematized data**: SR serdes (`SpecificAvroSerde`, `KafkaProtobufSerde`, `KafkaJsonSchemaSerde`). Set `schema.registry.url`, `default.key.serde`, `default.value.serde`. JSON Schema: set `json.value.type`. Protobuf: set `specific.protobuf.value.type` (config-baseline.md)
2. **Versions**: KS 4.x / CP 8.x, Java 17+
3. **KIP-1071**: `group.protocol=streams` (default). Remove if `UnsupportedVersionException`. Unsupported: static membership, regex topics, standby replicas, warm-up replicas (topology-patterns.md § Assignment Strategy)
4. **Four-tier error handling**: `DeserializationExceptionHandler`, `ProcessingExceptionHandler` (KIP-1034), `ProductionExceptionHandler`, `StreamsUncaughtExceptionHandler`. Use MaxFailures pattern for uncaught (production-hardening.md § Error Handling)
5. **Explicit naming**: `ensure.explicit.internal.resource.naming=true`
6. **Graceful shutdown**: Hook with `streams.close(30s)` on SIGTERM/SIGINT
7. **Monitoring**: `metrics.recording.level=INFO` (config-baseline.md)
8. **Log verbosity**: Generate `simplelogger.properties` (build-templates.md)
9. **Defensive topology**: Guard lambdas in prod (null checks, try/catch). Dev: simpler is fine.
10. **Schema parity**: Avro/Protobuf/JSON Schema all supported (build-templates.md, schema-patterns.md)
11. **Test caching**: TopologyTestDriver tests need `statestore.cache.max.bytes=0` to avoid non-deterministic assertions.
12. **Avro logical type Java mappings**: Avro 1.12+ generates `java.time.Instant` for `timestamp-millis`/`timestamp-micros`, `LocalDate` for `date`, `BigDecimal` for `decimal`, etc. **Never use raw `long`/`int` literals** with generated setter methods — use `Instant.EPOCH`, `Instant.now()`, `Instant.ofEpochMilli(...)`. Use `Instant.isAfter()`/`isBefore()` instead of `Math.max()`/`Math.min()` for timestamp comparisons. Applies to topology code, aggregation initializers, producers, AND test helpers (schema-patterns.md § Java type mapping).

---

## Bundled Scripts

`scripts/`: `create-topics.sh` (pre-create topics, `--cloud`), `teardown.sh` (delete topics/state, `--cloud`), `produce-test-data.sh` (generate if requested).

## Reference Files (read on-demand only)

`topology-patterns.md` — design, joins, windows, aggregations | `architecture.md` — internals, sizing | `debugging.md` — troubleshooting | `config-baseline.md` — config | `build-templates.md` — project structure | `schema-patterns.md` — Avro/Protobuf/JSON | `production-hardening.md` — prod setup | `cli-commands.md` — CLI | `docker-compose.md` — local dev | `verification.md` — checklists
