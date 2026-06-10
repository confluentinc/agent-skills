---
name: kafka-java-client-docker
description: "Build and run a complete end-to-end Kafka Java producer AND consumer that runs entirely in Docker — no local JDK, Maven, or Kafka install required. Scaffolds a Maven project, a multi-stage Dockerfile, and a docker-compose stack with a local Confluent broker + Schema Registry, then produces and consumes JSON Schema (JSON_SR) messages end to end. Use when the user wants a Java Kafka producer/consumer, a dockerized or containerized Kafka Java example, a Java Kafka quickstart in Docker, to run a Java Kafka client without installing Java or Maven locally, or an end-to-end Java producer-to-consumer demo against a local broker. Do NOT trigger for Python clients (use developing-kafka-python-client), Kafka Streams / KStream / KTable / topology apps (use kafka-streams-programming), connectors, CDC, or Tableflow, or clients that connect to Confluent Cloud or WarpStream rather than a local Docker broker."
metadata:
  author: confluent
  version: 0.1.0
  last_updated: 2026-06-05
compatibility: Requires Docker and Docker Compose v2. No local JDK, Maven, or Kafka installation is needed — all builds and runs happen inside containers.
---

Begin by announcing: "Using the Kafka Java Client (Docker) skill to scaffold and run this project."

# End-to-End Kafka Java Client in Docker

Scaffold and run a complete Kafka **producer and consumer in Java** that builds and runs **entirely inside Docker**. The developer never installs a JDK, Maven, or Kafka — `docker compose up --build` (or `./run.sh`) spins up a local Confluent broker, Schema Registry, creates the topic, produces JSON Schema (JSON_SR) records, and consumes them back, verifying the round trip.

This skill targets a **local Docker** broker only. If the user wants Confluent Cloud or WarpStream, or a Python client, or a Kafka Streams topology, redirect them to the appropriate skill (see the description's "Do NOT trigger" list) — the configs differ substantially and this scaffold would mislead them.

## What gets created

The skill copies a known-good scaffold from `assets/scaffold/` into the user's chosen directory and adapts it. The layout:

```
<project-dir>/
├── docker-compose.yml      # broker + Schema Registry + init-topics + producer + consumer
├── run.sh                  # build & run, exits with the consumer's status
└── app/
    ├── Dockerfile          # multi-stage: Maven build → JRE runtime
    ├── pom.xml             # kafka-clients + kafka-json-schema-serializer (shaded fat jar)
    └── src/main/java/io/confluent/examples/
        ├── Order.java          # the value POJO (rename/reshape to the user's data)
        ├── ProducerApp.java    # produces N records, flushes, exits 0
        └── ConsumerApp.java    # consumes from earliest until EXPECTED_COUNT, exits 0/1
```

A single Docker image backs both the producer and consumer; the `MAIN_CLASS` env var selects which one each service runs. See `references/architecture.md` for why the stack is wired this way (startup ordering, healthchecks, listener names).

## Step 1: Gather requirements (confirm before scaffolding)

Skip any question the user already answered, but always confirm your understanding before writing files. Do **not** scaffold in the same turn as the first message unless the user has explicitly told you to just go.

1. **Project directory?** Where should the scaffold be created? (Default: `./kafka-java-docker`.)
2. **Producer, consumer, or both?** (Default: both — the value of this skill is the end-to-end round trip. If they want only one, still scaffold both Java files but adjust the compose stack and what you run.)
3. **What data are they producing?** Field names and types, so you can reshape `Order.java` and the produced records. (Default: the demo `Order` — orderId, product, quantity, price.)
4. **Topic name?** (Default: `orders`.)
5. **Serialization?** Default is **JSON Schema (JSON_SR)** with the schema id in the Confluent wire format. Only ask if you suspect they need Avro or Protobuf; if so, read `references/serialization.md` for the dependency and config swaps.

Recap the answers as a short bulleted list and ask the user to confirm or correct.

## Step 2: Present the plan, then scaffold

Before creating files or running anything, show a numbered plan and wait for explicit confirmation — this skill writes files and starts Docker containers on the user's machine, so they should see the blast radius first:

```
Plan:
1. Copy the scaffold into <project-dir>/ (docker-compose.yml, run.sh, app/…)
2. Adapt Order.java + the produced records to your data: <fields>
3. Set topic=<topic>, message count=<n> in docker-compose.yml
4. (When you run it) docker compose up --build will:
   - start a local Kafka broker + Schema Registry (ports 9092, 8081)
   - create topic '<topic>'
   - run the producer (produces <n> records), then the consumer (reads them back)
   No data leaves your machine; no cloud account is used.
```

After confirmation, copy `assets/scaffold/` into the target directory and adapt:

- **Reshape `Order.java`** to the user's fields (rename the class if it helps; update the constructor, getters/setters, and `toString`). Keep the no-arg constructor and standard getters/setters — the JSON_SR deserializer needs them. See `references/serialization.md` for the JSON_SR contract and common pitfalls.
- **Update `ProducerApp.java`** where it constructs sample records so they match the new fields. The key should be a stable identifier field so per-entity ordering is preserved.
- **Update `ConsumerApp.java`** only if you renamed the value class (the `JSON_VALUE_TYPE` line and the generic type).
- **Edit `docker-compose.yml`**: set the `TOPIC`, `MESSAGE_COUNT`/`EXPECTED_COUNT`, and the `init-topics` `--topic` name to match. Keep `MESSAGE_COUNT` and `EXPECTED_COUNT` equal so the consumer's success check is exact.
- Make `run.sh` executable: `chmod +x <project-dir>/run.sh`.

Do not modify the broker/Schema Registry service definitions, listener names, or healthchecks unless the user has a specific reason — those values are load-bearing (see `references/architecture.md`).

## Step 3: Run and verify

Tell the user to run, from the project directory:

```bash
./run.sh
# or: docker compose up --build --abort-on-container-exit --exit-code-from consumer
```

A healthy run ends with the consumer printing `Success: consumed N/N records.` and the stack exiting 0. The first build downloads Maven dependencies and base images, so it can take a few minutes; reruns are fast thanks to Docker layer caching.

If it fails, consult `references/troubleshooting.md` — it covers the common failure modes (broker not ready, Schema Registry connection refused, `UnknownTopicOrPartitionException`, the deserializer returning a `Map` instead of the POJO, and shaded-jar service-loader issues).

To stop and clean up: `docker compose down -v` (the `-v` drops the topic data and consumer offsets so the next run starts fresh).

## Common agent mistakes

| Thought | Reality |
|---------|---------|
| "I'll generate the Java/pom/Dockerfile from scratch" | Copy `assets/scaffold/` instead. It is a known-good, tested baseline; regenerating boilerplate reintroduces the exact bugs the scaffold already solved (wire-format config, shaded service files, listener names). Adapt, don't rewrite. |
| "I'll install Maven or run `mvn` to check the build" | The whole point is Docker-only. The build happens in the Maven stage of the Dockerfile. Never run a host-side `mvn` or assume a JDK exists. |
| "The consumer can use a generic Map / I'll skip `json.value.type`" | Without `KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE` set to the fully-qualified value class, the deserializer returns a `LinkedHashMap`, not the POJO — downstream casts then throw `ClassCastException`. |
| "I'll add the serializer via Maven Central" | `io.confluent:kafka-json-schema-serializer` is only on `https://packages.confluent.io/maven/`. The `<repositories>` block in `pom.xml` is required or the build fails to resolve it. |
| "I'll use the maven-assembly plugin for the fat jar" | Use the shade plugin with `ServicesResourceTransformer`. Assembly's `jar-with-dependencies` clobbers `META-INF/services` files that Kafka and the serializers rely on for `ServiceLoader` lookup. |
| "I'll invent listener names in docker-compose" | `confluentinc/confluent-local` has built-in listeners (`PLAINTEXT`, `PLAINTEXT_HOST`, `CONTROLLER`). Only override their advertised values. New names cause broker boot loops. |
| "Producer and consumer should connect to localhost:9092" | Inside the Docker network they use `kafka:29092` (the `PLAINTEXT` listener). `localhost:9092` (`PLAINTEXT_HOST`) is only for clients running on the host. |
| "I'll set MESSAGE_COUNT to 100 but leave EXPECTED_COUNT at 10" | Keep them equal. The consumer exits 0 only after consuming `EXPECTED_COUNT`; a mismatch makes a healthy run look like a timeout failure (or vice versa). |

## Reference files

- `references/architecture.md` — how the docker-compose stack is wired: image choices, listener names, healthchecks, startup ordering, and why the single-image / `MAIN_CLASS` pattern is used.
- `references/serialization.md` — the JSON_SR contract for Java (POJO requirements, producer/consumer config, auto-register vs. explicit registration) and how to switch the scaffold to Avro or Protobuf.
- `references/troubleshooting.md` — symptom → cause → fix for the common end-to-end failures.

For authoritative Kafka Java client docs, see the [Confluent Java client documentation](https://docs.confluent.io/platform/current/clients/index.md) and the [JSON Schema Serializer reference](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-json.md).
