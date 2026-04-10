---
name: developing-kafka-python-client
description: "Create a Python Kafka producer/consumer project using confluent-kafka-python with Schema Registry JSON Schema serialization. Supports async (AIOProducer) and synchronous (Producer) modes, Confluent Cloud, Confluent Platform, open-source Apache Kafka, and local Docker for development."
---

# Confluent Kafka Python Client Creation

Generate a production-ready Python project for producing to and/or consuming from Kafka using `confluent-kafka-python`. Supports two target environments: **Confluent Cloud** (managed) and **Local Docker** (open-source Kafka), and two producer styles: **AsyncIO** (non-blocking) and **Synchronous** (blocking). The generated code follows Confluent's best practices.

## Step 1: Gather Requirements

**Always** ask the user these questions before generating — do not assume defaults for #1 or #2:

1. **Target environment?** — Confluent Cloud or local Kafka (Docker). **Always prompt for this, even if the user didn't mention it.** If they mention "open source", "local", "docker", "self-hosted", or just want to try Kafka without a cloud account, choose **local Docker**. If they mention "Confluent Cloud", "CC", or have existing cloud credentials, choose **Confluent Cloud**. Default to Confluent Cloud if they confirm they don't have a preference, but always ask first.
2. **Producer, consumer, or both?**
3. **Async or synchronous producer?** (Only if producer is requested.) Help the user choose:
   - **AsyncIO Producer** (`AIOProducer`): Use when code runs under an event loop — FastAPI/Starlette, aiohttp, Sanic, asyncio workers — and must not block.
   - **Synchronous Producer** (`Producer`): Use for scripts, batch jobs, and highest-throughput pipelines where the user controls threads/processes and can call `poll()`/`flush()` directly.
   If the user mentions an async framework (FastAPI, aiohttp, Sanic) or uses `asyncio`, default to **AsyncIO**. If they mention scripts, batch, ETL, or don't have a preference, default to **Synchronous**.
4. **Do you have an existing schema you'd like to use?** If yes, ask the user to paste it or provide the file path, then use it as the `schemas/value.schema.json` instead of generating one. If no, proceed to ask about their data fields.
5. **What kind of data are you producing?** (Only if the user doesn't have an existing schema. Get field names and types so you can generate a matching JSON Schema and sample data.)
6. **Topic name?** (Default: `demo-topic`)
7. **Consumer group ID?** (Only if consumer; default: `python-consumer-group`)

Don't ask about Schema Registry — always include it.

## Step 2: Generate the Project

Create this file structure in the user's chosen directory:

```
<project-dir>/
├── producer.py          # (if requested)
├── consumer.py          # (if requested)
├── common.py            # shared config loading + verification helpers
├── schemas/
│   └── value.schema.json # JSON Schema for the message value
├── tests/
│   └── test_project.py  # unit tests (always generated)
├── .env.example         # template for credentials
├── requirements.txt
├── docker-compose.yml   # (local Docker path only)
```

### Security

NEVER read, open, or display `.env` files. They contain API keys and secrets. Only generate `.env.example` with placeholder values. If the user asks you to debug a connection issue, ask them to verify their `.env` values themselves — do not read the file.

### Core Principles

These principles matter because they prevent the most common production issues with Kafka Python clients:

1. **Reuse the producer instance.** Creating a new producer per message is expensive — each one opens new TCP connections, does SASL handshakes, and fetches metadata. Create one producer and reuse it for all messages. The produce function should accept the producer as a parameter, not instantiate one.

2. **Always use Schema Registry with JSON Schema.** Schema Registry enforces a contract between producers and consumers. Without it, schema changes silently break downstream consumers. Always register schemas and use the appropriate serializer for the chosen producer style: `AsyncJSONSerializer` / `AsyncJSONDeserializer` from `confluent_kafka.schema_registry._async.json_schema` for async, or `JSONSerializer` / `JSONDeserializer` from `confluent_kafka.schema_registry.json_schema` for synchronous.

3. **Choose the right producer style.** The `confluent-kafka-python` library offers two producer APIs:
   - **AsyncIO Producer** (`AIOProducer` from `confluent_kafka.aio`): Non-blocking, integrates with `asyncio` event loops. Use with `AsyncJSONSerializer` from `confluent_kafka.schema_registry._async.json_schema` and `AsyncSchemaRegistryClient`. Best for applications already running an event loop (FastAPI, aiohttp, Sanic, asyncio workers).
   - **Synchronous Producer** (`Producer` from `confluent_kafka`): Blocking calls with delivery callbacks. Use with `JSONSerializer` from `confluent_kafka.schema_registry.json_schema` and `SchemaRegistryClient`. Best for scripts, batch jobs, and highest-throughput pipelines where the user controls threads/processes and can call `poll()`/`flush()` directly.
   Always ask the user which style fits their use case. The consumer always uses `AIOConsumer` (async) — long-running poll loops benefit from non-blocking I/O, and mixing sync/async consumer styles adds complexity with little benefit.

4. **Graceful shutdown.** Async producers must `flush()` and `close()` (both awaited) before exiting. Synchronous producers must call `flush()` before exiting — otherwise buffered messages are lost. Consumers must `unsubscribe()` then `close()` to leave the consumer group cleanly (avoiding unnecessary rebalances). Use `try/finally` blocks and handle `KeyboardInterrupt` / signals.

5. **Support both Confluent Cloud and local Docker.** When targeting Confluent Cloud, configure `SASL_SSL` with `PLAIN` mechanism and load API keys from `.env`. When targeting local Docker, use `PLAINTEXT` with no authentication. The `KAFKA_ENV` environment variable (`cloud` or `local`) controls which path is used. Load all settings from environment variables via `.env`.

6. **Verify connectivity before running.** Use `AdminClient.list_topics()` to verify the broker is reachable and the topic exists before producing or consuming. Verify Schema Registry connectivity with an HTTP health check.

### common.py

This module handles configuration loading and connectivity verification. Use `references/common.py` as the template.

### producer.py Pattern (AsyncIO)

When the user chooses the **AsyncIO producer**, use `references/producer.py` as the template.

Key points:
- `produce()` takes a producer instance as a parameter — it never creates one
- The producer is created once in `main()` and can be passed to multiple `produce()` calls
- The async serializer (`AsyncJSONSerializer`) must be `await`ed when calling it on a message
- `AIOProducer.produce()` is async and returns an `asyncio.Future`. You must `await` the method to get the Future, then `await` the Future to get the delivered `Message`: `future = await producer.produce(...); result = await future`
- `AIOProducer.flush()` and `close()` are coroutines — they must be `await`ed in the `finally` block
- Signal handlers set a shutdown event for graceful termination
- `create_json_serializer()` returns both the serializer and the schema ID. The schema ID is passed as a Kafka record header (`confluent.value.schemaId`) on every produced message so that consumers and downstream systems can identify the schema without parsing the wire-format prefix

### producer.py Pattern (Synchronous)

When the user chooses the **synchronous producer**, use `references/producer_sync.py` as the template.

Key points:
- `produce()` takes a producer instance as a parameter — it never creates one
- The producer is created once in `main()` and can be passed to multiple `produce()` calls
- Uses `JSONSerializer` (synchronous) from `confluent_kafka.schema_registry.json_schema` and `SchemaRegistryClient` from `confluent_kafka.schema_registry`
- `Producer.produce()` is non-blocking — it enqueues the message. Call `producer.poll(0)` after each produce to serve delivery callbacks and keep the internal queue from filling up
- Call `producer.flush()` after a batch to block until all in-flight messages are delivered
- Use a `delivery_callback(err, msg)` function to handle per-message delivery reports
- Signal handlers set a flag for graceful termination
- `flush()` in the `finally` block ensures no buffered messages are lost
- `create_json_serializer()` returns both the serializer and the schema ID. The schema ID is passed as a Kafka record header (`confluent.value.schemaId`) on every produced message so that consumers and downstream systems can identify the schema without parsing the wire-format prefix

### consumer.py Pattern

Use `references/consumer.py` as the template.

Key points in the consumer:
- Signal-based graceful shutdown — `unsubscribe()` then `close()` to leave the consumer group cleanly
- Deserialization via Schema Registry using `AsyncJSONDeserializer` (no fallback to raw JSON parsing — Schema Registry is required)
- Continuous polling loop until shutdown signal

### schemas/

Generate a JSON Schema file matching the user's data domain. The file should be placed at `schemas/value.schema.json`.

For example, if the user is producing financial transactions:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Transaction",
  "type": "object",
  "properties": {
    "transaction_id": {"type": "string"},
    "amount": {"type": "number"},
    "currency": {"type": "string"},
    "timestamp": {"type": "string"},
    "status": {"type": "string"}
  },
  "required": ["transaction_id", "amount", "currency", "timestamp", "status"]
}
```

Adapt the schema to whatever the user describes. If they don't have a specific domain, use a generic event schema with `id`, `type`, `timestamp`, and `payload` properties.

### docker-compose.yml (Local Docker Path Only)

When the user chooses local Docker, generate a `docker-compose.yml` using `references/docker-compose.yml` as the template. This starts a single-node Kafka broker (using `confluentinc/confluent-local`) and a Confluent Schema Registry. The user just runs `docker compose up -d` to get a working Kafka environment.

**IMPORTANT:** The `confluentinc/confluent-local` image uses KRaft mode and has built-in listener names: `PLAINTEXT` (internal, port 29092), `PLAINTEXT_HOST` (external, port 9092), and `CONTROLLER` (port 29093). Do NOT invent custom listener names — this will conflict with the image's internal configuration and cause boot loops. Only override `KAFKA_ADVERTISED_LISTENERS` and `KAFKA_LISTENERS` using these exact listener names. The internal `PLAINTEXT` listener must advertise the `kafka` hostname (not `localhost`) so Schema Registry can reach the broker from within the Docker network.

### .env.example

Generate the appropriate `.env.example` based on the target environment:

**Confluent Cloud:**
```
KAFKA_ENV=cloud
BOOTSTRAP_SERVER=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
API_KEY=your-api-key
API_SECRET=your-api-secret
TOPIC=demo-topic
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-east-2.aws.confluent.cloud
SR_API_KEY=your-sr-api-key
SR_API_SECRET=your-sr-api-secret
CLIENT_ID=python-client
GROUP_ID=python-consumer-group
```

**Local Docker:**
```
KAFKA_ENV=local
BOOTSTRAP_SERVER=localhost:9092
TOPIC=demo-topic
SCHEMA_REGISTRY_URL=http://localhost:8081
CLIENT_ID=python-client
GROUP_ID=python-consumer-group
```

### requirements.txt

```
confluent-kafka[json,schema_registry]>=2.13.2
jsonschema
python-dotenv
requests>=2.25.0
httpx
authlib
cachetools
attrs
pytest
pytest-asyncio
```

Every third-party package imported anywhere in the generated code (producer.py, consumer.py, common.py) must have a corresponding entry in requirements.txt. If the code does `from confluent_kafka import ...`, then `confluent-kafka` must be in requirements.txt. If it does `from dotenv import load_dotenv`, then `python-dotenv` must be listed. This includes transitive dependencies that aren't automatically installed — for example, the async Schema Registry client imports `httpx` and `authlib` at runtime, so both must be explicitly listed even though they aren't declared as dependencies of `confluent-kafka`. The user should be able to `pip install -r requirements.txt` and run the code with zero `ModuleNotFoundError`s.

Always include `pytest`. Include `pytest-asyncio` if the project uses the async producer or consumer. Only include `Faker` if the producer generates sample data with it.

### README.md

Generate a README.md that includes:

1. **Project title** — a descriptive name based on the user's data domain (e.g., "IoT Sensor Data Kafka Pipeline")
2. **Overview** — one paragraph explaining what the project does (produces to / consumes from Kafka using confluent-kafka-python with Schema Registry)
3. **Prerequisites** — Python 3.8+, plus Docker if using local mode or a Confluent Cloud account if using cloud mode
4. **Setup** section:
   - For **Confluent Cloud**: copy `.env.example` to `.env`, fill in bootstrap server, API keys, and Schema Registry URL (mention these are found in the Confluent Cloud Console)
   - For **Local Docker**: `docker compose up -d` to start Kafka and Schema Registry, then copy `.env.example` to `.env` (defaults work as-is)
5. **Install dependencies** — `pip install -r requirements.txt` (suggest using a virtualenv)
6. **Create topic** — include the command to create the topic if it doesn't already exist:
   - For **Local Docker**: `docker compose exec kafka kafka-topics --create --topic <topic-name> --bootstrap-server localhost:29092`
   - For **Confluent Cloud**: direct the user to create the topic via the Confluent Cloud Console, or use the Confluent CLI: `confluent kafka topic create <topic-name>`
7. **Usage** — commands to run the producer and/or consumer (`python producer.py`, `python consumer.py`), adapted to whichever components were generated
8. **Schema** — brief note that the JSON Schema is in `schemas/value.schema.json` and is auto-registered with Schema Registry on first produce
9. **Running tests** — `pytest tests/`
10. **Cleanup** (local Docker only) — `docker compose down` (mention `-v` to remove stored data)

Adapt the README to match what was actually generated — omit producer sections if only a consumer was requested, omit Docker sections for Confluent Cloud projects, etc. Keep it concise and actionable.

### tests/test_project.py

Always generate unit tests. Use `references/test_project.py` as the template. The tests must run without a live Kafka cluster or Schema Registry — mock all external dependencies so tests pass in CI and eval environments.

The tests should verify these properties of the generated code:

1. **common.py**: `load_config()` returns all required keys and uses correct defaults. `get_kafka_config()` produces a config with `SASL_SSL` and `PLAIN` when `KAFKA_ENV=cloud`, or `PLAINTEXT` with no SASL when `KAFKA_ENV=local`. `verify_kafka_setup()` and `verify_schema_registry()` return the right booleans when mocked to succeed or fail.

2. **producer.py** (if generated): `produce()` accepts a producer instance and a `schema_id` as parameters (never creates a producer). The producer class (`AIOProducer` for async, `Producer` for sync) is instantiated exactly once in the module. Messages are passed through the serializer before producing. The schema ID is included as a `confluent.value.schemaId` Kafka record header on every produced message. For synchronous producers, verify `flush()` is called after producing.

3. **consumer.py** (if generated): Uses `JSONDeserializer` or `AsyncJSONDeserializer` (no raw JSON parsing fallback). Calls `unsubscribe()` before `close()` for graceful shutdown.

4. **schemas/value.schema.json**: Valid JSON Schema with `type: object`, a `title`, and `properties` with at least one property. Each property has a `type`.

5. **Project structure**: `requirements.txt` exists and contains `confluent-kafka`, `python-dotenv`, and `requests`. `.env.example` exists.

Adapt the tests to the user's specific schema and data domain — if they have fields like `device_id` and `temperature`, the schema tests can check for those specific field names.

After generating all files, run `pytest tests/` to verify the tests pass. If any test fails, fix the generated code (not the tests) until they pass.

## Step 3: Guide the User

After generating the files, give the user instructions based on their target environment:

**Confluent Cloud:**

1. Copy `.env.example` to `.env` and fill in their Confluent Cloud credentials
2. Set up the value schema in Schema Registry — they can either paste the contents of `schemas/value.schema.json` into the Confluent Cloud Console under Schema Registry > Schemas for their topic's value subject, or let the producer auto-register it on first run
3. Create a virtualenv and install dependencies: `pip install -r requirements.txt`
4. Run the producer: `python producer.py`
5. Run the consumer: `python consumer.py`

Remind them that they can find their bootstrap server, API keys, and Schema Registry URL in the Confluent Cloud Console under their cluster and environment settings.

**Local Docker:**

1. Start Kafka and Schema Registry: `docker compose up -d`
2. Copy `.env.example` to `.env` (defaults are pre-filled for local Docker — no edits needed)
3. Create a virtualenv and install dependencies: `pip install -r requirements.txt`
4. Create the topic (if auto-creation is disabled): `docker compose exec kafka kafka-topics --create --topic demo-topic --bootstrap-server localhost:29092`
5. Run the producer: `python producer.py`
6. Run the consumer: `python consumer.py`
7. When done, stop the containers: `docker compose down` (add `-v` to also remove stored data)
