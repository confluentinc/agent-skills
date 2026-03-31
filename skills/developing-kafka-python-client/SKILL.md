---
name: developing-kafka-python-client
description: "Scaffold a production-ready Python project for producing to and/or consuming from Confluent Cloud using confluent-kafka-python, with Schema Registry and Avro serialization. Use this skill when the user wants to send messages to or read messages from Kafka in Python, set up a Python streaming app on Confluent Cloud, or build a Kafka producer or consumer — even if they don't mention specific libraries. Also trigger for questions about Avro serialization, Schema Registry integration, or async Kafka clients in Python. Do NOT trigger for debugging existing Kafka applications, Kafka Connect setup, Flink SQL, Tableflow, or non-Python Kafka clients."
---

# Confluent Kafka Python Client Scaffold

Generate a production-ready Python project for producing to and/or consuming from Confluent Cloud using `confluent-kafka-python`. The generated code follows Confluent's internal best practices from the dtx-template-registry.

## Step 1: Gather Requirements

Ask the user:

1. **Producer, consumer, or both?**
2. **What kind of data are you producing?** (Get field names and types so you can generate a matching Avro schema and sample data.)
3. **Topic name?** (Default: `demo-topic`)
4. **Consumer group ID?** (Only if consumer; default: `python-consumer-group`)

Don't ask about Schema Registry — always include it. Don't ask about Confluent Cloud — it's always the target.

## Step 2: Generate the Project

Create this file structure in the user's chosen directory:

```
<project-dir>/
├── producer.py          # (if requested)
├── consumer.py          # (if requested)
├── common.py            # shared config loading + verification helpers
├── schemas/
│   └── value.avsc       # Avro schema for the message value
├── tests/
│   └── test_project.py  # unit tests (always generated)
├── .env.example         # template for credentials
├── requirements.txt
```

### Security

NEVER read, open, or display `.env` files. They contain API keys and secrets. Only generate `.env.example` with placeholder values. If the user asks you to debug a connection issue, ask them to verify their `.env` values themselves — do not read the file.

### Core Principles

These principles matter because they prevent the most common production issues with Kafka Python clients:

1. **Reuse the producer instance.** Creating a new producer per message is expensive — each one opens new TCP connections, does SASL handshakes, and fetches metadata. Create one producer and reuse it for all messages. The produce function should accept the producer as a parameter, not instantiate one.

2. **Always use Schema Registry with Avro.** Schema Registry enforces a contract between producers and consumers. Without it, schema changes silently break downstream consumers. Always register schemas and use `AvroSerializer`/`AvroDeserializer`.

3. **Use the async API.** The `confluent-kafka-python` library provides `AIOProducer` and `AIOConsumer` in `confluent_kafka.aio`. Use these with `asyncio` for non-blocking I/O. This is the modern recommended approach.

4. **Graceful shutdown.** Producers must `flush()` and `close()` before exiting — otherwise buffered messages are lost. Consumers must `unsubscribe()` then `close()` to leave the consumer group cleanly (avoiding unnecessary rebalances). Use `try/finally` blocks and handle `KeyboardInterrupt` / signals.

5. **Target Confluent Cloud.** Always configure `SASL_SSL` with `PLAIN` mechanism. Load credentials from environment variables via `.env`.

6. **Verify connectivity before running.** Use `AdminClient.list_topics()` to verify the broker is reachable and the topic exists before producing or consuming. Verify Schema Registry connectivity with an HTTP health check.

### common.py

This module handles configuration loading and connectivity verification. Use `references/common.py` as the template.

### producer.py Pattern

Use `references/producer.py` as the template. The producer must follow this structure.

Key points in the producer:
- `produce()` takes a producer instance as a parameter — it never creates one
- The producer is created once in `main()` and can be passed to multiple `produce()` calls
- `flush()` and `close()` happen in a `finally` block so buffered messages are delivered even on error
- Signal handlers set a shutdown event for graceful termination

### consumer.py Pattern

Use `references/consumer.py` as the template.

Key points in the consumer:
- Signal-based graceful shutdown — `unsubscribe()` then `close()` to leave the consumer group cleanly
- Avro deserialization via Schema Registry (no fallback to raw JSON — Schema Registry is required)
- Continuous polling loop until shutdown signal

### schemas/value.avsc

Generate an Avro schema that matches the user's data domain. For example, if they're producing financial transactions:

```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.example",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "status", "type": "string"}
  ]
}
```

Adapt the schema to whatever the user describes. If they don't have a specific domain, use a generic event schema with `id`, `type`, `timestamp`, and `payload` fields.

### .env.example

```
CC_BOOTSTRAP_SERVER=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
CC_API_KEY=your-api-key
CC_API_SECRET=your-api-secret
CC_TOPIC=demo-topic
CC_SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-east-2.aws.confluent.cloud
CC_SR_API_KEY=your-sr-api-key
CC_SR_API_SECRET=your-sr-api-secret
CLIENT_ID=python-client
GROUP_ID=python-consumer-group
```

### requirements.txt

```
confluent-kafka[schema_registry]>=2.13.0
python-dotenv
requests>=2.25.0
fastavro
httpx
authlib
cachetools
attrs
pytest
pytest-asyncio
```

Every third-party package imported anywhere in the generated code (producer.py, consumer.py, common.py) must have a corresponding entry in requirements.txt. If the code does `from confluent_kafka import ...`, then `confluent-kafka` must be in requirements.txt. If it does `from dotenv import load_dotenv`, then `python-dotenv` must be listed. This includes transitive dependencies that aren't automatically installed — for example, the async Schema Registry client imports `httpx` and `authlib` at runtime, so both must be explicitly listed even though they aren't declared as dependencies of `confluent-kafka`. The user should be able to `pip install -r requirements.txt` and run the code with zero `ModuleNotFoundError`s.

Always include `pytest` and `pytest-asyncio` — tests are always generated. Only include `Faker` if the producer generates sample data with it.

### README.md

Use `references/README.md` as the template. Adapt it to match what was generated (producer only, consumer only, or both). Replace `<Project Name>` with something descriptive based on the user's domain and `<topic-name>` with their actual topic.

### tests/test_project.py

Always generate unit tests. Use `references/test_project.py` as the template. The tests must run without a live Kafka cluster or Schema Registry — mock all external dependencies so tests pass in CI and eval environments.

The tests should verify these properties of the generated code:

1. **common.py**: `load_config()` returns all required keys and uses correct defaults. `get_kafka_config()` produces a config with `SASL_SSL` and `PLAIN`. `verify_kafka_setup()` and `verify_schema_registry()` return the right booleans when mocked to succeed or fail.

2. **producer.py** (if generated): `produce()` accepts a producer instance as a parameter (never creates one). `AIOProducer` is instantiated exactly once in the module. Messages are passed through the serializer before producing.

3. **consumer.py** (if generated): Uses `AvroDeserializer` or `AsyncAvroDeserializer` (no raw JSON fallback). Calls `unsubscribe()` before `close()` for graceful shutdown.

4. **schemas/value.avsc**: Valid JSON with `type: record`, a `name`, and at least one field. Each field has `name` and `type`.

5. **Project structure**: `requirements.txt` exists and contains `confluent-kafka`, `python-dotenv`, and `requests`. `.env.example` exists.

Adapt the tests to the user's specific schema and data domain — if they have fields like `device_id` and `temperature`, the schema tests can check for those specific field names.

After generating all files, run `pytest tests/` to verify the tests pass. If any test fails, fix the generated code (not the tests) until they pass.

## Step 3: Guide the User

After generating the files, tell the user:

1. Copy `.env.example` to `.env` and fill in their Confluent Cloud credentials
2. Set up the value schema in Schema Registry — they can either paste the contents of `schemas/value.avsc` into the Confluent Cloud Console under Schema Registry > Schemas for their topic's value subject, or let the producer auto-register it on first run
3. Create a virtualenv and install dependencies: `pip install -r requirements.txt`
4. Run the producer: `python producer.py`
5. Run the consumer: `python consumer.py`

Remind them that they can find their bootstrap server, API keys, and Schema Registry URL in the Confluent Cloud Console under their cluster and environment settings.
