---
name: developing-kafka-java-client
description: "Scaffold a Java Kafka producer/consumer project using the Apache Kafka Java client with Schema Registry serialization (Avro). Generates Gradle build, configuration helpers, and unit tests. Supports Confluent Cloud and local Docker. Use this skill whenever the user wants to create a Java Kafka application, produce or consume messages with Java, or set up a Java project with Kafka and Schema Registry."
---

# Confluent Kafka Java Client Creation

Generate a production-ready Java project for producing to and/or consuming from Kafka using the official Apache Kafka Java client (`org.apache.kafka:kafka-clients`) with Confluent Schema Registry for Avro serialization. Supports two target environments: **Confluent Cloud** (managed) and **Local Docker** (open-source Kafka). The generated code follows Confluent's best practices and the patterns from the [Getting Started with Apache Kafka and Java](https://developer.confluent.io/get-started/java/) tutorial.

## Step 1: Gather Requirements

**Always** ask the user these questions before generating — do not assume defaults for #1 or #2:

1. **Target environment?** — Confluent Cloud or local Kafka (Docker). **Always prompt for this, even if the user didn't mention it.** If they mention "open source", "local", "docker", "self-hosted", or just want to try Kafka without a cloud account, choose **local Docker**. If they mention "Confluent Cloud", "CC", or have existing cloud credentials, choose **Confluent Cloud**. Default to Confluent Cloud if they confirm they don't have a preference, but always ask first.
2. **Producer, consumer, or both?**
3. **What kind of data are you producing?** (Get field names and types so you can generate a matching Avro schema and sample data.)
4. **Topic name?** (Default: `demo-topic`)
5. **Consumer group ID?** (Only if consumer; default: `java-consumer-group`)

Don't ask about Schema Registry — always include it.

## Step 2: Generate the Project

Create this file structure in the user's chosen directory:

```
<project-dir>/
├── build.gradle
├── settings.gradle
├── gradle.properties        # (Confluent Cloud credentials — .gitignored)
├── .env.example             # template showing required properties
├── docker-compose.yml       # (local Docker path only)
├── schemas/
│   └── value.avsc           # Avro schema for the message value
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── examples/
│   │   │       ├── ProducerExample.java   # (if requested)
│   │   │       ├── ConsumerExample.java   # (if requested)
│   │   │       └── KafkaConfig.java       # shared config loading
│   │   └── resources/
│   │       └── kafka.properties           # Kafka client properties template
│   └── test/
│       └── java/
│           └── examples/
│               └── KafkaProjectTest.java  # unit tests (always generated)
├── .gitignore
└── README.md
```

### Security

NEVER read, open, or display `gradle.properties` or any file containing API keys/secrets. Only generate `.env.example` and `kafka.properties` templates with placeholder values. If the user asks you to debug a connection issue, ask them to verify their credential values themselves — do not read the file.

### Core Principles

These principles matter because they prevent the most common production issues with Kafka Java clients:

1. **Reuse the producer/consumer instance.** Creating a new `KafkaProducer` per message is expensive — each one opens new TCP connections, does SASL handshakes, and fetches metadata. Create one producer in `main()` and pass it to the produce method. Same for the consumer.

2. **Always use Schema Registry with Avro.** Schema Registry enforces a contract between producers and consumers. Without it, schema changes silently break downstream consumers. Use `KafkaAvroSerializer` / `KafkaAvroDeserializer` from `io.confluent:kafka-avro-serializer` and configure `schema.registry.url` in the client properties.

3. **Use `GenericRecord` for Avro.** Rather than requiring code generation with the Avro Maven/Gradle plugin (which adds build complexity), use `GenericRecord` with a schema loaded at runtime. This keeps the project simple while still getting full Schema Registry integration.

4. **Graceful shutdown.** Producers must call `flush()` then `close()` before exiting — otherwise buffered messages are lost. Consumers must call `consumer.wakeup()` from a shutdown hook, catch `WakeupException` in the poll loop, then `close()` to commit final offsets and leave the consumer group cleanly. Use a `Runtime.getRuntime().addShutdownHook()` thread.

5. **Support both Confluent Cloud and local Docker.** When targeting Confluent Cloud, configure `SASL_SSL` with `PLAIN` mechanism and load API keys from properties. When targeting local Docker, use `PLAINTEXT` with no authentication. A shared `KafkaConfig` class loads a properties file and builds the appropriate configuration.

6. **Verify connectivity before running.** Use `AdminClient.listTopics()` to verify the broker is reachable and the topic exists before producing or consuming. Verify Schema Registry connectivity with an HTTP health check to `{sr_url}/subjects`.

### KafkaConfig.java

This class handles configuration loading from a `.properties` file and building Kafka client configs. Use `references/KafkaConfig.java` as the template.

Key points:
- Loads from a `kafka.properties` file on the classpath
- Detects environment (cloud vs local) from the `kafka.env` property
- For cloud: sets `security.protocol=SASL_SSL`, `sasl.mechanism=PLAIN`, `sasl.jaas.config` with credentials
- For local: sets `security.protocol=PLAINTEXT`
- Always configures Schema Registry URL and (for cloud) SR basic auth
- Includes `verifyKafkaSetup()` using AdminClient and `verifySchemaRegistry()` using HttpURLConnection

### ProducerExample.java

Use `references/ProducerExample.java` as the template.

Key points:
- Creates `KafkaProducer<String, GenericRecord>` once in `main()`
- Uses `KafkaAvroSerializer` for values (configured via properties)
- Loads the Avro schema from `schemas/value.avsc` to build `GenericRecord` instances
- The `produce()` method accepts the producer as a parameter — it never creates one
- Uses a `Callback` for async delivery confirmation (logs partition, offset, or error)
- Calls `producer.flush()` after sending a batch, `producer.close()` in a `finally` block
- Shutdown hook calls `producer.flush()` for graceful termination

### ConsumerExample.java

Use `references/ConsumerExample.java` as the template.

Key points:
- Creates `KafkaConsumer<String, GenericRecord>` once in `main()`
- Uses `KafkaAvroDeserializer` for values (configured via properties)
- Subscribes to the topic and polls in a loop
- Shutdown hook calls `consumer.wakeup()` — the poll loop catches `WakeupException` and exits
- After exiting the loop, calls `consumer.close()` in a `finally` block (commits offsets, leaves group)
- Prints each record's value as a string

### schemas/

Generate an Avro schema file matching the user's data domain. The file should be placed at `schemas/value.avsc`.

For example, if the user is producing financial transactions:

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

### build.gradle

Use `references/build.gradle` as the template. Key dependencies:

```groovy
dependencies {
    implementation 'org.apache.kafka:kafka-clients:3.9.0'
    implementation 'io.confluent:kafka-avro-serializer:7.9.0'
    implementation 'org.apache.avro:avro:1.12.0'
    implementation 'org.slf4j:slf4j-simple:2.0.16'
    testImplementation 'junit:junit:4.13.2'
    testImplementation 'org.mockito:mockito-core:5.14.2'
}
```

Must include:
- The `com.github.johnrengelman.shadow` plugin for building fat JARs
- The `confluent` Maven repository (`https://packages.confluent.io/maven/`)
- Separate `run` tasks for producer and consumer via JavaExec

### docker-compose.yml (Local Docker Path Only)

When the user chooses local Docker, generate a `docker-compose.yml` using `references/docker-compose.yml` as the template. This starts a single-node Kafka broker (using `confluentinc/confluent-local`) and a Confluent Schema Registry.

**IMPORTANT:** The `confluentinc/confluent-local` image uses KRaft mode and has built-in listener names: `PLAINTEXT` (internal, port 29092), `PLAINTEXT_HOST` (external, port 9092), and `CONTROLLER` (port 29093). Do NOT invent custom listener names. The internal `PLAINTEXT` listener must advertise the `kafka` hostname so Schema Registry can reach the broker from within the Docker network.

### kafka.properties

Generate the appropriate properties file based on the target environment:

**Confluent Cloud:**
```properties
kafka.env=cloud
bootstrap.servers=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='<API_KEY>' password='<API_SECRET>';
schema.registry.url=https://psrc-xxxxx.us-east-2.aws.confluent.cloud
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<SR_API_KEY>:<SR_API_SECRET>
topic=demo-topic
group.id=java-consumer-group
```

**Local Docker:**
```properties
kafka.env=local
bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT
schema.registry.url=http://localhost:8081
topic=demo-topic
group.id=java-consumer-group
```

### .env.example

Generate a human-readable `.env.example` showing the required configuration values (this is a reference for users, not loaded by Java code — the Java code loads `kafka.properties`):

**Confluent Cloud:**
```
# Copy this file's values into src/main/resources/kafka.properties
BOOTSTRAP_SERVER=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
API_KEY=your-api-key
API_SECRET=your-api-secret
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-east-2.aws.confluent.cloud
SR_API_KEY=your-sr-api-key
SR_API_SECRET=your-sr-api-secret
TOPIC=demo-topic
GROUP_ID=java-consumer-group
```

**Local Docker:**
```
# Copy this file's values into src/main/resources/kafka.properties
BOOTSTRAP_SERVER=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
TOPIC=demo-topic
GROUP_ID=java-consumer-group
```

### .gitignore

Generate a `.gitignore` that includes:
```
.gradle/
build/
gradle.properties
*.class
*.jar
.env
```

### README.md

Generate a README.md that includes:

1. **Project title** — a descriptive name based on the user's data domain
2. **Overview** — one paragraph explaining what the project does
3. **Prerequisites** — Java 11+, Gradle, plus Docker if using local mode or a Confluent Cloud account if using cloud mode
4. **Setup** section:
   - For **Confluent Cloud**: edit `src/main/resources/kafka.properties` with bootstrap server, SASL credentials, and Schema Registry URL
   - For **Local Docker**: `docker compose up -d` to start Kafka and Schema Registry, then properties defaults work as-is
5. **Build** — `gradle build`
6. **Create topic**:
   - For **Local Docker**: `docker compose exec kafka kafka-topics --create --topic <topic-name> --bootstrap-server localhost:29092`
   - For **Confluent Cloud**: create via Console or CLI
7. **Run** — `gradle runProducer` and `gradle runConsumer`
8. **Schema** — note that the Avro schema is in `schemas/value.avsc` and auto-registered on first produce
9. **Running tests** — `gradle test`
10. **Cleanup** (local Docker only) — `docker compose down -v`

### tests/KafkaProjectTest.java

Always generate unit tests. Use `references/KafkaProjectTest.java` as the template. Tests must run without a live Kafka cluster — mock all external dependencies with Mockito.

The tests should verify:

1. **KafkaConfig.java**: `loadConfig()` returns a valid Properties object. Cloud config contains SASL_SSL settings. Local config contains PLAINTEXT with no SASL.

2. **ProducerExample.java** (if generated): The `produce()` method accepts a KafkaProducer parameter. `flush()` is called. Uses `KafkaAvroSerializer`.

3. **ConsumerExample.java** (if generated): Uses `KafkaAvroDeserializer`. Has shutdown hook with `wakeup()`. Calls `close()`.

4. **schemas/value.avsc**: Valid JSON with `type: record`, a `name`, and at least one field with `name` and `type`.

5. **Project structure**: `build.gradle` exists and contains `kafka-clients` and `kafka-avro-serializer` dependencies.

After generating all files, run `gradle test` to verify the tests pass. If any test fails, fix the generated code (not the tests) until they pass.

## Step 3: Guide the User

After generating the files, give the user instructions based on their target environment:

**Confluent Cloud:**

1. Edit `src/main/resources/kafka.properties` with Confluent Cloud credentials (bootstrap server, API key/secret, Schema Registry URL and credentials)
2. Build: `gradle build`
3. Create the topic in Confluent Cloud Console or via CLI
4. Run the producer: `gradle runProducer`
5. Run the consumer: `gradle runConsumer`

Remind them to find credentials in the Confluent Cloud Console under their cluster and environment settings.

**Local Docker:**

1. Start Kafka and Schema Registry: `docker compose up -d`
2. The default `kafka.properties` is pre-filled for local Docker — no edits needed
3. Build: `gradle build`
4. Create the topic: `docker compose exec kafka kafka-topics --create --topic demo-topic --bootstrap-server localhost:29092`
5. Run the producer: `gradle runProducer`
6. Run the consumer: `gradle runConsumer`
7. When done: `docker compose down -v`
