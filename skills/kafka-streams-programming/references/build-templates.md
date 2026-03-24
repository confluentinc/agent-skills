# Build Templates

Project structure, build files, and testing templates for generated Kafka Streams apps.

## Project Structure

```
<app-name>/
├── build.gradle
├── settings.gradle
├── gradle.properties
├── gradle/
│   └── wrapper/
│       ├── gradle-wrapper.jar
│       └── gradle-wrapper.properties
├── gradlew                                # Gradle wrapper (Unix)
├── gradlew.bat                           # Gradle wrapper (Windows)
├── src/
│   ├── main/
│   │   ├── avro/                        # Avro schemas (NOT resources/avro/)
│   │   │   ├── InputEvent.avsc
│   │   │   └── OutputEvent.avsc
│   │   ├── java/
│   │   │   └── com/example/<appname>/
│   │   │       ├── App.java              # Main class with config + shutdown hook
│   │   │       ├── TopologyBuilder.java   # Topology definition (testable)
│   │   │       ├── SampleDataProducer.java # Test data producer (if requested)
│   │   │       └── serdes/               # Custom serdes if needed
│   │   ├── resources/
│   │   │   ├── application.properties    # Streams config
│   │   │   └── simplelogger.properties   # Log level config (suppress noise)
│   └── test/
│       └── java/
│           └── com/example/<appname>/
│               └── TopologyTest.java     # TopologyTestDriver test
├── docker-compose.yml                    # Local dev environment
├── Dockerfile                           # Production container (if prod target)
├── create-topics.sh                     # Pre-create source, output, DLQ topics
├── teardown.sh                          # Clean up all topics and state
├── .env.example                         # Template for credentials
├── .env                                 # Actual credentials (gitignored)
├── .gitignore                           # Excludes build/, .gradle/, .env, state/
└── README.md                            # How to run, configure, monitor
```

**IMPORTANT:** The Gradle Avro plugin expects schemas in `src/main/avro/`, NOT `src/main/resources/avro/`. Putting schemas in `resources/avro/` causes `NO-SOURCE` — the build succeeds but no Java classes are generated, leading to compilation errors. See `references/schema-patterns.md` for correct schema examples.

For Protobuf: use `src/main/proto/`. For JSON Schema: no schema files — define POJOs in `src/main/java/.../model/`.

Separate `TopologyBuilder` from `App` so the topology is independently testable with `TopologyTestDriver`.

### Generating the Gradle Wrapper

**Always generate the Gradle wrapper** in new projects. This allows users to run the project without having Gradle installed locally. The wrapper scripts (`gradlew` and `gradlew.bat`) download and use a specific Gradle version automatically.

To generate the wrapper files after creating `build.gradle` and `settings.gradle`:

```bash
gradle wrapper --gradle-version 8.12
```

This creates:
- `gradle/wrapper/gradle-wrapper.jar` — the wrapper runtime
- `gradle/wrapper/gradle-wrapper.properties` — Gradle version config
- `gradlew` — Unix/Mac executable script
- `gradlew.bat` — Windows batch script

**If the user doesn't have Gradle installed**, you can generate the wrapper by creating `gradle-wrapper.properties` manually and downloading the wrapper JAR. But for skill-generated projects, prefer running `gradle wrapper` directly.

After generating the wrapper, users run tasks with `./gradlew` instead of `gradle`:

```bash
./gradlew build
./gradlew run
./gradlew test
```

The wrapper ensures consistent Gradle versions across development environments and CI/CD pipelines.

## Gradle (default)

```groovy
plugins {
    id 'java'
    id 'application'
    id 'com.github.davidmc24.gradle.plugin.avro' version '1.9.1' // if Avro
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}

dependencies {
    implementation 'org.apache.kafka:kafka-streams:4.2.0'
    implementation 'io.confluent:kafka-streams-avro-serde:8.2.0'  // or protobuf/json-schema
    implementation 'org.apache.avro:avro:1.12.0'  // if Avro
    implementation 'org.slf4j:slf4j-simple:2.0.16'  // Dev mode; for production use ch.qos.logback:logback-classic:1.5.16

    testImplementation 'org.apache.kafka:kafka-streams-test-utils:4.2.0'
    testImplementation 'org.junit.jupiter:junit-jupiter:5.11.4'
}

application {
    mainClass = 'com.example.<appname>.App'
}

// Auto-load .env file so ./gradlew run just works
run {
    doFirst {
        def envFile = file('.env')
        if (envFile.exists()) {
            envFile.readLines().each { line ->
                if (line && !line.startsWith('#') && line.contains('=')) {
                    def (key, value) = line.split('=', 2)
                    environment key.trim(), value.trim()
                }
            }
        }
    }
    environment System.getenv()
}

// Separate task for running the sample data producer
// Use: ./gradlew produce
// This avoids state directory lock conflicts with the running Streams app.
tasks.register('produce', JavaExec) {
    classpath = sourceSets.main.runtimeClasspath
    mainClass = 'com.example.<appname>.SampleDataProducer'
    doFirst {
        def envFile = file('.env')
        if (envFile.exists()) {
            envFile.readLines().each { line ->
                if (line && !line.startsWith('#') && line.contains('=')) {
                    def (key, value) = line.split('=', 2)
                    environment key.trim(), value.trim()
                }
            }
        }
    }
    environment System.getenv()
}

test {
    useJUnitPlatform()
}
```

### Serde Dependency by Format

| Schema Format | Dependency |
|--------------|-----------|
| Avro | `io.confluent:kafka-streams-avro-serde:8.2.0` |
| Protobuf | `io.confluent:kafka-streams-protobuf-serde:8.2.0` |
| JSON Schema | `io.confluent:kafka-streams-json-schema-serde:8.2.0` |

Remove the Avro Gradle plugin (`com.github.davidmc24.gradle.plugin.avro`) if not using Avro.

### Protobuf Gradle Build

When the user chooses Protobuf, use this build instead of the Avro version. The `com.google.protobuf` plugin compiles `.proto` files from `src/main/proto/` into Java classes.

```groovy
plugins {
    id 'java'
    id 'application'
    id 'com.google.protobuf' version '0.9.4'
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}

protobuf {
    protoc {
        artifact = 'com.google.protobuf:protoc:4.31.1'
    }
}

dependencies {
    implementation 'org.apache.kafka:kafka-streams:4.2.0'
    implementation 'io.confluent:kafka-streams-protobuf-serde:8.2.0'
    implementation 'com.google.protobuf:protobuf-java:4.31.1'
    implementation 'org.slf4j:slf4j-simple:2.0.16'

    testImplementation 'org.apache.kafka:kafka-streams-test-utils:4.2.0'
    testImplementation 'org.junit.jupiter:junit-jupiter:5.11.4'
}

application {
    mainClass = 'com.example.<appname>.App'
}

// Auto-load .env file so ./gradlew run just works
run {
    doFirst {
        def envFile = file('.env')
        if (envFile.exists()) {
            envFile.readLines().each { line ->
                if (line && !line.startsWith('#') && line.contains('=')) {
                    def (key, value) = line.split('=', 2)
                    environment key.trim(), value.trim()
                }
            }
        }
    }
    environment System.getenv()
}

tasks.register('produce', JavaExec) {
    classpath = sourceSets.main.runtimeClasspath
    mainClass = 'com.example.<appname>.SampleDataProducer'
    doFirst {
        def envFile = file('.env')
        if (envFile.exists()) {
            envFile.readLines().each { line ->
                if (line && !line.startsWith('#') && line.contains('=')) {
                    def (key, value) = line.split('=', 2)
                    environment key.trim(), value.trim()
                }
            }
        }
    }
    environment System.getenv()
}

test {
    useJUnitPlatform()
}
```

Without the `com.google.protobuf` plugin, `.proto` files won't compile and no Java classes are generated — the build "succeeds" but compilation of your topology code fails with missing class errors. This is the Protobuf equivalent of the Avro `src/main/avro/` directory bug.

### JSON Schema Gradle Build

JSON Schema does not use code generation. Define POJOs manually in `src/main/java/.../model/`. No schema plugin is needed.

```groovy
plugins {
    id 'java'
    id 'application'
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}

dependencies {
    implementation 'org.apache.kafka:kafka-streams:4.2.0'
    implementation 'io.confluent:kafka-streams-json-schema-serde:8.2.0'
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.17.0'
    implementation 'org.slf4j:slf4j-simple:2.0.16'

    testImplementation 'org.apache.kafka:kafka-streams-test-utils:4.2.0'
    testImplementation 'org.junit.jupiter:junit-jupiter:5.11.4'
}

application {
    mainClass = 'com.example.<appname>.App'
}

// Auto-load .env file so ./gradlew run just works
run {
    doFirst {
        def envFile = file('.env')
        if (envFile.exists()) {
            envFile.readLines().each { line ->
                if (line && !line.startsWith('#') && line.contains('=')) {
                    def (key, value) = line.split('=', 2)
                    environment key.trim(), value.trim()
                }
            }
        }
    }
    environment System.getenv()
}

tasks.register('produce', JavaExec) {
    classpath = sourceSets.main.runtimeClasspath
    mainClass = 'com.example.<appname>.SampleDataProducer'
    doFirst {
        def envFile = file('.env')
        if (envFile.exists()) {
            envFile.readLines().each { line ->
                if (line && !line.startsWith('#') && line.contains('=')) {
                    def (key, value) = line.split('=', 2)
                    environment key.trim(), value.trim()
                }
            }
        }
    }
    environment System.getenv()
}

test {
    useJUnitPlatform()
}
```

### Maven (pom.xml)

Maven is supported as an alternative build tool but is not the default. When the user chooses Maven, generate the equivalent `pom.xml` following the same patterns. The Maven Avro plugin is `org.apache.avro:avro-maven-plugin`, Protobuf uses `org.xolstice.maven.plugins:protobuf-maven-plugin`, and JSON Schema needs no plugin.

Maven is not fully templated here — adapt the Gradle examples to Maven conventions. Key differences:
- Use `maven-compiler-plugin` for Java 17
- Confluent repo: `<repository><url>https://packages.confluent.io/maven/</url></repository>`
- Exec plugin: `exec-maven-plugin` for running the app
- Test: `maven-surefire-plugin` with JUnit 5

### Containerization (shadow plugin)

For production Dockerfiles, add the shadow plugin to create a fat jar:

```groovy
plugins {
    id 'com.github.johnrengelman.shadow' version '8.1.1'
}
```

Build with `./gradlew shadowJar`. The fat jar is at `build/libs/<appname>-all.jar`.

Alternatively, use the built-in Gradle `application` plugin's distribution: `./gradlew installDist` creates a runnable distribution in `build/install/<appname>/` with a startup script. This avoids the shadow plugin but produces a directory instead of a single jar.

For the Dockerfile, prefer `shadowJar` (single file copy) over `installDist` (directory copy).

## simplelogger.properties

**Always generate this file** at `src/main/resources/simplelogger.properties`. Without it, every Kafka Streams startup dumps hundreds of lines of AbstractConfig output, making logs unusable for demos and noisy for development.

```properties
# Suppress noisy Kafka config dumps on startup
org.slf4j.simpleLogger.log.org.apache.kafka.common.config=WARN
org.slf4j.simpleLogger.log.org.apache.kafka.clients=WARN
org.slf4j.simpleLogger.log.io.confluent.kafka.serializers=WARN

# App logging
org.slf4j.simpleLogger.log.com.example=INFO

# Streams runtime — INFO shows state transitions, rebalancing, thread lifecycle
org.slf4j.simpleLogger.log.org.apache.kafka.streams=INFO

org.slf4j.simpleLogger.defaultLogLevel=WARN
org.slf4j.simpleLogger.showDateTime=true
org.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS
org.slf4j.simpleLogger.showShortLogName=true
```

For production apps (Step 4), replace `slf4j-simple` with `logback-classic` and generate `logback.xml` with JSON encoder. See `references/production-hardening.md`.

## .env.example

Always generate a `.env.example` with placeholders. The user copies it to `.env` and fills in their values.

```bash
# Kafka Cluster
BOOTSTRAP_SERVERS=<pkc-xxxxx.region.provider.confluent.cloud:9092>
CLUSTER_API_KEY=<your-cluster-api-key>
CLUSTER_API_SECRET=<your-cluster-api-secret>

# Schema Registry
SCHEMA_REGISTRY_URL=<https://psrc-xxxxx.region.provider.confluent.cloud>
SCHEMA_REGISTRY_API_KEY=<your-sr-api-key>
SCHEMA_REGISTRY_API_SECRET=<your-sr-api-secret>
```

For local dev with docker-compose, the `.env` values are pre-filled:
```bash
BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
```

## Testing

Always generate a `TopologyTest.java` using `TopologyTestDriver`.

**Use `mock://<scope>` as the Schema Registry URL in test properties.** Confluent serdes recognize the `mock://` scheme and use an in-memory mock registry automatically. Do NOT create a separate `MockSchemaRegistryClient` — the `mock://` URL handles it.

```java
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, InputEvent> inputTopic;
    private TestOutputTopic<String, OutputEvent> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put("application.id", "test-app");
        props.put("bootstrap.servers", "dummy:9092");

        // mock:// URL — Confluent serdes auto-create an in-memory SR
        props.put("schema.registry.url", "mock://test-sr");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde");

        // Disable cache for deterministic test results
        // Without this, cache deduplicates updates and intermediate assertions may fail
        props.put("statestore.cache.max.bytes", "0");

        var topology = TopologyBuilder.build(props);
        driver = new TopologyTestDriver(topology, props);

        // Create serdes configured with mock:// URL
        var inputSerializer = new SpecificAvroSerializer<InputEvent>();
        inputSerializer.configure(
            java.util.Map.of("schema.registry.url", "mock://test-sr"), false);

        var outputDeserializer = new SpecificAvroDeserializer<OutputEvent>();
        outputDeserializer.configure(
            java.util.Map.of("schema.registry.url", "mock://test-sr"), false);

        inputTopic = driver.createInputTopic(
            "input-topic", new StringSerializer(), inputSerializer);
        outputTopic = driver.createOutputTopic(
            "output-topic", new StringDeserializer(), outputDeserializer);
    }

    @AfterEach
    void teardown() {
        if (driver != null) driver.close();
    }

    @Test
    void testTopology() {
        // Build an input record using the generated Avro builder
        var input = InputEvent.newBuilder()
            .setField("value")
            .build();

        inputTopic.pipeInput("key", input);

        assertFalse(outputTopic.isEmpty());
        var result = outputTopic.readKeyValue();
        assertEquals("key", result.key);
        // Assert on result.value fields
    }
}
```

### Protobuf Test Template

For Protobuf schemas, use `KafkaProtobufSerializer`/`KafkaProtobufDeserializer` instead of Avro serdes. The `mock://` URL scheme works the same way.

```java
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;

@BeforeEach
void setup() {
    Properties props = new Properties();
    props.put("application.id", "test-app");
    props.put("bootstrap.servers", "dummy:9092");
    props.put("schema.registry.url", "mock://test-sr");
    props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
    props.put("default.value.serde", "io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde");
    props.put("statestore.cache.max.bytes", "0");  // Deterministic tests

    var topology = TopologyBuilder.build(props);
    driver = new TopologyTestDriver(topology, props);

    var inputSerializer = new KafkaProtobufSerializer<InputProto>();
    inputSerializer.configure(
        java.util.Map.of("schema.registry.url", "mock://test-sr"), false);

    var outputDeserializer = new KafkaProtobufDeserializer<OutputProto>();
    outputDeserializer.configure(java.util.Map.of(
        "schema.registry.url", "mock://test-sr",
        KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, OutputProto.class.getName()
    ), false);

    inputTopic = driver.createInputTopic(
        "input-topic", new StringSerializer(), inputSerializer);
    outputTopic = driver.createOutputTopic(
        "output-topic", new StringDeserializer(), outputDeserializer);
}
```

The `SPECIFIC_PROTOBUF_VALUE_TYPE` config is required for the deserializer to return the correct Protobuf message type instead of `DynamicMessage`.

### JSON Schema Test Template

For JSON Schema, use `KafkaJsonSchemaSerializer`/`KafkaJsonSchemaDeserializer`. The critical difference is that `json.value.type` MUST be set per serde instance — without it, deserialization returns `LinkedHashMap` instead of your POJO.

```java
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

@BeforeEach
void setup() {
    Properties props = new Properties();
    props.put("application.id", "test-app");
    props.put("bootstrap.servers", "dummy:9092");
    props.put("schema.registry.url", "mock://test-sr");
    props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
    props.put("default.value.serde", "io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde");
    props.put("statestore.cache.max.bytes", "0");  // Deterministic tests

    var topology = TopologyBuilder.build(props);
    driver = new TopologyTestDriver(topology, props);

    var inputSerializer = new KafkaJsonSchemaSerializer<InputPojo>();
    inputSerializer.configure(java.util.Map.of(
        "schema.registry.url", "mock://test-sr"
    ), false);

    var outputDeserializer = new KafkaJsonSchemaDeserializer<OutputPojo>();
    outputDeserializer.configure(java.util.Map.of(
        "schema.registry.url", "mock://test-sr",
        KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, OutputPojo.class.getName()
    ), false);

    inputTopic = driver.createInputTopic(
        "input-topic", new StringSerializer(), inputSerializer);
    outputTopic = driver.createOutputTopic(
        "output-topic", new StringDeserializer(), outputDeserializer);
}
```

### Multiple Value Types in a Single Topology

When input and output schemas use different types (e.g., input is `Transaction`, output is `AccountSummary`), `default.value.serde` can only be set to one type. Internal topics (changelog, repartition) use the default serde, so the default must match the type used in state stores (usually the aggregation output type).

You MUST use explicit serdes in `Consumed.with()`, `Produced.with()`, and `Materialized.with()` for any topic whose value type differs from the default. If you rely on the default for the wrong type, changelog deserialization will fail with confusing errors.

```java
// Example: input is Transaction (Avro), output is AccountSummary (Avro)
// Set default to AccountSummary (used by changelog stores)
// Use explicit serde for input consumption
KStream<String, Transaction> input = builder.stream(
    "transactions",
    Consumed.with(Serdes.String(), transactionSerde)  // explicit — not default
        .withName("source-transactions"));

KTable<String, AccountSummary> summary = input
    .groupByKey(Grouped.with(Serdes.String(), transactionSerde)
        .withName("group-by-account"))
    .aggregate(
        AccountSummary::new,
        (key, txn, agg) -> agg.add(txn),
        Named.as("aggregate"),
        Materialized.<String, AccountSummary, KeyValueStore<Bytes, byte[]>>
            as("account-summary-store")
            .withKeySerde(Serdes.String())
            .withValueSerde(accountSummarySerde));  // matches default — but explicit is safer
```

This is the fastest way to validate topology logic without a running Kafka cluster.

## Reference Repositories

For working examples and inspiration, see:
- [Confluent Tutorials](https://github.com/confluentinc/tutorials) — Kafka Streams tutorials covering filtering, aggregation, joins, windowing, session windows, error handling, and serialization (Avro + Protobuf)
- [Confluent Examples](https://github.com/confluentinc/examples) — Broader examples including Confluent Cloud configurations
