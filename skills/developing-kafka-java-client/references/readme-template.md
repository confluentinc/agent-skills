# README Template

Generate a README.md that includes:

1. **Project title** — a descriptive name based on the user's data domain (e.g., "IoT Sensor Data Kafka Pipeline").
2. **Overview** — one paragraph: produces to / consumes from Kafka using the Apache Kafka Java clients with Confluent Schema Registry (Avro by default).
3. **Prerequisites** — JDK 17+, plus Maven 3.8+ or Gradle 8+ (whichever was generated), and Docker if using local mode or a Confluent Cloud account if using cloud mode.
4. **Setup** section:
   - For **Confluent Cloud**: copy `.properties.example` to `.properties`, fill in bootstrap server, API keys, and Schema Registry URL (found in the Confluent Cloud Console).
   - For **Local Docker**: `docker compose up -d` to start Kafka and Schema Registry, then copy `.properties.example` to `.properties` (defaults work as-is).
5. **Build** — generate Avro classes and compile:
   - Maven: `mvn generate-sources && mvn package`
   - Gradle: `./gradlew build`
6. **Create topic** — if it doesn't already exist:
   - **Local Docker**: `docker compose exec kafka kafka-topics --create --topic <topic-name> --bootstrap-server localhost:29092`
   - **Confluent Cloud**: create it in the Console, or with the Confluent CLI (after `confluent login` and selecting the environment/cluster): `confluent kafka topic create <topic-name>`
7. **Usage** — commands to run the producer and/or consumer, adapted to what was generated:
   - Maven: `mvn exec:java -Dexec.mainClass=com.example.kafka.AvroProducer` / `...AvroConsumer`
   - Gradle: `./gradlew run` (override the consumer with `-PmainClass=com.example.kafka.AvroConsumer`)
8. **Schema** — the Avro schema is in `src/main/avro/value.avsc` (or `src/main/resources/value.schema.json` for JSON Schema). The producer registers it explicitly via `registerSchema()` on startup (`auto.register.schemas=false`). Alternatively register it manually via the Confluent Cloud Console.
9. **Running tests** — `mvn test` (or `./gradlew test`).
10. **Cleanup** (local Docker only) — `docker compose down` (mention `-v` to remove stored data).

Adapt the README to match what was actually generated — omit producer sections if only a consumer was requested, omit Docker sections for Confluent Cloud projects, and use the correct build tool's commands throughout. Keep it concise and actionable. Note that `.properties` is gitignored and must never be committed.
