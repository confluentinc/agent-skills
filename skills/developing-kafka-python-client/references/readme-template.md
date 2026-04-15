# README Template

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
8. **Schema** — the JSON Schema is in `schemas/value.schema.json`. The producer registers it explicitly via `register_schema()` on startup (`auto.register.schemas` is `False`). Alternatively, register it manually via the Confluent Cloud Console.
9. **Running tests** — `pytest tests/`
10. **Cleanup** (local Docker only) — `docker compose down` (mention `-v` to remove stored data)

Adapt the README to match what was actually generated — omit producer sections if only a consumer was requested, omit Docker sections for Confluent Cloud projects, etc. Keep it concise and actionable.
