# <Project Name>

A Python application that [produces to / consumes from / produces to and consumes from] Kafka using `confluent-kafka-python` with Avro serialization and Schema Registry.

## Prerequisites

- Python 3.8+
- **Confluent Cloud path:** A Confluent Cloud cluster and Schema Registry with API keys
- **Local Docker path:** Docker and Docker Compose installed

## Setup

### Option A: Confluent Cloud

1. Copy `.env.example` to `.env` and fill in your Confluent Cloud credentials:

   ```bash
   cp .env.example .env
   ```

   You can find your bootstrap server, API keys, and Schema Registry URL in the [Confluent Cloud Console](https://confluent.cloud/) under your cluster and environment settings. Make sure `KAFKA_ENV=cloud` is set.

2. Create a virtual environment and install dependencies:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

### Option B: Local Docker (Open-Source Kafka)

1. Start Kafka and Schema Registry:

   ```bash
   docker compose up -d
   ```

2. Copy `.env.example` to `.env` — the defaults are pre-filled for local Docker, no edits needed:

   ```bash
   cp .env.example .env
   ```

   Make sure `KAFKA_ENV=local` is set.

3. Create a virtual environment and install dependencies:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

4. (Optional) Create the topic manually if auto-creation is disabled:

   ```bash
   docker compose exec kafka kafka-topics --create --topic <topic-name> --bootstrap-server localhost:29092
   ```

## Schema Setup

This project uses Avro serialization with Schema Registry.

- **Confluent Cloud:** Go to the [Confluent Cloud Console](https://confluent.cloud/), navigate to **Schema Registry** > **Schemas**, click **Add Schema**, select the topic (`<topic-name>-value` subject), choose **Avro**, and paste the contents of `schemas/value.avsc`. Alternatively, the producer will auto-register the schema on first run.

- **Local Docker:** The producer will auto-register the schema with the local Schema Registry on first run. No manual setup needed.

## Running

```bash
# Run the producer
python producer.py

# Run the consumer
python consumer.py
```

## Stopping (Local Docker)

```bash
docker compose down       # stop containers
docker compose down -v    # stop and remove stored data
```

## Project Structure

- `common.py` — Shared configuration loading and connectivity verification
- `producer.py` — Kafka producer with Avro serialization (AsyncIO or synchronous, depending on your choice)
- `consumer.py` — Async Kafka consumer with Avro deserialization
- `schemas/value.avsc` — Avro schema for the message value
- `.env.example` — Template for credentials / local config
- `docker-compose.yml` — Local Kafka + Schema Registry (Docker path only)
