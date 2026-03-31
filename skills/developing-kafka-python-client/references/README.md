# <Project Name>

A Python application that [produces to / consumes from / produces to and consumes from] Confluent Cloud using `confluent-kafka-python` with Avro serialization and Schema Registry.

## Prerequisites

- Python 3.8+
- A Confluent Cloud cluster with an API key
- A Schema Registry instance with an API key
- The value schema registered in Schema Registry (see [Schema Setup](#schema-setup))

## Setup

1. Create a virtual environment and install dependencies:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and fill in your Confluent Cloud credentials:

   ```bash
   cp .env.example .env
   ```

   You can find your bootstrap server, API keys, and Schema Registry URL in the [Confluent Cloud Console](https://confluent.cloud/) under your cluster and environment settings.

## Schema Setup

This project uses Avro serialization with Schema Registry. Before running the producer, you need to set up the value schema in Confluent Cloud:

1. Go to your environment in the [Confluent Cloud Console](https://confluent.cloud/)
2. Navigate to **Schema Registry** > **Schemas**
3. Click **Add Schema**, select the topic (`<topic-name>-value` subject), and choose **Avro**
4. Paste the contents of `schemas/value.avsc` and click **Create**

Alternatively, the producer will attempt to auto-register the schema on first run if auto-registration is enabled (the default in Confluent Cloud). You can verify your schema was registered by checking the Schema Registry subjects in the Cloud Console.

## Running

```bash
# Run the producer
python producer.py

# Run the consumer
python consumer.py
```

## Project Structure

- `common.py` — Shared configuration loading and connectivity verification
- `producer.py` — Async Kafka producer with Avro serialization
- `consumer.py` — Async Kafka consumer with Avro deserialization
- `schemas/value.avsc` — Avro schema for the message value
- `.env.example` — Template for Confluent Cloud credentials
