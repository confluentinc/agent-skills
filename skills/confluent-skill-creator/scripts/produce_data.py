#!/usr/bin/env python3
"""
Produce JSON_SR-encoded messages to a Kafka topic using Schema Registry.

Uses JSON serializer with schema ID in header for schema enforcement.

Usage:
    python produce_data.py --topic <topic-name> --schema <schema-file> --data <data-file>

Environment variables required:
    BOOTSTRAP_SERVERS
    CLUSTER_API_KEY
    CLUSTER_API_SECRET
    SCHEMA_REGISTRY_URL
    SCHEMA_REGISTRY_API_KEY
    SCHEMA_REGISTRY_API_SECRET

Example data file (JSON array):
    [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"}
    ]
"""

import argparse
import json
import os
import sys
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


def create_producer(bootstrap_servers: str, api_key: str, api_secret: str,
                   schema_registry_url: str, sr_api_key: str, sr_api_secret: str,
                   schema_str: str) -> SerializingProducer:
    """Create Kafka producer with JSON_SR serialization (schema ID in header)."""

    # Schema Registry client
    schema_registry_conf = {
        'url': schema_registry_url,
        'basic.auth.user.info': f'{sr_api_key}:{sr_api_secret}'
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # JSON serializer with schema ID in header
    json_serializer = JSONSerializer(
        schema_str,
        schema_registry_client,
        conf={'auto.register.schemas': True}
    )

    # Producer config
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'value.serializer': json_serializer,
        'client.id': 'produce-json-sr-data'
    }

    return SerializingProducer(producer_conf)


def delivery_callback(err, msg):
    """Callback for message delivery reports."""
    if err:
        print(f'Message delivery failed: {err}', file=sys.stderr)
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')


def main():
    parser = argparse.ArgumentParser(description="Produce JSON_SR messages to Kafka")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--schema", required=True, help="Path to JSON schema file (.json)")
    parser.add_argument("--data", required=True, help="Path to data file (JSON array)")
    parser.add_argument("--key-field", help="Field to use as message key (optional)")
    args = parser.parse_args()

    # Get credentials from environment
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
    api_key = os.getenv("CLUSTER_API_KEY")
    api_secret = os.getenv("CLUSTER_API_SECRET")
    sr_url = os.getenv("SCHEMA_REGISTRY_URL")
    sr_api_key = os.getenv("SCHEMA_REGISTRY_API_KEY")
    sr_api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET")

    missing = []
    if not bootstrap_servers:
        missing.append("BOOTSTRAP_SERVERS")
    if not api_key:
        missing.append("CLUSTER_API_KEY")
    if not api_secret:
        missing.append("CLUSTER_API_SECRET")
    if not sr_url:
        missing.append("SCHEMA_REGISTRY_URL")
    if not sr_api_key:
        missing.append("SCHEMA_REGISTRY_API_KEY")
    if not sr_api_secret:
        missing.append("SCHEMA_REGISTRY_API_SECRET")

    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    # Read schema
    try:
        with open(args.schema, 'r') as f:
            schema_str = f.read()
    except FileNotFoundError:
        print(f"ERROR: Schema file not found: {args.schema}", file=sys.stderr)
        sys.exit(1)

    # Read data
    try:
        with open(args.data, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Data file not found: {args.data}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in data file: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print("ERROR: Data file must contain a JSON array", file=sys.stderr)
        sys.exit(1)

    # Create producer
    try:
        producer = create_producer(
            bootstrap_servers, api_key, api_secret,
            sr_url, sr_api_key, sr_api_secret,
            schema_str
        )
    except Exception as e:
        print(f"ERROR: Failed to create producer: {e}", file=sys.stderr)
        sys.exit(1)

    # Produce messages
    print(f"Producing {len(data)} messages to topic '{args.topic}'...")
    produced = 0

    for record in data:
        try:
            # Use key field if specified
            key = str(record.get(args.key_field)) if args.key_field else None

            producer.produce(
                topic=args.topic,
                key=key,
                value=record,
                on_delivery=delivery_callback
            )
            produced += 1

        except Exception as e:
            print(f"Failed to produce message: {e}", file=sys.stderr)

    # Flush
    print("Flushing producer...")
    producer.flush()

    print(f"\nSuccessfully produced {produced}/{len(data)} messages to '{args.topic}'")


if __name__ == "__main__":
    main()
