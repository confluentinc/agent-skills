#!/usr/bin/env python3
"""
Consume JSON_SR messages from a Kafka topic and verify against expectations.

Usage:
    python consume_and_verify.py --topic <topic-name> --expected-count <num> [--timeout <seconds>]

Environment variables required:
    BOOTSTRAP_SERVERS
    CLUSTER_API_KEY
    CLUSTER_API_SECRET
    SCHEMA_REGISTRY_URL
    SCHEMA_REGISTRY_API_KEY
    SCHEMA_REGISTRY_API_SECRET

Verification:
    - Consumes messages from beginning
    - Checks if expected count is met
    - Outputs consumed messages as JSON
    - Returns exit code 0 if verification passes, 1 otherwise
"""

import argparse
import json
import os
import sys
import time
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer


def create_consumer(bootstrap_servers: str, api_key: str, api_secret: str,
                   schema_registry_url: str, sr_api_key: str, sr_api_secret: str,
                   group_id: str) -> DeserializingConsumer:
    """Create Kafka consumer with JSON_SR deserialization (schema ID in header)."""

    # Schema Registry client
    schema_registry_conf = {
        'url': schema_registry_url,
        'basic.auth.user.info': f'{sr_api_key}:{sr_api_secret}'
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # JSON deserializer (schema auto-fetched from SR via schema ID in header)
    json_deserializer = JSONDeserializer(None, schema_registry_client=schema_registry_client)

    # Consumer config
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': json_deserializer,
        'client.id': 'consume-and-verify'
    }

    return DeserializingConsumer(consumer_conf)


def main():
    parser = argparse.ArgumentParser(description="Consume and verify JSON_SR messages")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--expected-count", type=int, help="Expected message count")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout in seconds (default: 30)")
    parser.add_argument("--output", help="Output file for consumed messages (JSON)")
    parser.add_argument("--verify-field", help="Field to verify exists in all messages")
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

    # Create consumer
    group_id = f"verify-consumer-{int(time.time())}"
    try:
        consumer = create_consumer(
            bootstrap_servers, api_key, api_secret,
            sr_url, sr_api_key, sr_api_secret,
            group_id
        )
    except Exception as e:
        print(f"ERROR: Failed to create consumer: {e}", file=sys.stderr)
        sys.exit(1)

    # Subscribe to topic
    consumer.subscribe([args.topic])
    print(f"Consuming from topic '{args.topic}' (timeout: {args.timeout}s)...")

    messages = []
    start_time = time.time()
    verification_failed = False

    try:
        while time.time() - start_time < args.timeout:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue

            # Deserialize message
            value = msg.value()
            key = msg.key()

            messages.append({
                'key': key,
                'value': value,
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': msg.timestamp()[1]
            })

            print(f"✓ Consumed message [{len(messages)}]: partition={msg.partition()}, offset={msg.offset()}")

            # Verify field if specified
            if args.verify_field and args.verify_field not in value:
                print(f"✗ VERIFICATION FAILED: Field '{args.verify_field}' not found in message", file=sys.stderr)
                verification_failed = True

            # Check if we've reached expected count
            if args.expected_count and len(messages) >= args.expected_count:
                print(f"✓ Reached expected count ({args.expected_count})")
                break

    except KeyboardInterrupt:
        print("\nConsumption interrupted by user")
    finally:
        consumer.close()

    # Write output file
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(messages, f, indent=2, default=str)
        print(f"✓ Wrote {len(messages)} messages to {args.output}")

    # Print summary
    print(f"\n--- Summary ---")
    print(f"Messages consumed: {len(messages)}")
    if args.expected_count:
        print(f"Expected count: {args.expected_count}")
        if len(messages) == args.expected_count:
            print("✓ Count verification PASSED")
        else:
            print(f"✗ Count verification FAILED (expected {args.expected_count}, got {len(messages)})")
            verification_failed = True

    # Exit with appropriate code
    if verification_failed:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
