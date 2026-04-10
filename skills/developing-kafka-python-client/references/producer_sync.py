import os
import signal

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

import common


def create_json_serializer(topic, sr_url, sr_key, sr_secret):
    schema_file = os.path.join(os.path.dirname(__file__), "schemas", "value.schema.json")
    with open(schema_file) as f:
        schema_str = f.read()

    sr_conf = {"url": sr_url, "basic.auth.user.info": f"{sr_key}:{sr_secret}"}
    sr_client = SchemaRegistryClient(sr_conf)

    # Register schema and retrieve the schema ID
    subject = f"{topic}-value"
    json_schema = Schema(schema_str, schema_type="JSON")
    schema_id = sr_client.register_schema(subject, json_schema)
    print(f"Schema ID: {schema_id} for subject {subject}")

    serializer = JSONSerializer(schema_str, sr_client)
    return serializer, schema_id


def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced: partition={msg.partition()}, offset={msg.offset()}")


def produce(producer, topic, serializer, schema_id, messages):
    """Produce messages using an existing producer instance.

    The producer is passed in — never create a new producer per call.
    This function can be called multiple times with the same producer.
    """
    headers = {"confluent.value.schemaId": str(schema_id)}
    for value in messages:
        serialized = serializer(
            value, SerializationContext(topic, MessageField.VALUE)
        )
        producer.produce(topic, value=serialized, headers=headers, on_delivery=delivery_callback)
        # Serve delivery callbacks; keeps the internal queue from filling up
        producer.poll(0)

    # Block until all in-flight messages are delivered
    producer.flush()


def main():
    config = common.load_config()
    kafka_config = common.get_kafka_config(config)

    if not common.verify_kafka_setup(kafka_config, config["topic"]):
        raise RuntimeError("Failed to verify Kafka setup")
    print(f"Connected to Kafka ({config['bootstrap_server']})")

    if not common.verify_schema_registry(config["sr_url"], config["sr_key"], config["sr_secret"]):
        raise RuntimeError("Failed to connect to Schema Registry")
    print(f"Connected to Schema Registry ({config['sr_url']})")

    serializer, schema_id = create_json_serializer(
        config["topic"], config["sr_url"], config["sr_key"], config["sr_secret"]
    )

    # Create producer ONCE and reuse
    producer = Producer(kafka_config)

    # Handle graceful shutdown for continuous-produce loops.
    # For one-shot batch scripts the signal handler ensures flush() still
    # runs if the user hits Ctrl-C mid-batch.
    shutdown = False

    def _handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        # -- Generate sample messages here, adapted to the user's domain --
        # For continuous production, wrap in `while not shutdown:` and call
        # produce() with each batch.
        messages = [...]  # Replace with domain-specific sample data
        produce(producer, config["topic"], serializer, schema_id, messages)
    finally:
        producer.flush()
        print("Producer closed")


if __name__ == "__main__":
    main()
