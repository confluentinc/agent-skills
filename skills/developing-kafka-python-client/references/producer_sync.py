import os
import signal

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

import common


def register_schema(sr_client, topic, schema_str):
    """Register the schema as a separate explicit step.

    Errors (auth failures, network errors, permission denials) propagate
    immediately — never wrap this in a bare try/except.
    """
    subject = f"{topic}-value"
    json_schema = Schema(schema_str, schema_type="JSON")
    schema_id = sr_client.register_schema(subject, json_schema)
    print(f"Schema ID: {schema_id} for subject {subject}")
    return schema_id


def create_json_serializer(sr_client, schema_str):
    """Create the serializer with auto-registration disabled.

    Schema must already be registered via register_schema().
    """
    serializer = JSONSerializer(
        schema_str,
        sr_client,
        conf={'auto.register.schemas': False, 'use.latest.version': True}
    )
    return serializer


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

    schema_file = os.path.join(os.path.dirname(__file__), "schemas", "value.schema.json")
    with open(schema_file) as f:
        schema_str = f.read()

    sr_conf = {"url": config["sr_url"], "basic.auth.user.info": f"{config['sr_key']}:{config['sr_secret']}"}
    sr_client = SchemaRegistryClient(sr_conf)

    schema_id = register_schema(sr_client, config["topic"], schema_str)
    serializer = create_json_serializer(sr_client, schema_str)

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
