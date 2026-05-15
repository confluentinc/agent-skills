import os
import signal

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

import common


def _extract_key(value, key_field, index):
    """Extract the Kafka message key from a message dict."""
    if not key_field:
        return None
    if key_field not in value:
        raise KeyError(
            f"Message {index + 1} is missing key field {key_field!r}"
        )
    raw_key = value[key_field]
    if isinstance(raw_key, bytes):
        return raw_key
    return str(raw_key).encode("utf-8")


def register_schema(sr_client, topic, schema_str):
    """Register the Avro schema as a separate explicit step.

    Errors propagate immediately — never wrap this in a bare try/except.
    """
    subject = f"{topic}-value"
    avro_schema = Schema(schema_str, schema_type="AVRO")
    schema_id = sr_client.register_schema(subject, avro_schema)
    print(f"Schema ID: {schema_id} for subject {subject}")
    return schema_id


def create_avro_serializer(sr_client, schema_str):
    """Create the Avro serializer with auto-registration disabled.

    AvroSerializer's positional signature is (schema_registry_client,
    schema_str, ...) — NOT the same as JSONSerializer. Always pass both
    as keyword arguments so the call site is identical across formats.
    """
    serializer = AvroSerializer(
        schema_str=schema_str,
        schema_registry_client=sr_client,
        conf={'auto.register.schemas': False, 'use.latest.version': True},
    )
    return serializer


def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced: partition={msg.partition()}, offset={msg.offset()}")


def produce(producer, topic, serializer, schema_id, messages, key_field=None):
    """Produce messages using an existing producer instance.

    Sync producers can attach the schema ID as a record header in addition
    to the Avro wire-format prefix, which keeps the wire format readable
    for non-Confluent consumers.
    """
    headers = {"confluent.value.schemaId": str(schema_id)}
    for i, value in enumerate(messages):
        serialized = serializer(
            value, SerializationContext(topic, MessageField.VALUE)
        )
        key = _extract_key(value, key_field, i)
        producer.produce(topic, key=key, value=serialized, headers=headers, on_delivery=delivery_callback)
        producer.poll(0)

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

    schema_file = os.path.join(os.path.dirname(__file__), "schemas", "value.avsc")
    with open(schema_file) as f:
        schema_str = f.read()

    sr_conf = {"url": config["sr_url"]}
    if config.get("sr_key") and config.get("sr_secret"):
        sr_conf["basic.auth.user.info"] = f"{config['sr_key']}:{config['sr_secret']}"
    sr_client = SchemaRegistryClient(sr_conf)

    schema_id = register_schema(sr_client, config["topic"], schema_str)
    serializer = create_avro_serializer(sr_client, schema_str)

    producer = Producer(kafka_config)

    shutdown = False

    def _handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        messages = [...]  # Replace with domain-specific sample data
        produce(producer, config["topic"], serializer, schema_id, messages, key_field="entity_id")
    finally:
        producer.flush()
        print("Producer closed")


if __name__ == "__main__":
    main()
