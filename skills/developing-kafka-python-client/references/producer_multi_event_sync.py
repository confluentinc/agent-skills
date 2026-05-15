import os
import signal

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

import common


def _extract_key(value, key_field, index):
    """Extract the Kafka message key from a message dict.

    Raises a clear error if the field is missing. Coerces non-string
    scalars (ints, UUIDs) to str before UTF-8 encoding; bytes pass
    through unchanged.
    """
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
    """Register the union schema under the default TopicNameStrategy subject.

    The oneOf union schema is registered as a single subject (<topic>-value).
    No strategy change is needed — TopicNameStrategy is the default.
    """
    subject = f"{topic}-value"
    json_schema = Schema(schema_str, schema_type="JSON")
    schema_id = sr_client.register_schema(subject, json_schema)
    print(f"Schema ID: {schema_id} for subject {subject}")
    return schema_id


def create_json_serializer(sr_client, schema_str):
    """Create the serializer with auto-registration disabled.

    The oneOf in the schema handles validation — the serializer checks each
    message against the matching sub-schema at serialization time.
    """
    serializer = JSONSerializer(
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


def produce(producer, topic, serializer, schema_id, messages, key_field="order_id"):
    """Produce messages of varying event types using an existing producer.

    Each message must include an event_type field that matches one of the
    oneOf discriminators in the union schema. The serializer validates the
    message against the matching sub-schema at serialization time.

    key_field names the field used as the Kafka message key. For order
    events, "order_id" ensures all events for the same order land on the
    same partition, preserving per-order ordering guarantees.
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

    schema_file = os.path.join(os.path.dirname(__file__), "schemas", "value.schema.json")
    with open(schema_file) as f:
        schema_str = f.read()

    sr_conf = {"url": config["sr_url"], "basic.auth.user.info": f"{config['sr_key']}:{config['sr_secret']}"}
    sr_client = SchemaRegistryClient(sr_conf)

    schema_id = register_schema(sr_client, config["topic"], schema_str)
    serializer = create_json_serializer(sr_client, schema_str)

    # Create producer ONCE and reuse
    producer = Producer(kafka_config)

    shutdown = False

    def _handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        # -- Multiple event types on the same topic --
        messages = [
            {
                "event_type": "OrderCreated",
                "order_id": "ORD-001",
                "customer_id": "CUST-42",
                "total": 99.95,
                "currency": "USD",
                "timestamp": "2026-04-13T10:00:00Z",
            },
            {
                "event_type": "OrderUpdated",
                "order_id": "ORD-001",
                "updated_fields": ["total"],
                "new_total": 89.95,
                "timestamp": "2026-04-13T10:05:00Z",
            },
            {
                "event_type": "OrderCancelled",
                "order_id": "ORD-001",
                "reason": "Customer requested",
                "cancelled_by": "CUST-42",
                "timestamp": "2026-04-13T10:10:00Z",
            },
        ]
        produce(producer, config["topic"], serializer, schema_id, messages)
    finally:
        producer.flush()
        print("Producer closed")


if __name__ == "__main__":
    main()
