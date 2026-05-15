import asyncio
import os
import signal

from confluent_kafka.aio import AIOProducer
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient, Schema
from confluent_kafka.schema_registry._async.avro import AsyncAvroSerializer
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


async def register_schema(sr_client, topic, schema_str):
    """Register the Avro schema as a separate explicit step.

    Errors (auth failures, network errors, permission denials) propagate
    immediately — never wrap this in a bare try/except.
    """
    subject = f"{topic}-value"
    avro_schema = Schema(schema_str, schema_type="AVRO")
    schema_id = await sr_client.register_schema(subject, avro_schema)
    print(f"Schema ID: {schema_id} for subject {subject}")
    return schema_id


async def create_avro_serializer(sr_client, schema_str):
    """Create the Avro serializer with auto-registration disabled.

    Schema must already be registered via register_schema().

    AvroSerializer's positional signature is (schema_registry_client,
    schema_str, ...) — NOT the same as JSONSerializer. Always pass both
    as keyword arguments so the call site is identical across formats.
    """
    serializer = await AsyncAvroSerializer(
        schema_str=schema_str,
        schema_registry_client=sr_client,
        conf={'auto.register.schemas': False, 'use.latest.version': True},
    )
    return serializer


async def produce(producer, topic, serializer, schema_id, messages, key_field=None):
    """Produce messages using an existing producer instance.

    AIOProducer does not support custom headers in batch mode, so the
    schema is identified via the Avro wire-format prefix written by the
    serializer. schema_id is accepted for signature parity with the
    synchronous variant.
    """
    futures = []
    for i, value in enumerate(messages):
        serialized = await serializer(
            value, SerializationContext(topic, MessageField.VALUE)
        )
        key = _extract_key(value, key_field, i)
        future = await producer.produce(topic, key=key, value=serialized)
        futures.append(future)

    results = await asyncio.gather(*futures, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Message {i+1} delivery failed: {result}")
        elif result.error():
            print(f"Message {i+1} delivery failed: {result.error()}")
        else:
            print(f"Message {i+1} produced: partition={result.partition()}, offset={result.offset()}")


async def main():
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
    sr_client = AsyncSchemaRegistryClient(sr_conf)

    schema_id = await register_schema(sr_client, config["topic"], schema_str)
    serializer = await create_avro_serializer(sr_client, schema_str)

    producer = AIOProducer(kafka_config)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(signum, frame, _shutdown=shutdown, _loop=loop):
        _loop.call_soon_threadsafe(_shutdown.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            signal.signal(sig, _handle_signal)

    try:
        # -- Generate sample messages here, adapted to the user's domain --
        messages = [...]  # Replace with domain-specific sample data
        # Set key_field to the field that identifies the entity (e.g., "transaction_id").
        # On WarpStream, prefer key_field=None for sticky partitioning unless
        # per-entity ordering is required.
        await produce(producer, config["topic"], serializer, schema_id, messages, key_field="entity_id")
    finally:
        await producer.flush()
        await producer.close()
        print("Producer closed")


if __name__ == "__main__":
    asyncio.run(main())
