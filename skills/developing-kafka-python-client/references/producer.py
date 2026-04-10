import asyncio
import os
import signal

from confluent_kafka.aio import AIOProducer
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient, Schema
from confluent_kafka.schema_registry._async.json_schema import AsyncJSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

import common


async def create_json_serializer(topic, sr_url, sr_key, sr_secret):
    schema_file = os.path.join(os.path.dirname(__file__), "schemas", "value.schema.json")
    with open(schema_file) as f:
        schema_str = f.read()

    sr_conf = {"url": sr_url, "basic.auth.user.info": f"{sr_key}:{sr_secret}"}
    sr_client = AsyncSchemaRegistryClient(sr_conf)

    # Register schema and retrieve the schema ID
    subject = f"{topic}-value"
    json_schema = Schema(schema_str, schema_type="JSON")
    schema_id = await sr_client.register_schema(subject, json_schema)
    print(f"Schema ID: {schema_id} for subject {subject}")

    serializer = await AsyncJSONSerializer(sr_client, schema_str)
    return serializer, schema_id


async def produce(producer, topic, serializer, schema_id, messages):
    """Produce messages using an existing producer instance.

    The producer is passed in — never create a new producer per call.
    This function can be called multiple times with the same producer.
    """
    headers = {"confluent.value.schemaId": str(schema_id)}
    futures = []
    for i, value in enumerate(messages):
        serialized = await serializer(
            value, SerializationContext(topic, MessageField.VALUE)
        )
        future = await producer.produce(topic, value=serialized, headers=headers)
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

    serializer, schema_id = await create_json_serializer(
        config["topic"], config["sr_url"], config["sr_key"], config["sr_secret"]
    )

    # Create producer ONCE and reuse
    producer = AIOProducer(kafka_config)

    # Handle graceful shutdown
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(signum, frame, _shutdown=shutdown, _loop=loop):
        _loop.call_soon_threadsafe(_shutdown.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            # Fallback for platforms (e.g., Windows) where add_signal_handler is not supported
            signal.signal(sig, _handle_signal)

    try:
        # -- Generate sample messages here, adapted to the user's domain --
        messages = [...]  # Replace with domain-specific sample data
        await produce(producer, config["topic"], serializer, schema_id, messages)
    finally:
        await producer.flush()
        await producer.close()
        print("Producer closed")


if __name__ == "__main__":
    asyncio.run(main())
