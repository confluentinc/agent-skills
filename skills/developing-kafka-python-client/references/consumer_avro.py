import asyncio
import json
import os
import signal

from confluent_kafka import KafkaError
from confluent_kafka.aio import AIOConsumer
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry._async.avro import AsyncAvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import common


async def create_avro_deserializer(sr_url, sr_key, sr_secret):
    """Create the Avro deserializer.

    AvroDeserializer's positional signature is (schema_registry_client,
    schema_str=None, ...) — NOT the same as JSONDeserializer. Always pass
    both as keyword arguments so the call site is identical across formats.
    Passing schema_str positionally produces:
        TypeError: AsyncAvroDeserializer.__init_impl() got multiple values
        for argument 'schema_registry_client'
    """
    schema_file = os.path.join(os.path.dirname(__file__), "schemas", "value.avsc")
    with open(schema_file) as f:
        schema_str = f.read()

    sr_conf = {"url": sr_url}
    if sr_key and sr_secret:
        sr_conf["basic.auth.user.info"] = f"{sr_key}:{sr_secret}"
    sr_client = AsyncSchemaRegistryClient(sr_conf)
    return await AsyncAvroDeserializer(
        schema_str=schema_str,
        schema_registry_client=sr_client,
    )


async def consume(consumer, topic, deserializer):
    """Consume messages, deserializing with Schema Registry."""
    await consumer.subscribe([topic])
    print(f"Listening on topic: {topic}")

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown.set)
    except NotImplementedError:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(
                sig,
                lambda _sig, _frame, l=loop: l.call_soon_threadsafe(shutdown.set),
            )

    try:
        while not shutdown.is_set():
            msg = await consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                continue

            value = await deserializer(
                msg.value(), SerializationContext(topic, MessageField.VALUE)
            )
            print(json.dumps(value, default=str))
    finally:
        await consumer.unsubscribe()
        await consumer.close()
        print("Consumer closed")


async def main():
    config = common.load_config()
    kafka_config = common.get_kafka_config(config)
    kafka_config["group.id"] = config["group_id"]
    kafka_config["auto.offset.reset"] = "earliest"

    if not common.verify_kafka_setup(kafka_config, config["topic"]):
        raise RuntimeError("Failed to verify Kafka setup")
    print(f"Connected to Kafka ({config['bootstrap_server']})")

    if not common.verify_schema_registry(config["sr_url"], config["sr_key"], config["sr_secret"]):
        raise RuntimeError("Failed to connect to Schema Registry")
    print(f"Connected to Schema Registry ({config['sr_url']})")

    deserializer = await create_avro_deserializer(
        config["sr_url"], config["sr_key"], config["sr_secret"]
    )

    consumer = AIOConsumer(kafka_config)
    await consume(consumer, config["topic"], deserializer)


if __name__ == "__main__":
    asyncio.run(main())
