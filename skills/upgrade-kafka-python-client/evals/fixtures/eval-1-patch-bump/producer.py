from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv
import os
import json

load_dotenv()

def get_producer():
    sr_client = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL")})

    with open("schemas/value.avsc", "r") as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(sr_client, schema_str)

    producer_conf = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVER"),
        "client.id": "python-producer",
    }

    producer = Producer(producer_conf)
    return producer, avro_serializer


def produce_message(producer, serializer, topic, data):
    producer.produce(
        topic=topic,
        value=serializer(data, None),
    )
    producer.poll(0)


def main():
    producer, serializer = get_producer()
    topic = os.getenv("TOPIC", "demo-topic")

    try:
        for i in range(10):
            data = {"sensor_id": f"sensor-{i}", "temperature": 20.0 + i, "timestamp": "2024-01-01T00:00:00Z"}
            produce_message(producer, serializer, topic, data)
            print(f"Produced message {i}")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
