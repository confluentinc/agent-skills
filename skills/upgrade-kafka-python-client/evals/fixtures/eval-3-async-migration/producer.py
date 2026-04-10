"""Synchronous producer used inside a FastAPI app — should ideally be async."""
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv
import os

load_dotenv()

sr_client = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL")})

with open("schemas/event.avsc", "r") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(sr_client, schema_str)

producer_conf = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVER"),
    "client.id": "event-producer",
}

producer = Producer(producer_conf)


def produce_event(event: dict):
    """Called from FastAPI route handlers."""
    producer.produce(
        topic=os.getenv("TOPIC", "events"),
        value=avro_serializer(event, None),
    )
    producer.poll(0)
