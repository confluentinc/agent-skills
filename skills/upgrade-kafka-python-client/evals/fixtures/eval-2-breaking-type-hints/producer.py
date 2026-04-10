from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv
import os

load_dotenv()

def get_producer():
    sr_conf = {"url": os.getenv("SCHEMA_REGISTRY_URL")}
    sr_client = SchemaRegistryClient(sr_conf)

    with open("schemas/order.avsc", "r") as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(sr_client, schema_str)

    producer_conf = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVER"),
        "client.id": "order-producer",
        "message.max.bytes": 1048576,  # int instead of str - will break with type hints enforcement
        "linger.ms": 100,              # int instead of str
        "batch.size": 16384,           # int instead of str
    }

    producer = Producer(producer_conf)
    return producer, avro_serializer


def produce_order(producer, serializer, topic, order):
    producer.produce(
        topic=topic,
        key=str(order["order_id"]),
        value=serializer(order, None),
    )
    producer.poll(0)


def main():
    producer, serializer = get_producer()
    topic = os.getenv("TOPIC", "orders")

    try:
        for i in range(5):
            order = {
                "order_id": f"ORD-{i:04d}",
                "customer_id": f"CUST-{i}",
                "amount": 99.99 + i,
                "currency": "USD",
                "status": "pending",
            }
            produce_order(producer, serializer, topic, order)
            print(f"Produced order {order['order_id']}")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
