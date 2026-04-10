"""Payment processing producer that broke after upgrading to 2.13.0 due to type hint enforcement."""
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv
import os

load_dotenv()

def get_producer():
    sr_client = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL")})

    with open("schemas/payment.avsc", "r") as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(sr_client, schema_str)

    producer_conf = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVER"),
        "client.id": "payment-producer",
        "message.max.bytes": 2097152,
        "linger.ms": 50,
        "batch.size": 32768,
        "compression.type": "snappy",
        "retries": 3,
        "acks": "all",
    }

    producer = Producer(producer_conf)
    return producer, avro_serializer


def produce_payment(producer, serializer, topic, payment):
    producer.produce(
        topic=topic,
        key=str(payment["payment_id"]),
        value=serializer(payment, None),
    )
    producer.poll(0)


def main():
    producer, serializer = get_producer()
    topic = os.getenv("TOPIC", "payments")

    try:
        for i in range(5):
            payment = {
                "payment_id": f"PAY-{i:06d}",
                "merchant_id": f"MERCH-{i}",
                "amount": 149.99 + i,
                "currency": "USD",
                "status": "authorized",
            }
            produce_payment(producer, serializer, topic, payment)
            print(f"Produced payment {payment['payment_id']}")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
