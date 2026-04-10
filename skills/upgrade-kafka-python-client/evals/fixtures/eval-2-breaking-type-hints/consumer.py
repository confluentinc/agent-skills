from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from dotenv import load_dotenv
import os
import signal

load_dotenv()

running = True

def signal_handler(sig, frame):
    global running
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    sr_conf = {"url": os.getenv("SCHEMA_REGISTRY_URL")}
    sr_client = SchemaRegistryClient(sr_conf)

    with open("schemas/order.avsc", "r") as f:
        schema_str = f.read()

    avro_deserializer = AvroDeserializer(sr_client, schema_str)

    consumer_conf = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVER"),
        "group.id": os.getenv("GROUP_ID", "order-consumers"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,  # bool instead of str
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([os.getenv("TOPIC", "orders")])

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            order = avro_deserializer(msg.value(), None)
            print(f"Consumed order: {order}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
