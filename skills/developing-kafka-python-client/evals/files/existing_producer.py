from confluent_kafka import Producer
import json
import os

producer = Producer({'bootstrap.servers': os.environ['BOOTSTRAP_SERVER']})

def send_event(topic: str, event: dict):
    producer.produce(topic, value=json.dumps(event).encode('utf-8'))
    producer.poll(0)
