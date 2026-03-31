import os
import requests
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv


def load_config() -> dict:
    """Load configuration from .env file."""
    load_dotenv()
    return {
        "bootstrap_server": os.getenv("CC_BOOTSTRAP_SERVER"),
        "api_key": os.getenv("CC_API_KEY"),
        "api_secret": os.getenv("CC_API_SECRET"),
        "topic": os.getenv("CC_TOPIC", "demo-topic"),
        "sr_url": os.getenv("CC_SCHEMA_REGISTRY_URL"),
        "sr_key": os.getenv("CC_SR_API_KEY"),
        "sr_secret": os.getenv("CC_SR_API_SECRET"),
        "client_id": os.getenv("CLIENT_ID", "python-client"),
        "group_id": os.getenv("GROUP_ID", "python-consumer-group"),
    }


def get_kafka_config(config: dict) -> dict:
    """Build Kafka client configuration for Confluent Cloud."""
    return {
        "bootstrap.servers": config["bootstrap_server"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": config["api_key"],
        "sasl.password": config["api_secret"],
        "client.id": config["client_id"],
    }


def verify_kafka_setup(kafka_config: dict, topic: str) -> bool:
    """Verify Kafka broker connectivity and topic existence."""
    if not topic:
        print("No topic specified")
        return False
    try:
        admin_client = AdminClient(kafka_config)
        metadata = admin_client.list_topics(timeout=10)
        if topic not in metadata.topics:
            print(f"Topic '{topic}' not found. Available topics: {list(metadata.topics.keys())}")
            return False
        return True
    except Exception as e:
        print(f"Kafka connection error: {e}")
        return False


def verify_schema_registry(sr_url: str, sr_key: str, sr_secret: str) -> bool:
    """Verify Schema Registry connectivity."""
    try:
        auth = (sr_key, sr_secret) if sr_key and sr_secret else None
        response = requests.get(f"{sr_url}/subjects", auth=auth, timeout=5)
        return 200 <= response.status_code < 300
    except requests.exceptions.RequestException as e:
        print(f"Schema Registry connection error: {e}")
        return False
