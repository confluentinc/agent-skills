import os
import requests
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv


def load_config() -> dict:
    """Load configuration from .env file."""
    load_dotenv()
    return {
        "kafka_env": os.getenv("KAFKA_ENV", "cloud"),
        "bootstrap_server": os.getenv("BOOTSTRAP_SERVER"),
        "api_key": os.getenv("API_KEY"),
        "api_secret": os.getenv("API_SECRET"),
        "topic": os.getenv("TOPIC", "demo-topic"),
        "sr_url": os.getenv("SCHEMA_REGISTRY_URL"),
        "sr_key": os.getenv("SR_API_KEY"),
        "sr_secret": os.getenv("SR_API_SECRET"),
        "client_id": os.getenv("CLIENT_ID", "python-client"),
        "group_id": os.getenv("GROUP_ID", "python-consumer-group"),
    }


def get_kafka_config(config: dict) -> dict:
    """Build Kafka client configuration.

    Uses SASL_SSL for Confluent Cloud, PLAINTEXT for local Docker.
    """
    kafka_config = {
        "bootstrap.servers": config["bootstrap_server"],
        "client.id": config["client_id"],
    }

    if config.get("kafka_env") == "local":
        kafka_config["security.protocol"] = "PLAINTEXT"
    else:
        kafka_config["security.protocol"] = "SASL_SSL"
        kafka_config["sasl.mechanisms"] = "PLAIN"
        kafka_config["sasl.username"] = config["api_key"]
        kafka_config["sasl.password"] = config["api_secret"]

    return kafka_config


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
