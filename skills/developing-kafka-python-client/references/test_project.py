"""
Reference unit tests for the scaffolded Kafka Python project.

These tests run without a live Kafka cluster or Schema Registry.
All external dependencies are mocked so tests can run in CI or evals.
"""

import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest
import pytest_asyncio


# ---------------------------------------------------------------------------
# common.py tests
# ---------------------------------------------------------------------------

class TestLoadConfig:
    """Verify load_config reads all required env vars."""

    @patch.dict(os.environ, {
        "KAFKA_ENV": "cloud",
        "CC_BOOTSTRAP_SERVER": "pkc-test.us-east-1.aws.confluent.cloud:9092",
        "CC_API_KEY": "test-key",
        "CC_API_SECRET": "test-secret",
        "CC_TOPIC": "test-topic",
        "CC_SCHEMA_REGISTRY_URL": "https://psrc-test.us-east-2.aws.confluent.cloud",
        "CC_SR_API_KEY": "sr-key",
        "CC_SR_API_SECRET": "sr-secret",
        "CLIENT_ID": "test-client",
        "GROUP_ID": "test-group",
    })
    def test_load_config_returns_all_keys(self):
        import common
        config = common.load_config()

        assert config["kafka_env"] == "cloud"
        assert config["bootstrap_server"] == "pkc-test.us-east-1.aws.confluent.cloud:9092"
        assert config["api_key"] == "test-key"
        assert config["api_secret"] == "test-secret"
        assert config["topic"] == "test-topic"
        assert config["sr_url"] == "https://psrc-test.us-east-2.aws.confluent.cloud"
        assert config["sr_key"] == "sr-key"
        assert config["sr_secret"] == "sr-secret"

    @patch("common.load_dotenv")
    @patch.dict(os.environ, {
        "CC_BOOTSTRAP_SERVER": "broker:9092",
        "CC_API_KEY": "k",
        "CC_API_SECRET": "s",
        "CC_SCHEMA_REGISTRY_URL": "https://sr",
        "CC_SR_API_KEY": "srk",
        "CC_SR_API_SECRET": "srs",
    }, clear=False)
    def test_load_config_uses_defaults(self, mock_dotenv):
        # Remove optional vars so defaults kick in.
        # load_dotenv is patched to prevent .env files from polluting the test.
        os.environ.pop("KAFKA_ENV", None)
        os.environ.pop("CC_TOPIC", None)
        os.environ.pop("CLIENT_ID", None)
        os.environ.pop("GROUP_ID", None)
        import common
        config = common.load_config()

        assert config["kafka_env"] == "cloud"
        assert config["topic"] == "demo-topic"
        assert config["client_id"] == "python-client"
        assert config["group_id"] == "python-consumer-group"


class TestGetKafkaConfig:
    """Verify get_kafka_config builds the right config for each environment."""

    def test_cloud_contains_sasl_ssl(self):
        import common
        config = {
            "kafka_env": "cloud",
            "bootstrap_server": "broker:9092",
            "api_key": "key",
            "api_secret": "secret",
            "client_id": "client",
        }
        kafka_cfg = common.get_kafka_config(config)

        assert kafka_cfg["bootstrap.servers"] == "broker:9092"
        assert kafka_cfg["security.protocol"] == "SASL_SSL"
        assert kafka_cfg["sasl.mechanisms"] == "PLAIN"
        assert kafka_cfg["sasl.username"] == "key"
        assert kafka_cfg["sasl.password"] == "secret"

    def test_local_uses_plaintext(self):
        import common
        config = {
            "kafka_env": "local",
            "bootstrap_server": "localhost:9092",
            "client_id": "client",
        }
        kafka_cfg = common.get_kafka_config(config)

        assert kafka_cfg["bootstrap.servers"] == "localhost:9092"
        assert kafka_cfg["security.protocol"] == "PLAINTEXT"
        assert "sasl.mechanisms" not in kafka_cfg
        assert "sasl.username" not in kafka_cfg
        assert "sasl.password" not in kafka_cfg


class TestVerifyKafkaSetup:
    """Verify broker/topic checks with mocked AdminClient."""

    @patch("common.AdminClient")
    def test_returns_true_when_topic_exists(self, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin.list_topics.return_value = MagicMock(
            topics={"my-topic": MagicMock()}
        )
        mock_admin_cls.return_value = mock_admin
        import common
        assert common.verify_kafka_setup({"bootstrap.servers": "b"}, "my-topic") is True

    @patch("common.AdminClient")
    def test_returns_false_when_topic_missing(self, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin.list_topics.return_value = MagicMock(topics={})
        mock_admin_cls.return_value = mock_admin
        import common
        assert common.verify_kafka_setup({"bootstrap.servers": "b"}, "no-topic") is False

    def test_returns_false_when_no_topic_specified(self):
        import common
        assert common.verify_kafka_setup({}, "") is False


class TestVerifySchemaRegistry:
    """Verify SR health check with mocked requests."""

    @patch("common.requests.get")
    def test_returns_true_on_200(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200)
        import common
        assert common.verify_schema_registry("https://sr", "k", "s") is True

    @patch("common.requests.get")
    def test_returns_false_on_error(self, mock_get):
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError("unreachable")
        import common
        assert common.verify_schema_registry("https://sr", "k", "s") is False


# ---------------------------------------------------------------------------
# producer.py tests
# ---------------------------------------------------------------------------

class TestProducer:
    """Verify producer reuses instance and shuts down gracefully."""

    @pytest.mark.asyncio
    async def test_produce_accepts_existing_producer(self):
        """produce() must take a producer as a parameter, not create one."""
        import producer as prod
        import inspect
        sig = inspect.signature(prod.produce)
        param_names = list(sig.parameters.keys())
        assert "producer" in param_names, (
            "produce() must accept a 'producer' parameter"
        )

    @pytest.mark.asyncio
    async def test_produce_sends_messages(self):
        """Verify produce() calls producer.produce() for each message."""
        import producer as prod

        mock_producer = AsyncMock()
        mock_result = MagicMock()
        mock_result.error.return_value = None
        mock_result.partition.return_value = 0
        mock_result.offset.return_value = 1
        # AIOProducer.produce() is async and returns a Future.
        # AsyncMock handles the first await; set return_value to the mock_result
        # so the second await (on the Future) resolves to it.
        mock_producer.produce.return_value = mock_result

        # AvroSerializer is synchronous — use MagicMock, not AsyncMock
        mock_serializer = MagicMock(return_value=b"serialized")

        messages = [{"id": "1", "type": "test"}]
        await prod.produce(mock_producer, "test-topic", mock_serializer, messages)

        mock_producer.produce.assert_called_once()

    def test_main_creates_single_producer(self):
        """main() should create the producer once, not per-message."""
        import producer as prod
        import ast
        source = open(prod.__file__).read()
        tree = ast.parse(source)
        # Count how many times AIOProducer is instantiated
        producer_calls = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(getattr(node, "func", None), ast.Name)
            and node.func.id == "AIOProducer"
        ]
        # Also check for attribute-style calls like aio.AIOProducer
        producer_calls += [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(getattr(node, "func", None), ast.Attribute)
            and node.func.attr == "AIOProducer"
        ]
        assert len(producer_calls) == 1, (
            f"Expected exactly 1 AIOProducer instantiation, found {len(producer_calls)}"
        )


# ---------------------------------------------------------------------------
# consumer.py tests
# ---------------------------------------------------------------------------

class TestConsumer:
    """Verify consumer subscribes, deserializes, and shuts down cleanly."""

    def test_consumer_uses_schema_registry(self):
        """Consumer must use AvroDeserializer, not raw JSON."""
        import consumer as cons
        source = open(cons.__file__).read()
        assert "AvroDeserializer" in source or "AsyncAvroDeserializer" in source, (
            "Consumer must use AvroDeserializer from Schema Registry"
        )
        assert "json.loads" not in source or "json.dumps" in source, (
            "Consumer should not fall back to raw json.loads for deserialization"
        )

    def test_consumer_has_graceful_shutdown(self):
        """Consumer must call unsubscribe() then close()."""
        import consumer as cons
        source = open(cons.__file__).read()
        assert "unsubscribe" in source, "Consumer must call unsubscribe()"
        assert "close" in source, "Consumer must call close()"
        # unsubscribe should appear before close in the source
        unsub_pos = source.index("unsubscribe")
        close_pos = source.rindex("close")
        assert unsub_pos < close_pos, (
            "Consumer must unsubscribe() before close() for clean group leave"
        )


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestAvroSchema:
    """Verify the Avro schema is valid."""

    def test_schema_is_valid_json(self):
        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "schemas", "value.avsc"
        )
        # Adjust path if tests/ is a subdirectory
        if not os.path.exists(schema_path):
            schema_path = os.path.join(
                os.path.dirname(__file__), "schemas", "value.avsc"
            )
        with open(schema_path) as f:
            schema = json.load(f)

        assert schema["type"] == "record"
        assert "name" in schema
        assert "fields" in schema
        assert len(schema["fields"]) > 0

    def test_schema_fields_have_name_and_type(self):
        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "schemas", "value.avsc"
        )
        if not os.path.exists(schema_path):
            schema_path = os.path.join(
                os.path.dirname(__file__), "schemas", "value.avsc"
            )
        with open(schema_path) as f:
            schema = json.load(f)

        for field in schema["fields"]:
            assert "name" in field, f"Field missing 'name': {field}"
            assert "type" in field, f"Field missing 'type': {field}"


# ---------------------------------------------------------------------------
# Project structure tests
# ---------------------------------------------------------------------------

class TestProjectStructure:
    """Verify required files exist."""

    def test_requirements_txt_exists(self):
        req_path = os.path.join(os.path.dirname(__file__), "..", "requirements.txt")
        if not os.path.exists(req_path):
            req_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
        assert os.path.exists(req_path), "requirements.txt must exist"

    def test_requirements_has_confluent_kafka(self):
        req_path = os.path.join(os.path.dirname(__file__), "..", "requirements.txt")
        if not os.path.exists(req_path):
            req_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
        contents = open(req_path).read()
        assert "confluent-kafka" in contents

    def test_requirements_has_all_imports(self):
        """Every third-party import in the code must appear in requirements.txt."""
        req_path = os.path.join(os.path.dirname(__file__), "..", "requirements.txt")
        if not os.path.exists(req_path):
            req_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
        contents = open(req_path).read().lower()

        # These packages are used by the generated code
        required = ["confluent-kafka", "python-dotenv", "requests"]
        for pkg in required:
            assert pkg in contents, f"{pkg} missing from requirements.txt"

    def test_env_example_exists(self):
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env.example")
        if not os.path.exists(env_path):
            env_path = os.path.join(os.path.dirname(__file__), ".env.example")
        assert os.path.exists(env_path), ".env.example must exist"
