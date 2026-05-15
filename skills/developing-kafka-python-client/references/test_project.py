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
        "BOOTSTRAP_SERVER": "pkc-test.us-east-1.aws.confluent.cloud:9092",
        "API_KEY": "test-key",
        "API_SECRET": "test-secret",
        "TOPIC": "test-topic",
        "SCHEMA_REGISTRY_URL": "https://psrc-test.us-east-2.aws.confluent.cloud",
        "SR_API_KEY": "sr-key",
        "SR_API_SECRET": "sr-secret",
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
        "BOOTSTRAP_SERVER": "broker:9092",
        "API_KEY": "k",
        "API_SECRET": "s",
        "SCHEMA_REGISTRY_URL": "https://sr",
        "SR_API_KEY": "srk",
        "SR_API_SECRET": "srs",
    }, clear=False)
    def test_load_config_uses_defaults(self, mock_dotenv):
        # Remove optional vars so defaults kick in.
        # load_dotenv is patched to prevent .env files from polluting the test.
        os.environ.pop("KAFKA_ENV", None)
        os.environ.pop("TOPIC", None)
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
    """Verify producer reuses instance and shuts down gracefully.

    These tests work for both async (AIOProducer) and synchronous (Producer)
    producer styles. The AST-based single-instantiation check detects whichever
    producer class the generated code imports.
    """

    def test_produce_accepts_existing_producer(self):
        """produce() must take a producer as a parameter, not create one."""
        import producer as prod
        import inspect
        sig = inspect.signature(prod.produce)
        param_names = list(sig.parameters.keys())
        assert "producer" in param_names, (
            "produce() must accept a 'producer' parameter"
        )

    def test_produce_sends_messages(self):
        """Verify produce() calls producer.produce() for each message."""
        import producer as prod
        import asyncio
        import inspect

        messages = [{"id": "1", "type": "test"}]

        if inspect.iscoroutinefunction(prod.produce):
            # Async producer path — serializer and producer.produce are coroutines.
            # AIOProducer.produce() returns a Future that resolves to a Message.
            mock_result = MagicMock()
            mock_result.error.return_value = None
            mock_result.partition.return_value = 0
            mock_result.offset.return_value = 1

            mock_producer = AsyncMock()
            # await producer.produce() returns a future; await future returns the Message
            mock_future = AsyncMock(return_value=mock_result)
            mock_producer.produce.return_value = mock_future()

            mock_serializer = AsyncMock(return_value=b"serialized")

            asyncio.run(
                prod.produce(mock_producer, "test-topic", mock_serializer, 1, messages)
            )
        else:
            # Synchronous producer path
            mock_producer = MagicMock()
            mock_serializer = MagicMock(return_value=b"serialized")
            prod.produce(mock_producer, "test-topic", mock_serializer, 1, messages)

        mock_producer.produce.assert_called_once()

    def test_produce_serializes_and_headers(self):
        """Async: verify serializer is called (headers not supported in AIOProducer batch mode).
        Sync: verify schema ID is included in message headers."""
        import producer as prod
        import asyncio
        import inspect

        messages = [{"id": "1", "type": "test"}]
        schema_id = 42

        if inspect.iscoroutinefunction(prod.produce):
            # AIOProducer does not support headers in batch mode.
            # Verify the serializer is called to ensure schema handling works.
            mock_result = MagicMock()
            mock_result.error.return_value = None
            mock_result.partition.return_value = 0
            mock_result.offset.return_value = 1

            mock_producer = AsyncMock()
            mock_future = AsyncMock(return_value=mock_result)
            mock_producer.produce.return_value = mock_future()

            mock_serializer = AsyncMock(return_value=b"serialized")

            asyncio.run(
                prod.produce(mock_producer, "test-topic", mock_serializer, schema_id, messages)
            )
            mock_serializer.assert_called_once()
            call_kwargs = mock_producer.produce.call_args
            assert call_kwargs.kwargs.get("value") == b"serialized"
        else:
            # Synchronous Producer supports headers — verify schema ID header.
            mock_producer = MagicMock()
            mock_serializer = MagicMock(return_value=b"serialized")
            prod.produce(mock_producer, "test-topic", mock_serializer, schema_id, messages)
            call_kwargs = mock_producer.produce.call_args
            headers = call_kwargs.kwargs.get("headers", {})
            assert "confluent.value.schemaId" in headers, (
                "Headers must include 'confluent.value.schemaId'"
            )
            assert headers["confluent.value.schemaId"] == str(schema_id)

    def test_main_creates_single_producer(self):
        """main() should create the producer once, not per-message."""
        import producer as prod
        import ast
        source = open(prod.__file__).read()
        tree = ast.parse(source)

        # Check for AIOProducer (async) or Producer (sync) instantiation
        producer_classes = ("AIOProducer", "Producer")
        producer_calls = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(getattr(node, "func", None), ast.Name)
            and node.func.id in producer_classes
        ]
        # Also check for attribute-style calls like aio.AIOProducer
        producer_calls += [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(getattr(node, "func", None), ast.Attribute)
            and node.func.attr in producer_classes
        ]
        assert len(producer_calls) == 1, (
            f"Expected exactly 1 producer instantiation, found {len(producer_calls)}"
        )


# ---------------------------------------------------------------------------
# consumer.py tests
# ---------------------------------------------------------------------------

class TestConsumer:
    """Verify consumer subscribes, deserializes, and shuts down cleanly."""

    def test_consumer_uses_schema_registry(self):
        """Consumer must use a Schema Registry deserializer, not raw parsing.

        Accepts any of the three formats (JSON Schema, Avro, Protobuf) in
        either their sync or async variant — whichever the project was
        scaffolded with.
        """
        import consumer as cons
        source = open(cons.__file__).read()
        accepted = (
            "JSONDeserializer", "AsyncJSONDeserializer",
            "AvroDeserializer", "AsyncAvroDeserializer",
            "ProtobufDeserializer", "AsyncProtobufDeserializer",
        )
        assert any(name in source for name in accepted), (
            "Consumer must use a Schema Registry deserializer "
            f"(one of {accepted})"
        )
        # Raw json.loads is only a deserialization-fallback red flag on the
        # JSON-Schema path; Avro/Protobuf consumers may legitimately have no
        # json import at all, but if they do, we still reject json.loads as
        # a value-decoding fallback.
        assert "json.loads" not in source, (
            "Consumer should deserialize via the Schema Registry "
            "deserializer, not raw json.loads"
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

def _find_schema_path(filename):
    """Locate a schema file relative to either the tests dir or project root.

    Returns None if the file does not exist (used by tests to skip the
    classes that don't apply to the chosen schema format).
    """
    for candidate in (
        os.path.join(os.path.dirname(__file__), "..", "schemas", filename),
        os.path.join(os.path.dirname(__file__), "schemas", filename),
    ):
        if os.path.exists(candidate):
            return candidate
    return None


@pytest.mark.skipif(
    _find_schema_path("value.schema.json") is None,
    reason="No value.schema.json — project uses Avro or Protobuf",
)
class TestJsonSchema:
    """Verify the JSON Schema is valid."""

    @pytest.fixture
    def schema(self):
        with open(_find_schema_path("value.schema.json")) as f:
            return json.load(f)

    def test_schema_is_valid_json_schema(self, schema):
        assert schema["type"] == "object"
        assert "title" in schema
        assert "properties" in schema
        assert len(schema["properties"]) > 0

    def test_schema_has_description(self, schema):
        """Top-level schema must have a description for governance."""
        assert "description" in schema, "Schema must have a top-level 'description'"

    def test_schema_properties_have_descriptions(self, schema):
        """Every property must have a description for discoverability."""
        for prop_name, prop_def in schema["properties"].items():
            assert "description" in prop_def, (
                f"Property '{prop_name}' missing 'description': {prop_def}"
            )

    def test_schema_properties_have_type(self, schema):
        for prop_name, prop_def in schema["properties"].items():
            has_type = "type" in prop_def or "enum" in prop_def or "oneOf" in prop_def
            assert has_type, (
                f"Property '{prop_name}' must have 'type', 'enum', or 'oneOf': {prop_def}"
            )

    def test_timestamp_fields_use_format(self, schema):
        """Timestamp fields typed as string must use format: date-time."""
        timestamp_indicators = ("timestamp", "_at", "_date", "_time")
        for prop_name, prop_def in schema["properties"].items():
            if any(prop_name.endswith(ind) or ind in prop_name for ind in timestamp_indicators):
                if prop_def.get("type") == "string":
                    assert prop_def.get("format") == "date-time", (
                        f"Timestamp property '{prop_name}' must have "
                        f"'format': 'date-time', got: {prop_def}"
                    )


@pytest.mark.skipif(
    _find_schema_path("value.avsc") is None,
    reason="No value.avsc — project uses JSON Schema or Protobuf",
)
class TestAvroSchema:
    """Verify the Avro schema is valid.

    Avro schemas are JSON-encoded, so basic structural checks can be done
    without importing the avro library.
    """

    @pytest.fixture
    def schema(self):
        with open(_find_schema_path("value.avsc")) as f:
            return json.load(f)

    def test_schema_is_record_type(self, schema):
        assert schema.get("type") == "record", (
            "Top-level Avro schema must be type 'record'"
        )
        assert "name" in schema, "Avro record must have a 'name'"
        assert "fields" in schema, "Avro record must have 'fields'"
        assert len(schema["fields"]) > 0, "Avro record must have at least one field"

    def test_schema_has_doc(self, schema):
        """Top-level Avro record should have a 'doc' field for governance."""
        assert "doc" in schema, "Avro record must have a top-level 'doc'"

    def test_fields_have_doc(self, schema):
        """Every Avro field should have a 'doc' for discoverability."""
        for field in schema["fields"]:
            assert "doc" in field, (
                f"Avro field '{field.get('name')}' missing 'doc': {field}"
            )

    def test_fields_have_name_and_type(self, schema):
        for field in schema["fields"]:
            assert "name" in field, f"Avro field missing 'name': {field}"
            assert "type" in field, f"Avro field missing 'type': {field}"


@pytest.mark.skipif(
    _find_schema_path("value.proto") is None,
    reason="No value.proto — project uses JSON Schema or Avro",
)
class TestProtobufSchema:
    """Verify the Protobuf schema is structurally sane.

    Lightweight checks — full validation requires `protoc`, which is not
    a reasonable test dependency for a scaffolded project.
    """

    @pytest.fixture
    def source(self):
        with open(_find_schema_path("value.proto")) as f:
            return f.read()

    def test_declares_syntax(self, source):
        assert 'syntax = "proto3"' in source or "syntax = 'proto3'" in source, (
            "Protobuf file must declare proto3 syntax"
        )

    def test_has_at_least_one_message(self, source):
        import re
        assert re.search(r"^\s*message\s+\w+\s*\{", source, re.MULTILINE), (
            "Protobuf file must define at least one message"
        )


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
