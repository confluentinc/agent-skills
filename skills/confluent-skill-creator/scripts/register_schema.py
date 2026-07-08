#!/usr/bin/env python3
"""
Register a JSON schema with Confluent Schema Registry.

Usage:
    python register_schema.py --subject <subject-name> --schema <schema-file>

Environment variables required:
    SCHEMA_REGISTRY_URL
    SCHEMA_REGISTRY_API_KEY
    SCHEMA_REGISTRY_API_SECRET

Subject naming:
    - For topic value schemas: <topic-name>-value
    - For topic key schemas: <topic-name>-key
"""

import argparse
import json
import os
import sys
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema


def register_schema(sr_client: SchemaRegistryClient, subject: str, schema_str: str) -> int:
    """
    Register schema with Schema Registry.

    Returns schema ID.
    """
    schema = Schema(schema_str, schema_type="JSON")
    schema_id = sr_client.register_schema(subject, schema)
    return schema_id


def main():
    parser = argparse.ArgumentParser(description="Register JSON schema with Schema Registry")
    parser.add_argument("--subject", required=True, help="Schema subject name (e.g., orders-value)")
    parser.add_argument("--schema", required=True, help="Path to JSON schema file (.json)")
    parser.add_argument("--check-compatibility", action="store_true",
                       help="Check compatibility before registering")
    args = parser.parse_args()

    # Get credentials from environment
    sr_url = os.getenv("SCHEMA_REGISTRY_URL")
    sr_api_key = os.getenv("SCHEMA_REGISTRY_API_KEY")
    sr_api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET")

    if not sr_url or not sr_api_key or not sr_api_secret:
        print("ERROR: Missing Schema Registry credentials", file=sys.stderr)
        print("Required: SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET",
              file=sys.stderr)
        sys.exit(1)

    # Read schema file
    try:
        with open(args.schema, 'r') as f:
            schema_str = f.read()
            # Validate it's valid JSON
            json.loads(schema_str)
    except FileNotFoundError:
        print(f"ERROR: Schema file not found: {args.schema}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in schema file: {e}", file=sys.stderr)
        sys.exit(1)

    # Create Schema Registry client
    schema_registry_conf = {
        'url': sr_url,
        'basic.auth.user.info': f'{sr_api_key}:{sr_api_secret}'
    }
    sr_client = SchemaRegistryClient(schema_registry_conf)

    # Check compatibility if requested
    if args.check_compatibility:
        try:
            schema = Schema(schema_str, schema_type="JSON")
            is_compatible = sr_client.test_compatibility(args.subject, schema)
            if is_compatible:
                print(f"✓ Schema is compatible with existing versions of '{args.subject}'")
            else:
                print(f"✗ Schema is NOT compatible with existing versions of '{args.subject}'",
                     file=sys.stderr)
                print("Fix compatibility issues or update compatibility mode", file=sys.stderr)
                sys.exit(1)
        except Exception as e:
            # Subject might not exist yet, which is fine
            print(f"Note: Could not check compatibility (subject might not exist): {e}")

    # Register schema
    try:
        schema_id = register_schema(sr_client, args.subject, schema_str)
        print(f"✓ Schema registered successfully")
        print(f"  Subject: {args.subject}")
        print(f"  Schema ID: {schema_id}")
        sys.exit(0)
    except Exception as e:
        print(f"✗ Failed to register schema: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
