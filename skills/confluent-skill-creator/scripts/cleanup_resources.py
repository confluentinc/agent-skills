#!/usr/bin/env python3
"""
Clean up Confluent resources after testing (LOCAL ONLY).

IMPORTANT: This script should only be used for local Confluent environments.
For Confluent Cloud, resources should be manually reviewed and deleted.

Usage:
    python cleanup_resources.py --topics <topic1,topic2> [--schemas <subject1,subject2>]

Environment variables required:
    BOOTSTRAP_SERVERS
    CLUSTER_API_KEY
    CLUSTER_API_SECRET
    SCHEMA_REGISTRY_URL (optional, for schema cleanup)
    SCHEMA_REGISTRY_API_KEY (optional, for schema cleanup)
    SCHEMA_REGISTRY_API_SECRET (optional, for schema cleanup)
"""

import argparse
import os
import sys
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient


def delete_topics(admin_client: AdminClient, topics: list) -> bool:
    """Delete Kafka topics. Returns True if successful."""
    try:
        # Delete topics
        fs = admin_client.delete_topics(topics, operation_timeout=30)

        # Wait for deletion
        for topic, f in fs.items():
            try:
                f.result()
                print(f"✓ Deleted topic: {topic}")
            except Exception as e:
                print(f"✗ Failed to delete topic {topic}: {e}", file=sys.stderr)
                return False

        return True

    except Exception as e:
        print(f"✗ Error deleting topics: {e}", file=sys.stderr)
        return False


def deprecate_schemas(sr_client: SchemaRegistryClient, subjects: list) -> bool:
    """
    Soft-delete (deprecate) schemas rather than hard-deleting.
    This is safer and follows Confluent best practices.
    """
    success = True
    for subject in subjects:
        try:
            # Get all versions
            versions = sr_client.get_versions(subject)

            # Delete each version (soft delete)
            for version in versions:
                sr_client.delete_version(subject, str(version))
                print(f"✓ Deprecated schema: {subject} version {version}")

        except Exception as e:
            print(f"✗ Failed to deprecate schema {subject}: {e}", file=sys.stderr)
            success = False

    return success


def main():
    parser = argparse.ArgumentParser(description="Clean up Confluent test resources (LOCAL ONLY)")
    parser.add_argument("--topics", help="Comma-separated list of topics to delete")
    parser.add_argument("--schemas", help="Comma-separated list of schema subjects to deprecate")
    parser.add_argument("--force", action="store_true",
                       help="Skip confirmation prompt (use with caution)")
    args = parser.parse_args()

    if not args.topics and not args.schemas:
        print("ERROR: Must specify --topics or --schemas to clean up", file=sys.stderr)
        sys.exit(1)

    # Parse lists
    topics = [t.strip() for t in args.topics.split(',')] if args.topics else []
    schemas = [s.strip() for s in args.schemas.split(',')] if args.schemas else []

    # Safety check: confirm this is local environment
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "")
    if "confluent.cloud" in bootstrap_servers or "ccloud" in bootstrap_servers:
        print("=" * 60, file=sys.stderr)
        print("WARNING: This appears to be Confluent Cloud!", file=sys.stderr)
        print("This cleanup script is intended for LOCAL environments only.", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("\nFor Confluent Cloud, manually delete resources via:", file=sys.stderr)
        print("  - Confluent Cloud UI (https://confluent.cloud)", file=sys.stderr)
        print("  - Confluent CLI: confluent kafka topic delete <topic>", file=sys.stderr)
        print("\nDo NOT proceed unless you're absolutely sure.", file=sys.stderr)

        if not args.force:
            response = input("\nType 'DELETE' to proceed anyway: ")
            if response != "DELETE":
                print("Aborted.")
                sys.exit(0)

    # Confirmation prompt
    if not args.force:
        print("Resources to clean up:")
        if topics:
            print(f"  Topics: {', '.join(topics)}")
        if schemas:
            print(f"  Schemas: {', '.join(schemas)}")

        response = input("\nProceed? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted.")
            sys.exit(0)

    # Clean up topics
    if topics:
        api_key = os.getenv("CLUSTER_API_KEY")
        api_secret = os.getenv("CLUSTER_API_SECRET")

        if not bootstrap_servers or not api_key or not api_secret:
            print("ERROR: Missing Kafka credentials", file=sys.stderr)
            sys.exit(1)

        admin_conf = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': api_key,
            'sasl.password': api_secret
        }

        admin_client = AdminClient(admin_conf)
        success = delete_topics(admin_client, topics)

        if not success:
            print("Topic deletion failed", file=sys.stderr)
            sys.exit(1)

    # Clean up schemas
    if schemas:
        sr_url = os.getenv("SCHEMA_REGISTRY_URL")
        sr_api_key = os.getenv("SCHEMA_REGISTRY_API_KEY")
        sr_api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET")

        if not sr_url or not sr_api_key or not sr_api_secret:
            print("ERROR: Missing Schema Registry credentials", file=sys.stderr)
            sys.exit(1)

        schema_registry_conf = {
            'url': sr_url,
            'basic.auth.user.info': f'{sr_api_key}:{sr_api_secret}'
        }
        sr_client = SchemaRegistryClient(schema_registry_conf)

        success = deprecate_schemas(sr_client, schemas)

        if not success:
            print("Schema deprecation failed", file=sys.stderr)
            sys.exit(1)

    print("\n✓ Cleanup completed successfully")


if __name__ == "__main__":
    main()
