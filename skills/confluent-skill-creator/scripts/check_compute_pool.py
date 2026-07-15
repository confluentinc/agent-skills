#!/usr/bin/env python3
"""
Check if a Flink compute pool is available and has capacity.

Usage:
    python check_compute_pool.py --pool-id <pool-id>

Environment variables required:
    CONFLUENT_CLOUD_API_KEY
    CONFLUENT_CLOUD_API_SECRET
    FLINK_API_KEY (optional, for detailed pool info)
    FLINK_API_SECRET (optional, for detailed pool info)
"""

import argparse
import os
import sys
import requests
from requests.auth import HTTPBasicAuth


def check_compute_pool(pool_id: str, cloud_api_key: str, cloud_api_secret: str) -> dict:
    """
    Check Flink compute pool status.

    Returns dict with:
        - available: bool
        - status: str (PROVISIONING, RUNNING, FAILED, etc.)
        - capacity: dict with current/max values
        - message: str (human-readable status)
    """
    base_url = "https://api.confluent.cloud"
    endpoint = f"/fcpm/v2/compute-pools/{pool_id}"

    try:
        response = requests.get(
            f"{base_url}{endpoint}",
            auth=HTTPBasicAuth(cloud_api_key, cloud_api_secret),
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 404:
            return {
                "available": False,
                "status": "NOT_FOUND",
                "capacity": {},
                "message": f"Compute pool {pool_id} not found"
            }

        response.raise_for_status()
        data = response.json()

        status = data.get("status", {}).get("phase", "UNKNOWN")
        spec = data.get("spec", {})

        # Check if pool is running
        available = status == "RUNNING"

        # Get capacity info if available
        capacity = {}
        if "max_cfu" in spec:
            capacity["max_cfu"] = spec["max_cfu"]

        message = f"Pool {pool_id} status: {status}"
        if not available:
            if status == "PROVISIONING":
                message += " (still provisioning, wait a few minutes)"
            elif status == "FAILED":
                message += " (failed, check Confluent Cloud UI for details)"

        return {
            "available": available,
            "status": status,
            "capacity": capacity,
            "message": message
        }

    except requests.exceptions.RequestException as e:
        return {
            "available": False,
            "status": "ERROR",
            "capacity": {},
            "message": f"Error checking pool: {str(e)}"
        }


def main():
    parser = argparse.ArgumentParser(description="Check Flink compute pool availability")
    parser.add_argument("--pool-id", required=True, help="Flink compute pool ID")
    args = parser.parse_args()

    # Get credentials from environment
    cloud_api_key = os.getenv("CONFLUENT_CLOUD_API_KEY")
    cloud_api_secret = os.getenv("CONFLUENT_CLOUD_API_SECRET")

    if not cloud_api_key or not cloud_api_secret:
        print("ERROR: Missing CONFLUENT_CLOUD_API_KEY or CONFLUENT_CLOUD_API_SECRET", file=sys.stderr)
        print("Please set these in your .env file", file=sys.stderr)
        sys.exit(1)

    # Check the pool
    result = check_compute_pool(args.pool_id, cloud_api_key, cloud_api_secret)

    # Print results
    print(result["message"])

    if result["available"]:
        print("✓ Compute pool is available and ready")
        if result["capacity"]:
            print(f"  Max CFU: {result['capacity'].get('max_cfu', 'unknown')}")
        sys.exit(0)
    else:
        print("✗ Compute pool is not available")
        sys.exit(1)


if __name__ == "__main__":
    main()
