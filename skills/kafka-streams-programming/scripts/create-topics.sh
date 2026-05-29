#!/usr/bin/env bash
# Create topics for a Kafka Streams application.
# Customize the variables below for your app, then run this script.
#
# Usage:
#   Local:           ./create-topics.sh
#   Confluent Cloud: ./create-topics.sh --cloud
#
# This script creates source, output, and DLQ topics.
# Changelog and repartition topics are auto-created by Kafka Streams — don't create those manually.

set -euo pipefail

# ── Customize these ──────────────────────────────────────────────────────────
APP_ID="my-streams-app"              # Must match application.id in your config
INPUT_TOPICS=("input-events")        # Source topics your app reads from
OUTPUT_TOPICS=("output-events")      # Output topics your app writes to
PARTITIONS=6                         # Match your expected parallelism
REPLICATION_FACTOR=3                 # 3 for production, 1 for local dev
DLQ_RETENTION_MS=604800000           # 7 days for DLQ topics

# Local connection
BOOTSTRAP_SERVER="localhost:9092"

# ── Parse arguments ──────────────────────────────────────────────────────────
USE_CLOUD=false
if [[ "${1:-}" == "--cloud" ]]; then
    USE_CLOUD=true
    # Load credentials from .env if available
    if [[ -f .env ]]; then
        # shellcheck disable=SC1091
        source .env
    fi
    BOOTSTRAP_SERVER="${BOOTSTRAP_SERVERS:-}"
    if [[ -z "$BOOTSTRAP_SERVER" ]]; then
        echo "Error: BOOTSTRAP_SERVERS not set. Create a .env file or export the variable."
        exit 1
    fi
fi

# ── Helper functions ─────────────────────────────────────────────────────────
detect_kafka_topics_cmd() {
    if command -v kafka-topics &>/dev/null; then
        echo "kafka-topics"
    elif command -v kafka-topics.sh &>/dev/null; then
        echo "kafka-topics.sh"
    elif docker exec broker kafka-topics --version &>/dev/null 2>&1; then
        echo "docker exec broker kafka-topics"
    else
        echo "ERROR: kafka-topics command not found." >&2
        echo "" >&2
        echo "Tried:" >&2
        echo "  1. kafka-topics        (Confluent Platform on PATH)" >&2
        echo "  2. kafka-topics.sh     (Apache Kafka on PATH)" >&2
        echo "  3. docker exec broker  (docker-compose setup)" >&2
        echo "" >&2
        echo "To fix, either:" >&2
        echo "  - Install Confluent Platform: https://docs.confluent.io/platform/current/installation/overview.md" >&2
        echo "  - Install Apache Kafka: https://kafka.apache.org/downloads" >&2
        echo "  - Start your docker-compose environment: docker-compose up -d" >&2
        exit 1
    fi
}

create_topic_local() {
    local topic="$1"
    local extra_config="${2:-}"
    local kafka_topics_cmd
    kafka_topics_cmd=$(detect_kafka_topics_cmd)
    echo "Creating topic: $topic"
    local cmd="$kafka_topics_cmd --create --topic $topic --partitions $PARTITIONS --replication-factor 1 --bootstrap-server $BOOTSTRAP_SERVER --if-not-exists"
    if [[ -n "$extra_config" ]]; then
        cmd="$cmd --config $extra_config"
    fi
    eval "$cmd"
}

create_topic_cloud() {
    local topic="$1"
    local extra_config="${2:-}"
    echo "Creating topic: $topic"
    local cmd="confluent kafka topic create $topic --partitions $PARTITIONS"
    if [[ -n "$extra_config" ]]; then
        cmd="$cmd --config $extra_config"
    fi
    local output
    if output=$(eval "$cmd" 2>&1); then
        echo "  Created."
    elif echo "$output" | grep -q "already exists"; then
        echo "  Already exists (OK)."
    else
        echo "  ERROR: $output" >&2
        exit 1
    fi
}

create_topic() {
    if $USE_CLOUD; then
        create_topic_cloud "$@"
    else
        create_topic_local "$@"
    fi
}

# ── Create topics ────────────────────────────────────────────────────────────
echo "=== Creating source topics ==="
for topic in "${INPUT_TOPICS[@]}"; do
    create_topic "$topic"
done

echo ""
echo "=== Creating output topics ==="
for topic in "${OUTPUT_TOPICS[@]}"; do
    create_topic "$topic"
done

echo ""
echo "=== Creating DLQ topics ==="
for topic in "${INPUT_TOPICS[@]}"; do
    dlq_topic="${APP_ID}-${topic}-dlq"
    create_topic "$dlq_topic" "retention.ms=$DLQ_RETENTION_MS"
done

echo ""
echo "Done. Changelog and repartition topics will be auto-created by Kafka Streams."
