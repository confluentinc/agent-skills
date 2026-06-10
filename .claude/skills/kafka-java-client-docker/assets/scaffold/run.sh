#!/usr/bin/env bash
# Build and run the full end-to-end demo in Docker.
# The whole stack exits with the consumer's exit code: 0 means the consumer
# received every produced record, non-zero means the pipeline broke.
set -euo pipefail

cd "$(dirname "$0")"

docker compose up --build --abort-on-container-exit --exit-code-from consumer
status=$?

# Always tear down so a rerun starts clean (drops topics + offsets).
docker compose down -v >/dev/null 2>&1 || true

exit $status
