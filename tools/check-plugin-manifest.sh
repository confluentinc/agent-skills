#!/usr/bin/env bash
#
# check-plugin-manifest.sh — run `claude plugin validate .` and fail the build
# on either a non-zero exit code or a "Found N warnings" message in the output,
# since the CLI exits 0 even when it reports warnings.
#
# The command's output is always printed, whether it passes or fails.
#
# Usage: tools/check-plugin-manifest.sh

set -uo pipefail

output="$(claude plugin validate . 2>&1)"
status=$?

echo "$output"

if [[ $status -ne 0 ]]; then
  echo "claude plugin validate . exited with status $status"
  exit "$status"
fi

if echo "$output" | grep -qE 'Found [0-9]+ warnings'; then
  echo "claude plugin validate . reported warnings"
  exit 1
fi
