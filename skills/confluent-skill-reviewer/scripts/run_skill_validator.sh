#!/usr/bin/env bash
# Opportunistic wrapper around the external `skill-validator` binary.
# Never blocks the calling review pipeline — exits 0 even when the binary
# is missing or the validator finds issues. The caller parses the JSON
# output (or absence) to decide severity.
#
# Usage:
#   scripts/run_skill_validator.sh --probe          # exit 0 if installed, 1 otherwise
#   scripts/run_skill_validator.sh <skill-path>     # run validator, emit JSON on stdout

set -u

INSTALL_HINT='skill-validator not installed. Install with:
  brew tap agent-ecosystem/tap && brew install skill-validator
  # or
  go install github.com/agent-ecosystem/skill-validator/cmd/skill-validator@latest'

if [ "${1:-}" = "--probe" ]; then
  if command -v skill-validator >/dev/null 2>&1; then
    exit 0
  else
    exit 1
  fi
fi

if [ $# -lt 1 ]; then
  echo "usage: $0 [--probe] <skill-path>" >&2
  exit 0
fi

SKILL_PATH="$1"

if ! command -v skill-validator >/dev/null 2>&1; then
  echo "$INSTALL_HINT" >&2
  exit 0
fi

# --allow-dirs=evals is required because this repo intentionally uses evals/.
# JSON output so the agent can parse results[] directly.
skill-validator check -o json --allow-dirs=evals "$SKILL_PATH"
# Deliberately ignore the validator's exit code — Phase A maps levels itself.
exit 0
