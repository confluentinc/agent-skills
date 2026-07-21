#!/usr/bin/env bash
#
# check-plugin-versions.sh — verify the plugin-level version is in sync across
# .claude-plugin/plugin.json and .cursor-plugin/plugin.json.
#
# The two manifests are maintained by hand (see "Versioning" in CLAUDE.md) and
# must be bumped together whenever a skill receives a MINOR or MAJOR version
# bump. This catches the case where only one file was updated.
#
# Exit codes: 0 = versions match, 1 = versions differ or a file/field is missing.
#
# Usage: tools/check-plugin-versions.sh

set -uo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "Missing dependency: jq"
  exit 1
fi

CLAUDE_PLUGIN_JSON=".claude-plugin/plugin.json"
CURSOR_PLUGIN_JSON=".cursor-plugin/plugin.json"

for f in "$CLAUDE_PLUGIN_JSON" "$CURSOR_PLUGIN_JSON"; do
  if [[ ! -f "$f" ]]; then
    echo "Missing file: $f"
    exit 1
  fi
done

claude_version="$(jq -r '.version // empty' "$CLAUDE_PLUGIN_JSON")"
cursor_version="$(jq -r '.version // empty' "$CURSOR_PLUGIN_JSON")"

if [[ -z "$claude_version" ]]; then
  echo "No .version field found in $CLAUDE_PLUGIN_JSON"
  exit 1
fi

if [[ -z "$cursor_version" ]]; then
  echo "No .version field found in $CURSOR_PLUGIN_JSON"
  exit 1
fi

if [[ "$claude_version" != "$cursor_version" ]]; then
  echo "Plugin version mismatch: $CLAUDE_PLUGIN_JSON=$claude_version, $CURSOR_PLUGIN_JSON=$cursor_version"
  exit 1
fi

echo "Plugin versions match ($claude_version)."
