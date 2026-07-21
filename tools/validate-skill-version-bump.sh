#!/usr/bin/env bash
#
# validate-skill-version-bump.sh — on a feature branch, verify that if any
# commit introduces a new skill or changes an existing skill's
# metadata.version, the plugin-level version in both
# .claude-plugin/plugin.json and .cursor-plugin/plugin.json was bumped too
# (see "Versioning" in CLAUDE.md).
#
# Compares the branch's commits against the merge-base with origin's default
# branch.
#
# Exit codes:
#   0 = no new/changed skill versions, or the plugin version was bumped to match
#   1 = skill(s) changed but the plugin version wasn't bumped, or a required
#       file/field is missing
#
# Usage: tools/validate-skill-version-bump.sh [base-branch]  (default: main)

set -uo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "Missing dependency: jq"
  exit 1
fi

BASE_BRANCH="${1:-main}"
CLAUDE_PLUGIN_JSON=".claude-plugin/plugin.json"
CURSOR_PLUGIN_JSON=".cursor-plugin/plugin.json"

git fetch --quiet origin "$BASE_BRANCH"
base_ref="origin/$BASE_BRANCH"

merge_base="$(git merge-base "$base_ref" HEAD)"
if [[ -z "$merge_base" ]]; then
  echo "Could not find a merge base with $base_ref"
  exit 1
fi
range="${merge_base}..HEAD"

# Extracts metadata.version from a SKILL.md's YAML frontmatter.
skill_version() {
  awk '
    /^metadata:/ { infm=1; next }
    infm && /^[^[:space:]]/ { infm=0 }
    infm && /version:/ {
      line=$0
      sub(/^[^:]*:[ \t]*/, "", line)
      gsub(/^"|"$/, "", line)
      gsub(/^'"'"'|'"'"'$/, "", line)
      print line
      exit
    }
  ' "$1" 2>/dev/null
}

# Top-level `skills/<name>/SKILL.md` files only — a plain 'skills/*/SKILL.md'
# pathspec also matches nested eval fixtures (e.g. evals/mock-skills/*/SKILL.md),
# so filter with a regex instead of relying on glob pathspec magic.
changed_skill_files="$(git diff --name-only "$range" -- skills | grep -E '^skills/[^/]+/SKILL\.md$' || true)"

skill_changes=()
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  [[ ! -f "$file" ]] && continue  # deleted skill, not this check's concern

  skill_name="$(basename "$(dirname "$file")")"
  new_version="$(skill_version "$file")"

  old_version=""
  if git show "${merge_base}:${file}" > /tmp/svb_old.$$ 2>/dev/null; then
    old_version="$(skill_version /tmp/svb_old.$$)"
    rm -f /tmp/svb_old.$$
  fi

  if [[ -z "$old_version" ]]; then
    skill_changes+=("${skill_name}: new skill (${new_version:-no version field})")
  elif [[ "$old_version" != "$new_version" ]]; then
    skill_changes+=("${skill_name}: ${old_version} -> ${new_version}")
  fi
done <<< "$changed_skill_files"

if [[ ${#skill_changes[@]} -eq 0 ]]; then
  echo "No new skills or skill version changes detected since $base_ref."
  exit 0
fi

echo "Skill changes detected since $base_ref:"
printf '  - %s\n' "${skill_changes[@]}"

plugin_version_at() {
  git show "${1}:${2}" 2>/dev/null | jq -r '.version // empty' 2>/dev/null
}

claude_new="$(jq -r '.version // empty' "$CLAUDE_PLUGIN_JSON" 2>/dev/null)"
cursor_new="$(jq -r '.version // empty' "$CURSOR_PLUGIN_JSON" 2>/dev/null)"

if [[ -z "$claude_new" ]]; then
  echo "No .version field found in $CLAUDE_PLUGIN_JSON"
  exit 1
fi
if [[ -z "$cursor_new" ]]; then
  echo "No .version field found in $CURSOR_PLUGIN_JSON"
  exit 1
fi

claude_old="$(plugin_version_at "$merge_base" "$CLAUDE_PLUGIN_JSON")"
cursor_old="$(plugin_version_at "$merge_base" "$CURSOR_PLUGIN_JSON")"

failed=0
if [[ "$claude_old" == "$claude_new" ]]; then
  echo "Error: skill(s) changed but $CLAUDE_PLUGIN_JSON version was not bumped (still $claude_new)."
  failed=1
fi
if [[ "$cursor_old" == "$cursor_new" ]]; then
  echo "Error: skill(s) changed but $CURSOR_PLUGIN_JSON version was not bumped (still $cursor_new)."
  failed=1
fi

if [[ "$failed" -eq 1 ]]; then
  exit 1
fi

echo "Plugin versions bumped ($claude_new), consistent with skill changes."
