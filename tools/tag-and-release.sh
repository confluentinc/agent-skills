#!/usr/bin/env bash
#
# tag-and-release.sh — on a plugin version bump, tag the repo v<version>,
# push the tag, and cut a GitHub Release grouped by skill.
#
# Runs on main only (see .semaphore/semaphore.yml, "Tag Plugin Release"
# block), on every push to main. It's a no-op unless v<version> is a tag
# that doesn't exist yet on origin — i.e. this push was the version bump.
#
# Requires: jq, git, gh (authenticated with repo push + release scope).
#
# Usage: tools/tag-and-release.sh

set -euo pipefail

PLUGIN_JSON=".claude-plugin/plugin.json"

version="$(jq -r '.version // empty' "$PLUGIN_JSON")"
if [[ -z "$version" ]]; then
  echo "No .version field found in $PLUGIN_JSON"
  exit 1
fi
tag="v${version}"

if git ls-remote --exit-code --tags origin "$tag" >/dev/null 2>&1; then
  echo "Tag $tag already exists on origin, nothing to do."
  exit 0
fi

git fetch --tags --quiet origin

previous_tag="$(git tag --list 'v*' --sort=-v:refname | head -n1)"
if [[ -n "$previous_tag" ]]; then
  range="${previous_tag}..HEAD"
else
  range="HEAD"
fi

# Extracts metadata.version from a SKILL.md's YAML frontmatter (reads stdin
# when given /dev/stdin, so it works on both worktree files and `git show` output).
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
if [[ -n "$previous_tag" ]]; then
  changed_skill_files="$(git diff --name-only "$range" -- skills | grep -E '^skills/[^/]+/SKILL\.md$' || true)"
else
  changed_skill_files="$(find skills -mindepth 2 -maxdepth 2 -name SKILL.md | sort -u)"
fi

skill_lines=()
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  skill_name="$(basename "$(dirname "$file")")"
  new_version="$(skill_version "$file")"
  [[ -z "$new_version" ]] && continue

  old_version=""
  if [[ -n "$previous_tag" ]] && git show "${previous_tag}:${file}" > /tmp/skill_version_old.$$ 2>/dev/null; then
    old_version="$(skill_version /tmp/skill_version_old.$$)"
    rm -f /tmp/skill_version_old.$$
  fi

  if [[ -z "$old_version" ]]; then
    skill_lines+=("- **${skill_name}**: ${new_version} (new)")
  elif [[ "$old_version" != "$new_version" ]]; then
    skill_lines+=("- **${skill_name}**: ${old_version} → ${new_version}")
  fi
done <<< "$changed_skill_files"

commit_lines=()
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  commit_lines+=("- ${line}")
done < <(git log "$range" --no-merges --pretty='format:%h %s')

notes_file="$(mktemp)"
trap 'rm -f "$notes_file"' EXIT

{
  if [[ ${#skill_lines[@]} -gt 0 ]]; then
    echo "## Skill versions"
    printf '%s\n' "${skill_lines[@]}"
    echo
  fi
  echo "## Commits"
  if [[ ${#commit_lines[@]} -gt 0 ]]; then
    printf '%s\n' "${commit_lines[@]}"
  else
    echo "- No commits since ${previous_tag:-repo start}."
  fi
} > "$notes_file"

# Tag and push first — the release is cut only after the tag lands on origin.
git tag "$tag"
git push origin "$tag"

gh release create "$tag" --title "$tag" --notes-file "$notes_file"
