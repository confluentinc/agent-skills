#!/usr/bin/env bash
#
# check-links.sh — sweep every Markdown file under skills/ for broken external links.
#
# Complements skill-validator's `validate links`, which only inspects SKILL.md and
# trusts HTTP status codes. This sweep additionally:
#   * covers references/*.md (where most doc links live),
#   * detects docs.confluent.io "soft 404s" — pages that return HTTP 200 but render
#     the generic "Confluent Documentation | Confluent Documentation" not-found page, and
#   * enforces that docs.confluent.io links use the LLM-friendly `.md` rendition
#     (e.g. .../overview.md, not .../overview.html or a trailing-slash directory).
#     A `#fragment` after `.md` is allowed.
#
# Exit codes: 0 = no broken links, 1 = broken link(s) found.
# Unverifiable responses (401/403/405/429/5xx, timeouts) are reported as warnings
# and do NOT fail the run, mirroring skill-validator's handling of bot-blocked sites.
#
# Usage: tools/check-links.sh [root-dir]   (default root: skills)

set -uo pipefail

ROOT="${1:-skills}"
SOFT_404_TITLE='<title>Confluent Documentation &#124; Confluent Documentation</title>'
UA='Mozilla/5.0 (compatible; agent-skills-linkcheck/1.0)'

# Links we can't (or shouldn't) resolve over the network: RFC 6570 templates,
# angle-bracket / xxxxx / your- placeholders, local endpoints, the auth-gated
# Confluent API host, and hosts behind a bot challenge that will yield a 403
# (like support.confluent.io). These are illustrative or unverifiable, not browsable docs.
should_skip() {
  case "$1" in
    *'{'*|*'}'*|*'<'*|*'>'*) return 0 ;;
    *xxxxx*|*your-*) return 0 ;;
    *localhost*|*0.0.0.0*|*:8081*) return 0 ;;
    https://api.confluent.cloud*) return 0 ;;
    https://support.confluent.io*) return 0 ;;
  esac
  return 1
}

# Collect unique http(s) links from all Markdown under ROOT.
links=()
while IFS= read -r line; do
  links+=("$line")
done < <(
  grep -rhoE 'https?://[^ )"`'"'"'>]+' "$ROOT" --include='*.md' \
    | sed -E 's/[.,;:*]+$//' \
    | sort -u
)

broken=()
warned=()

for url in "${links[@]}"; do
  should_skip "$url" && continue

  # Convention check (no network): docs.confluent.io links must point at the `.md`
  # rendition. The `.html`/trailing-slash variants resolve too, but only `.md` is
  # guaranteed stable and LLM-readable. Strip any ?query and #fragment before testing.
  if [[ "$url" == https://docs.confluent.io/* ]]; then
    path="${url%%\?*}"   # drop query string
    path="${path%%#*}"   # drop fragment
    if [[ "$path" != *.md ]]; then
      broken+=("[not-.md] $url")
      continue
    fi
  fi

  body="$(curl -sSL --max-time 20 -A "$UA" -w '\n__HTTP_STATUS__%{http_code}' "$url" 2>/dev/null)"
  code="${body##*__HTTP_STATUS__}"
  body="${body%__HTTP_STATUS__*}"

  case "$code" in
    404|410)
      broken+=("[$code] $url")
      ;;
    200|301|302|303|307|308)
      # Detect Confluent soft-404s (HTTP 200 but the generic not-found page).
      if [[ "$url" == https://docs.confluent.io/* ]] && \
         grep -qiF "$SOFT_404_TITLE" <<<"$body"; then
        broken+=("[soft-404] $url")
      fi
      ;;
    *)
      warned+=("[$code] $url")
      ;;
  esac
done

if [[ ${#warned[@]} -gt 0 ]]; then
  echo "::group::Unverifiable links (not failing the run)"
  printf '  ⚠ %s\n' "${warned[@]}"
  echo "::endgroup::"
fi

if [[ ${#broken[@]} -gt 0 ]]; then
  echo "Broken links found:"
  printf '  ✗ %s\n' "${broken[@]}"
  exit 1
fi

echo "All ${#links[@]} external links resolved (no 404s or soft-404s)."
