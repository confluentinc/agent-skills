#!/usr/bin/env python3
"""Detect trigger-keyword collisions between SKILL.md descriptions.

Walks every `skills/*/SKILL.md` under the given repo root, parses the
`description:` field from YAML frontmatter, and reports pairs whose
descriptions share trigger keywords without mutual anti-triggers.

Usage:
    python3 check_trigger_overlap.py <repo-root>

Exit codes:
    0 — no collisions (or only nit-level single-keyword overlap on common terms)
    1 — at least one blocking collision found
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# Domain-common words that are not collision-worthy on their own.
STOPWORDS = {
    "a", "an", "and", "any", "are", "as", "at", "be", "build", "by", "code",
    "common", "data", "do", "for", "from", "have", "how", "i", "if", "in",
    "into", "is", "it", "its", "may", "must", "new", "no", "not", "of", "on",
    "or", "other", "out", "over", "should", "skill", "so", "such", "than",
    "that", "the", "their", "them", "then", "there", "these", "they", "this",
    "to", "trigger", "up", "use", "used", "user", "uses", "using", "want",
    "was", "we", "what", "when", "where", "which", "while", "who", "why",
    "will", "with", "without", "you", "your",
    # frequent verbs/adverbs in skill descriptions
    "asks", "wants", "needs", "mentions", "trigger", "triggers", "triggered",
    "verify", "verifies", "check", "checks", "make", "makes",
    # generic locator nouns
    "repo", "repos", "repository", "folder", "directory", "branch", "branches",
    "pr", "prs", "merge",
}
# These terms appear in many Confluent skills — too broad to flag on alone.
# Any skill in this repo touches Kafka and runs against some Confluent surface;
# overlap on these is coincidence, not a trigger collision.
DOMAIN_BROAD = {
    "confluent", "kafka", "cluster", "topic", "topics", "schema", "schemas",
    "cloud", "platform", "apache", "java", "jvm", "python", "json", "avro",
    "protobuf", "broker", "brokers", "message", "messages", "stream", "streams",
    "producer", "producers", "consumer", "consumers", "application", "applications",
    "deployment", "environment", "code", "file", "files", "project", "projects",
    "change", "changes", "system", "service", "services", "client", "clients",
    "config", "configuration", "data",
}


def _read_description(skill_md: Path) -> str | None:
    try:
        text = skill_md.read_text(encoding="utf-8")
    except OSError:
        return None
    m = re.search(
        r"^---\s*\n(.*?)\n---", text, flags=re.DOTALL | re.MULTILINE
    )
    if not m:
        return None
    fm = m.group(1)
    desc_match = re.search(
        r"^description:\s*(.*?)(?=^\w+:|\Z)",
        fm,
        flags=re.DOTALL | re.MULTILINE,
    )
    if not desc_match:
        return None
    return desc_match.group(1).strip()


def _split_anti(description: str) -> tuple[str, str]:
    """Return (positive part, anti-trigger part). Anti is '' if absent."""
    m = re.search(
        r"\bdo\s+not\s+trigger\b(.*)$", description, flags=re.IGNORECASE | re.DOTALL
    )
    if not m:
        return description, ""
    anti = m.group(0)
    positive = description[: m.start()]
    return positive, anti


def _tokens(text: str) -> set[str]:
    raw = re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", text.lower())
    return {t for t in raw if t not in STOPWORDS and t not in DOMAIN_BROAD}


def collect(repo_root: Path) -> dict[str, dict]:
    out = {}
    for skill_dir in sorted((repo_root / "skills").glob("*/")):
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.is_file():
            continue
        desc = _read_description(skill_md)
        if not desc:
            continue
        positive, anti = _split_anti(desc)
        out[skill_dir.name] = {
            "path": str(skill_md),
            "description": desc,
            "positive_tokens": _tokens(positive),
            "anti_text": anti.lower(),
        }
    return out


def find_collisions(skills: dict[str, dict]) -> list[dict]:
    findings = []
    names = sorted(skills)
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            overlap = skills[a]["positive_tokens"] & skills[b]["positive_tokens"]
            if not overlap:
                continue
            # Does each side anti-trigger the *other's* domain?
            a_names_b = any(tok in skills[a]["anti_text"] for tok in skills[b]["positive_tokens"])
            b_names_a = any(tok in skills[b]["anti_text"] for tok in skills[a]["positive_tokens"])
            mutual = a_names_b and b_names_a
            if len(overlap) >= 3 and not mutual:
                severity = "blocking"
            elif len(overlap) == 2 and not mutual:
                severity = "warning"
            elif len(overlap) == 1:
                severity = "nit"
            else:
                continue
            findings.append(
                {
                    "skills": [a, b],
                    "overlap_keywords": sorted(overlap),
                    "severity": severity,
                    "a_anti_names_b": a_names_b,
                    "b_anti_names_a": b_names_a,
                }
            )
    return findings


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_trigger_overlap.py <repo-root>", file=sys.stderr)
        return 2
    repo_root = Path(sys.argv[1]).resolve()
    if not (repo_root / "skills").is_dir():
        print(f"no skills/ directory under {repo_root}", file=sys.stderr)
        return 2

    skills = collect(repo_root)
    findings = find_collisions(skills)
    report = {
        "repo_root": str(repo_root),
        "skills_scanned": sorted(skills.keys()),
        "findings": findings,
    }
    print(json.dumps(report, indent=2))
    has_blocking = any(f["severity"] == "blocking" for f in findings)
    return 1 if has_blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
