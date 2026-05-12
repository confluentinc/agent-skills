#!/usr/bin/env python3
"""Detect trigger-keyword collisions between SKILL.md descriptions.

Accepts either a repo root (scans `<root>/skills/*/SKILL.md`) or a
"skills root" directory (scans `<root>/*/SKILL.md`) — the latter is used
to scan eval fixtures under `evals/mock-skills/`. Parses the
`description:` field from YAML frontmatter and reports pairs whose
descriptions share trigger keywords without mutual anti-triggers.

Usage:
    python3 check_trigger_overlap.py <repo-root-or-skills-root>

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
        r"^description:\s*(.*?)(?=^[A-Za-z0-9_-]+:|\Z)",
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


def _resolve_skills_root(root: Path) -> Path | None:
    """Return the directory whose immediate children are skill dirs, or None.

    Accepts either a repo root (containing `skills/`) or a skills root
    (containing `<name>/SKILL.md` directly).
    """
    if (root / "skills").is_dir():
        return root / "skills"
    # Treat as a skills root if any immediate child has a SKILL.md.
    if root.is_dir() and any(
        (child / "SKILL.md").is_file() for child in root.iterdir() if child.is_dir()
    ):
        return root
    return None


def _anti_tokens(text: str) -> set[str]:
    """Tokenise anti-trigger text the same way as positive descriptions."""
    return {t for t in re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", text.lower())}


def collect(skills_root: Path) -> dict[str, dict]:
    out = {}
    for skill_dir in sorted(skills_root.glob("*/")):
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
            "anti_tokens": _anti_tokens(anti),
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
            # Whole-token comparison — substring matches caused false positives
            # when a short token appeared inside an unrelated longer word.
            a_names_b = bool(skills[a]["anti_tokens"] & skills[b]["positive_tokens"])
            b_names_a = bool(skills[b]["anti_tokens"] & skills[a]["positive_tokens"])
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
        print("usage: check_trigger_overlap.py <repo-root-or-skills-root>", file=sys.stderr)
        return 2
    root = Path(sys.argv[1]).resolve()
    skills_root = _resolve_skills_root(root)
    if skills_root is None:
        print(
            f"{root} is neither a repo root (containing skills/) "
            f"nor a skills root (containing <name>/SKILL.md)",
            file=sys.stderr,
        )
        return 2

    skills = collect(skills_root)
    findings = find_collisions(skills)
    report = {
        "scan_root": str(skills_root),
        "skills_scanned": sorted(skills.keys()),
        "findings": findings,
    }
    print(json.dumps(report, indent=2))
    has_blocking = any(f["severity"] == "blocking" for f in findings)
    return 1 if has_blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
