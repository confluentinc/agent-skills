#!/usr/bin/env python3
"""Scan skill files for embedded PII / sensitive customer data.

The confluent-skill-reviewer runs this in Phase B to catch real customer
data or secrets that have been committed into a skill's files — SKILL.md,
`references/`, eval prompts in `evals.json`, on-disk fixtures, and bundled
scripts. The design intent is that eval prompts, fixtures, and sample
records use *synthetic* data only; real customer data must never land in
the repo.

Detectors (severity):
  - blocking: US SSN, Luhn-valid payment card number, AWS access key id,
              private key header block
  - warning:  email address on a non-example domain, phone number

Emails on documentation/example domains (`example.com`, `*.example`,
`*.invalid`, `*.test`, `localhost`) and obvious placeholders (`your-...`,
`changeme`, ...) are treated as synthetic and NOT flagged.

Findings are emitted as JSON so the reviewer can map them into its report.

Usage:
    python3 scan_pii.py <path> [<path> ...] [--exclude GLOB ...]

Exit codes:
    0 — no findings
    1 — at least one finding (blocking or warning)
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path

# Only text formats a skill would legitimately contain. Everything else
# (images, archives, compiled artifacts) is skipped. Cert/key extensions are
# included so an accidentally committed private key can't dodge the scanner on
# its file type alone. NOTE: extension-less files (e.g. `id_rsa`) are still
# skipped — a stricter "scan any UTF-8-decodable file" mode would close that
# remaining gap.
TEXT_SUFFIXES = {
    ".md", ".markdown", ".json", ".yaml", ".yml", ".txt", ".csv", ".tsv",
    ".py", ".sh", ".bash", ".sql", ".java", ".js", ".ts", ".env", ".template",
    ".properties", ".conf", ".ini", ".toml",
    ".pem", ".key", ".crt", ".cert",
}

SKIP_DIR_NAMES = {".git", "__pycache__", "node_modules", ".venv", "venv"}

# Domains that mark an email as synthetic/documentation data. Only TLDs
# reserved for documentation/testing (RFC 2606 / RFC 6761) count. `.local`
# (RFC 6762 mDNS) is deliberately NOT here — it is used for real internal
# addresses, so treating it as safe would let real data through.
SAFE_EMAIL_SUFFIXES = (
    "example.com", "example.org", "example.net",
    ".example", ".invalid", ".test", ".localhost",
)
# Placeholder tokens that, when present in an email, mark it as a template.
# Kept narrow on purpose: broad English words like "example"/"sample" as bare
# substrings would skip real emails that merely contain them (the example
# domains are already covered by SAFE_EMAIL_SUFFIXES).
EMAIL_PLACEHOLDER_TOKENS = (
    "your-", "yourdomain", "your_domain", "changeme", "placeholder",
    "domain.com", "email.com", "user@host",
)

SSN_RE = re.compile(r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)")
AWS_KEY_RE = re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")
PRIVATE_KEY_RE = re.compile(r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
# 10-digit phone with separators, optional +1/1 country code and area parens.
PHONE_RE = re.compile(
    r"(?<![\w.])(?:\+?1[\s.\-])?(?:\(\d{3}\)\s?|\d{3}[\s.\-])\d{3}[\s.\-]\d{4}(?![\w])"
)
# Candidate card: 15-16 digits, possibly grouped by spaces or dashes.
CARD_CANDIDATE_RE = re.compile(r"(?<![\w.])(\d[\d \-]{13,17}\d)(?![\w.])")


def _luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = ord(ch) - 48
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _email_is_synthetic(email: str) -> bool:
    low = email.lower()
    if any(tok in low for tok in EMAIL_PLACEHOLDER_TOKENS):
        return True
    domain = low.split("@", 1)[1]
    return any(domain == s or domain.endswith(s) for s in SAFE_EMAIL_SUFFIXES)


def _finding(severity: str, file: str, line: int, kind: str, match: str, message: str) -> dict:
    return {
        "severity": severity,
        "file": file,
        "line": line,
        "kind": kind,
        "match": match,
        "message": message,
    }


def _redact(_text: str) -> str:
    """Never echo any part of a potential-PII value — the JSON is printed into
    CI logs, and even a tail (e.g. the last 4 of an SSN) is sensitive. The
    finding's ``kind``/``file``/``line`` are enough to locate the match."""
    return "[redacted]"


def scan_line(rel: str, lineno: int, line: str) -> list[dict]:
    findings: list[dict] = []

    for m in SSN_RE.finditer(line):
        findings.append(_finding(
            "blocking", rel, lineno, "us-ssn", _redact(m.group()),
            "Looks like a US Social Security Number. Replace it with an obviously fake, clearly non-real token rather than any real-looking nine-digit value.",
        ))

    for m in AWS_KEY_RE.finditer(line):
        findings.append(_finding(
            "blocking", rel, lineno, "aws-access-key", _redact(m.group()),
            "Looks like an AWS access key id. Remove it and rotate the key if it was ever real.",
        ))

    if PRIVATE_KEY_RE.search(line):
        findings.append(_finding(
            "blocking", rel, lineno, "private-key", "-----BEGIN … PRIVATE KEY-----",
            "Private key material committed to the repo. Remove it and rotate the key.",
        ))

    for m in CARD_CANDIDATE_RE.finditer(line):
        digits = re.sub(r"[ \-]", "", m.group(1))
        if len(digits) in (15, 16) and digits[0] in "3456" and _luhn_ok(digits):
            findings.append(_finding(
                "blocking", rel, lineno, "payment-card", _redact(digits),
                "Luhn-valid payment card number. Use a clearly fake, non-Luhn placeholder in fixtures/prompts.",
            ))

    for m in EMAIL_RE.finditer(line):
        # Skip URL userinfo like `abfss://container@host` or `scheme://user@host` —
        # the "@host" there is an authority component, not an email address.
        if m.start() > 0 and line[m.start() - 1] == "/":
            continue
        email = m.group()
        if not _email_is_synthetic(email):
            findings.append(_finding(
                "warning", rel, lineno, "email", _redact(email),
                f"Email on a non-example domain ({email.split('@',1)[1]}). Use example.com / a `.example` domain for synthetic data.",
            ))

    for m in PHONE_RE.finditer(line):
        findings.append(_finding(
            "warning", rel, lineno, "phone", _redact(m.group().strip()),
            "Looks like a real phone number. Use a fictional 555-01xx number or an obviously fake token.",
        ))

    return findings


def iter_files(root: Path, excludes: list[str]):
    if root.is_file():
        candidates = [root]
    else:
        candidates = [
            p for p in root.rglob("*")
            if p.is_file() and not any(part in SKIP_DIR_NAMES for part in p.parts)
        ]
    for p in candidates:
        # `.env.template` has suffix .template; `.env` has suffix .env — both covered.
        if p.suffix.lower() not in TEXT_SUFFIXES and p.name not in {".env.template"}:
            continue
        rel = str(p)
        if any(fnmatch.fnmatch(rel, g) or fnmatch.fnmatch(p.name, g) for g in excludes):
            continue
        yield p


def scan_path(root: Path, excludes: list[str]) -> list[dict]:
    findings: list[dict] = []
    for p in iter_files(root, excludes):
        try:
            text = p.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            findings.extend(scan_line(str(p), lineno, line))
    return findings


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Scan skill files for embedded PII / secrets.")
    ap.add_argument("paths", nargs="+", help="files or directories to scan")
    ap.add_argument("--exclude", action="append", default=[],
                    help="glob of paths/filenames to skip (repeatable); e.g. '*/mock-skills/leaky-data/*'")
    args = ap.parse_args(argv)

    all_findings: list[dict] = []
    for raw in args.paths:
        root = Path(raw)
        if not root.exists():
            print(json.dumps({"error": f"path not found: {raw}"}), file=sys.stderr)
            return 2
        all_findings.extend(scan_path(root, args.exclude))

    blocking = sum(1 for f in all_findings if f["severity"] == "blocking")
    warning = sum(1 for f in all_findings if f["severity"] == "warning")
    print(json.dumps({
        "summary": {"blocking": blocking, "warning": warning, "total": len(all_findings)},
        "findings": all_findings,
    }, indent=2))
    return 1 if all_findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
