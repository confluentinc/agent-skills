#!/usr/bin/env python3
"""Validate evals.json against this repo's schema and flag weak expectations.

Accepts both the `expectations` and `assertions` keys. Either key may hold:
  - a list of strings, e.g. `["Generates producer.py", ...]`, or
  - a list of objects, e.g. `[{id, type, description, path, pattern}, ...]`
(string entries are checked as expectation text; object entries must carry at
least a `description`).

Findings reported as JSON, with severity blocking/warning/nit. Fixture sync
is checked by resolving each `files: [path]` relative to the skill root.

Usage:
    python3 check_eval_schema.py <skill-path>/evals/evals.json
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

SPECIFIC_HINTS = re.compile(
    r"(/|\.[a-zA-Z]{1,5}\b|[A-Z][a-z]+[A-Z]|\bNOT\b|[a-z]+\.[a-z]+|"
    r"--?[a-z][a-z-]+|\"[^\"]+\"|\b[A-Z]{2,}(?:_[A-Z0-9]+)+\b|`[^`]+`)"
)
ABSTRACT_PROMPT_PREFIX = re.compile(r"^\s*(build me a|write a|make a|create a)\b", re.IGNORECASE)
PLACEHOLDER_TOKENS = re.compile(r"<[A-Z_]+>|\bTODO\b|\bXXX\b")


def _finding(severity: str, where: str, message: str) -> dict:
    return {"severity": severity, "where": where, "message": message}


def _check_expectation_string(text: str, where: str) -> list[dict]:
    issues = []
    if len(text.strip()) < 8:
        issues.append(_finding("warning", where, f"expectation is too short to be verifiable: {text!r}"))
        return issues
    if not SPECIFIC_HINTS.search(text):
        issues.append(
            _finding(
                "warning",
                where,
                f"expectation lacks a concrete identifier (path, CamelCase, NOT, CLI flag, quoted string): {text!r}",
            )
        )
    return issues


def _check_assertion_object(obj: dict, where: str) -> list[dict]:
    issues = []
    required = {"description"}
    missing = required - set(obj)
    if missing:
        issues.append(_finding("blocking", where, f"assertion missing required keys: {sorted(missing)}"))
    desc = obj.get("description", "")
    if isinstance(desc, str) and desc:
        issues.extend(_check_expectation_string(desc, where + ".description"))
    return issues


def _check_check_list(items: object, where: str) -> list[dict]:
    """Validate an `expectations`/`assertions` list. Entries may be strings
    (checked as expectation text) or objects (checked for a `description`)."""
    issues = []
    if not isinstance(items, list) or not items:
        return [_finding("blocking", where, "must be a non-empty array")]
    for i, item in enumerate(items):
        if isinstance(item, str):
            issues.extend(_check_expectation_string(item, f"{where}[{i}]"))
        elif isinstance(item, dict):
            issues.extend(_check_assertion_object(item, f"{where}[{i}]"))
        else:
            issues.append(_finding("blocking", f"{where}[{i}]", "must be a string or an object"))
    return issues


def _check_file_entry(entry: object, where: str, skill_root: Path, skill_root_resolved: Path) -> list[dict]:
    """Validate one `files[i]` entry. Two forms are allowed:
      - a string path to an on-disk fixture, relative to the skill root, or
      - an inline fixture object `{"path": <str>, "content": <str>}`.
    """
    if isinstance(entry, dict):
        issues = []
        if not isinstance(entry.get("path"), str) or not entry.get("path"):
            issues.append(_finding("blocking", where, "inline fixture must have a non-empty string `path`"))
        if "content" in entry and not isinstance(entry["content"], str):
            issues.append(_finding("blocking", where, "inline fixture `content` must be a string"))
        return issues
    if not isinstance(entry, str):
        return [_finding("blocking", where, "fixture must be a path string or an inline {path, content} object")]
    if Path(entry).is_absolute() or ".." in Path(entry).parts:
        return [
            _finding(
                "blocking",
                where,
                f"fixture path must be relative to the skill root and stay inside it: {entry}",
            )
        ]
    resolved = (skill_root / entry).resolve()
    try:
        resolved.relative_to(skill_root_resolved)
    except ValueError:
        return [_finding("blocking", where, f"fixture path escapes the skill root: {entry}")]
    if not resolved.exists():
        return [_finding("blocking", where, f"fixture path does not exist: {entry}")]
    return []


def _check_prompt(prompt: str, where: str) -> list[dict]:
    issues = []
    if len(prompt.strip()) < 40:
        issues.append(_finding("warning", where, f"prompt is short; real users write more context"))
    if ABSTRACT_PROMPT_PREFIX.match(prompt) and len(prompt.strip()) < 80:
        issues.append(_finding("warning", where, "prompt starts abstractly with 'Build me a / Write a / ...' and lacks detail"))
    if PLACEHOLDER_TOKENS.search(prompt):
        issues.append(_finding("warning", where, "prompt contains placeholder tokens (<X>, TODO, XXX)"))
    return issues


def validate(evals_path: Path) -> dict:
    findings: list[dict] = []
    if not evals_path.is_file():
        return {"path": str(evals_path), "findings": [_finding("blocking", "<file>", "evals.json not found")]}

    try:
        data = json.loads(evals_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return {"path": str(evals_path), "findings": [_finding("blocking", "<file>", f"invalid JSON: {e}")]}

    if not isinstance(data, dict):
        findings.append(_finding("blocking", "<root>", "top level must be an object"))
        return {"path": str(evals_path), "findings": findings}

    if "skill_name" not in data or not isinstance(data["skill_name"], str):
        findings.append(_finding("blocking", "skill_name", "missing or non-string"))
    if "evals" not in data or not isinstance(data["evals"], list):
        findings.append(_finding("blocking", "evals", "missing or not an array"))
        return {"path": str(evals_path), "findings": findings}

    skill_root = evals_path.parent.parent  # evals/evals.json -> skill root
    saw_expectations = False
    saw_assertions = False

    skill_root_resolved = skill_root.resolve()

    for idx, eval_obj in enumerate(data["evals"]):
        where = f"evals[{idx}]"
        if not isinstance(eval_obj, dict):
            findings.append(_finding("blocking", where, "eval entry is not an object"))
            continue
        expected_types = {"id": int, "prompt": str, "expected_output": str}
        for key, expected_type in expected_types.items():
            if key not in eval_obj:
                findings.append(_finding("blocking", f"{where}.{key}", "missing required field"))
            elif not isinstance(eval_obj[key], expected_type):
                findings.append(
                    _finding(
                        "blocking",
                        f"{where}.{key}",
                        f"must be {expected_type.__name__}, got {type(eval_obj[key]).__name__}",
                    )
                )
        if "files" not in eval_obj:
            findings.append(
                _finding(
                    "blocking",
                    f"{where}.files",
                    "missing required field (use [] for evals without fixtures)",
                )
            )
        elif not isinstance(eval_obj["files"], list):
            findings.append(_finding("blocking", f"{where}.files", "files must be an array"))
        else:
            for fi, entry in enumerate(eval_obj["files"]):
                findings.extend(
                    _check_file_entry(entry, f"{where}.files[{fi}]", skill_root, skill_root_resolved)
                )
        prompt = eval_obj.get("prompt", "")
        if isinstance(prompt, str):
            findings.extend(_check_prompt(prompt, f"{where}.prompt"))

        has_exp = "expectations" in eval_obj
        has_assert = "assertions" in eval_obj
        if has_exp and has_assert:
            findings.append(
                _finding(
                    "warning",
                    where,
                    "eval has both `expectations` and `assertions` — pick one shape",
                )
            )
        if not has_exp and not has_assert:
            findings.append(_finding("blocking", where, "eval has no `expectations` or `assertions`"))
        if has_exp:
            saw_expectations = True
            findings.extend(_check_check_list(eval_obj["expectations"], f"{where}.expectations"))
        if has_assert:
            saw_assertions = True
            findings.extend(_check_check_list(eval_obj["assertions"], f"{where}.assertions"))

    if saw_expectations and saw_assertions:
        findings.append(
            _finding(
                "warning",
                "<file>",
                "this evals.json mixes the `expectations` and `assertions` keys across entries — pick one and stay consistent",
            )
        )

    return {"path": str(evals_path), "findings": findings}


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_eval_schema.py <evals.json>", file=sys.stderr)
        return 2
    report = validate(Path(sys.argv[1]).resolve())
    print(json.dumps(report, indent=2))
    return 1 if any(f["severity"] == "blocking" for f in report["findings"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
