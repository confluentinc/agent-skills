#!/usr/bin/env python3
"""Validate evals.json against this repo's schema and flag weak expectations.

Accepts both expectation shapes used in this repo:
  - kafka-streams style: `expectations: ["...", ...]` (array of strings)
  - python-client style: `assertions: [{id, type, description, path, pattern}]`

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

    for idx, eval_obj in enumerate(data["evals"]):
        where = f"evals[{idx}]"
        if not isinstance(eval_obj, dict):
            findings.append(_finding("blocking", where, "eval entry is not an object"))
            continue
        for key in ("id", "prompt", "expected_output"):
            if key not in eval_obj:
                findings.append(_finding("blocking", f"{where}.{key}", "missing required field"))
        if "files" in eval_obj:
            if not isinstance(eval_obj["files"], list):
                findings.append(_finding("blocking", f"{where}.files", "files must be an array"))
            else:
                for fi, fpath in enumerate(eval_obj["files"]):
                    if not isinstance(fpath, str):
                        continue
                    resolved = (skill_root / fpath).resolve()
                    if not resolved.exists():
                        findings.append(
                            _finding(
                                "blocking",
                                f"{where}.files[{fi}]",
                                f"fixture path does not exist: {fpath}",
                            )
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
            exps = eval_obj["expectations"]
            if not isinstance(exps, list) or not exps:
                findings.append(_finding("blocking", f"{where}.expectations", "must be a non-empty array"))
            else:
                for ei, exp in enumerate(exps):
                    if not isinstance(exp, str):
                        findings.append(_finding("blocking", f"{where}.expectations[{ei}]", "must be a string"))
                        continue
                    findings.extend(_check_expectation_string(exp, f"{where}.expectations[{ei}]"))
        if has_assert:
            saw_assertions = True
            asserts = eval_obj["assertions"]
            if not isinstance(asserts, list) or not asserts:
                findings.append(_finding("blocking", f"{where}.assertions", "must be a non-empty array"))
            else:
                for ai, a in enumerate(asserts):
                    if not isinstance(a, dict):
                        findings.append(_finding("blocking", f"{where}.assertions[{ai}]", "must be an object"))
                        continue
                    findings.extend(_check_assertion_object(a, f"{where}.assertions[{ai}]"))

    if saw_expectations and saw_assertions:
        findings.append(
            _finding(
                "warning",
                "<file>",
                "this evals.json mixes string-expectation and object-assertion shapes across entries — pick one and stay consistent",
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
