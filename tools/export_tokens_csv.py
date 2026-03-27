#!/usr/bin/env python3
"""
Export all token usage entries from a Claude session JSONL to CSV.

Writes every assistant message's usage block alongside its stop_reason,
so you can verify which entries have tokens and whether filtering on
stop_reason produces correct totals.

Usage:
    python tools/export_tokens_csv.py --session-id <id> [--project-path /path] [--output tokens.csv]
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path


def find_session_log(session_id: str, project_path: str | None = None) -> Path | None:
    claude_dir = Path.home() / ".claude"

    if project_path:
        safe_name = project_path.replace("/", "-")
        project_dir = claude_dir / "projects" / safe_name
        candidate = project_dir / f"{session_id}.jsonl"
        if candidate.exists():
            return candidate

    projects_dir = claude_dir / "projects"
    if projects_dir.exists():
        for d in projects_dir.iterdir():
            if d.is_dir():
                candidate = d / f"{session_id}.jsonl"
                if candidate.exists():
                    return candidate

    return None


def main():
    parser = argparse.ArgumentParser(description="Export token usage from a Claude session to CSV")
    parser.add_argument("--session-id", required=True, help="Claude session ID")
    parser.add_argument("--project-path", help="Project path (defaults to cwd)")
    parser.add_argument("--output", "-o", default="tokens.csv", help="Output CSV path (default: tokens.csv)")
    args = parser.parse_args()

    project_path = args.project_path or os.getcwd()
    session_path = find_session_log(args.session_id, project_path)
    if not session_path:
        print(f"Error: Could not find session log for {args.session_id}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading: {session_path}")

    rows = []
    with open(session_path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)

            if entry.get("type") != "assistant" or "message" not in entry:
                continue

            msg = entry["message"]
            if not isinstance(msg, dict):
                continue

            usage = msg.get("usage", {})
            if not usage:
                continue

            rows.append({
                "line": line_num,
                "timestamp": entry.get("timestamp", ""),
                "msg_id": msg.get("id", ""),
                "model": msg.get("model", ""),
                "stop_reason": msg.get("stop_reason", ""),
                "input_tokens": usage.get("input_tokens", 0),
                "output_tokens": usage.get("output_tokens", 0),
                "cache_creation_input_tokens": usage.get("cache_creation_input_tokens", 0),
                "cache_read_input_tokens": usage.get("cache_read_input_tokens", 0),
            })

    fieldnames = [
        "line", "timestamp", "msg_id", "model", "stop_reason",
        "input_tokens", "output_tokens",
        "cache_creation_input_tokens", "cache_read_input_tokens",
    ]

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Print summary
    all_input = sum(r["input_tokens"] for r in rows)
    all_output = sum(r["output_tokens"] for r in rows)
    final_only = [r for r in rows if r["stop_reason"]]
    final_input = sum(r["input_tokens"] for r in final_only)
    final_output = sum(r["output_tokens"] for r in final_only)

    print(f"Wrote {len(rows)} rows to {args.output}")
    print(f"\nAll entries:              input={all_input:,}  output={all_output:,}")
    print(f"With stop_reason only:    input={final_input:,}  output={final_output:,}")
    print(f"Entries with stop_reason: {len(final_only)} / {len(rows)}")


if __name__ == "__main__":
    main()
