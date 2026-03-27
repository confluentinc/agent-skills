#!/usr/bin/env python3
"""
Skill PR Review Dashboard

Serves a localhost HTML dashboard that parses Claude session logs to show
corrections, tokens, and user messages.

Requires Python 3.10+.

Usage:
    python tools/skill_review_dashboard.py \
        --session-id ea2636f8-e42f-4a23-b15b-9b86bfc79a6c \
        [--project-path /path/to/project]  # defaults to cwd
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path


def find_session_log(session_id: str, project_path: str | None = None) -> Path | None:
    """Find the session JSONL file in Claude's project directories."""
    claude_dir = Path.home() / ".claude"

    # Try project-specific directory first
    if project_path:
        # Claude stores project directories under ~/.claude/projects with the
        # original project path's "/" separators replaced by "-".
        # e.g. "/Users/alice/my-project" -> "-Users-alice-my-project"
        safe_name = project_path.replace("/", "-")
        project_dir = claude_dir / "projects" / safe_name
        candidate = project_dir / f"{session_id}.jsonl"
        if candidate.exists():
            return candidate

    # Search all project directories
    projects_dir = claude_dir / "projects"
    if projects_dir.exists():
        for d in projects_dir.iterdir():
            if d.is_dir():
                candidate = d / f"{session_id}.jsonl"
                if candidate.exists():
                    return candidate

    return None


def parse_session(session_path: Path) -> dict:
    """Parse a Claude session JSONL file and extract metrics."""
    entries = []
    with open(session_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(
                    f"Warning: Skipping malformed JSON line in {session_path}: {e}",
                    file=sys.stderr,
                )

    # Token tracking
    total_input_tokens = 0
    total_output_tokens = 0
    total_cache_creation = 0
    total_cache_read = 0

    # User message tracking (for corrections)
    human_messages = []
    interruptions = 0

    model = None
    session_start = None
    session_end = None

    for entry in entries:
        ts = entry.get("timestamp", "")

        if session_start is None and ts:
            session_start = ts
        if ts:
            session_end = ts

        entry_type = entry.get("type", "")

        if entry_type == "assistant" and "message" in entry:
            msg = entry["message"]
            if isinstance(msg, dict):
                if not model:
                    model = msg.get("model", "")

                # Only count usage from completed responses to avoid
                # double-counting streaming progress entries
                if msg.get("stop_reason") is not None:
                    usage = msg.get("usage", {})
                    if usage:
                        total_input_tokens += usage.get("input_tokens", 0)
                        total_output_tokens += usage.get("output_tokens", 0)
                        total_cache_creation += usage.get("cache_creation_input_tokens", 0)
                        total_cache_read += usage.get("cache_read_input_tokens", 0)

        elif entry_type == "user":
            msg = entry.get("message", {})
            if isinstance(msg, dict):
                content = msg.get("content", "")
                if isinstance(content, str) and content.strip():
                    text = content.strip()
                    is_interruption = "[Request interrupted" in text
                    if is_interruption:
                        interruptions += 1
                    human_messages.append({
                        "text": text[:500],
                        "timestamp": ts,
                        "is_interruption": is_interruption,
                        "is_correction": False,
                    })
                elif isinstance(content, list):
                    texts = []
                    for c in content:
                        if isinstance(c, dict) and c.get("type") == "text":
                            t = c.get("text", "")
                            texts.append(t)
                            if "[Request interrupted" in t:
                                interruptions += 1
                    if texts:
                        combined = " ".join(texts).strip()
                        if combined:
                            human_messages.append({
                                "text": combined[:500],
                                "timestamp": ts,
                                "is_interruption": "[Request interrupted" in combined,
                                "is_correction": False,
                            })

    # Heuristic: detect corrections
    # A correction is a human message that follows the first message and isn't just a tool result
    # Common patterns: "no", "actually", "instead", "don't", "wrong", "fix", "change"
    correction_patterns = re.compile(
        r'\b(no[,.]?\s|actually|instead|don\'t|wrong|fix\s|change\s|not\s+what|'
        r'that\'s\s+not|stop|wait|cancel|undo|revert|should\s+be|'
        r'make\s+it|directly\s+in)\b',
        re.IGNORECASE,
    )
    corrections = 0
    for i, msg in enumerate(human_messages):
        if i == 0:
            continue  # first message is the prompt, not a correction
        if msg["is_interruption"]:
            corrections += 1
            msg["is_correction"] = True
        elif correction_patterns.search(msg["text"]):
            corrections += 1
            msg["is_correction"] = True

    return {
        "session_id": session_path.stem,
        "model": model,
        "session_start": session_start,
        "session_end": session_end,
        "tokens": {
            "total_input": total_input_tokens,
            "total_output": total_output_tokens,
            "total_cache_creation": total_cache_creation,
            "total_cache_read": total_cache_read,
        },
        "corrections": {
            "count": corrections,
            "total_human_messages": len(human_messages),
            "interruptions": interruptions,
            "messages": human_messages,
        },
    }


def generate_html(session_data: dict) -> str:
    """Generate the dashboard HTML by injecting session data into the template."""
    template_path = Path(__file__).parent / "dashboard_template.html"
    template = template_path.read_text()
    # Escape JSON for safe embedding inside a <script> tag to prevent
    # content containing "</script>" from breaking out of the script block.
    safe_json = json.dumps(session_data).replace("</", "<\\/")
    return template.replace("__SESSION_DATA__", safe_json)


def main():
    parser = argparse.ArgumentParser(description="Skill PR Review Dashboard")
    parser.add_argument("--session-id", required=True, help="Claude session ID to analyze")
    parser.add_argument("--project-path", help="Project path (for finding session logs). Defaults to cwd.")
    parser.add_argument("--port", type=int, default=8789, help="Port for the dashboard server (default: 8789)")
    parser.add_argument("--static", help="Write static HTML file instead of starting a server")
    args = parser.parse_args()

    project_path = args.project_path or os.getcwd()

    # Find and parse session
    session_path = find_session_log(args.session_id, project_path)
    if not session_path:
        print(f"Error: Could not find session log for {args.session_id}", file=sys.stderr)
        print("Searched in ~/.claude/projects/*/", file=sys.stderr)
        sys.exit(1)

    print(f"Found session log: {session_path}")
    session_data = parse_session(session_path)

    html = generate_html(session_data)

    if args.static:
        with open(args.static, "w") as f:
            f.write(html)
        print(f"Dashboard written to: {args.static}")
        return

    # Serve via HTTP using a temp directory that auto-cleans on exit
    with tempfile.TemporaryDirectory(prefix="skill-review-") as tmpdir:
        html_path = os.path.join(tmpdir, "index.html")
        with open(html_path, "w") as f:
            f.write(html)

        class QuietHandler(SimpleHTTPRequestHandler):
            def __init__(self, *a, **kw):
                super().__init__(*a, directory=tmpdir, **kw)

            def log_message(self, format, *a):
                pass  # suppress request logs

        server = HTTPServer(("localhost", args.port), QuietHandler)
        url = f"http://localhost:{args.port}"
        print(f"Dashboard running at: {url}")
        webbrowser.open(url)

        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")
            server.shutdown()


if __name__ == "__main__":
    main()
