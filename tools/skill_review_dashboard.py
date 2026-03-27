#!/usr/bin/env python3
"""
Skill PR Review Dashboard

Serves a localhost HTML dashboard that parses Claude session logs to show
corrections, tokens, and user messages.

Usage:
    python tools/skill_review_dashboard.py \
        --session-id ea2636f8-e42f-4a23-b15b-9b86bfc79a6c \
        [--project-path /path/to/project]  # defaults to cwd
"""

import argparse
import json
import os
import re
import sys
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import tempfile


def find_session_log(session_id: str, project_path: str | None = None) -> Path | None:
    """Find the session JSONL file in Claude's project directories."""
    claude_dir = Path.home() / ".claude"

    # Try project-specific directory first
    if project_path:
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
    with open(session_path) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))

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
    """Generate the dashboard HTML."""
    session_json = json.dumps(session_data)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Skill PR Review Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e1e4e8; }}

  .panel {{ padding: 24px; max-width: 1200px; margin: 0 auto; }}

  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 16px; }}
  .card h3 {{ color: #58a6ff; margin-bottom: 12px; font-size: 16px; }}

  .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }}
  .stat-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; text-align: center; }}
  .stat-value {{ font-size: 28px; font-weight: 700; color: #58a6ff; }}
  .stat-label {{ font-size: 12px; color: #9CA3AF; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .stat-card.warning .stat-value {{ color: #d29922; }}
  .stat-card.danger .stat-value {{ color: #f85149; }}
  .stat-card.success .stat-value {{ color: #3fb950; }}

  .message-list {{ max-height: 400px; overflow-y: auto; }}
  .message {{ padding: 10px 12px; border-left: 3px solid #30363d; margin-bottom: 8px; font-size: 13px; background: #0d1117; border-radius: 0 4px 4px 0; }}
  .message.correction {{ border-left-color: #f85149; background: #1a0f0f; }}
  .message.interruption {{ border-left-color: #d29922; background: #1a1700; }}
  .message .ts {{ color: #9CA3AF; font-size: 11px; margin-bottom: 4px; }}
  .message .badge {{ display: inline-block; font-size: 10px; padding: 1px 6px; border-radius: 8px; margin-left: 6px; font-weight: 600; }}
  .message .badge.correction {{ background: #f8514933; color: #f85149; }}
  .message .badge.interruption {{ background: #d2992233; color: #d29922; }}

</style>
</head>
<body>

<div id="session-panel" class="panel"></div>

<script>
const sessionData = {session_json};

function formatNumber(n) {{
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return n.toString();
}}

function renderSession() {{
  const panel = document.getElementById('session-panel');
  const s = sessionData;

  let html = `<h2 style="margin-bottom:16px;color:#e1e4e8">Session: ${{s.session_id}}</h2>
    <div style="color:#9CA3AF;font-size:13px;margin-bottom:16px">
      Model: <strong style="color:#e1e4e8">${{s.model || 'unknown'}}</strong> &middot;
      ${{s.session_start ? new Date(s.session_start).toLocaleString() : '?'}} &mdash;
      ${{s.session_end ? new Date(s.session_end).toLocaleString() : '?'}}
    </div>`;

  // Stats cards
  const durationMin = (s.session_start && s.session_end)
    ? Math.round((new Date(s.session_end) - new Date(s.session_start)) / 60000)
    : '?';
  const corrClass = s.corrections.count > 3 ? 'danger' : s.corrections.count > 0 ? 'warning' : 'success';
  const corrIcon = s.corrections.count > 3 ? '!!' : s.corrections.count > 0 ? '!' : '';
  html += '<div class="stats-grid">';
  html += `<div class="stat-card"><div class="stat-value">${{durationMin}}<span style="font-size:14px;color:#9CA3AF"> min</span></div><div class="stat-label">Session Duration</div></div>`;
  html += `<div class="stat-card ${{corrClass}}"><div class="stat-value">${{corrIcon ? corrIcon + ' ' : ''}}${{s.corrections.count}}</div><div class="stat-label">Corrections</div></div>`;
  html += `<div class="stat-card"><div class="stat-value">${{s.corrections.total_human_messages}}</div><div class="stat-label">User Messages</div></div>`;
  html += `<div class="stat-card"><div class="stat-value">${{s.corrections.interruptions}}</div><div class="stat-label">Interruptions</div></div>`;
  html += '</div>';

  // Full-price tokens box
  html += `<div class="card"><h3>Full-Price Tokens</h3>
    <div class="stats-grid">
      <div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.total_input)}}</div><div class="stat-label">Input Tokens</div></div>
      <div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.total_output)}}</div><div class="stat-label">Output Tokens</div></div>
      <div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.total_input + s.tokens.total_output)}}</div><div class="stat-label">Total Full-Price</div></div>
    </div></div>`;

  // Cache tokens box
  html += `<div class="card"><h3>Cache Tokens</h3>
    <div class="stats-grid">
      <div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.total_cache_creation)}}</div><div class="stat-label">Cache Write</div></div>
      <div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.total_cache_read)}}</div><div class="stat-label">Cache Read</div></div>
      <div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.total_cache_creation + s.tokens.total_cache_read)}}</div><div class="stat-label">Total Cache</div></div>
    </div></div>`;

  // User messages / corrections
  html += `<div class="card"><h3 id="msg-heading">User Messages (${{s.corrections.count}} correction${{s.corrections.count !== 1 ? 's' : ''}} detected)</h3><div class="message-list" role="region" aria-labelledby="msg-heading" tabindex="0">`;
  for (const msg of s.corrections.messages) {{
    let cls = '';
    let badges = '';
    if (msg.is_correction) {{
      cls = 'correction';
      badges += '<span class="badge correction" role="status">&#x2716; correction</span>';
    }}
    if (msg.is_interruption) {{
      cls = cls || 'interruption';
      badges += '<span class="badge interruption" role="status">&#x26A0; interruption</span>';
    }}
    html += `<div class="message ${{cls}}">
      <div class="ts">${{msg.timestamp ? new Date(msg.timestamp).toLocaleTimeString() : ''}}${{badges}}</div>
      <div>${{msg.text}}</div>
    </div>`;
  }}
  html += '</div></div>';

  panel.innerHTML = html;
}}

renderSession();
</script>
</body>
</html>"""


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

    # Serve via HTTP
    tmpdir = tempfile.mkdtemp(prefix="skill-review-")
    html_path = os.path.join(tmpdir, "index.html")
    with open(html_path, "w") as f:
        f.write(html)

    os.chdir(tmpdir)

    class QuietHandler(SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
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
