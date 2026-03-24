#!/usr/bin/env python3
"""
Skill PR Review Dashboard

Serves a localhost HTML dashboard with two panels:
1. Evals Dashboard - displays benchmark/grading results from a skill workspace
2. Session Review - parses Claude session logs to show corrections, tokens, context, tools/skills

Usage:
    python tools/skill_review_dashboard.py \
        --session-id ea2636f8-e42f-4a23-b15b-9b86bfc79a6c \
        --workspace ~/.claude/skills/confluent-kafka-python-client-workspace/iteration-1 \
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
        if safe_name.startswith("-"):
            pass  # keep as-is, that's how Claude stores it
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
    per_turn_tokens = []

    # Tool tracking
    tools_used = {}
    skills_used = []

    # User message tracking (for corrections)
    human_messages = []
    interruptions = 0

    # Context tracking
    last_usage = None
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
                    inp = usage.get("input_tokens", 0)
                    out = usage.get("output_tokens", 0)
                    cc = usage.get("cache_creation_input_tokens", 0)
                    cr = usage.get("cache_read_input_tokens", 0)

                    total_input_tokens += inp
                    total_output_tokens += out
                    total_cache_creation += cc
                    total_cache_read += cr
                    last_usage = usage

                    per_turn_tokens.append({
                        "input": inp,
                        "output": out,
                        "cache_creation": cc,
                        "cache_read": cr,
                        "timestamp": ts,
                    })

                # Extract tool uses
                for content in msg.get("content", []):
                    if isinstance(content, dict) and content.get("type") == "tool_use":
                        name = content.get("name", "")
                        tools_used[name] = tools_used.get(name, 0) + 1
                        if name == "Skill":
                            skill_name = content.get("input", {}).get("skillName", "unknown")
                            skills_used.append(skill_name)

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
                    has_text = False
                    texts = []
                    for c in content:
                        if isinstance(c, dict) and c.get("type") == "text":
                            has_text = True
                            t = c.get("text", "")
                            texts.append(t)
                            if "[Request interrupted" in t:
                                interruptions += 1
                    if has_text and texts:
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

    # Final context size estimate (from last usage)
    final_context_tokens = 0
    if last_usage:
        final_context_tokens = (
            last_usage.get("input_tokens", 0)
            + last_usage.get("cache_creation_input_tokens", 0)
            + last_usage.get("cache_read_input_tokens", 0)
        )

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
            "tokens_burnt": total_input_tokens + total_output_tokens + total_cache_creation,
            "total_all": total_input_tokens + total_output_tokens + total_cache_creation + total_cache_read,
            "final_context_size": final_context_tokens,
        },
        "per_turn_tokens": per_turn_tokens,
        "tools": tools_used,
        "skills": skills_used,
        "corrections": {
            "count": corrections,
            "total_human_messages": len(human_messages),
            "interruptions": interruptions,
            "messages": human_messages,
        },
    }


def load_eval_data(workspace_path: str) -> dict | None:
    """Load benchmark and grading data from a skill workspace directory."""
    workspace = Path(workspace_path)
    if not workspace.exists():
        return None

    result = {"benchmark": None, "evals": None, "gradings": []}

    # Load benchmark.json
    benchmark_file = workspace / "benchmark.json"
    if benchmark_file.exists():
        with open(benchmark_file) as f:
            result["benchmark"] = json.load(f)

    # Load evals.json (check workspace and parent)
    for evals_path in [workspace / "evals" / "evals.json", workspace.parent / "evals" / "evals.json"]:
        if evals_path.exists():
            with open(evals_path) as f:
                result["evals"] = json.load(f)
            break

    # Find all grading.json files
    for grading_file in sorted(workspace.rglob("grading.json")):
        with open(grading_file) as f:
            grading = json.load(f)
        grading["_path"] = str(grading_file.relative_to(workspace))
        result["gradings"].append(grading)

    # Find timing.json files
    timings = []
    for timing_file in sorted(workspace.rglob("timing.json")):
        with open(timing_file) as f:
            timing = json.load(f)
        timing["_path"] = str(timing_file.relative_to(workspace))
        timings.append(timing)
    result["timings"] = timings

    return result


def generate_html(session_data: dict, eval_data: dict | None) -> str:
    """Generate the dashboard HTML."""
    session_json = json.dumps(session_data)
    eval_json = json.dumps(eval_data) if eval_data else "null"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Skill PR Review Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e1e4e8; }}

  .tabs {{ display: flex; background: #161b22; border-bottom: 1px solid #30363d; position: sticky; top: 0; z-index: 10; }}
  .tab {{ padding: 12px 24px; cursor: pointer; border-bottom: 2px solid transparent; color: #8b949e; font-size: 14px; font-weight: 500; }}
  .tab:hover {{ color: #e1e4e8; background: #1c2128; }}
  .tab.active {{ color: #58a6ff; border-bottom-color: #58a6ff; }}

  .panel {{ display: none; padding: 24px; max-width: 1200px; margin: 0 auto; }}
  .panel.active {{ display: block; }}

  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 16px; }}
  .card h3 {{ color: #58a6ff; margin-bottom: 12px; font-size: 16px; }}

  .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }}
  .stat-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; text-align: center; }}
  .stat-value {{ font-size: 28px; font-weight: 700; color: #58a6ff; }}
  .stat-label {{ font-size: 12px; color: #8b949e; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .stat-card.warning .stat-value {{ color: #d29922; }}
  .stat-card.danger .stat-value {{ color: #f85149; }}
  .stat-card.success .stat-value {{ color: #3fb950; }}

  .bar-chart {{ display: flex; align-items: flex-end; gap: 4px; height: 120px; padding: 8px 0; }}
  .bar {{ background: #58a6ff; border-radius: 2px 2px 0 0; min-width: 20px; flex: 1; position: relative; cursor: pointer; transition: opacity 0.2s; }}
  .bar:hover {{ opacity: 0.8; }}
  .bar .tooltip {{ display: none; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%); background: #30363d; color: #e1e4e8; padding: 4px 8px; border-radius: 4px; font-size: 11px; white-space: nowrap; z-index: 5; }}
  .bar:hover .tooltip {{ display: block; }}

  .tool-list {{ display: flex; flex-wrap: wrap; gap: 8px; }}
  .tool-badge {{ background: #1f2937; border: 1px solid #374151; border-radius: 16px; padding: 4px 12px; font-size: 13px; }}
  .tool-count {{ color: #58a6ff; font-weight: 600; margin-left: 4px; }}

  .message-list {{ max-height: 400px; overflow-y: auto; }}
  .message {{ padding: 10px 12px; border-left: 3px solid #30363d; margin-bottom: 8px; font-size: 13px; background: #0d1117; border-radius: 0 4px 4px 0; }}
  .message.correction {{ border-left-color: #f85149; background: #1a0f0f; }}
  .message.interruption {{ border-left-color: #d29922; background: #1a1700; }}
  .message .ts {{ color: #6e7681; font-size: 11px; margin-bottom: 4px; }}
  .message .badge {{ display: inline-block; font-size: 10px; padding: 1px 6px; border-radius: 8px; margin-left: 6px; font-weight: 600; }}
  .message .badge.correction {{ background: #f8514933; color: #f85149; }}
  .message .badge.interruption {{ background: #d2992233; color: #d29922; }}

  /* Eval dashboard styles */
  .benchmark-table {{ width: 100%; border-collapse: collapse; }}
  .benchmark-table th, .benchmark-table td {{ padding: 10px 14px; text-align: left; border-bottom: 1px solid #30363d; font-size: 13px; }}
  .benchmark-table th {{ color: #8b949e; font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: 0.5px; }}
  .benchmark-table tr:hover {{ background: #1c2128; }}
  .pass-rate {{ font-weight: 600; }}
  .pass-rate.high {{ color: #3fb950; }}
  .pass-rate.medium {{ color: #d29922; }}
  .pass-rate.low {{ color: #f85149; }}

  .grading-card {{ margin-bottom: 12px; }}
  .expectation {{ display: flex; align-items: center; gap: 8px; padding: 6px 0; font-size: 13px; }}
  .expectation .icon {{ width: 18px; text-align: center; }}
  .expectation .pass {{ color: #3fb950; }}
  .expectation .fail {{ color: #f85149; }}
  .expectation .evidence {{ color: #8b949e; font-style: italic; margin-left: auto; }}

  .delta-badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 600; }}
  .delta-badge.positive {{ background: #3fb95033; color: #3fb950; }}
  .delta-badge.negative {{ background: #f8514933; color: #f85149; }}
  .delta-badge.neutral {{ background: #30363d; color: #8b949e; }}

  .no-data {{ text-align: center; padding: 40px; color: #6e7681; }}

  .context-bar {{ height: 24px; background: #0d1117; border-radius: 12px; overflow: hidden; display: flex; margin: 8px 0; }}
  .context-segment {{ height: 100%; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 600; }}
  .context-segment.input {{ background: #58a6ff; }}
  .context-segment.cache-create {{ background: #bc8cff; }}
  .context-segment.cache-read {{ background: #3fb950; }}
  .context-segment.output {{ background: #f0883e; }}

  .legend {{ display: flex; gap: 16px; margin-top: 8px; flex-wrap: wrap; }}
  .legend-item {{ display: flex; align-items: center; gap: 4px; font-size: 12px; color: #8b949e; }}
  .legend-dot {{ width: 10px; height: 10px; border-radius: 2px; }}
</style>
</head>
<body>

<div class="tabs">
  <div class="tab active" onclick="switchTab('evals')">Evals Dashboard</div>
  <div class="tab" onclick="switchTab('session')">Session Review</div>
</div>

<div id="evals-panel" class="panel active"></div>
<div id="session-panel" class="panel"></div>

<script>
const sessionData = {session_json};
const evalData = {eval_json};

function switchTab(tab) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelector(`.tab:nth-child(${{tab === 'evals' ? 1 : 2}})`).classList.add('active');
  document.getElementById(`${{tab}}-panel`).classList.add('active');
}}

function formatNumber(n) {{
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return n.toString();
}}

function passRateClass(rate) {{
  if (rate >= 0.8) return 'high';
  if (rate >= 0.5) return 'medium';
  return 'low';
}}

// === Evals Dashboard ===
function renderEvals() {{
  const panel = document.getElementById('evals-panel');

  if (!evalData || (!evalData.benchmark && evalData.gradings.length === 0)) {{
    panel.innerHTML = '<div class="no-data">No eval data found. Provide --workspace pointing to an iteration directory.</div>';
    return;
  }}

  let html = '';

  // Benchmark summary
  if (evalData.benchmark) {{
    const bm = evalData.benchmark;
    html += `<h2 style="margin-bottom:16px;color:#e1e4e8">Benchmark: ${{bm.skill_name}} (Iteration ${{bm.iteration || '?'}})</h2>`;

    // Delta card
    if (bm.delta) {{
      const deltaVal = bm.delta.mean_pass_rate;
      const deltaClass = deltaVal > 0 ? 'positive' : deltaVal < 0 ? 'negative' : 'neutral';
      const sign = deltaVal > 0 ? '+' : '';
      html += `<div class="card"><h3>Overall Impact</h3>
        <span class="delta-badge ${{deltaClass}}">${{sign}}${{(deltaVal * 100).toFixed(0)}}pp pass rate</span>
        <span style="margin-left:12px;color:#8b949e;font-size:13px">${{bm.delta.interpretation}}</span></div>`;
    }}

    // Config comparison table
    html += '<div class="card"><h3>Pass Rates by Configuration</h3><table class="benchmark-table"><thead><tr><th>Config</th><th>Eval</th><th>Pass Rate</th><th>Passed</th><th>Total</th></tr></thead><tbody>';
    for (const config of bm.configs) {{
      for (let i = 0; i < config.evals.length; i++) {{
        const ev = config.evals[i];
        const rate = ev.pass_rate;
        html += `<tr>
          ${{i === 0 ? `<td rowspan="${{config.evals.length}}" style="font-weight:600">${{config.name}}<br><span style="font-size:11px;color:#8b949e">mean: ${{(config.aggregate.mean_pass_rate * 100).toFixed(0)}}% &plusmn; ${{(config.aggregate.stddev_pass_rate * 100).toFixed(0)}}%</span></td>` : ''}}
          <td>${{ev.eval_name}}</td>
          <td><span class="pass-rate ${{passRateClass(rate)}}">${{(rate * 100).toFixed(0)}}%</span></td>
          <td>${{ev.passed}}</td>
          <td>${{ev.total}}</td>
        </tr>`;
      }}
    }}
    html += '</tbody></table></div>';

    // Visual comparison bars
    html += '<div class="card"><h3>Visual Comparison</h3><div style="display:flex;gap:32px;flex-wrap:wrap">';
    for (const config of bm.configs) {{
      html += `<div style="flex:1;min-width:250px"><div style="font-weight:600;margin-bottom:8px">${{config.name}}</div><div class="bar-chart">`;
      for (const ev of config.evals) {{
        const h = Math.max(4, ev.pass_rate * 100);
        const color = ev.pass_rate >= 0.8 ? '#3fb950' : ev.pass_rate >= 0.5 ? '#d29922' : '#f85149';
        html += `<div class="bar" style="height:${{h}}%;background:${{color}}"><div class="tooltip">${{ev.eval_name}}: ${{(ev.pass_rate*100).toFixed(0)}}%</div></div>`;
      }}
      html += '</div></div>';
    }}
    html += '</div></div>';
  }}

  // Detailed gradings
  if (evalData.gradings.length > 0) {{
    html += '<div class="card"><h3>Detailed Assertion Results</h3>';
    for (const grading of evalData.gradings) {{
      const title = grading.eval_name || grading._path;
      const variant = grading.variant || '';
      html += `<div class="grading-card"><div style="font-weight:600;font-size:14px;margin:12px 0 6px">${{title}} <span style="color:#8b949e;font-weight:400">${{variant}}</span> <span class="pass-rate ${{passRateClass(grading.pass_rate || 0)}}">${{((grading.pass_rate || 0) * 100).toFixed(0)}}%</span></div>`;
      for (const exp of (grading.expectations || [])) {{
        const passed = exp.passed;
        html += `<div class="expectation">
          <span class="icon ${{passed ? 'pass' : 'fail'}}">${{passed ? '&#10003;' : '&#10007;'}}</span>
          <span>${{exp.text}}</span>
          <span class="evidence">${{exp.evidence || ''}}</span>
        </div>`;
      }}
      html += '</div>';
    }}
    html += '</div>';
  }}

  // Eval prompts
  if (evalData.evals) {{
    html += '<div class="card"><h3>Test Prompts</h3>';
    for (const ev of evalData.evals.evals) {{
      html += `<div style="margin-bottom:12px;padding:10px;background:#0d1117;border-radius:4px">
        <div style="font-weight:600;font-size:13px;color:#58a6ff">Eval ${{ev.id}}</div>
        <div style="font-size:13px;margin:4px 0">${{ev.prompt}}</div>
        <div style="font-size:12px;color:#8b949e">Expected: ${{ev.expected_output}}</div>
      </div>`;
    }}
    html += '</div>';
  }}

  panel.innerHTML = html;
}}

// === Session Review Dashboard ===
function renderSession() {{
  const panel = document.getElementById('session-panel');
  const s = sessionData;

  let html = `<h2 style="margin-bottom:16px;color:#e1e4e8">Session: ${{s.session_id}}</h2>
    <div style="color:#8b949e;font-size:13px;margin-bottom:16px">
      Model: <strong style="color:#e1e4e8">${{s.model || 'unknown'}}</strong> &middot;
      ${{s.session_start ? new Date(s.session_start).toLocaleString() : '?'}} &mdash;
      ${{s.session_end ? new Date(s.session_end).toLocaleString() : '?'}}
    </div>`;

  // Stats cards
  const corrClass = s.corrections.count > 3 ? 'danger' : s.corrections.count > 0 ? 'warning' : 'success';
  html += '<div class="stats-grid">';
  html += `<div class="stat-card ${{corrClass}}"><div class="stat-value">${{s.corrections.count}}</div><div class="stat-label">Corrections</div></div>`;
  html += `<div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.tokens_burnt)}}</div><div class="stat-label">Tokens Burnt</div><div style="font-size:11px;color:#6e7681;margin-top:2px">input + output + cache write</div></div>`;
  html += `<div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.total_cache_read)}}</div><div class="stat-label">Cache Read</div><div style="font-size:11px;color:#6e7681;margin-top:2px">reused context (low cost)</div></div>`;
  html += `<div class="stat-card"><div class="stat-value">${{formatNumber(s.tokens.final_context_size)}}</div><div class="stat-label">Final Context Size</div></div>`;
  html += `<div class="stat-card"><div class="stat-value">${{Object.keys(s.tools).length}}</div><div class="stat-label">Unique Tools</div></div>`;
  html += `<div class="stat-card"><div class="stat-value">${{s.corrections.total_human_messages}}</div><div class="stat-label">User Messages</div></div>`;
  html += `<div class="stat-card"><div class="stat-value">${{s.corrections.interruptions}}</div><div class="stat-label">Interruptions</div></div>`;
  html += '</div>';

  // Token breakdown
  html += `<div class="card"><h3>Token Breakdown</h3>`;
  const tokenTotal = s.tokens.total_all || 1;
  const segments = [
    {{ label: 'Input', value: s.tokens.total_input, color: '#58a6ff', key: 'input' }},
    {{ label: 'Cache Creation', value: s.tokens.total_cache_creation, color: '#bc8cff', key: 'cache-create' }},
    {{ label: 'Cache Read', value: s.tokens.total_cache_read, color: '#3fb950', key: 'cache-read' }},
    {{ label: 'Output', value: s.tokens.total_output, color: '#f0883e', key: 'output' }},
  ];
  html += '<div class="context-bar">';
  for (const seg of segments) {{
    const pct = (seg.value / tokenTotal * 100);
    if (pct > 0.5) {{
      html += `<div class="context-segment ${{seg.key}}" style="width:${{pct}}%;background:${{seg.color}}" title="${{seg.label}}: ${{formatNumber(seg.value)}}">${{pct > 5 ? formatNumber(seg.value) : ''}}</div>`;
    }}
  }}
  html += '</div><div class="legend">';
  for (const seg of segments) {{
    html += `<div class="legend-item"><div class="legend-dot" style="background:${{seg.color}}"></div>${{seg.label}}: ${{formatNumber(seg.value)}}</div>`;
  }}
  html += '</div></div>';

  // User messages / corrections
  html += `<div class="card"><h3>User Messages (${{s.corrections.count}} correction${{s.corrections.count !== 1 ? 's' : ''}} detected)</h3><div class="message-list">`;
  for (const msg of s.corrections.messages) {{
    let cls = '';
    let badges = '';
    if (msg.is_correction) {{
      cls = 'correction';
      badges += '<span class="badge correction">correction</span>';
    }}
    if (msg.is_interruption) {{
      cls = cls || 'interruption';
      badges += '<span class="badge interruption">interruption</span>';
    }}
    html += `<div class="message ${{cls}}">
      <div class="ts">${{msg.timestamp ? new Date(msg.timestamp).toLocaleTimeString() : ''}}${{badges}}</div>
      <div>${{msg.text}}</div>
    </div>`;
  }}
  html += '</div></div>';

  panel.innerHTML = html;
}}

renderEvals();
renderSession();
</script>
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(description="Skill PR Review Dashboard")
    parser.add_argument("--session-id", required=True, help="Claude session ID to analyze")
    parser.add_argument("--workspace", help="Path to skill eval workspace iteration directory (e.g., .../iteration-1)")
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

    # Load eval data
    eval_data = None
    if args.workspace:
        eval_data = load_eval_data(args.workspace)
        if eval_data:
            print(f"Loaded eval data from: {args.workspace}")
        else:
            print(f"Warning: No eval data found at {args.workspace}", file=sys.stderr)

    html = generate_html(session_data, eval_data)

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
