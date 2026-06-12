#!/usr/bin/env python3
"""Run skill evals with pydantic-evals and an Anthropic (Claude) judge.

For each ``skills/<name>/evals/evals.json`` this script:

1. Loads the skill's ``SKILL.md`` as the system prompt for a pydantic-ai
   ``Agent`` and exposes a ``read_skill_file`` tool so the agent can
   lazy-load ``references/*.md`` the same way a real agent would.
2. Runs every eval ``prompt`` through that agent (this is the
   pydantic-evals "task").
3. Scores each ``assertion`` with an LLM judge (a second Claude agent),
   surfaced through a custom pydantic-evals ``Evaluator``.
4. Enforces a per-skill pass-rate threshold (default 90%, matching the
   evals-as-contract rule in CLAUDE.md). Exits non-zero if any skill is
   below threshold so CI fails the run.

This is an LLM-judge harness: it does not execute generated code. For
assertions about runtime behavior (e.g. "all unit tests pass with
pytest") the judge assesses correctness from the generated code itself.

Usage:
    export ANTHROPIC_API_KEY=sk-ant-...
    python tools/evals/run_evals.py                 # all skills
    python tools/evals/run_evals.py --skill kafka-streams-programming
    python tools/evals/run_evals.py --threshold 0.9 --max-concurrency 4

Models are configurable via env vars:
    EVAL_AGENT_MODEL  (default: anthropic:claude-sonnet-4-6)
    EVAL_JUDGE_MODEL  (default: anthropic:claude-sonnet-4-6)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_evals import Case, Dataset
from pydantic_evals.evaluators import EvaluationReason, Evaluator, EvaluatorContext

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILLS_DIR = REPO_ROOT / "skills"

DEFAULT_AGENT_MODEL = os.environ.get("EVAL_AGENT_MODEL", "anthropic:claude-sonnet-4-6")
DEFAULT_JUDGE_MODEL = os.environ.get("EVAL_JUDGE_MODEL", "anthropic:claude-sonnet-4-6")
DEFAULT_THRESHOLD = 0.90

SKILL_SYSTEM_PROMPT = """\
You are an AI agent equipped with the skill below. Use it to fulfill the \
user's request exactly as the skill instructs.

When the skill tells you to consult a reference file (e.g. \
references/<topic>.md) or any other file inside the skill, call the \
`read_skill_file` tool to read it on demand. Do not assume the contents.

When the request asks you to scaffold or generate a project, respond with \
the COMPLETE contents of every file you would create, each introduced by \
its path and shown in a fenced code block, so your work can be reviewed. \
Do not abbreviate file contents with placeholders.

--- SKILL ---
{skill_md}
"""

JUDGE_SYSTEM_PROMPT = """\
You are a strict evaluator of AI "skill" outputs. You are given a user \
request, the high-level expected outcome, the skill's actual output, and a \
SINGLE assertion. Decide whether the output satisfies that one assertion.

Rules:
- Judge only what the output actually contains. Do not give credit for \
things that are merely plausible or implied but absent.
- You cannot execute code. For assertions about runtime behavior (e.g. \
"all unit tests pass with pytest"), assess whether the generated code and \
tests are correct and would plausibly pass, and say so in your reason.
- Return passed=true only when the assertion is clearly met.
"""


class Judgement(BaseModel):
    """Structured verdict the judge agent returns for one assertion."""

    passed: bool
    reason: str


@dataclass
class ResultCollector:
    """Authoritative record of every assertion verdict, used for thresholds.

    pydantic-evals runs cases inside a single asyncio event loop, so plain
    list appends here are safe (no true parallelism).
    """

    rows: list[tuple[str, str, bool]] = field(default_factory=list)

    def record(self, skill: str, assertion: str, passed: bool) -> None:
        self.rows.append((skill, assertion, passed))

    def skill_rows(self, skill: str) -> list[tuple[str, str, bool]]:
        return [r for r in self.rows if r[0] == skill]

    def pass_rate(self, skill: str) -> float:
        rows = self.skill_rows(skill)
        if not rows:
            return 0.0
        return sum(1 for r in rows if r[2]) / len(rows)


def build_skill_agent(skill_dir: Path, model: str) -> Agent[None, str]:
    """Build a pydantic-ai agent that runs the skill and can read its files."""
    skill_md = (skill_dir / "SKILL.md").read_text()
    skill_root = skill_dir.resolve()
    agent: Agent[None, str] = Agent(
        model,
        system_prompt=SKILL_SYSTEM_PROMPT.format(skill_md=skill_md),
    )

    @agent.tool_plain
    def read_skill_file(relative_path: str) -> str:
        """Read a file inside the skill directory (e.g. 'references/foo.md')."""
        target = (skill_root / relative_path).resolve()
        if not target.is_relative_to(skill_root):
            return "Error: path escapes the skill directory."
        if not target.is_file():
            return f"Error: '{relative_path}' not found in this skill."
        return target.read_text()

    return agent


@dataclass
class AssertionJudge(Evaluator[str, str, dict]):
    """Custom evaluator: judge each per-case assertion with an LLM.

    Returns a mapping of assertion -> EvaluationReason(bool, why), which
    pydantic-evals surfaces as named assertions in the report. Verdicts are
    also pushed into the shared collector for threshold enforcement.
    """

    judge_model: str
    collector: ResultCollector

    def __post_init__(self) -> None:
        self._judge: Agent[None, Judgement] = Agent(
            self.judge_model,
            output_type=Judgement,
            system_prompt=JUDGE_SYSTEM_PROMPT,
        )

    async def evaluate(
        self, ctx: EvaluatorContext[str, str, dict]
    ) -> Mapping[str, EvaluationReason]:
        meta = ctx.metadata or {}
        skill = meta.get("skill", "unknown")
        assertions: list[str] = meta.get("assertions", [])
        results: dict[str, EvaluationReason] = {}
        for i, assertion in enumerate(assertions):
            prompt = (
                f"USER REQUEST:\n{ctx.inputs}\n\n"
                f"EXPECTED OUTCOME:\n{ctx.expected_output}\n\n"
                f"SKILL OUTPUT:\n{ctx.output}\n\n"
                f"ASSERTION TO CHECK:\n{assertion}"
            )
            verdict = (await self._judge.run(prompt)).output
            self.collector.record(skill, assertion, verdict.passed)
            key = f"{i:02d}:{assertion[:70]}"
            results[key] = EvaluationReason(value=verdict.passed, reason=verdict.reason)
        return results


def load_eval_file(skill_dir: Path) -> dict | None:
    eval_path = skill_dir / "evals" / "evals.json"
    if not eval_path.is_file():
        return None
    return json.loads(eval_path.read_text())


def read_fixture_context(skill_dir: Path, files: list[str]) -> str:
    """Inline fixture files referenced by an eval so the judge has context."""
    chunks: list[str] = []
    for rel in files:
        target = (skill_dir / rel).resolve()
        if not target.is_relative_to(skill_dir.resolve()) or not target.is_file():
            chunks.append(f"# {rel}\n(could not read fixture)\n")
            continue
        chunks.append(f"# {rel}\n```\n{target.read_text()}\n```\n")
    if not chunks:
        return ""
    return "\n\nPROVIDED PROJECT FILES (context for this request):\n" + "\n".join(chunks)


def build_dataset(
    skill: str, skill_dir: Path, evals: list[dict], collector: ResultCollector, judge_model: str
) -> Dataset[str, str, dict]:
    cases: list[Case[str, str, dict]] = []
    for e in evals:
        assertions = e.get("assertions") or e.get("expectations") or []
        prompt = e["prompt"] + read_fixture_context(skill_dir, e.get("files") or [])
        cases.append(
            Case(
                name=f"{skill}-{e.get('id', len(cases))}",
                inputs=prompt,
                expected_output=e.get("expected_output", ""),
                metadata={"assertions": assertions, "skill": skill},
            )
        )
    return Dataset(
        name=skill,
        cases=cases,
        evaluators=[AssertionJudge(judge_model=judge_model, collector=collector)],
    )


async def run_skill(
    skill_dir: Path,
    collector: ResultCollector,
    agent_model: str,
    judge_model: str,
    max_concurrency: int,
) -> None:
    skill = skill_dir.name
    data = load_eval_file(skill_dir)
    if not data or not data.get("evals"):
        print(f"  (no evals.json — skipped)")
        return

    agent = build_skill_agent(skill_dir, agent_model)

    async def task(prompt: str) -> str:
        return (await agent.run(prompt)).output

    dataset = build_dataset(skill, skill_dir, data["evals"], collector, judge_model)
    report = await dataset.evaluate(task, name=skill, max_concurrency=max_concurrency)
    report.print(include_input=False, include_output=False, include_durations=False)


async def main_async(args: argparse.Namespace) -> int:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY is not set.", file=sys.stderr)
        return 2

    skill_dirs = sorted(d for d in SKILLS_DIR.iterdir() if d.is_dir())
    if args.skill:
        skill_dirs = [d for d in skill_dirs if d.name in set(args.skill)]
        if not skill_dirs:
            print(f"ERROR: no matching skill(s): {args.skill}", file=sys.stderr)
            return 2

    collector = ResultCollector()
    failures: list[str] = []

    for skill_dir in skill_dirs:
        print(f"\n=== {skill_dir.name} ===")
        try:
            await run_skill(
                skill_dir, collector, args.agent_model, args.judge_model, args.max_concurrency
            )
        except Exception as exc:  # noqa: BLE001 — report and keep going
            print(f"  ERROR running evals: {exc}", file=sys.stderr)
            failures.append(skill_dir.name)

    print("\n" + "=" * 60)
    print(f"SUMMARY (threshold {args.threshold:.0%})")
    print("=" * 60)
    any_run = False
    for skill_dir in skill_dirs:
        skill = skill_dir.name
        rows = collector.skill_rows(skill)
        if not rows:
            continue
        any_run = True
        rate = collector.pass_rate(skill)
        passed = sum(1 for r in rows if r[2])
        status = "PASS" if rate >= args.threshold else "FAIL"
        if rate < args.threshold:
            failures.append(skill)
        print(f"  [{status}] {skill}: {rate:.1%} ({passed}/{len(rows)} assertions)")

    if not any_run and not failures:
        print("  No evals were run.")
        return 2

    if failures:
        uniq = sorted(set(failures))
        print(f"\nFAILED: {', '.join(uniq)}")
        return 1

    print("\nAll skills meet the threshold.")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run skill evals with pydantic-evals.")
    p.add_argument("--skill", action="append", help="Skill name(s) to run (repeatable). Default: all.")
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="Per-skill pass rate (0-1).")
    p.add_argument("--agent-model", default=DEFAULT_AGENT_MODEL, help="Model that runs the skill.")
    p.add_argument("--judge-model", default=DEFAULT_JUDGE_MODEL, help="Model that judges assertions.")
    p.add_argument("--max-concurrency", type=int, default=4, help="Concurrent eval cases per skill.")
    return p.parse_args()


def main() -> None:
    sys.exit(asyncio.run(main_async(parse_args())))


if __name__ == "__main__":
    main()
