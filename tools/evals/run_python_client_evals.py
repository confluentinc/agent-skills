# Requires Python 3.9+. Runs this skill's evals and writes one JSON line per case to $EVAL_OUT (project/suite/case/verdict/score/notes), per the cc-agent-evals contract.

import json
import os
from pathlib import Path

import boto3

SKILL_DIR = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"required environment variable {name} is not set")
    return value


def read_skill_file(skill_root: Path, relative_path: str) -> str:
    target = (skill_root / relative_path).resolve()
    if not target.is_relative_to(skill_root.resolve()):
        raise ValueError(f"path escapes the skill directory: {relative_path}")
    if not target.is_file():
        raise FileNotFoundError(f"not found in skill: {relative_path}")
    return target.read_text()


def inline_fixtures(skill_dir: Path, files: list[str]) -> str:
    if not files:
        return ""
    chunks = [f"# {path}\n```\n{read_skill_file(skill_dir, path)}\n```\n" for path in files]
    return "\n\nPROVIDED PROJECT FILES (context for this request):\n" + "\n".join(chunks)


def rollup_verdict(assertion_results: list[dict]) -> tuple[str, float, str]:
    total = len(assertion_results)
    passed = sum(1 for r in assertion_results if r["passed"])
    verdict = "pass" if passed == total else "fail"
    score = passed / total
    failed = [r["assertion"] for r in assertion_results if not r["passed"]]
    notes = "; ".join(failed)
    return verdict, score, notes


READ_SKILL_FILE_TOOL = {
    "toolSpec": {
        "name": "read_skill_file",
        "description": "Read a file inside the skill directory, e.g. references/producer.py",
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {"relative_path": {"type": "string"}},
                "required": ["relative_path"],
            }
        },
    }
}

MAX_AGENT_ITERATIONS = 8


def run_agent_loop(client, model_id: str, system_prompt: str, user_message: str, skill_root: Path) -> str:
    messages = [{"role": "user", "content": [{"text": user_message}]}]
    for _ in range(MAX_AGENT_ITERATIONS):
        response = client.converse(
            modelId=model_id,
            system=[{"text": system_prompt}],
            messages=messages,
            toolConfig={"tools": [READ_SKILL_FILE_TOOL]},
        )
        message = response["output"]["message"]
        if response["stopReason"] != "tool_use":
            return "".join(block["text"] for block in message["content"] if "text" in block)

        messages.append(message)
        tool_result_blocks = []
        for block in message["content"]:
            if "toolUse" not in block:
                continue
            tool_use = block["toolUse"]
            try:
                content_text = read_skill_file(skill_root, tool_use["input"]["relative_path"])
            except (ValueError, FileNotFoundError) as exc:
                content_text = f"Error: {exc}"
            tool_result_blocks.append(
                {"toolResult": {"toolUseId": tool_use["toolUseId"], "content": [{"text": content_text}]}}
            )
        if not tool_result_blocks:
            raise RuntimeError("model reported stopReason=tool_use but returned no toolUse blocks")
        messages.append({"role": "user", "content": tool_result_blocks})

    raise RuntimeError(f"agent did not finish within {MAX_AGENT_ITERATIONS} tool-use iterations")


JUDGE_SYSTEM_PROMPT = (
    "You are a strict evaluator of AI coding-assistant output. You are given a user request, "
    "the high-level expected outcome, the assistant's actual output, and a list of assertions. "
    "For EACH assertion, decide whether the output satisfies it. Judge only what the output "
    "actually contains — do not credit anything merely plausible or implied but absent. "
    "Reply with ONLY a JSON array, one object per assertion, each shaped exactly as "
    '{"assertion": <the assertion text>, "passed": <true|false>, "reason": <short reason>}.'
)


def judge_case(
    client, model_id: str, *, prompt: str, expected_output: str, output: str, assertions: list[str]
) -> list[dict]:
    user_message = (
        f"USER REQUEST:\n{prompt}\n\n"
        f"EXPECTED OUTCOME:\n{expected_output}\n\n"
        f"ASSISTANT OUTPUT:\n{output}\n\n"
        f"ASSERTIONS TO CHECK:\n{json.dumps(assertions)}"
    )
    response = client.converse(
        modelId=model_id,
        system=[{"text": JUDGE_SYSTEM_PROMPT}],
        messages=[{"role": "user", "content": [{"text": user_message}]}],
    )
    reply_text = "".join(
        block["text"] for block in response["output"]["message"]["content"] if "text" in block
    )
    try:
        start, end = reply_text.index("["), reply_text.rindex("]") + 1
        parsed = json.loads(reply_text[start:end])
        return [{"assertion": r["assertion"], "passed": bool(r["passed"]), "reason": r["reason"]} for r in parsed]
    except (ValueError, KeyError, TypeError):
        return [
            {"assertion": a, "passed": False, "reason": f"could not parse judge response: {reply_text!r}"}
            for a in assertions
        ]


PROJECT = "agent-skills"
SUITE = "developing-kafka-python-client"


def run_case(client, agent_model: str, judge_model: str, skill_dir: Path, skill_md: str, case: dict) -> dict:
    user_message = case["prompt"] + inline_fixtures(skill_dir, case.get("files") or [])
    output = run_agent_loop(client, agent_model, skill_md, user_message, skill_dir)
    assertion_results = judge_case(
        client,
        judge_model,
        prompt=case["prompt"],
        expected_output=case.get("expected_output", ""),
        output=output,
        assertions=case["assertions"],
    )
    verdict, score, notes = rollup_verdict(assertion_results)
    return {
        "project": PROJECT,
        "suite": SUITE,
        "case": case["id"],
        "verdict": verdict,
        "score": score,
        "notes": notes,
    }


def write_eval_out_line(out_path: Path, record: dict) -> None:
    with open(out_path, "a") as f:
        f.write(json.dumps(record) + "\n")


def main() -> None:
    out_path = Path(require_env("EVAL_OUT"))
    agent_model = require_env("EVAL_AGENT_MODEL")
    judge_model = require_env("EVAL_JUDGE_MODEL")

    skill_md = (SKILL_DIR / "SKILL.md").read_text()
    cases = json.loads((SKILL_DIR / "evals" / "evals.json").read_text())["evals"]
    client = boto3.client("bedrock-runtime")

    for case in cases:
        record = run_case(client, agent_model, judge_model, SKILL_DIR, skill_md, case)
        write_eval_out_line(out_path, record)
        print(f"  [{record['verdict'].upper()}] case {record['case']}: score {record['score']:.2f}")

    print(f"\nWrote {len(cases)} case results to {out_path}")


if __name__ == "__main__":
    main()
