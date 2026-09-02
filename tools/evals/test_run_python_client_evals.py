import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from run_python_client_evals import (
    inline_fixtures,
    judge_case,
    read_skill_file,
    require_env,
    rollup_verdict,
    run_agent_loop,
    run_case,
    write_eval_out_line,
)


class ReadSkillFileTests(unittest.TestCase):
    def test_reads_file_inside_skill_root(self):
        root = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"
        content = read_skill_file(root, "references/common.py")
        self.assertIn("def get_kafka_config", content)

    def test_rejects_path_escaping_skill_root(self):
        root = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"
        with self.assertRaises(ValueError):
            read_skill_file(root, "../../CLAUDE.md")

    def test_missing_file_raises(self):
        root = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"
        with self.assertRaises(FileNotFoundError):
            read_skill_file(root, "references/does_not_exist.py")


class InlineFixturesTests(unittest.TestCase):
    def test_empty_file_list_returns_empty_string(self):
        skill_dir = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"
        self.assertEqual(inline_fixtures(skill_dir, []), "")

    def test_inlines_fixture_file_content_with_path_header(self):
        skill_dir = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"
        result = inline_fixtures(skill_dir, ["evals/files/existing_producer.py"])
        self.assertIn("evals/files/existing_producer.py", result)
        self.assertIn("from confluent_kafka import Producer", result)


class RollupVerdictTests(unittest.TestCase):
    def test_all_passed_gives_pass_verdict_and_score_one(self):
        results = [{"assertion": "a", "passed": True}, {"assertion": "b", "passed": True}]
        verdict, score, notes = rollup_verdict(results)
        self.assertEqual(verdict, "pass")
        self.assertEqual(score, 1.0)
        self.assertEqual(notes, "")

    def test_one_failure_gives_fail_verdict_and_lists_it_in_notes(self):
        results = [{"assertion": "a", "passed": True}, {"assertion": "b", "passed": False}]
        verdict, score, notes = rollup_verdict(results)
        self.assertEqual(verdict, "fail")
        self.assertEqual(score, 0.5)
        self.assertIn("b", notes)
        self.assertNotIn("a", notes)


class RequireEnvTests(unittest.TestCase):
    def test_returns_value_when_set(self):
        with mock.patch.dict(os.environ, {"EVAL_OUT": "/tmp/out.jsonl"}, clear=False):
            self.assertEqual(require_env("EVAL_OUT"), "/tmp/out.jsonl")

    def test_raises_clear_error_when_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit) as ctx:
                require_env("EVAL_OUT")
            self.assertIn("EVAL_OUT", str(ctx.exception))


class WriteEvalOutLineTests(unittest.TestCase):
    def test_appends_one_json_line_per_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "results.jsonl"
            write_eval_out_line(out_path, {"case": 0, "verdict": "pass"})
            write_eval_out_line(out_path, {"case": 1, "verdict": "fail"})
            lines = out_path.read_text().splitlines()
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[0]), {"case": 0, "verdict": "pass"})
            self.assertEqual(json.loads(lines[1]), {"case": 1, "verdict": "fail"})


class RunAgentLoopTests(unittest.TestCase):
    def test_returns_final_text_with_no_tool_calls(self):
        client = mock.Mock()
        client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "final answer"}]}},
            "stopReason": "end_turn",
        }
        skill_root = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"

        result = run_agent_loop(client, "some-model", "system prompt", "hello", skill_root)

        self.assertEqual(result, "final answer")
        client.converse.assert_called_once()

    def test_executes_requested_tool_call_then_returns_final_text(self):
        client = mock.Mock()
        client.converse.side_effect = [
            {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "toolUse": {
                                    "toolUseId": "t1",
                                    "name": "read_skill_file",
                                    "input": {"relative_path": "references/common.py"},
                                }
                            }
                        ],
                    }
                },
                "stopReason": "tool_use",
            },
            {
                "output": {"message": {"role": "assistant", "content": [{"text": "done"}]}},
                "stopReason": "end_turn",
            },
        ]
        skill_root = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"

        result = run_agent_loop(client, "some-model", "system prompt", "hello", skill_root)

        self.assertEqual(result, "done")
        self.assertEqual(client.converse.call_count, 2)
        second_call_messages = client.converse.call_args_list[1].kwargs["messages"]
        tool_result_message = second_call_messages[-1]
        self.assertEqual(tool_result_message["role"], "user")
        tool_result_text = tool_result_message["content"][0]["toolResult"]["content"][0]["text"]
        self.assertIn("def get_kafka_config", tool_result_text)

    def test_raises_if_agent_never_stops_calling_tools(self):
        client = mock.Mock()
        client.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "t1",
                                "name": "read_skill_file",
                                "input": {"relative_path": "references/common.py"},
                            }
                        }
                    ],
                }
            },
            "stopReason": "tool_use",
        }
        skill_root = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"

        with self.assertRaises(RuntimeError):
            run_agent_loop(client, "some-model", "system prompt", "hello", skill_root)


class JudgeCaseTests(unittest.TestCase):
    def test_parses_json_array_of_verdicts_from_judge_response(self):
        client = mock.Mock()
        judge_reply = json.dumps(
            [
                {"assertion": "producer.py is generated", "passed": True, "reason": "present"},
                {"assertion": "consumer.py is generated", "passed": False, "reason": "missing"},
            ]
        )
        client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": judge_reply}]}},
            "stopReason": "end_turn",
        }

        results = judge_case(
            client,
            "judge-model",
            prompt="build me a producer",
            expected_output="a producer project",
            output="<generated files>",
            assertions=["producer.py is generated", "consumer.py is generated"],
        )

        self.assertEqual(
            results,
            [
                {"assertion": "producer.py is generated", "passed": True, "reason": "present"},
                {"assertion": "consumer.py is generated", "passed": False, "reason": "missing"},
            ],
        )

    def test_unparseable_judge_response_marks_all_assertions_failed(self):
        client = mock.Mock()
        client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "not json at all"}]}},
            "stopReason": "end_turn",
        }

        results = judge_case(
            client,
            "judge-model",
            prompt="build me a producer",
            expected_output="a producer project",
            output="<generated files>",
            assertions=["producer.py is generated"],
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["assertion"], "producer.py is generated")
        self.assertFalse(results[0]["passed"])
        self.assertIn("judge response", results[0]["reason"])


class RunCaseTests(unittest.TestCase):
    def test_builds_full_eval_out_record_from_agent_and_judge_calls(self):
        client = mock.Mock()
        agent_response = {
            "output": {"message": {"role": "assistant", "content": [{"text": "producer.py generated"}]}},
            "stopReason": "end_turn",
        }
        judge_reply = json.dumps([{"assertion": "producer.py is generated", "passed": True, "reason": "ok"}])
        judge_response = {
            "output": {"message": {"role": "assistant", "content": [{"text": judge_reply}]}},
            "stopReason": "end_turn",
        }
        client.converse.side_effect = [agent_response, judge_response]
        skill_dir = Path(__file__).resolve().parents[2] / "skills" / "developing-kafka-python-client"
        case = {
            "id": 0,
            "prompt": "build me a producer",
            "expected_output": "a producer project",
            "files": [],
            "assertions": ["producer.py is generated"],
        }

        record = run_case(client, "agent-model", "judge-model", skill_dir, "SKILL.md contents", case)

        self.assertEqual(
            record,
            {
                "project": "agent-skills",
                "suite": "developing-kafka-python-client",
                "case": 0,
                "verdict": "pass",
                "score": 1.0,
                "notes": "",
            },
        )


if __name__ == "__main__":
    unittest.main()
