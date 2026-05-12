# skill-validator integration — reference

External tool: https://github.com/agent-ecosystem/skill-validator (Go, MIT, current v1.5.6).

Consulted in Phase A when interpreting the wrapper script's output, or when proposing how to install the tool to a contributor.

## Install

```bash
brew tap agent-ecosystem/tap && brew install skill-validator
# or
go install github.com/agent-ecosystem/skill-validator/cmd/skill-validator@latest
```

No config file. All flags via CLI; LLM-scoring features pick up `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` from env.

## What the wrapper does

`scripts/run_skill_validator.sh <skill-path>`:

1. `--probe` flag → exit 0 if binary on PATH, exit 1 otherwise. SKILL.md preflight uses this.
2. No flag → run `skill-validator check -o json --allow-dirs=evals <skill-path>`, print JSON to stdout.
3. Binary missing → print an install hint on stderr, exit 0 (do not block the review). The agent should report "skill-validator not installed" as a Warning in the final report and fall back to native checks listed in `references/spec-conformance.md`.

`--allow-dirs=evals` is required because this repo uses `evals/` (a Confluent convention not in the spec). Without it, the validator emits orphan-directory warnings for every skill in the repo.

## JSON output shape

```json
{
  "skill_dir": "skills/confluent-skill-reviewer",
  "passed": false,
  "errors": 2,
  "warnings": 1,
  "results": [
    {
      "level": "error",
      "category": "frontmatter",
      "message": "description exceeds 1024 characters",
      "file": "skills/confluent-skill-reviewer/SKILL.md",
      "line": 3
    },
    {
      "level": "warning",
      "category": "tokens",
      "message": "SKILL.md exceeds 5000 tokens",
      "file": "skills/confluent-skill-reviewer/SKILL.md",
      "line": null
    }
  ],
  "token_counts": { "files": [...], "total": 2070 },
  "content_analysis": { "word_count": 1250, "code_block_ratio": 0.32 },
  "contamination_analysis": { "contamination_score": 0.35, "contamination_level": "medium" }
}
```

Map to report severity:

- `level == "error"` → **Blocking**
- `level == "warning"` → **Warning**
- Anything else → ignore (the validator also emits `pass` entries which are not findings).

## Exit codes

| Code | Meaning | What the wrapper does |
|---|---|---|
| 0 | Clean pass | Wrapper exits 0, no findings to map |
| 1 | Validation errors | Wrapper exits 0 still — caller parses JSON to decide |
| 2 | Warnings only | Wrapper exits 0, caller parses JSON |
| 3 | CLI/usage error | Wrapper exits 0 but logs the stderr; agent should note "validator crashed" as Warning |

The wrapper deliberately never propagates non-zero exit codes — the review pipeline owns the decision about what blocks. Treating warnings as blocking is the agent's call based on context.

## Multi-skill mode

If the path is a directory of skills (no `SKILL.md` at the root, e.g. running against `skills/` itself), the validator auto-detects and returns `{"skills": [...]}` instead of a single result. The wrapper passes this through unchanged; the agent iterates.

## Flags worth knowing

| Flag | When to use |
|---|---|
| `--strict` | Treat warnings as errors. Don't use by default — Phase A separates Blocking vs Warning itself. |
| `--per-file` | Per-reference-file token breakdown. Useful when a reference is suspected of bloat. |
| `--skip-orphans` | Skip orphan-file warnings. The wrapper uses `--allow-dirs=evals` instead, which is more precise. |
| `--full-content` | For `score evaluate` — sends entire file to the LLM scorer instead of truncating. |
| `-o markdown` | Pretty output. Don't use in the wrapper (the agent needs JSON to parse). |

## When the validator is wrong

The validator is conservative on contamination (multi-language code blocks). For skills that legitimately mix languages — e.g. a Python build script alongside a Java template — contamination warnings can be safely downgraded to **Nit** in the report. Flag it for human judgment rather than treating it as Blocking.

Similarly, `score evaluate` returns LLM-judged scores 1–5 on clarity / actionability / etc. Those are signals, not gates. This skill does not invoke `score evaluate`.
