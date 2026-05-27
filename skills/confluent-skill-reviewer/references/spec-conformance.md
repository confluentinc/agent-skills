# Agent Skills spec — conformance reference

Authoritative source: https://agentskills.io/specification

This reference is consulted when a Phase A finding fires and the agent needs to interpret a `skill-validator` error or perform native spec checks because the validator is not installed.

## Required structure

```
skill-name/
├── SKILL.md          # required
├── scripts/          # optional, executable code
├── references/       # optional, lazy-loaded docs
└── assets/           # optional, templates / static resources
```

- The directory name MUST equal the `name:` frontmatter field exactly.
- The only recognized top-level directories are `scripts/`, `references/`, `assets/`. Anything else (e.g. `evals/`, `docs/`) is non-spec and the validator will flag it as orphan unless explicitly allowed via `--allow-dirs`. **This repo intentionally uses `evals/` — that is a Confluent convention, not a spec violation. Pass `--allow-dirs=evals` to the validator.**
- Files should not be nested more than one level below `SKILL.md`. Deeply nested reference chains are flagged.

## Frontmatter rules

YAML frontmatter at the top of `SKILL.md`, followed by Markdown body.

| Field | Required | Constraints |
|---|---|---|
| `name` | yes | 1–64 chars. Lowercase `a-z`, digits, hyphens. No leading/trailing/consecutive hyphens. Must match parent directory name. |
| `description` | yes | 1–1024 chars. Non-empty. Should include both *what* and *when*. |
| `license` | no | Free text, short. |
| `compatibility` | no | ≤500 chars. Only include if the skill has specific environment requirements. |
| `metadata` | no | Map of string→string. Use unique key names. |
| `allowed-tools` | no | Space-separated tool list (experimental). |

### `name` — common failures

- `Kafka-Streams` → uppercase invalid.
- `-kafka-streams` → leading hyphen invalid.
- `kafka--streams` → consecutive hyphens invalid.
- `kafka_streams` → underscore invalid.

### `description` — common failures

- Exceeds 1024 characters (very long descriptions with many trigger phrases).
- Missing — empty or absent field is a hard fail.
- Too vague to trigger ("Helps with Kafka.") — not a spec failure, but flagged by `skill-validator score evaluate` as low clarity, and by this repo's CLAUDE.md as a Confluent convention violation.
- **Unquoted colon followed by space** inside the value (e.g. `Do NOT trigger for: general code review`). YAML interprets `: ` as a key/value separator inside a plain scalar and parsing fails with `mapping values are not allowed in this context`. The error line number often points at line 2 even when the offending colon is on line 3 — do not trust the line number. Fixes, in order of preference: (1) rephrase to drop the colon (`Do NOT trigger for general code review…`); (2) replace `: ` with ` — ` or `;`; (3) wrap the entire value in double quotes and escape any embedded `"`. Other punctuation that is safe to leave unquoted: `;`, `,`, `()`, backticks, `/`.

## Body content rules

No format restrictions, but:

- **SKILL.md body ≤ ~5,000 tokens** (~500 lines). Validator emits a warning above this.
- Use relative paths for file references: `references/REFERENCE.md`, `scripts/extract.py`.
- Move long detail to references — the body is loaded every time the skill activates; references load only when read.

## Progressive disclosure (why this matters)

The agent only loads:
1. `name` + `description` for every skill (~100 tokens each, always in context).
2. Full SKILL.md body when the skill activates.
3. References, scripts, assets only when the SKILL.md tells the agent to read or execute them.

A 4,000-line SKILL.md that inlines every reference defeats the design — every activation pays the full context cost. That is why CLAUDE.md in this repo treats inlined references as a **Blocking** finding.

## Validator findings — quick map

`skill-validator check -o json <skill-path>` returns `results[]` with `level`, `category`, `message`, `file`, `line`.

| validator category | typical message | severity |
|---|---|---|
| `frontmatter` | "name must be lowercase" / "description exceeds 1024 chars" | Blocking |
| `structure` | "extraneous file: README.md" / "orphan directory: evals" | Blocking unless allowlisted |
| `tokens` | "SKILL.md exceeds 5000 tokens" | Warning |
| `code-fence` | "unclosed code fence on line N" | Blocking |
| `internal-link` | "broken relative link to references/foo.md" | Blocking |
| `contamination` | "Python and JS code blocks in same skill" | Warning |

For full validator semantics: https://github.com/agent-ecosystem/skill-validator.

## Native checks if validator is absent

Walk this list, in order — these are the same checks the validator does, abbreviated:

1. `SKILL.md` exists in the skill directory.
2. Parse YAML frontmatter. If parse fails or `name`/`description` missing → Blocking.
3. `name` matches `^[a-z0-9](?:[a-z0-9]|-(?!-))*[a-z0-9]$` and length ≤ 64. Else Blocking.
4. `name` equals parent directory basename. Else Blocking.
5. `description` length ≤ 1024 chars. Else Blocking.
6. SKILL.md body line count ≤ 500 (proxy for token cap). Above → Warning.
7. Every relative link in the body resolves to an existing file. Broken → Blocking.
8. Code fences are balanced (count of ``` is even). Else Blocking.
9. No top-level directories other than `{scripts,references,assets,evals}`. Else Warning (Confluent uses `evals/`; everything else is suspicious).
