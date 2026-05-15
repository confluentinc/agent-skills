# Evals contract — reference

Consulted in Phase D when a finding fires on `evals/evals.json` shape, content, or fixture sync.

## Schema (this repo)

Top level:

```json
{
  "skill_name": "<string>",
  "evals": [ /* one eval per id */ ]
}
```

Each eval:

```json
{
  "id": 0,
  "prompt": "<realistic user message>",
  "expected_output": "<one-line description of success>",
  "files": ["<optional fixture paths, relative to skill root>"],
  "expectations": ["<string>", "<string>"]
}
```

OR (object form, used by `developing-kafka-python-client`):

```json
{
  "id": 0,
  "prompt": "...",
  "expected_output": "...",
  "files": [],
  "assertions": [
    {
      "id": "tests-pass",
      "type": "file_exists",
      "description": "pytest passes on generated project",
      "path": "tests/",
      "pattern": null
    }
  ]
}
```

**Both shapes are valid in this repo.** Mixing both within the same file is a **Warning** — pick one and stay consistent. For *new* skills, prefer the string-`expectations` shape; it is the default in this repo's authoring workflow. The object-`assertions` shape stays supported for skills that already use it (`developing-kafka-python-client`).

## Strong vs weak expectations

`CLAUDE.md` § Evals are the contract says expectations encode hard-won correctness. Each one should be:

- **Specific** — names a file path, a class name, a config key, a CLI flag, an exact string.
- **Verifiable** — a human (or grader) can decide pass/fail by reading the output.
- **Hard-won** — encodes something the skill got wrong before. Look at PR #16's expectation:

> `App.java imports StreamsUncaughtExceptionHandler from org.apache.kafka.streams.errors (NOT as a nested class under KafkaStreams — that type does not exist in KS 4.x)`

That one assertion encodes a real-world bug. That is the bar.

### Weak — flag as Warning

- "The code is well written."
- "Includes error handling."
- "Asks the right questions."
- "Generates a working app."

### Strong — accept

- "Sets `statestore.cache.max.bytes` in application.properties."
- "TopologyTest uses `mock://` URL scheme for Schema Registry."
- "Avro schemas are in `src/main/avro/` (NOT `src/main/resources/avro/`)."
- "Does NOT use `kafka-console-producer` — uses schema-aware producers."

Heuristic for the script: an expectation is *probably specific* if it contains at least one of: a path-like token (`/`, `.java`, `.properties`), an identifier in `CamelCase` or `dot.case`, the word `NOT`, or a quoted CLI flag.

## Fixture sync

If `evals/` contains `mock-repos/` or `mock-skills/`, every path referenced from `files: [...]` in `evals.json` must resolve on disk. Stale references are **Blocking** — graders silently skip them, masking regressions.

`skills/kafka-schema-registry/evals/mock-repos/payment-service/` is the canonical example. When the schema for the fixture changes, the corresponding `expectations[]` must change in the same PR.

## Prompts

Prompts should look like a real user message, not a test fixture. CLAUDE.md doesn't enforce this directly, but PR review history (PR #16) tightened this up.

Heuristics flagged by `check_eval_schema.py`:

- Prompt length < 40 chars → probably abstract, **Warning**.
- Prompt starts with "Build me a" and contains no domain detail → **Warning**.
- Prompt contains placeholder text like `<TOPIC_NAME>`, `XXX`, `TODO` → **Warning**.

Good prompts from existing evals (kafka-streams-programming/evals/evals.json):

> "I need a Kafka Streams app that aggregates customer order totals. The orders come in on a topic and I need a running total per customer. Using Avro, self-managed Confluent Platform 8.2 cluster with SASL_SSL."

Notice: specific data shape, named environment, named auth, named schema format. That is what a real user types.

## 90% threshold

The PR template requires `Evals pass at 90%+ threshold`. The math is:

```
score = (sum of passing expectations across all evals) / (total expectations)
```

If `score < 0.9`, the PR cannot merge. This is verified by the skill author (and SME reviewer) running the skill-creator harness — `confluent-skill-reviewer` does not run evals itself, but checks that the score has been declared.
