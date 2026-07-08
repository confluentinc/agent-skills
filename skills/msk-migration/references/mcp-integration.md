# MCP Integration

The skill uses **Local Confluent MCP** for target-side verification when it's connected. MCP is optional — the core workflow runs on KCP CLI + Confluent CLI. MCP enhances the experience when available but never gates any step.

## Local Confluent MCP

Open source at [github.com/confluentinc/mcp-confluent](https://github.com/confluentinc/mcp-confluent). Installed by the user via stdio per standard Claude Code MCP configuration.

Tool surface (verify against the repo README — names may change):

- `list-environments`, `list-clusters` — CC environment introspection
- `list-topics`, `list-schemas`, `list-connectors` — target-side inventory
- `consume-messages` — canary verification

## When to Prefer MCP Over CLI

- **Real-time conversational queries.** "Are the topics on CC now?" is faster via MCP than via `confluent kafka topic list`.
- **Target-side verification.** `list-topics`, `list-schemas`, `list-connectors`, `consume-messages` are the fastest way to verify migration state on CC.
- **Interactive troubleshooting.** MCP's tool-call format fits conversational debugging better than batch CLI runs.

## When to Prefer CLI

- **Durable state artifacts.** Confluent CLI output can be captured, piped, and stored. MCP queries don't produce a persistent artifact by default.
- **Scripted workflows.** If the user is running a sequence of target-side operations in a script, stay in the CLI.

## Interpreting MCP Results

- **Read tools** (list, get, describe) are enabled by default. Freely used by the skill.
- **Write tools** (create, alter, delete) require user opt-in per DTX's `destructiveHint` annotation. Always warn the user before invoking a write tool.
- **Error responses** should follow a consistent structure. If the tool returns a free-text error, that's acceptable; the skill surfaces it to the user as-is.

## Source of Truth

- Local Confluent MCP tool list and installation — [github.com/confluentinc/mcp-confluent](https://github.com/confluentinc/mcp-confluent) README. Verify tool names before invoking.
