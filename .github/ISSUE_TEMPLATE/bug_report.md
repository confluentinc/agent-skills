---
name: 'Report a bug'
about: Report a bug with a Confluent AI agent skill.
title: ''
labels: 'bug, needs triage'
assignees: ''
---

Thanks for taking the time to tell us about your issue using Confluent's AI agent skills!

Requests for features and improvements to a skill or its documentation should be opened in [discussions](https://github.com/confluentinc/agent-skills/discussions/new?category=ideas). For more information about opening a feature request, [read more](https://github.com/confluentinc/agent-skills/discussions).

Before opening a new issue, please do a [search](https://github.com/confluentinc/agent-skills/issues) of existing issues and :+1: upvote the existing issue instead. This will result in a quicker resolution.

## Code of Conduct

By submitting this issue, you agree to follow our [Code of Conduct](https://github.com/confluentinc/agent-skills/blob/main/CODE_OF_CONDUCT.md).

- [ ] I agree to follow this project's Code of Conduct

## Affected skill(s)

Which skill(s) is this bug in? (e.g. `kafka-streams-programming`, `developing-kafka-python-client`, `kafka-schema-registry`, `confluent-cloud-cdc-tableflow`, the plugin/marketplace packaging, the evals harness)

## Skill version / commit

What version, release, or commit SHA of the skill were you using? (e.g. `v0.3.0`, commit `abc1234`, or "installed via `/plugin install streaming-skills-plugin@confluent-agent-skills` on 2026-04-27")

## AI agent and model

Which agent and model were you using when you hit the bug? (e.g. Claude Code with Opus 4.7, Cursor with Sonnet 4.6, the `skills` CLI with another agent)

## Installation method

How was the skill installed?

- [ ] `/plugin install` via the Claude marketplace
- [ ] `npx skills add confluentinc/agent-skills`
- [ ] Cloned this repo locally
- [ ] Other (please describe)

## Operating system

On what operating system did you see the problem? (Select all that apply)

- [ ] macOS (Intel/x64)
- [ ] macOS (Apple/arm64)
- [ ] Linux (x64)
- [ ] Linux (arm64)
- [ ] Windows (x64)
- [ ] Other

## To Reproduce

A step-by-step description of how to reproduce the issue. Include the prompt you gave the agent, any relevant project state (language, build tool, Confluent Cloud vs. local, schema format), and what the agent did. Screenshots can be provided in the issue body below. If using code blocks, make sure that [syntax highlighting is correct](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/creating-and-highlighting-code-blocks#syntax-highlighting) and double-check that the rendered preview is not broken.

1.
2. Asked the agent: "..."
3. The skill triggered / did not trigger and produced ...

## Current vs. Expected behavior

A clear and concise description of what the bug is, and what you expected the skill to do instead.

> Following the steps above, I expected the skill to A, but it did B instead.

## Relevant log or agent output

Please paste any relevant agent transcript, generated code, or error output. For Claude Code, the `~/.claude/projects/` transcripts can help. Trim to the relevant portion.

```shell

```

## Which area(s) are affected? (Select all that apply)

- [ ] Not sure
- [ ] Skill triggering / discovery (the right skill didn't activate, or the wrong one did)
- [ ] Generated code correctness (compile errors, runtime errors, wrong API usage)
- [ ] Schema Registry integration (Avro / JSON Schema / Protobuf)
- [ ] Confluent Cloud configuration (auth, endpoints, ACLs)
- [ ] Local / Docker setup
- [ ] CDC / Tableflow / Debezium pipeline
- [ ] Flink (Table API / SQL)
- [ ] Kafka Streams topology, state stores, or rebalancing
- [ ] Python client (sync / async producer / consumer)
- [ ] Plugin / marketplace packaging or installation
- [ ] Evals harness or eval results
- [ ] Documentation in `SKILL.md`

## Additional context

Any extra information that might help us investigate. Does the issue happen every time, or intermittently? Does it reproduce with a different model? Is your project private/internal, or can you share a minimal repro repo?

> Yes, the issue happens (almost) every time I perform the steps above.
>
> or
>
> No, the issue is intermittent — it reproduces about 1 in 3 runs with the same prompt.
