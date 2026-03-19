# AI Agent Skills by Confluent

A collection of AI skills for developing data streaming applications and pipelines with Confluent. These skills help developers using coding assistants quickly build production-ready Kafka producers, Flink applications, and real-time data pipelines by providing guided assistance and code generation. Each skill promotes developer best practices, including proper Schema Registry usage, security configuration, and error handling.

## Installation

### Claude

```shell
/plugin marketplace add confluentinc/agent-skills
/plugin install streaming-skills-plugin@confluent-agent-skills
```

### `skills` CLI

This lets you pick specific skills to install and supports most agents.

```shell
npx skills add confluentinc/agent-skills
```

## Quick start

1. Install the skills using one of the methods above
2. Open a project where you want to build streaming applications
3. Ask your AI agent (e.g., Claude or Cursor) to help with Kafka producers, Flink Table API applications, or other streaming tasks
4. The relevant skill will automatically activate based on your request

Example prompts:
- "Create a Kafka producer that sends user profile update events to Confluent Cloud"
- "Build a Flink Table API application in Python to filter streaming data"
- "Set up a streaming pipeline with Schema Registry"

## Repository layout

```
agent-skills/
├── skills/               # Individual skill definitions
│   ├── skill-name/       # Example skill directory
│   │   ├── SKILL.md      # Skill description
│   │   ├── evals/        # Evaluation tests for the skill
│   │   └── references/   # Assets referenced by SKILL.md
│   └── ...               # Additional skills
├── README.md             # This file
├── .claude-plugin/       # Claude marketplace and plugin definition
└── .cursor-plugin/       # Cursor plugin definition
```

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.