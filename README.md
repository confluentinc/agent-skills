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

## Skills

This repository includes the following skills:

| Skill                          | Description |
|--------------------------------|-------------|
| **kafka-streams-programming**  | Architect, build, and debug Kafka Streams applications that run as a library inside your JVM with no separate cluster required. Handles topology design, pattern selection (joins, windows, aggregations), code generation for complete projects with proper Schema Registry integration, and troubleshooting production issues like rebalancing loops, state store problems, and performance tuning. |
| **schema-registry-adopting**   | Scan a project or repository to identify Kafka applications, extract schemas from data models, tag PII fields, generate Terraform for Confluent Schema Registry registration, and produce a migration report with rollout ordering. Automates the migration path from unmanaged schemas to Schema Registry with proper governance and compliance.                          

## Repository layout

```
agent-skills/
├── skills/               # Individual skill definitions
│   ├── skill-name/       # Individual skill directory
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