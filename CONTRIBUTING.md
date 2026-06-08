# Contributing to Confluent's AI Agent Skills

This contributing guide outlines best practices for contributing an AI agent skill to this repository or improving an existing skill.

Developers use the skills in this repo for developing data streaming applications and pipelines. These skills help developers using AI coding assistants (like Claude Code and Cursor) quickly build prototype or production-ready data streaming applications.

## Prerequisites

- **Claude Code or another tool that supports [Agent Skills](https://agentskills.io/home)** in order to test
- **Go** and the `skill-validator` tool for skill validation
  ```bash
  go install github.com/agent-ecosystem/skill-validator/cmd/skill-validator@1.3.0
  ```

## Getting Started

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/<owner>/agent-skills.git
   cd agent-skills
   ```

2. **Install the repo's authoring skills locally**

   This repo ships two skills to help you author and review skills:
   `confluent-skill-creator` and `confluent-skill-reviewer`. Install them
   locally by copying them into your coding agent's local skills directory, e.g., for Claude:

   ```bash
   mkdir -p ~/.claude/skills
   cp -r skills/confluent-skill-creator ~/.claude/skills/
   cp -r skills/confluent-skill-reviewer ~/.claude/skills/
   ```

   Restart your coding agent so the skills are picked up (or, if using Claude, run `/reload-plugins`).

## Adding a New Skill

Bear in mind the following guiding principles for skills in this repository:

- Each skill should promote developer best practices, including proper Schema Registry usage (if applicable), security configuration, and error handling
- Keep skills scoped to a particular use case or technological focus area. Refer to the existing skills in this repo for examples of skill scope. If you are unsure about skill scope, open a [GitHub Issue](https://github.com/confluentinc/agent-skills/issues) to discuss.
- Skills are nondeterministic by nature (LLM-based), so all outputs should be carefully reviewed
- Skills should include built-in feedback loops and structural guardrails to enable agent iteration
- Eval test cases should pass at 90%+ threshold

Once you've installed the authoring skills (see [Getting Started](#getting-started)), the easiest way to create a new skill is to trigger `confluent-skill-creator` in your coding agent. It walks you through gathering requirements, scaffolding the directory structure, writing the `SKILL.md`, adding reference files, optimizing the description, and writing evals. For example, just ask:

```
Create a new skill for <your use case>
```

The manual steps below document what the skill does for you, and are useful as a reference or if you prefer to author by hand or validate what the skill creator generated.

### Step 1: Create the skill directory structure

```bash
mkdir -p skills/your-skill-name/{evals,references}
touch skills/your-skill-name/SKILL.md
```

### Step 2: Write the SKILL.md File

The `SKILL.md` file contains the skill's instructions, logic, and guardrails. A coding agent will first load the skill's name and description into its context in order to decide whether to trigger a skill. Once triggered, SKILL.md is loaded into context. From there, SKILL.md may contain branching logic to further load reference files. The progressive loading based on prompts and skill instructions is called "progressive disclosure." It's critical in preventing unnecessary token usage and context bloat, which cost money and reduce model efficacy.

**Template:**

```markdown
---
name: your-skill-name
description: [Brief description of when to use this skill. Be specific about trigger conditions.]
---

# Skill Title

[Main instructions for the skill]

## Step 1: Gather Requirements

[Questions the agent should ask the user]

## Step 2: [Action/Generation]

[Detailed instructions for what the agent should do]

## Common Mistakes

| Thought | Reality |
|---------|---------|
| [Common misconception] | [Correct approach] |

## Quality Gates

[Checks the agent should perform before completing]
```

### Step 3: Add Reference Files (Optional)

Reference files provide additional context, templates, or examples that the SKILL.md file can reference. Common types:

- **Templates**: Code scaffolding, configuration files
- **Patterns**: Architecture patterns, best practices
- **Examples**: Sample implementations
- **Decision trees**: Flowcharts or logic diagrams

Place these in `skills/your-skill-name/references/` and reference them from SKILL.md using relative paths.

### Step 4: Optimize the Description

The skill description determines when the skill is triggered. Follow these guidelines:

- **Be specific** about use cases and trigger conditions
- **Include keywords** users are likely to mention
- **Mention technologies** explicitly (e.g., "Kafka Streams", "Python", "Schema Registry")
- **Describe what/when**, not implementation details

**Example:**
```
Good: "Use when the user wants to build a Python Kafka producer or consumer, add Schema Registry to existing Python code, migrate from raw JSON to schema-backed serialization, or scaffold a confluent-kafka-python project."

Bad: "Helps with Kafka Python development"
```

See [optimizing descriptions](https://agentskills.io/skill-creation/optimizing-descriptions) for more guidance.

## Writing Evals

Evals are test cases that verify your skill works as expected across different scenarios. They help ensure quality and catch regressions.

### Eval File Structure

Create `skills/your-skill-name/evals/evals.json`:

```json
{
  "skill_name": "your-skill-name",
  "evals": [
    {
      "id": 0,
      "prompt": "User's input that should trigger this skill",
      "expected_output": "High-level description of what should be generated",
      "files": [],
      "expectations": [
        "Specific behavior or check #1",
        "Specific behavior or check #2",
        "Generated file X contains Y",
        "Does NOT do Z (anti-pattern)"
      ]
    }
  ]
}
```

### Eval Components

- **id**: Unique integer identifier (0, 1, 2, ...)
- **prompt**: The user's request that triggers the skill
- **expected_output**: High-level summary of desired outcome
- **files**: Array of file paths to provide as context (optional)
- **expectations**: Array of specific, measurable criteria

### Writing Good Expectations

Expectations should be:

1. **Specific**: Check for exact behavior, not vague outcomes
   - Good: `"Avro schemas are in src/main/avro/ (NOT src/main/resources/avro/)"`
   - Bad: `"Schemas are in the right place"`

2. **Verifiable**: Something that can be objectively checked
   - Good: `"docker-compose.yml includes schema-registry service"`
   - Bad: `"Docker setup looks good"`

3. **Comprehensive**: Cover both positive and negative cases
   - Positive: `"Generates .env.example with placeholder values"`
   - Negative: `"Does NOT use kafka-console-producer anywhere"`

4. **Structural**: Check for guardrails and process, not just final output
   - `"Asks about or confirms the target environment before generating code"`
   - `"Presents a confirmation summary before generating files"`

### Example Eval

```json
{
  "id": 0,
  "prompt": "Create a Kafka producer for customer events using Avro and Confluent Cloud",
  "expected_output": "A producer project with Avro schema, .env file for CC credentials, proper SASL_SSL config",
  "files": [],
  "expectations": [
    "Asks about or confirms whether this is a greenfield project or modifying existing code",
    "Asks about target environment (Confluent Cloud vs local Docker)",
    "Generates a .env file for credentials",
    "Generates .env.example with placeholder values (NOT real credentials)",
    "Configures SASL_JAAS_CONFIG with PLAIN mechanism for Confluent Cloud",
    "Sets security.protocol=SASL_SSL",
    "Avro schema is in schemas/ or src/main/avro/ directory",
    "Does NOT hardcode credentials in code",
    "Includes error handling for produce failures",
    "Includes graceful shutdown with flush()"
  ]
}
```

### Test Coverage Guidelines

Aim for 5-10 eval cases per skill that cover:

- **Happy path**: 1-2 evals for typical use cases
- **Edge cases**: 1-2 evals for less common scenarios
- **Anti-patterns**: At least 1 eval that checks the skill avoids common mistakes
- **Environment variations**: 1 eval per major environment (Cloud vs local, async vs sync, etc.)

## Running Evals

### Using the skill-creator Skill

The easiest way to run evals is using the `skill-creator` skill in Claude Code:

1. **Install skill-creator** (if not already installed):
   ```bash
   /plugin install skill-creator@claude-plugins-official
   ```

2. **Run evals for your skill**:
   ```
   Run evals for the <skill name> skill
   ```

3. **Review results**: The skill will provide a detailed report showing:
   - Pass/fail for each expectation
   - Overall pass rate
   - Specific failures with context

### Manual Testing

You can also manually test your skill:

1. **Open a new Claude Code session** in a test directory
2. **Install your skill locally**:
   ```bash
   /plugin dev /path/to/agent-skills
   ```
3. **Trigger the skill** with one of your eval prompts
4. **Verify expectations** manually against the generated output

### CI/CD Validation

When you submit a PR, Semaphore CI automatically:
- Validates all skills with `skill-validator`
- Checks for structural issues
- Ensures no breaking changes to skill definitions

## Validate and review the skill

### Validate the skill

```bash
skill-validator check skills/your-skill-name
```

Fix any validation errors before proceeding. False positive warnings are acceptable, but fix any warnings that aren't false positive. Note that `skill-validator` returns exit code 2 if there are any warnings.

### Review the skill with `confluent-skill-reviewer`

Where `skill-validator` checks structural and spec conformance, the `confluent-skill-reviewer` skill reviews your skill against Confluent-specific skill patterns and best practices — including the conventions in `CLAUDE.md`, lazy-loading of references, trigger/anti-trigger overlap with neighboring skills, the PR template gates, and the evals-as-contract rule.

Once you've installed the authoring skills (see [Getting Started](#getting-started)), trigger it in your coding agent, for example:

```
Review the skills/your-skill-name skill
```

Address the findings before opening a pull request.

## Testing Locally

### Testing Skill Triggering

Verify your skill triggers correctly:

```bash
# In Claude Code
/plugin dev /path/to/agent-skills

# Then try triggering with various prompts:
# "Create a Kafka producer..."
# "Build a streaming app..."
# etc.
```

### Testing Skill Execution

1. **Create a test project directory**
   ```bash
   mkdir ~/test-skill-output
   cd ~/test-skill-output
   ```

2. **Load your skill**
   
   For example, in Claude Code:
   ```bash
   /plugin dev /path/to/agent-skills
   ```

3. **Execute with real prompts**

   Use prompts from your evals or variations

4. **Verify output**
   - Generated files are correct
   - Code compiles/runs
   - Configurations are valid
   - Best practices are followed

## Submitting a Pull Request

Ensure the following before opening a pull request:

- [ ] Skill description is optimized for accurate triggering
- [ ] SKILL.md includes any mandatory requirements to gather
- [ ] Built-in feedback loops enable agent iteration
- [ ] Evals are comprehensive and pass at 90%+ threshold
- [ ] `skill-validator check skills/your-skill-name` passes
- [ ] Manually tested the skill with real prompts
- [ ] Reference files are accurate and tested

## Additional Resources

### Documentation

- [Agent Skills Documentation](https://agentskills.io/)
- [Skill Creation Guide](https://agentskills.io/skill-creation/)
- [Optimizing Descriptions](https://agentskills.io/skill-creation/optimizing-descriptions)

### Support

- **Issues**: [GitHub Issues](https://github.com/confluentinc/agent-skills/issues)
