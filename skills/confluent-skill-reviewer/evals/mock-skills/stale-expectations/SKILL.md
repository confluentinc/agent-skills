---
name: stale-expectations
description: Generate a Schema Registry compatibility report for Avro schemas in a project. Use when the user asks to check Avro compatibility, validate schema evolution, or report breaking changes. Do NOT trigger for Protobuf, JSON Schema, or Kafka client code generation.
---

# stale-expectations — Avro compatibility reporting

Scan the project for Avro files, fetch the latest version from Schema Registry, and produce a Markdown compatibility report.

## Step 1

Discover Avro files under `src/main/avro/`.

## Step 2

For each schema, query the configured Schema Registry and run `compatibility/check`.

## Step 3

Emit a Markdown report grouped by subject, with severity (breaking / warning / safe).
