---
name: good-skill
description: Generate a Confluent Cloud topic creation script with idempotency checks. Use when the user asks to create a topic, provision topics, or write a `create-topics.sh` for Confluent Cloud. Do NOT trigger for self-managed Apache Kafka, schema registration, Terraform generation, or Kafka Streams topology authoring.
---

# good-skill — Confluent Cloud topic provisioning

## ⚠️ Lazy-load references

Do not read every reference up front. Each phase below routes to the one file it needs.

- User asks "what partition count?" → read `references/sizing.md` § Partition Count Decision
- User asks "how do I authenticate?" → read `references/auth.md` § Cloud API Keys

## Steps

1. Confirm the user is on Confluent Cloud (not self-managed). If self-managed, hand off — not in scope.
2. Gather: topic names, partition counts, RF, cleanup policy.
3. Generate `create-topics.sh` using `confluent kafka topic create --cluster ...`.
4. Include idempotency: check `confluent kafka topic list` before each create.
5. Save script to project root and emit a one-paragraph summary.
