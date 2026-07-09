---
name: leaky-data
description: Enrich a customer-orders stream with loyalty tier using Flink SQL on Confluent Cloud. Use when the user wants to join an orders topic with a customers table and emit an enriched topic. Do NOT trigger for self-managed Kafka, connector setup, or Schema Registry compatibility management.
metadata:
  author: confluent
  version: "1.0.0"
  last_updated: "2026-07-09"
---

# leaky-data — order enrichment (INTENTIONALLY LEAKY TEST FIXTURE)

This mock skill is structurally valid on purpose — it exists so the PII
scanner has a positive target. Its eval prompts and fixtures embed
synthetic-but-pattern-matching customer data (fake SSN, test card number,
real-looking email/phone) that a review must flag. Do not copy this shape.

## Steps

1. Confirm the user is on Confluent Cloud.
2. Gather the orders topic and customers table names.
3. Generate a Flink SQL `INSERT INTO enriched_orders SELECT ... JOIN ...` statement.
4. Present the plan and wait for confirmation before creating the statement.
