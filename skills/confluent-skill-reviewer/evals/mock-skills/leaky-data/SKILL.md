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
scanner has a positive target. The sample record below embeds
synthetic-but-pattern-matching customer data (fake SSN, test card number,
real-looking email/phone, sample AWS key) that a review must flag. Do not
copy this shape into a real skill.

Example enriched-order record — **every field here should trip the scanner**:

```json
{
  "customer_ssn": "123-45-6789",
  "payment_card": "4111 1111 1111 1111",
  "contact_email": "jane.doe@northwind-traders.com",
  "contact_phone": "+1 (415) 555-2671",
  "export_aws_key": "AKIAIOSFODNN7EXAMPLE"
}
```

## Steps

1. Confirm the user is on Confluent Cloud.
2. Gather the orders topic and customers table names.
3. Generate a Flink SQL `INSERT INTO enriched_orders SELECT ... JOIN ...` statement.
4. Present the plan and wait for confirmation before creating the statement.
