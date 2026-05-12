---
name: trigger-overlap
description: Build a Confluent Cloud topic provisioning script with retention and compaction. Use when the user asks to create a topic, write a `create-topics.sh`, set retention, set compaction policy, provision topics for Confluent Cloud, or generate idempotent topic scripts.
---

# trigger-overlap — topic provisioning

Generate `create-topics.sh` with retention/compaction settings.

## Step 1

Ask for topic names, retention, cleanup policy.

## Step 2

Generate the script with `confluent kafka topic create --config retention.ms=... --config cleanup.policy=...`.
