# Example: Creating an Enrichment Skill

Full workflow for creating a "real-time order enrichment" skill:

**Step 1 — Understand scope and platform:**
User: "I want to create a skill for enriching order data with customer information"
You: "Great! So the use case is 'real-time customer order enrichment using Flink SQL' — is that correct?"
You: "Should this skill target Confluent Cloud specifically, or work across all platforms?"
User: "Confluent Cloud only"
You: "Got it — the skill name will include 'confluent-cloud' (e.g., `confluent-cloud-order-enrichment`)."

**Step 2-3 — Interview and write skill:**
- Topics: orders, customers, enriched_orders
- Schemas: JSON_SR for all
- Flink SQL for joining
- Interaction method: CLI for topic/schema setup + Flink SQL statements via REST API
- Plan-before-execute: skill must present all operations and wait for confirmation
- Sample data for testing

The skill includes:
- Step-by-step instructions with specific CLI commands and API calls in order
- Schema definitions
- Flink SQL join statement
- Link to https://docs.confluent.io/cloud/current/flink/reference/queries/joins.md

**Step 4 — Validate spec compliance:**
- `name`: `confluent-cloud-order-enrichment` — lowercase+hyphens, includes platform, matches directory
- `description`: under 1024 chars, includes what + when + Confluent Cloud target
- `metadata`: author: confluent, version: "1.0"
- SKILL.md body under 500 lines
- `evals/evals.json` exists with test cases
- `.env.template` exists

**Step 5 — Set up environment:**
- Test against Confluent Cloud (matches target platform)
- Create .env.template with BOOTSTRAP_SERVERS, API keys, SR URL, Flink pool
- Wait for user to fill in credentials

**Step 6 — Create test and show plan:**
```
Plan for test case: order-enrichment-e2e

Environment: Confluent Cloud
Interaction method: CLI + REST API

Operations (in execution order):
1. Check Flink compute pool availability
   Command: confluent flink compute-pool describe lfcp-xxxxx
2. Pre-create topics
   Command: confluent kafka topic create orders --partitions 3
   Command: confluent kafka topic create customers --partitions 3
   Command: confluent kafka topic create enriched_orders --partitions 3
3. Register JSON schemas for all topics
   Endpoint: POST /subjects/orders-value/versions
4. Execute Flink SQL: CREATE TABLE + INSERT query for enrichment
   Endpoint: POST /sql/v1/organizations/{org}/environments/{env}/statements
5. Produce 10 sample orders and 5 sample customers
   Script: python scripts/produce_data.py
6. Verify 5 enriched orders appear in enriched_orders topic
   Script: python scripts/consume_and_verify.py
7. Cleanup: stop Flink statement (topics remain)

Proceed? [yes/no]
```

**Step 7-8 — Run tests, evaluate, iterate:**
- Spawn with-skill and without-skill subagents
- Draft assertions: "5 enriched orders produced", "All orders have customer name/email", etc.
- User reviews in browser, improve based on feedback

**Step 9 — Optimize description:**
- Run trigger optimization
- Final description includes: "real-time enrichment", "Flink SQL", "join", "order data", "Confluent Cloud"

**Step 10 — Package:**
- Bundle with `produce_data.py`, `consume_and_verify.py`, `check_compute_pool.py`
- Include the credential template (`.env.template` or `credentials.yaml.template`) and evals/
- Run `skills-ref validate ./confluent-cloud-order-enrichment`
- Deliver .skill file
