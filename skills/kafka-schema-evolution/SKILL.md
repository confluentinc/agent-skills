---
name: kafka-schema-evolution
description: "Evolve an already-registered Schema Registry schema safely. Use when the user wants to change an existing Avro / JSON Schema / Protobuf schema and needs to know if the change is compatible, which compatibility mode to set (BACKWARD, FORWARD, FULL, and their _TRANSITIVE variants, or NONE), how to order the producer/consumer rollout, how to diagnose a rejected registration or a failed compatibility check, or how to wire compatibility checks into CI before deploy. Covers Confluent Cloud, Confluent Platform, and Apache Kafka / community Schema Registry. Do NOT trigger for scanning a repo to inventory Kafka apps or generating Terraform / a migration report for first-time Schema Registry adoption (use kafka-schema-registry); for scaffolding a new producer or consumer project (use developing-kafka-java-client or developing-kafka-python-client); or for Flink SQL / Table API schema handling."
compatibility: "Advisory skill — reads schema files and talks to a Schema Registry over its REST API or the confluent CLI. No language runtime required. Live compatibility checks need reachable Schema Registry endpoint + credentials; offline reasoning works without them."
metadata:
  author: confluent
  version: "1.0.0"
  last_updated: "2026-09-01"
---

# Kafka Schema Evolution

Change a schema that is **already registered** in Schema Registry without breaking existing
producers or consumers. This skill reasons about compatibility, not first-time onboarding —
if the schema is not registered yet, or the user wants a repo scan / Terraform / migration
report, hand off to the **kafka-schema-registry** skill.

## ⚠️ Lazy-load references

Read only the reference file the current task needs. Most tasks need 1–2. Do **not** read
every file up front.

| You need… | Read |
|---|---|
| What each compatibility mode allows / forbids, how to set it | `references/compatibility-modes.md` |
| Whether a specific Avro change is compatible | `references/avro-rules.md` |
| Whether a specific JSON Schema change is compatible | `references/json-schema-rules.md` |
| Whether a specific Protobuf change is compatible | `references/protobuf-rules.md` |
| Rollout ordering, CI checks, diagnosing a rejected change | `references/rollout-and-troubleshooting.md` |
| Platform-specific CLI / REST / config detail | `references/confluent-cloud.md`, `references/confluent-platform.md`, or `references/apache-kafka.md` |

## Step 1: Establish context

Confirm before giving an answer (ask only for what the user has not already said):

1. **Platform** — Confluent Cloud, Confluent Platform (self-managed), or Apache Kafka /
   community Schema Registry (incl. Apicurio in SR-compat mode). Config keys, CLI, and
   contexts differ — read the matching `references/<platform>.md`.
2. **Schema format** — Avro, JSON Schema, or Protobuf. Compatibility rules differ per format.
3. **Subject + subject-name strategy** — `TopicNameStrategy` (`<topic>-value` / `<topic>-key`,
   the default), `RecordNameStrategy`, or `TopicRecordNameStrategy`. This determines *which*
   subject's compatibility config and version history apply.
4. **Current compatibility mode** — the effective mode for the subject (subject-level config
   overrides global). Get it before reasoning: see `references/compatibility-modes.md` §
   "Read the current mode". Never assume `BACKWARD` just because it is the default.
5. **The change** — old schema (or subject/version) and the proposed new schema, or a prose
   description of the edit.

## Step 2: Determine the mode

Modes chosen for **how the user's clients deploy**, from `references/compatibility-modes.md`:

| If the user says… | Mode |
|---|---|
| "consumers get deployed before producers" / "I can't coordinate producers" | `BACKWARD` (or `BACKWARD_TRANSITIVE`) |
| "producers get deployed first" / "old consumers must keep working with new data" | `FORWARD` (or `FORWARD_TRANSITIVE`) |
| "I can't control deploy order" / "mixed old and new on both sides" | `FULL` (or `FULL_TRANSITIVE`) |
| "consumers must read the entire history of this subject" | a `_TRANSITIVE` variant |
| "this is a dev topic / I'll manage it out of band" | `NONE` (call out the risk explicitly) |

Default recommendation when the user has no constraint: `BACKWARD` for event/log topics
(the Schema Registry default; matches the common "upgrade consumers first" pattern), or
`FULL_TRANSITIVE` for schemas shared across many independently-deployed teams.

## Step 3: Evaluate the change

1. Read the format's rules file. Classify each field-level edit as compatible / incompatible
   **under the mode from Step 2**.
2. Reason offline first and show the user the per-change verdict with the reason
   (e.g. "adding `region` without a default breaks BACKWARD — a consumer on the new schema
   can't read old records that lack the field").
3. Then verify against the live registry if reachable — the check-without-registering call
   in `references/compatibility-modes.md` § "Check before you register". Offline reasoning
   and the registry can disagree (especially for JSON Schema); the registry is authoritative.
4. If the change is **incompatible** under the current mode, present options:
   - Rewrite the change to be compatible (add a default, keep the field, widen the type…).
   - Split into an intermediate compatible step, then a second change.
   - Move to a new subject / new topic (versioned name) for a true breaking change.
   - Data Contracts **migration rules** with a compatibility group (advanced; Cloud / CP
     only) — see `references/rollout-and-troubleshooting.md` § "Breaking changes".
   Do **not** recommend switching the mode to `NONE` to force a bad change through.

## Step 4: Plan the rollout

Give an ordered checklist from `references/rollout-and-troubleshooting.md` § "Rollout order":
who deploys first (producers or consumers, driven by the mode), how to register the new
version, and how to verify no consumer lag / deserialization errors after each stage.

## Step 5: Changing the compatibility mode (guarded)

Setting `PUT /config` or `PUT /config/{subject}` **mutates registry state** and changes what
future registrations are allowed. Before making the call:

1. Show the exact request (endpoint, subject scope, old mode → new mode).
2. State the blast radius: which subjects it affects, and that it does **not** re-validate
   existing versions.
3. Wait for explicit confirmation. Never change global config to fix one subject — scope it
   to the subject.

## Common mistakes

| Thought | Reality |
|---|---|
| "BACKWARD means old code reads new data" | BACKWARD = the **new** schema reads **old** data. Old-reads-new is FORWARD. |
| "The mode is BACKWARD, it's the default" | Subject-level config overrides global. Read the effective mode for the subject first. |
| "Adding a field is always safe" | Only under FORWARD. Under BACKWARD it needs a default; under FULL it needs a default. |
| "Removing a field is always safe" | Only under BACKWARD. Under FORWARD/FULL the removed field must have had a default. |
| "Compatibility is checked against every past version" | Only for `_TRANSITIVE` modes. Plain BACKWARD/FORWARD/FULL check against the latest version only. |
| "Protobuf field rename is a breaking change" | On the wire the field **number** is identity, so SR allows renames — but it breaks JSON encoding and generated code. Flag it. |
| "JSON Schema evolves like Avro" | It doesn't. Open vs closed content model (`additionalProperties`) drives the result; always confirm with a live check. |
| "Switch to NONE, register, switch back" | Leaves an unreadable version in history and defeats the purpose. Use a new subject for real breaks. |

## Quality gates

Before telling the user the change is safe:

- [ ] Effective compatibility mode for the subject was read, not assumed.
- [ ] Every field-level edit classified with a reason, not just an overall pass/fail.
- [ ] Verified with a check-without-registering call against the live registry, or the user
      was told the verdict is offline-only and why that matters for JSON Schema.
- [ ] Rollout order stated (producers-first vs consumers-first) and tied to the mode.
- [ ] Any `PUT /config` change was shown and confirmed before execution.
