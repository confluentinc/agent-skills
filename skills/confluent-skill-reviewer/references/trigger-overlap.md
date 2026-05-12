# Trigger overlap — reference

Consulted in Phase C when `scripts/check_trigger_overlap.py` reports a collision.

## Why this matters

Every active skill's `name` + `description` is in the agent's context at startup. When two skills' descriptions share trigger keywords, the agent picks whichever it sees first — non-deterministic. Either skill can wrongly fire on prompts meant for the other, and the user sees confusing behavior.

The fix is not to dilute the descriptions. It's to make each description *explicit about what it is and is not for*. Confluent's convention: every description ends with a `Do NOT trigger for…` clause that names the adjacent skills.

## The algorithm `check_trigger_overlap.py` uses

1. Parse `description:` from every `skills/*/SKILL.md`.
2. Tokenise into nouns/verbs (skip stopwords + common domain words like "Confluent", "Kafka" — those are too broad to collide on alone).
3. For each pair of skills, compute the keyword intersection.
4. For each pair, by overlap count and anti-trigger mutuality:
   - ≥3 distinct keywords overlap, neither side anti-triggers the other → **Blocking**.
   - 2 keywords overlap, no mutual anti-trigger → **Warning** (review the wording).
   - 1 keyword overlap → **Nit** (probably coincidence).
   - Any count with mutual anti-triggers naming each other's domain → **pass**.

Domain-broad words (`confluent`, `kafka`, `cluster`, `topic`, `schema`, `cloud`, `platform`, `java`, `python`, `json`, `avro`, `protobuf`, `stream(s)`, `producer`, `consumer`, etc.) are filtered out before counting — every skill in this repo touches them, so they would dominate the signal.

## Worked examples — existing skills

### Pair: `kafka-streams-programming` ↔ `developing-kafka-python-client`

Overlap keywords: "Kafka", "producer", "consumer", "stream".

`kafka-streams-programming` description ends with:
> Do NOT trigger for Flink, connectors, CDC, or plain producer/consumer.

`developing-kafka-python-client` description ends with:
> Use when the user wants to build a Python Kafka producer or consumer, add Schema Registry...

Verdict: kafka-streams names "plain producer/consumer" as out-of-scope → pass. The Python client doesn't need to name kafka-streams because Streams is JVM-only and the Python client description is scoped to Python explicitly.

### Pair: `kafka-streams-programming` ↔ `confluent-cloud-cdc-tableflow`

Overlap: "Kafka", "stream".

kafka-streams description: "Do NOT trigger for Flink, connectors, CDC..." → CDC named explicitly.
cdc-tableflow description (per CLAUDE.md exploration): names Flink/Tableflow, doesn't claim Streams territory.

Verdict: pass.

## Writing an anti-trigger clause — how to phrase it

If your script flags a collision, propose a fix that:

1. Names the *thing* the other skill owns, not the other skill itself ("Flink" not "the Flink skill" — the agent might encounter the other skill under a different name in a different deployment).
2. Lists 2–4 specific exclusions, not a vague catch-all.
3. Lives at the *end* of the `description:`, after the positive triggers.

Bad fix:
> Do NOT use for other skills.

Good fix:
> Do NOT trigger for Flink, connectors, CDC, or plain producer/consumer.

## What NOT to flag

- A skill mentions "Kafka" and another mentions "Kafka". One-word overlap on a domain term is not a collision; it would force every skill in a Kafka repo to anti-trigger every other one.
- Skill A targets Python, Skill B targets JVM. If language is named in both descriptions, the agent disambiguates correctly without anti-triggers.
- A skill explicitly anti-triggers a domain that has no current corresponding skill (e.g. "Do NOT trigger for Spark" — Spark isn't in this repo). That's defensive future-proofing, not a violation.

## Limits

The script uses keyword overlap as a proxy. It will miss semantic overlap when two skills use different vocabulary for the same domain (e.g. "schema evolution" vs "compatibility checking"). When in doubt during a review, read both descriptions side by side and ask: *if a user said X, which skill should win?* If you can't answer instantly, the descriptions need more anti-triggering regardless of what the script said.
