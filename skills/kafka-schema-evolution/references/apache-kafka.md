# Apache Kafka / community Schema Registry

"Apache Kafka" here means a cluster **without** Confluent Platform — you are running the
Confluent Community-licensed Schema Registry, or a compatible one (Apicurio in
Confluent-SR-compat mode, Redpanda's SR, Karapace).

## Confluent Community Schema Registry

Same REST API and compatibility engine as CP — everything in `compatibility-modes.md`,
`avro-rules.md`, `json-schema-rules.md`, and `protobuf-rules.md` applies unchanged.

- Endpoint: `http://<host>:8081`, usually unsecured in dev → `$SR_AUTH` is empty.
- No `confluent` CLI assumption; use `curl` + the bundled scripts.
- Data Contracts / `ruleSet` / migration rules / CSFLE / compatibility groups: **not
  available** in the community build. For a breaking change the only options are a new
  subject / new topic or a dual-write transform (`rollout-and-troubleshooting.md`
  § "Breaking changes", options 1 and 3).
- Schema contexts: available in recent community versions (7.4+) but rarely used here.

## Karapace (Aiven)

- REST-compatible with Confluent SR for `config`, `compatibility`, `subjects`.
- Compatibility checking covers Avro and JSON Schema well; Protobuf support has historically
  lagged — verify a Protobuf verdict against the actual Karapace instance, don't trust the
  tables blindly.
- No Data Contracts.

## Apicurio Registry

- Only API-compatible when configured for the **Confluent Schema Registry API** compatibility
  endpoint (`/apis/ccompat/v7`). Set `$SR_URL` to that base path.
- Apicurio's *native* rules (`VALIDITY`, `COMPATIBILITY` with `BACKWARD` etc.) map to the same
  concepts but the native REST shape differs — if the user is on the native API, the `config`
  and `compatibility` calls in this skill won't match; point them at ccompat or their native
  docs.
- No Confluent Data Contracts; Apicurio has its own content-rules system.

## Redpanda Schema Registry

- Confluent SR API compatible for the subset this skill uses (`/config`, `/compatibility`,
  `/subjects`). Avro / Protobuf / JSON Schema compatibility supported.
- No Data Contracts / migration rules.

## What to tell the user

Compatibility *reasoning* (the rules files) is portable. Compatibility *tooling* (CLI,
Data Contracts, Terraform provider, Maven plugin against a secured endpoint) assumes
Confluent. On a community/third-party registry, gate CI with a raw `curl` to
`POST /compatibility/subjects/{subject}/versions/latest?verbose=true` and keep prior schemas
in the repo.
