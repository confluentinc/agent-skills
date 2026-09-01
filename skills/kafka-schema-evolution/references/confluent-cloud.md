# Confluent Cloud — Schema Registry specifics

## Endpoint & auth

- SR endpoint: Environment → Stream Governance → **API endpoint**
  (`https://psrc-xxxxx.<region>.aws.confluent.cloud`).
- Auth: a **Schema Registry API key/secret** (distinct from cluster API keys), passed as
  HTTP basic auth: `curl -u "$SR_KEY:$SR_SECRET" ...`.
- Env vars this skill assumes: `SR_URL`, `SR_KEY`, `SR_SECRET`. Never print `$SR_SECRET`;
  verify presence with `test -n "$SR_SECRET"`.

## CLI

```bash
confluent login
confluent environment use <env-id>
confluent schema-registry cluster describe          # confirm endpoint

# current compatibility level
confluent schema-registry compatibility level describe --subject orders-value

# set it (mutation — confirm with the user first; prefer --subject over global)
confluent schema-registry compatibility level update FULL_TRANSITIVE --subject orders-value

# validate a candidate schema WITHOUT registering
confluent schema-registry schema validate \
  --schema orders-value.avsc --subject orders-value --type avro

# subjects / versions
confluent schema-registry subject list
confluent schema-registry schema describe --subject orders-value --version latest
```

Flag names drift between CLI versions — verify with `confluent schema-registry <cmd> --help`.
CLI reference: <https://docs.confluent.io/confluent-cli/current/command-reference/schema-registry/index.html>

## Stream Governance packages

- **Essentials**: BACKWARD/FORWARD/FULL and transitive variants, subject config. No contexts,
  no Data Contract rules.
- **Advanced**: schema contexts, Data Contracts (`ruleSet`, migration rules, CSFLE), broader
  schema linking. Compatibility-group breaking-change flow needs Advanced.

## Contexts

Advanced governance can namespace subjects: `:.<context>:<subject>`. The default context is
`.` (implicit). Confirm the user's subject isn't in a non-default context before comparing
versions — `confluent schema-registry ... --context <name>`.

## Terraform

`confluent_subject_config` sets compatibility; `confluent_schema` registers a version.
Managing evolution through Terraform means the compatibility check runs at `apply` — a plan
that would break compatibility fails there. This is a good CI gate, but note Terraform will
want to own the version history.
