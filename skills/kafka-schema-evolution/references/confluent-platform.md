# Confluent Platform (self-managed) — Schema Registry specifics

## Endpoint & auth

- SR endpoint: `http://<sr-host>:8081` (or `https://` with TLS). Config in
  `schema-registry.properties` (`listeners`, `kafkastore.bootstrap.servers`).
- Auth varies by deployment: none, HTTP basic (`SchemaRegistrySimpleAuthenticationProvider`),
  mTLS, or RBAC/MDS bearer tokens. Build `$SR_AUTH` accordingly; never log the credential.
- Env vars assumed: `SR_URL`, and `SR_AUTH` (e.g. `-u user:pass` or `-H "Authorization: ..."`).

## CLI

`confluent schema-registry ...` also works against CP if the CLI is logged in to MDS. Without
the CLI, use the bundled scripts and REST:

```bash
# effective compatibility for a subject
curl -s "$SR_URL/config/orders-value?defaultToGlobal=true" $SR_AUTH

# check without registering
curl -s -X POST "$SR_URL/compatibility/subjects/orders-value/versions/latest?verbose=true" \
  $SR_AUTH -H "Content-Type: application/json" -d @request.json

# set subject compatibility (mutation — confirm first)
curl -s -X PUT "$SR_URL/config/orders-value" $SR_AUTH \
  -H "Content-Type: application/json" -d '{"compatibility":"FULL_TRANSITIVE"}'
```

REST API reference:
<https://docs.confluent.io/platform/current/schema-registry/develop/api.md>

## Maven / Gradle CI gate

CP ships the `kafka-schema-registry-maven-plugin` — the primary way teams gate evolution in
CI. See `rollout-and-troubleshooting.md` § "CI checks". Example binding:

```xml
<plugin>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-schema-registry-maven-plugin</artifactId>
  <version>${confluent.version}</version>
  <configuration>
    <schemaRegistryUrls><param>${env.SR_URL}</param></schemaRegistryUrls>
    <subjects>
      <orders-value>src/main/avro/orders-value.avsc</orders-value>
    </subjects>
  </configuration>
  <executions>
    <execution><goals><goal>test-compatibility</goal></goals></execution>
  </executions>
</plugin>
```

## Version notes

- Schema contexts: CP **7.4+**.
- Data Contracts (`ruleSet`, migration rules, CSFLE): CP **7.4+**, and clients need a serde
  version that understands rules (Confluent serdes 7.4+).
- `?defaultToGlobal=true` on `GET /config/{subject}`: CP **7.3+**. On older CP, a 404 from
  `GET /config/{subject}` means "no override, using global".
- Mode `NONE` and the `_TRANSITIVE` variants: CP 5.4+ (assume present).

## Multi-DC

If Schema Registry runs in multiple data centers with **schema linking** or a single primary,
confirm which registry the producer and consumer each talk to. A consumer querying a lagging
secondary can 404 on a schema ID the producer just registered against the primary.
