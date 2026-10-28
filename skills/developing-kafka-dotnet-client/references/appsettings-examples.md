# appsettings.json.example Templates

Generate the appropriate `appsettings.json.example` for the target environment. It is a flat JSON
object, loaded by `KafkaConfig.LoadEnv()` via `System.Text.Json`.

**Confluent Cloud:**
```json
{
  "KAFKA_ENV": "cloud",
  "BOOTSTRAP_SERVER": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
  "API_KEY": "your-api-key",
  "API_SECRET": "your-api-secret",
  "TOPIC": "demo-topic",
  "SCHEMA_REGISTRY_URL": "https://psrc-xxxxx.us-east-2.aws.confluent.cloud",
  "SR_API_KEY": "your-sr-api-key",
  "SR_API_SECRET": "your-sr-api-secret",
  "CLIENT_ID": "dotnet-client",
  "GROUP_ID": "dotnet-consumer-group"
}
```

**Local Docker:**
```json
{
  "KAFKA_ENV": "local",
  "BOOTSTRAP_SERVER": "localhost:9092",
  "TOPIC": "demo-topic",
  "SCHEMA_REGISTRY_URL": "http://localhost:8081",
  "CLIENT_ID": "dotnet-client",
  "GROUP_ID": "dotnet-consumer-group"
}
```

**WarpStream:**
```json
{
  "KAFKA_ENV": "warpstream",
  "BOOTSTRAP_SERVER": "your-warpstream-bootstrap-url:9092",
  "TOPIC": "demo-topic",
  "SCHEMA_REGISTRY_URL": "http://your-schema-registry:8081",
  "CLIENT_ID": "dotnet-client,ws_az=us-east-1a",
  "GROUP_ID": "dotnet-consumer-group"
}
```
If the WarpStream deployment requires SASL auth or a Confluent Cloud Schema Registry, add `API_KEY`/
`API_SECRET` and/or `SR_API_KEY`/`SR_API_SECRET` as in the Confluent Cloud example.
