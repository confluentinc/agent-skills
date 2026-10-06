# Step 3: Guide the User (Post-Generation)

After generating the files, give instructions based on the target environment. Adapt to what was generated.

**Confluent Cloud:**
1. Copy `appsettings.json.example` to `appsettings.json` inside `src/<ProjectName>/` and fill in
   Confluent Cloud credentials (bootstrap server, API keys, Schema Registry URL -- all in the
   Confluent Cloud Console under cluster/environment settings).
2. Build: `dotnet build`. For the Avro path, run `avrogen -s Avro/value.avsc Avro/` first.
3. If a producer was generated, the schema is registered explicitly on first run via
   `RegisterSchemaAsync()` (`AutoRegisterSchemas = false`). If only a consumer was generated, register
   the schema manually in the Console under Schema Registry for the topic's value subject.
4. Run the producer: `dotnet run --project src/<ProjectName> -p:StartupObject=<Namespace>.JsonSchemaProducer`.
5. Run the consumer: `dotnet run --project src/<ProjectName> -p:StartupObject=<Namespace>.JsonSchemaConsumer`.

**Local Docker:**
1. Start Kafka + Schema Registry: `docker compose up -d`.
2. Copy `appsettings.json.example` to `appsettings.json` (defaults are pre-filled for local Docker).
3. Build: `dotnet build`.
4. Create the topic if auto-creation is disabled: `docker compose exec kafka kafka-topics --create
   --topic demo-topic --bootstrap-server localhost:29092`.
5. Run the producer / consumer as above.
6. When done: `docker compose down` (add `-v` to remove stored data).

**WarpStream:**
1. Copy `appsettings.json.example` to `appsettings.json` and fill in the WarpStream bootstrap server,
   Schema Registry URL, and (if applicable) credentials. Set `CLIENT_ID` to include
   `ws_az=<availability-zone>` for zone-aware routing.
2. Build: `dotnet build`.
3. Create the topic if it doesn't exist.
4. Run the producer / consumer as above.

Remind WarpStream users that produce latency (~250ms p50) is higher than standard Kafka -- this is
expected. If throughput is low, verify the overrides from `references/warpstream-optimization.md` are
applied (especially `EnableIdempotence=false` and large batch/fetch sizes).
