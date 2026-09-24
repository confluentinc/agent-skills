# README Template

Generate a README.md that includes:

1. **Project title** -- a descriptive name based on the user's data domain (e.g., "IoT Sensor Data
   Kafka Pipeline").
2. **Overview** -- one paragraph: produces to / consumes from Kafka using `Confluent.Kafka` (the
   .NET client) with Confluent Schema Registry (JSON Schema by default).
3. **Prerequisites** -- .NET 8 SDK (the example targets net8.0; Confluent.Kafka also publishes
   net10.0, netstandard2.0, and net462 builds if the project needs a different target), and Docker
   if using local mode or a Confluent Cloud account if using cloud mode.
4. **Setup** section:
   - For **Confluent Cloud**: copy `appsettings.json.example` to `appsettings.json` in the project
     directory, fill in bootstrap server, API keys, and Schema Registry URL (found in the Confluent
     Cloud Console).
   - For **Local Docker**: `docker compose up -d` to start Kafka and Schema Registry, then copy
     `appsettings.json.example` to `appsettings.json` (defaults work as-is).
5. **Build** -- `dotnet build` from the solution root. For the Avro path, generate the C# classes
   first: `avrogen -s Avro/value.avsc Avro/` (install with `dotnet tool install --global
   Apache.Avro.Tools` if not already installed).
6. **Create topic** -- if it doesn't already exist:
   - **Local Docker**: `docker compose exec kafka kafka-topics --create --topic <topic-name> --bootstrap-server localhost:29092`
   - **Confluent Cloud**: create it in the Console, or with the Confluent CLI (after `confluent
     login` and selecting the environment/cluster): `confluent kafka topic create <topic-name>`
7. **Usage** -- commands to run the producer and/or consumer, adapted to what was generated. Since
   multiple classes in the project can define `static Main`, select which one runs with
   `StartupObject`:
   - `dotnet run --project src/<ProjectName> -p:StartupObject=<Namespace>.JsonSchemaProducer`
   - `dotnet run --project src/<ProjectName> -p:StartupObject=<Namespace>.JsonSchemaConsumer`
8. **Schema** -- the JSON Schema is derived from `Value.cs` at registration time (or
   `Avro/value.avsc` for the Avro path). The producer registers it explicitly via
   `RegisterSchemaAsync()` on startup (`AutoRegisterSchemas = false`). Alternatively register it
   manually via the Confluent Cloud Console.
9. **Running tests** -- `dotnet test tests/<ProjectName>.Tests`.
10. **Cleanup** (local Docker only) -- `docker compose down` (mention `-v` to remove stored data).

Adapt the README to match what was actually generated -- omit producer sections if only a consumer
was requested, omit Docker sections for Confluent Cloud projects. Keep it concise and actionable.
Note that `appsettings.json` is gitignored and must never be committed.
