# Schema Generation Rules

When generating or adapting a schema to the user's domain, follow these rules strictly. Without them,
the schema lacks discoverability, breaks on evolution, and creates governance issues. The default
format for this skill is **JSON Schema**; the Avro rules follow for the alternative path.

## JSON Schema (default -- code-first via `Value.cs`)

Unlike Avro, .NET's JSON Schema path is **code-first**: the C# class is the source of truth, and
`NJsonSchema.JsonSchema.FromType<Value>()` derives the JSON Schema from it at registration time
(the opposite direction from Avro, where the `.avsc` file is the source of truth and the class is
generated FROM it).

1. **A `[Description]` on the class and every property.** These surface in the Schema Registry UI
   and governance tooling, and NJsonSchema maps `System.ComponentModel.DescriptionAttribute` onto the
   generated schema's `description` fields.
2. **PascalCase properties.** Standard C# convention (`OrderId`, not `order_id` or `orderId`). The
   JSON Schema property names in the generated schema will match the C# property names unless you
   add `[JsonPropertyName]` to override them -- don't override unless the user has an existing wire
   format to match.
3. **Defaults on non-identifier properties.** Every property that is not the entity identifier should
   have a sensible default value (`""` for strings, `0` for numbers, `false` for booleans) so
   consumers on an older schema version can still deserialize newer messages missing new fields.
4. **Enums for fixed value sets.** Status codes, event types, and categories should be a C# `enum`;
   NJsonSchema renders it as a JSON Schema `enum` with the member names as string values.
5. **Mark required fields with `[Required]`.** Use `System.ComponentModel.DataAnnotations.Required`
   on identifier fields the message cannot be valid without. Everything else is implicitly optional.
6. **Timestamps as ISO 8601 strings.** Represent points in time as `string` properties holding ISO
   8601 (`DateTime.UtcNow.ToString("O")` produces one). Document the format in the `[Description]`.
7. **Include a nullable field for future extensibility.** A `string?` property (e.g. `Metadata`) gives
   room to add optional context later without a breaking schema change.

## Avro (alternative -- `Avro/value.avsc`)

1. **`doc` everywhere.** The record itself and every field MUST have a `doc`. These surface in the
   Schema Registry UI and governance tooling.
2. **camelCase field names -- and expect camelCase C# properties.** Use camelCase (`transactionId`,
   not `transaction_id`), matching standard Avro convention. Unlike the JSON Schema path, `avrogen`
   does **not** PascalCase-convert field names: a `transactionId` field generates a `transactionId`
   C# property (verbatim casing), not `TransactionId`. Write producer/consumer code against that
   generated casing (`record.transactionId`, not `record.TransactionId`) -- don't assume PascalCase.
3. **`name` and `namespace` drive codegen.** The record `name` becomes the generated C# class name and
   `namespace` its C# namespace -- e.g. `"name": "Transaction"`, `"namespace": "ExampleKafka.Avro"`
   generates `ExampleKafka.Avro.Transaction`. Keep the namespace aligned with the project's structure.
4. **Defaults on non-identifier fields.** Every field that is not the entity identifier MUST have a
   `default`: `""` for strings, `0` for numbers, `false` for booleans, the first symbol for enums, and
   `null` for nullable unions. Backward-compatible evolution requires defaults.
5. **Enums for fixed value sets.** Status codes, event types, and categories MUST use an Avro `enum`
   with explicit `symbols`. The enum `name` generates a C# enum (e.g. `Status.completed`).
6. **Nullable fields use a union with `null` first.** Optional fields use `["null", "<type>"]` with
   `"default": null`. Avro requires `null` to be the first branch when the default is null. Include at
   least one nullable field (e.g. a `metadata` field) for future extensibility.
7. **Timestamps.** Represent points in time as ISO 8601 strings (`"type": "string"`) for portability,
   or use the Avro logical type `{"type": "long", "logicalType": "timestamp-millis"}` when the user
   wants native temporal types. Be consistent and document the choice in the field `doc`.
8. **Generate the C# class with `avrogen` before building.** Install once with
   `dotnet tool install --global Apache.Avro.Tools`, then run `avrogen -s Avro/value.avsc Avro/`
   whenever the schema changes. There is no MSBuild-integrated codegen step (unlike Protobuf's
   `Grpc.Tools`) -- this is a manual step the user must re-run after editing the `.avsc`.

If the user has no specific domain, use a generic event schema with `id`, `type`, `timestamp`, and
`payload` fields -- but still apply every rule above.
