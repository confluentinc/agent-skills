# Protobuf evolution rules

Protobuf wire format identifies fields by **field number**, not name. That makes most
additive changes safe in both directions, but makes a handful of changes silently corrupting.

Confluent algorithm reference:
<https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html#protobuf-schema-compatibility-rules>

## Field-level changes

| Change | BACKWARD | FORWARD | Notes |
|---|---|---|---|
| Add a field (new number) | ✅ | ✅ | proto3: readers without it see the default |
| Remove a field | ✅ | ✅ *if* the number **and** name are `reserved` | Failing to reserve → break |
| Rename a field (same number) | ✅ | ✅ | wire-safe, but breaks JSON encoding + generated code — **flag it** |
| Change field number | ❌ | ❌ | destroys identity; treated as remove + add |
| `int32` ↔ `int64` ↔ `uint32` ↔ `uint64` ↔ `bool` | ✅ | ✅ | all varint on the wire; value truncation possible |
| `sint32` ↔ `sint64` | ✅ | ✅ | zigzag varint |
| `fixed32` ↔ `sfixed32`, `fixed64` ↔ `sfixed64` | ✅ | ✅ | same width |
| `string` ↔ `bytes` | ✅ | ✅ | length-delimited; only valid if bytes are UTF-8 |
| Any other type change | ❌ | ❌ | different wire type |
| `optional` → `repeated` (scalar) | ✅ | ✅ | reader sees last / list |
| Move a field into or out of a `oneof` | ❌ | ❌ | except a single field into its own new `oneof` |
| Add a value to an `enum` | ✅ | ✅ | unknown values preserved as ints in proto3 |
| Remove an `enum` value | ✅ | ✅ *if* `reserved` | |
| Add / remove a nested `message` or `enum` type | ✅ | ✅ | as long as no field's type is orphaned |
| Add / remove an `import` | depends | depends | the imported schema is a separate subject/check |
| Reorder fields in the `.proto` | ✅ | ✅ | number is what matters |
| Change `package` | ❌ | ❌ | changes the message's fully-qualified name |

## Gotchas

- **Always `reserved` on removal**: removing field 4 named `region` requires
  `reserved 4; reserved "region";`. Without it, a future schema could reuse number 4 for a
  different type and old consumers would misread it. The registry enforces this.
- **`optional` in proto3**: the `optional` keyword adds field presence (a synthetic `oneof`).
  Adding `optional` to a pre-existing plain field, or removing it, changes presence semantics —
  the registry may allow it but consumer code behaviour changes.
- **Default values are not on the wire** in proto3. A consumer on a schema that added field 7
  cannot tell "producer set it to 0/empty" from "producer doesn't know field 7". If that
  distinction matters, use `optional` or a wrapper type.
- **JSON serialization** (`PROTOBUF` with the JSON encoder, or downstream Flink/Tableflow)
  keys by field **name** — so a rename that is wire-safe still breaks those consumers.
- **Message references** (`import "common/address.proto";`): the imported schema is registered
  as its own subject. Evolving it is checked against *its* subject's config; pass the
  `references` array in the compatibility call.

## Recommended default posture

`BACKWARD` (or `FULL_TRANSITIVE` for shared contracts). Only add fields with fresh numbers;
`reserved` every removal; never change a field number or a non-varint type.
