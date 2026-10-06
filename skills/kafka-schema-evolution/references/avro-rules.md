# Avro evolution rules

Avro resolves data using **both** the writer schema (embedded via schema ID) and the reader
schema. Compatibility is about whether the reader can resolve data written by the other side.

Spec: <https://avro.apache.org/docs/current/specification/#schema-resolution>

## Field-level changes

| Change | BACKWARD (new reads old) | FORWARD (old reads new) | FULL |
|---|---|---|---|
| Add field **with** default | ✅ | ✅ | ✅ |
| Add field **without** default | ❌ (old data has no value) | ✅ (old reader ignores it) | ❌ |
| Remove field **with** default | ✅ (new reader ignores it) | ✅ (old reader uses default) | ✅ |
| Remove field **without** default | ✅ | ❌ (old reader has no value) | ❌ |
| Rename field (no alias) | ❌ | ❌ | ❌ |
| Rename field **with** `aliases` on new schema | ✅ | ❌ | ❌ |
| Widen type (`int`→`long`, `int`→`float`, `long`→`double`, `float`→`double`) | ✅ | ❌ | ❌ |
| Narrow type (`long`→`int`, …) | ❌ | ✅ | ❌ |
| Change type to unrelated | ❌ | ❌ | ❌ |
| Add branch to a `union` | ✅ | ❌ | ❌ |
| Remove branch from a `union` | ❌ | ✅ | ❌ |
| Make field nullable: `T` → `["null","T"]` **default `null`** | ✅ | ❌ | ❌ |
| `["null","T"]` → `T` | ❌ | ✅ | ❌ |
| Add `enum` symbol, enum has a default | ✅ | ❌ | ❌ |
| Add `enum` symbol, no enum default | ❌ | ❌ | ❌ |
| Remove `enum` symbol | ❌ | depends on default | ❌ |
| Reorder fields | ✅ | ✅ | ✅ (Avro matches by name) |
| Change field `doc` / add non-reserved property | ✅ | ✅ | ✅ |
| Change `namespace` or top-level `name` | ❌ | ❌ | ❌ |

Legend: ✅ compatible, ❌ incompatible.

## Gotchas

- **Union default position**: a field of type `["null","string"]` must have its default
  (`null`) as the **first** branch's type. `["string","null"]` with default `null` is invalid.
- **`aliases` go on the reader**: to rename `user` → `customer` under BACKWARD, the new schema
  declares `"aliases":["user"]` on the `customer` field (or the record). Old writers still
  emit `user`; the new reader maps it. This does **not** make the rename FORWARD-compatible.
- **Adding a field with default `null` but non-nullable type** is still incompatible — the
  default's type must match the field type. Use `["null","T"]`.
- **Logical types** (`timestamp-millis`, `decimal`, `uuid`): changing the underlying primitive
  or the `decimal` precision/scale is a type change and follows the type-change rows above.
- **Record nested in a field**: the nested record evolves by the same rules, recursively.
- **`default` is only meaningful for the reader schema** — a default on a field that later
  writers omit is what makes removal FORWARD-safe. Add defaults *now* to fields you might
  remove *later*.
- Enum default support requires Avro 1.9.0+ on every consumer; older consumers will still
  fail on an unknown symbol even though the registry accepted the schema.

## Recommended default posture

For BACKWARD (the common case): **every field gets a default.** This makes adds and removes
both safe and buys you FULL later with no schema churn.
