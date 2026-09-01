# JSON Schema evolution rules

**JSON Schema compatibility does not behave like Avro.** Confluent implements a dedicated
diff algorithm. Always confirm a verdict with a live check-without-registering call — offline
reasoning is a guide, not the authority.

Confluent algorithm reference:
<https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-json.html#json-schema-compatibility-rules>

## Content model is the pivot

| `additionalProperties` | Content model | Effect |
|---|---|---|
| `true` or absent (default) | **open** | Consumers tolerate unknown properties |
| `false` | **closed** | Unknown properties are a validation error |

Most compatibility outcomes flip depending on this. Confirm it before answering.

## Field-level changes (typical outcomes)

| Change | BACKWARD | FORWARD | Notes |
|---|---|---|---|
| Add an **optional** property, open model | ✅ | ✅ | The safe, common case |
| Add an **optional** property, closed model | ✅ | ❌ | Old writer can't emit it; closed new reader rejects nothing, but forward check fails on the closed transition |
| Add a **required** property | ❌ | ✅ | Old data lacks it → backward break |
| Remove an **optional** property, open model | ✅ | ✅ | |
| Remove a **required** property | ✅ | ❌ | |
| Add a `required` entry for an existing property | ❌ | ✅ | Tightening |
| Remove a `required` entry (make it optional) | ✅ | ❌ | Loosening |
| Narrow a type (`["string","null"]` → `string`) | ❌ | ✅ | |
| Widen a type (`string` → `["string","null"]`) | ✅ | ❌ | |
| Add `enum` value | ❌ | ✅ | opposite of intuition — a wider enum is a forward change |
| Remove `enum` value | ✅ | ❌ | |
| Tighten `minimum`/`maximum`/`minLength`/`pattern` | ❌ | ✅ | any constraint tightening is backward-incompatible |
| Loosen a numeric/string constraint | ✅ | ❌ | |
| `additionalProperties: true` → `false` | ❌ | ✅ | closing the model is a backward break |
| `additionalProperties: false` → `true` | ✅ | ❌ | |
| Change `$id` / `title` / `description` | ✅ | ✅ | metadata only |
| Reorder `properties` | ✅ | ✅ | order is not significant |

## Gotchas

- **`oneOf` / `anyOf` unions** (used for multi-event topics with a discriminator): adding a
  branch is FORWARD-compatible, removing one is BACKWARD-compatible — same shape as Avro
  unions. Changing an existing branch's sub-schema evolves recursively.
- **`$ref` to a referenced schema**: the referenced schema is its own subject with its own
  compatibility config. Evolving it is a separate check; include the `references` array in
  the compatibility call.
- **Draft version**: Confluent supports draft-07 and 2019-09/2020-12. Changing the declared
  `$schema` draft is treated as a wholesale change and usually fails.
- **`default` in JSON Schema** is advisory for validation but Confluent's serializers do not
  inject it into missing fields the way Avro does — do not rely on it to make a field
  "optional with a value" at read time.
- **Closed content models are brittle to evolve.** If the user controls all clients and wants
  strict validation, recommend `additionalProperties: false` **plus** `FORWARD` or
  `FULL_TRANSITIVE` and a discipline of only-additive optional fields. Otherwise default to
  the open model.

## Recommended default posture

Open content model (`additionalProperties` absent), `BACKWARD`, add only optional properties.
