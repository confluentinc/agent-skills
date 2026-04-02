---
name: schema-registry-adopting
description: Scan a project to identify Kafka applications, extract schemas from data models, tag PII fields, generate Terraform for Confluent Schema Registry registration, and produce a migration report with rollout ordering. Use this skill when a user asks to analyze a folder or repo for Kafka usage, extract schemas, audit producer/consumer configurations, or generate Terraform for Schema Registry.
---

# Schema Registry Adoption

Scan a project to identify Kafka applications, extract schemas, generate Terraform for Schema Registry registration, and produce a comprehensive analysis report.

## When to Use

Invoke this skill when:
- A user asks to analyze a project for Kafka usage
- A user wants to extract schemas from Kafka producers
- A user wants Terraform to register schemas to Confluent Schema Registry
- A user wants to audit Kafka producer/consumer configurations

## Deliverables

This skill produces 3 outputs in the target project:

1. **`schema-report.md`** — Full analysis report with findings, risks, and upgrade recommendations
2. **`schemas/`** — Extracted schema files (Avro, JSON Schema, Protobuf)
3. **`terraform/`** — Terraform configs using the Confluent provider to register schemas

---

## Phase 0: Initialize

**If `schema_status` MCP tool is available:**
```
Call schema_status with:
  path: <project root>
```
This provides context on any existing schema project configuration (schema.yaml, registered schemas, environments). Use this to avoid duplicating work or conflicting with existing schema management.

**If MCP tools are not available:**
- Check if a `schema.yaml` file already exists in the project
- Check if a `schemas/` directory already exists
- Note any existing schema infrastructure in the report

---

## Phase 1: Project Scan & Kafka Detection

### 1.1 Find Build Files & Dependencies

Search the project for build/dependency files and check for Kafka libraries.

**Glob patterns to search:**
```
**/pom.xml
**/build.gradle
**/build.gradle.kts
**/requirements.txt
**/pyproject.toml
**/setup.py
**/setup.cfg
**/Pipfile
**/*.csproj
**/packages.config
**/Directory.Packages.props
**/go.mod
**/package.json
```

**Dependency patterns to match:**

| Language | Dependency Strings |
|----------|-------------------|
| Java | `spring-kafka`, `kafka-clients`, `kafka-streams`, `spring-cloud-stream`, `io.confluent`, `confluent-kafka` |
| Python | `confluent-kafka`, `confluent_kafka`, `kafka-python`, `faust-streaming`, `faust` |
| .NET | `Confluent.Kafka`, `Confluent.SchemaRegistry`, `Confluent.SchemaRegistry.Serdes` |
| Go | `confluent-kafka-go`, `github.com/Shopify/sarama`, `github.com/IBM/sarama`, `github.com/segmentio/kafka-go` |
| Node/TS | `kafkajs`, `node-rdkafka`, `@confluentinc/kafka-javascript`, `kafka-node` |

### 1.2 Find Producer & Consumer Code

For each app with Kafka dependencies, search source files for producer/consumer patterns.

**Producer detection patterns (grep):**

| Language | Patterns |
|----------|----------|
| Java | `KafkaTemplate`, `KafkaProducer`, `ProducerRecord`, `@SendTo`, `StreamBridge`, `ProducerFactory`, `KStream`, `KTable`, `StreamsBuilder`, `.to(`, `.through(` |
| Python | `Producer(`, `SerializingProducer(`, `AvroProducer(`, `.produce(`, `send(topic` |
| .NET | `ProducerBuilder`, `IProducer`, `ProduceAsync`, `.Produce(` |
| Go | `kafka.NewProducer`, `sarama.NewSyncProducer`, `sarama.NewAsyncProducer`, `kafka.NewWriter` |
| Node/TS | `producer.send(`, `kafka.producer(`, `producer.produce(`, `.sendBatch(` |

**Consumer detection patterns (grep):**

| Language | Patterns |
|----------|----------|
| Java | `@KafkaListener`, `KafkaConsumer`, `ConsumerRecords`, `KafkaMessageListenerContainer`, `ConcurrentMessageListenerContainer` |
| Python | `Consumer(`, `DeserializingConsumer(`, `AvroConsumer(`, `.subscribe(`, `.poll(` |
| .NET | `ConsumerBuilder`, `IConsumer`, `.Consume(`, `ConsumerConfig` |
| Go | `kafka.NewConsumer`, `sarama.NewConsumerGroup`, `kafka.NewReader`, `.ReadMessage(` |
| Node/TS | `consumer.run(`, `kafka.consumer(`, `consumer.subscribe(`, `eachMessage` |

### 1.3 Extract Topic Names

Search for topic names in:
- String literals passed to `send()`, `produce()`, `ProducerRecord`, `@KafkaListener`, `@SendTo`
- Configuration properties: `spring.kafka.template.default-topic`, `TOPIC_NAME`, topic config constants
- YAML/properties files: `spring.kafka.consumer.topics`, `spring.kafka.producer.topic`
- Environment variables referenced for topics

### 1.4 Identify Serializers

Search for serializer configuration to determine the data format:

**Grep patterns:**
```
key.serializer
value.serializer
key.deserializer
value.deserializer
KafkaAvroSerializer
KafkaJsonSchemaSerializer
KafkaProtobufSerializer
StringSerializer
ByteArraySerializer
JsonSerializer
AvroSerializer
ProtobufSerializer
HeaderSchemaIdSerializer
schema.registry.url
SchemaRegistryClient
CachedSchemaRegistryClient
```

**Determine format from serializer:**

| Serializer Found | Schema Format | SR Integrated? |
|-----------------|---------------|----------------|
| `KafkaAvroSerializer` / `AvroSerializer` | AVRO | Yes |
| `KafkaJsonSchemaSerializer` / `JsonSchemaSerializer` | JSON | Yes |
| `KafkaProtobufSerializer` / `ProtobufSerializer` | PROTOBUF | Yes |
| `StringSerializer` + JSON data in code | JSON (infer) | No — flag for upgrade |
| `ByteArraySerializer` + Avro in code | AVRO (infer) | No — flag for upgrade |
| `JsonSerializer` (Spring default) | JSON (infer) | No — flag for upgrade |
| Custom serializer (see 1.4b) | Infer from code | No — flag for upgrade |
| No serializer / raw produce | JSON (infer) | No — flag for upgrade |

### 1.4b Detect Custom Serializers

Search the project for classes/functions that implement serialization interfaces but do **not** use Confluent Schema Registry. These are producers serializing data themselves — bypassing SR governance entirely.

**Java — Custom serializer detection (grep):**
```
implements Serializer<
implements Serializer\b
extends Serializer<
class.*Serializer.*implements
org.apache.kafka.common.serialization.Serializer
```

Look for classes that:
- Implement `org.apache.kafka.common.serialization.Serializer<T>`
- Contain `serialize(String topic,` method
- Use `ObjectMapper`, `Gson`, `Jackson`, `org.json`, or manual JSON construction inside `serialize()`
- Use `GenericDatumWriter`, `SpecificDatumWriter`, `BinaryEncoder`, or manual Avro serialization inside `serialize()`
- Use `com.google.protobuf`, `toByteArray()`, or manual Protobuf serialization inside `serialize()`
- Do NOT reference `schema.registry.url`, `SchemaRegistryClient`, or any Confluent SR class

**Determine the data format inside the custom serializer:**
- If it uses `ObjectMapper`, `Gson`, `org.json`, `Jackson` → JSON format
- If it uses `GenericDatumWriter`, `SpecificDatumWriter`, `DatumWriter`, `BinaryEncoder`, `avro` imports → AVRO format
- If it uses `com.google.protobuf`, `toByteArray()`, `Parser`, `GeneratedMessageV3` → PROTOBUF format
- Record the format — it determines the upgrade recommendation (see Phase 4)

**Python — Custom serializer detection (grep):**
```
def serializer(
def serialize(
def value_serializer(
json.dumps.*produce
json.dumps.*send
msgpack.pack
pickle.dumps
fastavro
avro.io
DatumWriter
BinaryEncoder
```

Look for:
- Lambda or function passed as `value_serializer=` to Producer config
- Inline `json.dumps()` calls in `produce()` or `send()` arguments
- `fastavro.write` or `avro.io.DatumWriter` / `BinaryEncoder` for manual Avro serialization
- Custom functions that convert objects to bytes without SR

**Format determination for Python:**
- `json.dumps()` → Generate **JSON Schema** (`.json` extension)
- `fastavro` or `avro.io` → Generate **Avro** (`.avsc` extension)
- `protobuf` → Generate **Protobuf** (`.proto` extension)

**.NET — Custom serializer detection (grep):**
```
ISerializer<
IAsyncSerializer<
class.*:.*ISerializer
class.*:.*IAsyncSerializer
JsonConvert.SerializeObject
System.Text.Json.JsonSerializer.Serialize
Avro.IO
Avro.Specific
Avro.Generic
Google.Protobuf
```

Look for classes implementing `ISerializer<T>` or `IAsyncSerializer<T>` that use `Newtonsoft.Json`, `System.Text.Json`, `Apache.Avro`, or `Google.Protobuf` without `SchemaRegistryClient`.

**Format determination for .NET:**
- `JsonConvert` (Newtonsoft.Json) or `System.Text.Json` → Generate **JSON Schema** (`.json` extension)
- `Avro.IO`, `Avro.Specific`, or `Avro.Generic` → Generate **Avro** (`.avsc` extension)
- `Google.Protobuf` → Generate **Protobuf** (`.proto` extension)

**Go — Custom serializer detection (grep):**
```
json.Marshal
json.NewEncoder
encoding/json
proto.Marshal
goavro
avro.Marshal
avro.NewCodec
```

Look for `json.Marshal()`, `proto.Marshal()`, `goavro` codec, or similar called directly before `Produce()` without SR integration.

**Format determination for Go:**
- `json.Marshal()` or `encoding/json` → Generate **JSON Schema** (`.json` extension)
- `goavro`, `avro.Marshal`, or `avro.NewCodec` → Generate **Avro** (`.avsc` extension)
- `proto.Marshal()` → Generate **Protobuf** (`.proto` extension)

**Node/TS — Custom serializer detection (grep):**
```
JSON.stringify.*send
JSON.stringify.*produce
Buffer.from.*JSON
serialize.*value
```

Look for `JSON.stringify()` inline in `producer.send({ value: ... })` calls.

**Classification:** Any producer using a custom serializer without SR integration is **Category E** (see Phase 4). The data model being serialized inside the custom serializer is the schema source — extract it.

### 1.5 Build App Catalog

Compile findings into a structured catalog:

```
For each Kafka application found:
  - app_name: directory or module name
  - language: Java | Python | .NET | Go | Node/TS
  - role: producer | consumer | both
  - topics: [list of topic names]
  - serializer_class: the value.serializer being used
  - custom_serializer: true | false (implements Serializer interface or inline serialization)
  - custom_serializer_file: file:line where custom serializer is defined
  - schema_format: AVRO | JSON | PROTOBUF | UNKNOWN
  - sr_integrated: true | false
  - sr_url: schema registry URL if configured
  - auto_register: true | false
  - category: A | B | C | D | E (see Phase 4)
```

**IMPORTANT:** The `category` field MUST be populated for every application. This category classification will be used throughout the report and Terraform comments. See Phase 4 for category definitions.

### 1.6 Detect Multi-Schema Topics

After building the app catalog, check if **multiple data models produce to the same topic**.
This happens when different services (or different code paths in the same service) send
different event types to a single topic.

**How to detect:**
1. Group all producers by topic name from the catalog
2. For each topic, check if there are multiple producers with **different** data models
3. Same data model to same topic = normal (just dedup the schema)
4. Different data models to same topic = **multi-schema topic** — requires special handling

**What to look for:**
- Two producers with different generic types: `KafkaTemplate<String, OrderEvent>` and `KafkaTemplate<String, PaymentEvent>` both sending to `"transaction-events"`
- Two services with different Pydantic models / structs producing to the same topic
- A single producer that sends different types conditionally: `if (type == "user") send(topic, userEvent) else send(topic, paymentEvent)`

**When a multi-schema topic is found:**

1. Register each event type as its own subject (not the topic-based subject):
   - e.g., `UserEvent` → subject `user-event`, `PaymentEvent` → subject `payment-event`

2. Create a **wrapper schema** using `oneOf` (JSON Schema), union (Avro), or `oneof` (Protobuf)
   that references the individual event schemas. Register it as the topic subject:

   **JSON Schema wrapper:**
   ```json
   {
     "$schema": "http://json-schema.org/draft-07/schema#",
     "title": "{TopicName}Event",
     "oneOf": [
       { "$ref": "{event-type-1}.json" },
       { "$ref": "{event-type-2}.json" }
     ]
   }
   ```

   **Avro wrapper:**
   ```json
   [
     "{namespace}.EventType1",
     "{namespace}.EventType2"
   ]
   ```

   **Protobuf wrapper:**
   ```protobuf
   import "{event_type_1}.proto";
   import "{event_type_2}.proto";

   message {TopicName}Event {
     oneof event {
       EventType1 type1 = 1;
       EventType2 type2 = 2;
     }
   }
   ```

3. Generate Terraform with `schema_reference` blocks:
   ```hcl
   # Individual event schemas registered first
   resource "confluent_schema" "user_event" {
     subject_name = "user-event"
     format       = "{FORMAT}"
     schema       = file("../schemas/{dir}/user-event.{ext}")
   }

   resource "confluent_schema" "payment_event" {
     subject_name = "payment-event"
     format       = "{FORMAT}"
     schema       = file("../schemas/{dir}/payment-event.{ext}")
   }

   # Wrapper schema with references
   resource "confluent_schema" "{topic}_value" {
     subject_name = "{topic}-value"
     format       = "{FORMAT}"
     schema       = file("../schemas/{dir}/{topic}-value.{ext}")

     schema_reference {
       name         = "{reference_name}"
       subject_name = confluent_schema.user_event.subject_name
       version      = confluent_schema.user_event.version
     }

     schema_reference {
       name         = "{reference_name}"
       subject_name = confluent_schema.payment_event.subject_name
       version      = confluent_schema.payment_event.version
     }
   }
   ```

4. Flag multi-schema topics prominently in the report with a cross-reference table

**Same data model, multiple topics (dedup):**
If the same class produces to multiple topics (e.g., `order-events` and `order-events-dlq`),
generate one schema file and multiple Terraform `confluent_schema` resources pointing to the
same file:
```hcl
resource "confluent_schema" "order_events_value" {
  subject_name = "order-events-value"
  schema       = file("../schemas/json/order-event.json")
  ...
}

resource "confluent_schema" "order_events_dlq_value" {
  subject_name = "order-events-dlq-value"
  schema       = file("../schemas/json/order-event.json")  # same file
  ...
}
```

---

## Phase 2: Risk Detection — `auto.register.schemas=true`

### 2.1 Scan for auto-registration

Search **all files** in the project for auto-register patterns:

**Grep patterns (case-insensitive):**
```
auto.register.schemas\s*=\s*true
auto\.register\.schemas.*true
AutoRegisterSchemas\s*=\s*true
auto_register_schemas.*True
autoRegisterSchemas.*true
```

**Files to prioritize:**
```
**/*.properties
**/*.yml
**/*.yaml
**/application*.properties
**/application*.yml
**/*.java
**/*.py
**/*.cs
**/*.go
**/*.ts
**/*.js
**/*.json (config files)
```

### 2.2 Scan for `use.latest.version`

Also search for `use.latest.version` configuration — this is relevant for migration planning:

**Grep patterns:**
```
use.latest.version\s*=\s*true
use\.latest\.version.*true
UseLatestVersion\s*=\s*true
```

If a producer has `auto.register.schemas=true` but also `use.latest.version=true`, the migration to Terraform-managed schemas is simpler — the producer will automatically pick up the latest schema version after auto-register is disabled.

### 2.3 Record Each Occurrence

For each match, record:
- File path and line number
- The application it belongs to (from Phase 1 catalog)
- Associated topic(s)
- Whether it's in production config or test config
- Whether `use.latest.version` is also set (eases migration)

---

## Phase 3: Schema Inference

For each **producer** identified in Phase 1, extract or infer a schema.

### 3.1 Check for Existing Schema Files

Search the project for existing schema definitions:

```
**/*.avsc          (Avro schema)
**/*.avro          (Avro schema)
**/*.proto         (Protobuf)
**/schema*.json    (JSON Schema)
**/*.schema.json   (JSON Schema)
**/schemas/**      (schema directories)
**/avro/**         (Avro directories)
```

If found, map them to the topics they serve by checking:
- File names matching topic names
- Import/reference paths in producer code
- Schema registry subject naming (`{topic}-value`, `{topic}-key`)

### 3.2 Infer from Data Models

If no schema files exist, find the data classes/models being serialized and convert them to schemas.

**Java — Find data classes:**
- Classes used as generic type in `KafkaTemplate<K, V>` or `ProducerRecord<K, V>`
- Classes with `@JsonProperty`, `@JsonInclude`, Jackson annotations
- Avro-generated classes extending `SpecificRecord`
- Protobuf-generated classes extending `GeneratedMessageV3`
- Java Records used in producer calls
- POJOs with getters/setters passed to `send()`

**Python — Find data models:**
- `@dataclass` decorated classes used in `produce()` calls
- Pydantic `BaseModel` subclasses
- `TypedDict` definitions
- Named tuples
- Dict literals passed to `produce()` — infer field types from values
- Avro schema dicts defined in code (`{"type": "record", ...}`)

**.NET — Find data models:**
- Classes/records with `[JsonProperty]`, `[DataMember]`, or `[ProtoMember]` attributes
- Types used as generic parameter in `IProducer<TKey, TValue>`
- Classes in a `Models` or `Events` namespace near producer code

**Go — Find data structs:**
- Struct types with `json:"field_name"` tags
- Struct types used in `Produce()` calls after `json.Marshal()`
- Struct types with `avro:"field_name"` tags

**TypeScript/Node — Find type definitions:**
- Interfaces or types used in `producer.send({ value: ... })`
- Zod schemas (`z.object({...})`)
- io-ts codecs
- JSON objects passed directly to send

### 3.2b Infer from Inline Key-Value Data (No Class/Model)

If a producer sends data as a raw map, dictionary, or inline JSON object — with no typed class — infer the schema from the code that constructs the data.

**Java — HashMap / Map.of / JSONObject:**
```
// Detection patterns (grep)
new HashMap<>
Map.of(
Map.ofEntries(
new JSONObject(
put("field_name",
```

Look for:
- `Map<String, Object>` or `HashMap<>` populated with `.put("key", value)` near `send()` / `ProducerRecord`
- `Map.of("key1", val1, "key2", val2)` passed directly to send
- `new JSONObject().put("key", value)` chains
- Infer field names from the string keys in `.put()` calls
- Infer types from the values: string literals → `string`, numeric literals → `number`/`integer`, boolean → `boolean`, variables → trace the variable type

**Python — dict literals / dict construction:**
```
# Detection patterns (grep)
produce.*{
send.*{
json.dumps.*{
dict(
```

Look for:
- Dict literals `{"key": value, ...}` passed to `produce()`, `send()`, or `json.dumps()`
- `dict(key=value, ...)` construction
- Dicts built incrementally: `data = {}; data["key"] = value`
- Infer field names from dict keys, types from values

**Go — map[string]interface{} / map[string]any:**
```
// Detection patterns (grep)
map\[string\]interface
map\[string\]any
```

Look for:
- `map[string]interface{}` or `map[string]any` populated with string keys
- Inline map literals: `map[string]any{"key": value, ...}`
- Infer field names from keys, types from values

**Node/TS — plain objects:**
```
// Detection patterns (grep)
producer.send.*value:.*{
send.*{
```

Look for:
- Object literals passed directly to `producer.send({ value: { key: val, ... } })`
- Variables assigned an object literal then passed to send
- Infer field names from property names, types from values or TypeScript type annotations

**.NET — Dictionary / anonymous objects:**
```
// Detection patterns (grep)
new Dictionary<string
new {
anonymous
```

Look for:
- `Dictionary<string, object>` with `.Add("key", value)` or initializer syntax
- Anonymous objects `new { key = value, ... }` serialized and sent
- Infer field names from keys/properties, types from values

**Other inline data patterns to detect (all languages):**

**JSON string construction (manual JSON building):**
```
// Java
String json = "{\"order_id\":\"" + orderId + "\",\"amount\":" + amount + "}";
String.format("{\"order_id\":\"%s\",\"amount\":%f}", orderId, amount);
new StringBuilder().append("{\"order_id\":\"").append(orderId)...

# Python
f'{{"order_id": "{order_id}", "amount": {amount}}}'
'{"order_id": "%s"}' % order_id
"{\"order_id\": \"" + order_id + "\"}"

// Go
fmt.Sprintf(`{"order_id":"%s","amount":%f}`, orderID, amount)

// Node/TS
`{"order_id": "${orderId}", "amount": ${amount}}`
```

Infer field names from the JSON keys in the string. Infer types from the interpolated variables.

**JSON tree / node APIs (building JSON without a class):**
```
// Java — Jackson JsonNode / ObjectNode
ObjectNode node = mapper.createObjectNode();
node.put("order_id", orderId);
node.put("amount", amount);

// Java — Gson JsonObject
JsonObject obj = new JsonObject();
obj.addProperty("order_id", orderId);

// .NET — JObject (Newtonsoft) / JsonNode (System.Text.Json)
var obj = new JObject { ["order_id"] = orderId, ["amount"] = amount };
var node = new JsonObject { ["order_id"] = orderId };

// Go — map or gjson
data := map[string]interface{}{"order_id": id, "amount": amt}
```

Infer fields from `.put()`, `.addProperty()`, or property assignments.

**Builder / fluent patterns:**
```
// Java
Event.builder().orderId(id).amount(amt).build();
new EventBuilder().setOrderId(id).setAmount(amt).build();

// Kotlin
Event(orderId = id, amount = amt)

// Scala
Event(orderId = id, amount = amt)
case class Event(orderId: String, amount: Double)
```

Trace the builder class to find all setter methods — each setter corresponds to a field.

**Database row / ORM object forwarding:**
```
// Java — JPA/Hibernate entity sent to Kafka
kafkaTemplate.send("topic", entity.getId(), objectMapper.writeValueAsString(entity));

# Python — SQLAlchemy / Django model
producer.produce("topic", json.dumps(model.__dict__))
producer.produce("topic", json.dumps(model_to_dict(instance)))

// Go — GORM / sqlx struct
json.Marshal(dbRow)

// Node — Sequelize / Prisma
producer.send({ value: JSON.stringify(dbRecord) })
```

Look for the ORM model / entity class definition — it IS the schema. Extract fields from the entity annotations (`@Column`, `@Field`, model fields).

**Protobuf builders without SR:**
```
// Java
MyEvent.newBuilder().setOrderId(id).setAmount(amt).build();
producer.send(new ProducerRecord<>("topic", event.toByteArray()));

# Python
event = MyEvent()
event.order_id = id
producer.produce("topic", event.SerializeToString())

// Go
event := &pb.MyEvent{OrderId: id, Amount: amt}
data, _ := proto.Marshal(event)
```

The `.proto` file IS the schema — find it via the generated class import path. This is Category E (custom Protobuf serialization without SR).

**CSV / delimited strings:**
```
// Java
String csv = orderId + "," + amount + "," + email;
producer.send(new ProducerRecord<>("topic", csv));

# Python
producer.produce("topic", f"{order_id},{amount},{email}".encode())
```

Look for string joining with delimiters (`,`, `|`, `\t`) near `send()`/`produce()`. Field names are not in the data — check for comments, header rows, or variable names to infer them. This is **Category D** if field names cannot be determined.

**How to build the schema from any of these patterns:**
1. Collect all field names from keys, setters, properties, or interpolated variable names
2. For each field, determine the value type:
   - String literal or `String` variable → `"type": "string"`
   - Integer literal or `int`/`long` variable → `"type": "integer"`
   - Float/double literal → `"type": "number"`
   - Boolean → `"type": "boolean"`
   - Nested map/dict/object → `"type": "object"` with nested properties
   - List/array → `"type": "array"`
   - If type is ambiguous (e.g., `Object`, `interface{}`, `any`), default to `"type": "string"` and add a TODO comment
3. Mark fields as `required` if they are always set (not conditionally)
4. Tag PII fields using the patterns in section 3.3b
5. Classify as **Category B** if schema can be inferred, **Category D** if field names cannot be determined (e.g., raw CSV with no header)

### 3.3 Convert Data Models to Schemas

For each data model found, generate a schema file. **Tag potential PII fields** with `confluent:tags` (see 3.3b).

**To JSON Schema:**
- Map language types to JSON Schema types: `string→string`, `int/long→integer`, `float/double→number`, `boolean→boolean`, `List→array`, `Map→object`
- Include `required` array for non-nullable fields
- Add `$schema: "http://json-schema.org/draft-07/schema#"`
- Add `title` matching the class/model name
- Add `confluent:tags` to PII fields (see 3.3b)

Example with PII tags:
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Customer",
  "type": "object",
  "properties": {
    "customer_id": { "type": "string" },
    "email": {
      "type": "string",
      "confluent:tags": ["PII"]
    },
    "phone_number": {
      "type": "string",
      "confluent:tags": ["PII"]
    },
    "order_total": { "type": "number" }
  },
  "required": ["customer_id", "email"]
}
```

**To Avro:**
- Use `type: "record"` with `namespace` from package/module
- Map types: `String→string`, `int→int`, `long→long`, `float→float`, `double→double`, `boolean→boolean`, `List→array`, `Map→map`
- Use `["null", "type"]` union for nullable/optional fields with `"default": null`
- Add `confluent:tags` to PII fields (see 3.3b)

Example with PII tags:
```json
{
  "type": "record",
  "name": "Customer",
  "namespace": "com.example.events",
  "fields": [
    { "name": "customer_id", "type": "string" },
    {
      "name": "email",
      "type": "string",
      "confluent:tags": ["PII"]
    },
    {
      "name": "ssn",
      "type": "string",
      "confluent:tags": ["PII", "PRIVATE"]
    },
    { "name": "order_total", "type": "double" }
  ]
}
```

**To Protobuf:**
- Use `syntax = "proto3"`
- Map types: `String→string`, `int→int32`, `long→int64`, `float→float`, `double→double`, `boolean→bool`, `List→repeated`, `Map→map<K,V>`
- Add `package` from namespace
- Add `confluent:tags` via field meta annotations (see 3.3b)

Example with PII tags:
```protobuf
syntax = "proto3";

package com.example.events;

import "confluent/meta.proto";

message Customer {
  string customer_id = 1;
  string email = 2 [(confluent.field_meta).tags = "PII"];
  string ssn = 3 [
    (confluent.field_meta).tags = "PII",
    (confluent.field_meta).tags = "PRIVATE"
  ];
  double order_total = 4;
}
```

### 3.3b Tag Potential PII Fields

When generating schemas, scan every field name for potential PII and add `confluent:tags`. This enables Confluent Stream Governance for data classification, masking, and compliance.

**PII field name patterns (case-insensitive):**

The "Tag" column below shows the EXACT tags to use in `confluent:tags`. Use these exact values, not derivative tags.

| Pattern | Tag | Examples |
|---------|-----|---------|
| `email`, `e_mail`, `email_address`, `emailAddress` | `PII` | user_email, contact_email |
| `phone`, `phone_number`, `phoneNumber`, `mobile`, `telephone`, `tel` | `PII` | home_phone, mobile_number |
| `ssn`, `social_security`, `socialSecurity`, `social_security_number` | `PII`, `PRIVATE` | ssn_last4 |
| `name`, `first_name`, `firstName`, `last_name`, `lastName`, `full_name`, `fullName` | `PII` | customer_name, user_first_name |
| `address`, `street`, `city`, `state`, `zip`, `zip_code`, `zipCode`, `postal_code`, `postalCode` | `PII` | billing_address, shipping_street |
| `date_of_birth`, `dateOfBirth`, `dob`, `birth_date`, `birthday` | `PII` | customer_dob |
| `ip`, `ip_address`, `ipAddress`, `client_ip`, `remote_addr` | `PII` | source_ip, request_ip |
| `credit_card`, `creditCard`, `card_number`, `cardNumber`, `ccn`, `pan` | `PII`, `PRIVATE` | payment_card_number |
| `passport`, `passport_number`, `passportNumber` | `PII`, `PRIVATE` | |
| `driver_license`, `driverLicense`, `license_number` | `PII`, `PRIVATE` | |
| `account_number`, `accountNumber`, `bank_account`, `iban`, `routing_number` | `PII`, `PRIVATE` | |
| `password`, `secret`, `token`, `api_key`, `apiKey` | `PRIVATE` | auth_token, access_key |
| `salary`, `income`, `compensation`, `wage` | `SENSITIVE` | annual_salary |
| `gender`, `sex`, `race`, `ethnicity`, `religion`, `nationality` | `SENSITIVE` | |
| `medical`, `diagnosis`, `prescription`, `health` | `SENSITIVE`, `PHI` | medical_record |

**Supported Confluent tag values:**

| Tag | Meaning |
|-----|---------|
| `PII` | Personally Identifiable Information — can identify an individual |
| `PRIVATE` | Highly sensitive — should be encrypted or masked |
| `SENSITIVE` | Sensitive but not directly identifying |
| `PHI` | Protected Health Information (HIPAA) |
| `PUBLIC` | Safe for broad access |

**How to apply tags:**

**CRITICAL:** Use ONLY the exact tag values from the "Supported Confluent tag values" table above (PII, PRIVATE, SENSITIVE, PHI, PUBLIC). Do NOT create custom tags like "SSN", "EMAIL", "NAME" - these are not valid Confluent tags.

- **Avro:** Add `"confluent:tags": ["PII"]` as a sibling to `name` and `type` on the field
  - For multiple tags: `"confluent:tags": ["PII", "PRIVATE"]`
- **JSON Schema:** Add `"confluent:tags": ["PII"]` as a sibling to `type` on the property
  - For multiple tags: `"confluent:tags": ["PII", "PRIVATE"]`
- **Protobuf:** Add `[(confluent.field_meta).tags = "PII"]` after the field number; must import `confluent/meta.proto`
  - For multiple tags, add multiple tag attributes

**Report PII findings:** In the report, add a PII summary table showing all tagged fields, their schemas, and the tags applied. This gives teams visibility into what PII is flowing through Kafka.

### 3.4 Infer from Sample Data

If sample data files exist (`.json`, `.ndjson`, test fixtures):

**If `schema_infer` MCP tool is available:**
```
Call schema_infer with:
  path: <path to sample data file>
  format: json (default) | avro | protobuf
  name: <schema name based on topic>
```

**If MCP tools are not available:**
- Read the sample data file
- Manually infer field names, types, required/optional from the JSON structure
- Generate a JSON Schema (draft-07) or Avro schema by hand based on the data shape
- Note in the report that schemas were inferred manually and should be reviewed

### 3.5 Validate Schemas

After extracting/generating schemas:

**If `schema_lint` MCP tool is available:**
```
Call schema_lint with:
  path: <schema file or schemas/ directory>
  fix: true
```
Fix any warnings — they prevent real problems during schema evolution. See [Schema Registry compatibility types](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) for details on BACKWARD, FORWARD, and FULL compatibility modes.

**If MCP tools are not available:**
- Manually review each schema for:
  - Missing `default` values on optional fields (required for backward-compatible evolution)
  - Fields that may contain PII (email, phone, ssn, address, name) — add documentation
  - Naming conventions (camelCase or snake_case consistency)
  - Missing `doc` / `description` on fields
- Add a note in the report: "Schemas were not machine-validated. Run `schema_lint` before registering."

---

## Phase 4: Categorize Producers

Classify each producer into a category based on findings:

| Category | Criteria | Action |
|----------|----------|--------|
| **A: Compliant** | Uses Confluent serializer + schema.registry.url configured + no auto.register | Report as compliant. Still extract schema to Terraform if not already managed by IaC. |
| **A→Header: Already on SR, migrating to headers** | Uses Confluent serializer + SR, wants to move schema ID from payload prefix to Kafka headers | No schema extraction needed. Add `HeaderSchemaIdSerializer` to producers. Consumers need no changes — Confluent deserializers on supported versions automatically check both headers and payload for schema ID. See rollout ordering below. |
| **B: Schema in code, no SR** | Has data models/classes but uses StringSerializer, JsonSerializer (Spring), kafka-python, kafkajs raw, or no Confluent SR integration | Extract schema → `terraform/schemas.tf` + add upgrade recommendation to report |
| **C: Auto-register** | Has `auto.register.schemas=true` | Extract schema → `terraform/flagged-auto-register.tf` (commented out) + flag risk in report |
| **D: No schema** | Raw strings/bytes, no discernible data model, hardcoded JSON strings | Flag in report with recommendation to adopt schema-first approach |
| **E: Custom serializer** | Implements `Serializer<T>` interface, uses `json.dumps`/`JSON.stringify`/`JsonConvert`/`json.Marshal`/`GenericDatumWriter`/`fastavro`/`proto.Marshal` inline, or has a custom serialization function — all without SR | Extract schema from the data model inside the custom serializer → `terraform/schemas.tf` + recommend replacing with Confluent serializer + `HeaderSchemaIdSerializer`. Consumers must be upgraded first using a composite deserializer pattern (Java). See upgrade rules below. |

**CRITICAL - CATEGORY LABELING REQUIREMENTS:**

After categorizing each producer, you MUST use the exact phrase "Category X" (where X = A/B/C/D/E) in EVERY location below. Do NOT paraphrase or substitute (e.g., don't say "auto-registration pattern" instead of "Category C").

Required locations for explicit category labels:
1. App catalog (Phase 1.5) - internal data structure field: `category: "C"`
2. **"Applications Discovered" table** (Phase 7) - Category column showing the letter
3. **Report section headers** - When describing an app, state its category: "order-processor — Category C"
4. **Upgrade recommendation sections** - Include "Category X" in the heading or first paragraph
5. **Terraform comment blocks** (Phase 6.4) - `# Category: C` line
6. **Risk sections** - When flagging issues, mention which category: "Category C applications with auto-registration..."

**Example of correct category usage:**
- ✅ "The order-processor application is **Category C** (auto-register)"
- ✅ "## Applications Discovered" table with Category column showing "C"
- ✅ Terraform comment: `# Category: C`
- ❌ WRONG: "The application uses auto-registration" (missing explicit "Category C")

---

## Phase 5: Create Schema Files

### 5.1 Directory Structure

Create:
```
schemas/
├── avro/
│   ├── {topic}-value.avsc
│   └── ...
├── json/
│   ├── {topic}-value.json
│   └── ...
└── proto/
    ├── {topic}-value.proto
    └── ...
```

### 5.2 File Naming

**CRITICAL - FILE NAMING CONVENTION:**

All schema file names MUST use **kebab-case** (lowercase with hyphens), even if the topic name or data class uses a different case convention.

- Value schemas: `{topic}-value.{ext}`
- Key schemas (if applicable): `{topic}-key.{ext}`
- Extensions: `.avsc` (Avro), `.json` (JSON Schema), `.proto` (Protobuf)

**Case conversion examples:**
- Topic: `OrderCreatedEvent` → File: `order-created-event-value.avsc` ✓
- Topic: `user_notifications` → File: `user-notifications-value.json` ✓
- Topic: `PaymentProcessed` → File: `payment-processed-value.avsc` ✓
- ❌ WRONG: `OrderCreatedEvent-value.avsc` (PascalCase)
- ❌ WRONG: `user_notifications-value.json` (snake_case)

### 5.3 Initialize Schema Project

**If `schema_init` MCP tool is available:**
```
Call schema_init with:
  path: <project root or schemas/ directory>
```

**If MCP tools are not available:**
- Manually create `schema.yaml` at the schemas directory root

In either case, update `schema.yaml` to include:
- All schema files under `schemas:` with `path`, `subject`, and `type`
- Schema Registry environment configuration:

```yaml
environments:
  dev:
    url: ${SCHEMA_REGISTRY_URL}
    api_key: ${SCHEMA_REGISTRY_API_KEY}
    api_secret: ${SCHEMA_REGISTRY_API_SECRET}
```

### 5.4 Lint & Validate

**If MCP tools are available:**
```
Call schema_lint with:
  path: schemas/
  fix: true

Call schema_validate with:
  path: <each schema file>
  against: main  (or live_sr if SR URL is configured)
```

**If MCP tools are not available:**
- Skip automated lint/validate
- Add to report: "⚠ Schemas were not lint-checked or compatibility-validated. Before registering, install the schema-registry MCP server and run `schema_lint` + `schema_validate`, or manually validate using the Confluent Schema Registry REST API."

---

## Phase 6: Generate Terraform

See [Confluent Terraform Provider docs](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs) for full resource reference.

**CRITICAL - TERRAFORM FILE STRUCTURE:**

You MUST create these separate files. Do NOT combine everything into main.tf:

**Required files:**
1. `terraform/providers.tf` - Provider configuration only
2. `terraform/variables.tf` - Variable definitions only
3. `terraform/tags.tf` - Tag resources ONLY (if any PII fields exist)
4. `terraform/schemas.tf` - Schema resources for Category A/B/E
5. `terraform/flagged-auto-register.tf` - Commented-out schemas for Category C (if any exist)
6. `terraform/outputs.tf` - Output values only

**File separation rules:**
- schemas.tf = Active schemas (Categories A, B, E) - NOT commented out
- flagged-auto-register.tf = Category C schemas ONLY - MUST be commented out
- tags.tf = MUST exist if ANY schema uses confluent:tags (PII/PRIVATE/SENSITIVE)
- Do NOT put schema resources in main.tf or providers.tf

### 6.1 `terraform/providers.tf`

```hcl
terraform {
  required_version = ">= 1.3.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

provider "confluent" {
  schema_registry_id            = var.schema_registry_id
  schema_registry_rest_endpoint = var.schema_registry_rest_endpoint
  schema_registry_api_key       = var.schema_registry_api_key
  schema_registry_api_secret    = var.schema_registry_api_secret
}
```

### 6.2 `terraform/variables.tf`

```hcl
variable "schema_registry_id" {
  description = "Schema Registry cluster ID (e.g., lsrc-abc123)"
  type        = string
}

variable "schema_registry_rest_endpoint" {
  description = "Schema Registry REST endpoint URL"
  type        = string
}

variable "schema_registry_api_key" {
  description = "Schema Registry API key"
  type        = string
  sensitive   = true
}

variable "schema_registry_api_secret" {
  description = "Schema Registry API secret"
  type        = string
  sensitive   = true
}
```

### 6.3 `terraform/tags.tf`

**MANDATORY IF ANY PII EXISTS:** If ANY schema generated in Phase 3 contains `confluent:tags`, you MUST create this file. Check all schemas - if even one has PII/PRIVATE/SENSITIVE tags, create tags.tf.

**Important:** Confluent Stream Governance requires tags to be pre-created in the catalog before schemas can embed `confluent:tags`. See [Stream Governance tags](https://docs.confluent.io/cloud/current/stream-governance/stream-catalog-rest-apis.html). Generate a `confluent_tag` resource for each tag used in the schemas:

```hcl
# ──────────────────────────────────────────────
# Confluent Stream Governance Tags
# Must exist before schemas can use confluent:tags
# ──────────────────────────────────────────────

resource "confluent_tag" "pii" {
  name        = "PII"
  description = "Personally Identifiable Information — can identify an individual"
}

resource "confluent_tag" "private" {
  name        = "PRIVATE"
  description = "Highly sensitive data — should be encrypted or masked"
}

resource "confluent_tag" "sensitive" {
  name        = "SENSITIVE"
  description = "Sensitive information that requires restricted access"
}

# Add additional tags here if PHI or other custom tags are used in schemas
```

Only include tags that are actually used in the extracted schemas. Check the PII tagging results from Phase 3.3b.

### 6.4 `terraform/schemas.tf`

For each Category A, B, and E producer, generate a `confluent_schema` resource. **If any schema uses `confluent:tags`, add `depends_on` to ensure tags are created first:**

**MANDATORY COMMENT BLOCK - OUTPUT EXACTLY THIS FORMAT:**

Every single `confluent_schema` resource MUST be preceded by this EXACT comment block. Do NOT skip this or use a shortened version.

```hcl
# ──────────────────────────────────────────────
# Topic: {topic_name}
# App: {app_name} ({language})
# Source: {file_path where producer was found}
# Category: {A|B|C|E}
# ──────────────────────────────────────────────
resource "confluent_schema" "{sanitized_topic_name}_value" {
  subject_name = "{topic_name}-value"
  format       = "{AVRO|JSON|PROTOBUF}"
  schema       = file("../schemas/{format_dir}/{topic_name}-value.{ext}")

  depends_on = [confluent_tag.pii, confluent_tag.private, confluent_tag.sensitive]

  lifecycle {
    prevent_destroy = true
  }
}
```

**CORRECT Example:**
```hcl
# ──────────────────────────────────────────────
# Topic: user-events
# App: user-service (Java)
# Source: src/main/java/com/acme/users/UserEventProducer.java:42
# Category: B
# ──────────────────────────────────────────────
resource "confluent_schema" "user_events_value" {
  subject_name = "user-events-value"
  format       = "AVRO"
  schema       = file("../schemas/avro/user-events-value.avsc")
  
  lifecycle {
    prevent_destroy = true
  }
}
```

**WRONG - Missing comment block:**
```hcl
resource "confluent_schema" "user_events_value" {  ← NO COMMENT BLOCK
  subject_name = "user-events-value"
  ...
}
```

**WRONG - Incomplete comment block:**
```hcl
# user-events schema  ← Too short, missing required fields
resource "confluent_schema" "user_events_value" {
```

Only include tag references in `depends_on` that the schema actually uses. If a schema has no PII fields, the `depends_on` can be omitted.

**Resource naming rules:**
- Replace dots, hyphens, and special characters with underscores
- Prefix with format if multiple formats exist for same topic
- Add `_value` or `_key` suffix

**Schema references:** If a schema references another (e.g., Avro union types, Protobuf imports), add `schema_reference` blocks:

```hcl
  schema_reference {
    name         = "{referenced_type_name}"
    subject_name = "{referenced_subject}"
    version      = {version}
  }
```

### 6.5 `terraform/flagged-auto-register.tf`

**CREATE THIS FILE ONLY IF Category C producers exist.** If no applications have auto.register.schemas=true, skip this file entirely.

For each Category C producer, generate **commented-out** resources in this separate file (NOT in schemas.tf):

```hcl
# ╔══════════════════════════════════════════════════════════════╗
# ║  FLAGGED: auto.register.schemas=true                        ║
# ║                                                              ║
# ║  The following schemas are currently auto-registered by the  ║
# ║  producer at runtime. This is a risk because:                ║
# ║  - Schema evolution is uncontrolled                          ║
# ║  - Breaking changes can be registered accidentally           ║
# ║  - No review process for schema changes                      ║
# ║                                                              ║
# ║  To fix:                                                     ║
# ║  1. Set auto.register.schemas=false in the producer config   ║
# ║  2. Uncomment the resources below                            ║
# ║  3. Run terraform apply to register schemas via IaC          ║
# ║  4. Set use.latest.version=true in the producer config       ║
# ╚══════════════════════════════════════════════════════════════╝

# ──────────────────────────────────────────────
# Topic: {topic_name}
# App: {app_name} ({language})
# auto.register.schemas=true found at: {file}:{line}
# ──────────────────────────────────────────────
# resource "confluent_schema" "{sanitized_topic_name}_value" {
#   subject_name = "{topic_name}-value"
#   format       = "{AVRO|JSON|PROTOBUF}"
#   schema       = file("../schemas/{format_dir}/{topic_name}-value.{ext}")
#
#   lifecycle {
#     prevent_destroy = true
#   }
# }
```

### 6.6 Importing Existing Schemas

If Category A or C producers already have schemas registered in Schema Registry (via auto-register or manual registration), the Terraform resources will conflict on `terraform apply`. Add import instructions to the report:

```hcl
# For schemas already registered in SR, import them before applying:
# terraform import confluent_schema.{resource_name} {sr_cluster_id}/{subject_name}/latest
#
# Required environment variables:
#   IMPORT_SCHEMA_REGISTRY_API_KEY
#   IMPORT_SCHEMA_REGISTRY_API_SECRET
#   IMPORT_SCHEMA_REGISTRY_REST_ENDPOINT
```

Add a `terraform/import.sh` helper script:

```bash
#!/bin/bash
# Import existing schemas from Schema Registry into Terraform state.
# Set these environment variables before running:
#   IMPORT_SCHEMA_REGISTRY_API_KEY
#   IMPORT_SCHEMA_REGISTRY_API_SECRET
#   IMPORT_SCHEMA_REGISTRY_REST_ENDPOINT

# {Repeat for each Category A/C schema that is already in SR}
terraform import confluent_schema.{resource_name} "{sr_cluster_id}/{subject_name}/latest"
```

### 6.7 `terraform/outputs.tf`

```hcl
# Outputs for each registered schema (uncommented resources only)
output "{sanitized_topic_name}_value_schema_id" {
  description = "Schema ID for {topic_name}-value"
  value       = confluent_schema.{sanitized_topic_name}_value.schema_identifier
}

output "{sanitized_topic_name}_value_version" {
  description = "Schema version for {topic_name}-value"
  value       = confluent_schema.{sanitized_topic_name}_value.version
}
```

---

## Phase 7: Generate Report — `schema-report.md`

Create a comprehensive markdown report at the project root:

```markdown
# Kafka Schema Analysis Report

> Generated by Schema Registry Adoption Analyzer on {date}
> Project: {project_name}

---

## Executive Summary

| Metric | Count |
|--------|-------|
| Kafka applications found | N |
| Producers | N |
| Consumers | N |
| Languages detected | Java, Python, ... |
| Topics identified | N |
| Schemas extracted | N |
| Risks found | N |
| PII fields tagged | N |
| Upgrade recommendations | N |

### Category Breakdown

| Category | Count | Description |
|----------|-------|-------------|
| A: Compliant | N | Using Confluent serializer + SR |
| B: Needs SR | N | Schema in code but no SR integration |
| C: Auto-register | N | Using auto.register.schemas=true |
| D: No schema | N | No discernible schema |
| E: Custom serializer | N | Custom Serializer/inline serialization without SR |

---

## Applications Discovered

**OUTPUT EXACTLY THIS TABLE FORMAT - DO NOT CREATE ALTERNATIVE SECTIONS LIKE "SERVICES OVERVIEW" OR INDIVIDUAL SERVICE WRITE-UPS:**

Use this EXACT markdown table structure. The Category column is MANDATORY and MUST show the letter (A/B/C/D/E).

```markdown
## Applications Discovered

| # | App | Language | Role | Topics | Serializer | SR? | Category |
|---|-----|----------|------|--------|------------|-----|----------|
| 1 | {app_name} | {lang} | producer | {topics} | {serializer} | {yes/no} | {A/B/C/D/E} |
| ... |
```

**CORRECT Example:**
```markdown
## Applications Discovered

| # | App | Language | Role | Topics | Serializer | SR? | Category |
|---|-----|----------|------|--------|------------|-----|----------|
| 1 | user-service | Java | producer | user-events | JsonSerializer | no | B |
| 2 | order-processor | Python | producer | orders | AvroSerializer | yes (auto) | C |
| 3 | analytics | Python | consumer | user-events, orders | AvroDeserializer | yes | A |
```

**WRONG - Do NOT create narrative sections instead:**
```markdown
## Services Overview  ← WRONG FORMAT

### 1. user-service (Java)
**Status:** Producer
...
```

After the table, you MAY add detailed per-service sections, but the table MUST come first.

---

## RISKS

### auto.register.schemas=true

> **Impact:** Schema evolution is uncontrolled. Breaking changes can be
> registered without review, potentially breaking all downstream consumers.

| # | App | File | Line | Topics Affected |
|---|-----|------|------|----------------|
| 1 | {app} | {file} | {line} | {topics} |
| ... |

**Recommendation:**
1. Set `auto.register.schemas=false` in all producer configurations
2. Register schemas via Terraform (see `terraform/flagged-auto-register.tf`)
3. Set `use.latest.version=true` so producers fetch the latest registered schema
4. Add schema validation to CI/CD pipeline

### Custom Serializers Without Schema Registry

> **Impact:** Producers using custom serializer implementations or inline
> serialization (json.dumps, JSON.stringify, ObjectMapper, etc.) bypass
> Schema Registry entirely. Schema changes are invisible — there is no
> contract enforcement, no compatibility checking, and no schema evolution
> governance. If the data shape changes, consumers break silently.

| # | App | Custom Serializer | File:Line | Topics Affected | Data Model |
|---|-----|------------------|-----------|----------------|------------|
| 1 | {app} | {class or function name} | {file}:{line} | {topics} | {data class/model being serialized} |
| ... |

**Recommendation:**

Replace the custom serializer with a Confluent serializer + `HeaderSchemaIdSerializer`.
The payload format will change, so **consumers must be upgraded first**.

1. Register the schema in Schema Registry via Terraform (already generated in `terraform/schemas.tf`)
2. **Upgrade consumers first** — Java: configure a composite deserializer that can read both the old (custom) format and the new (Confluent) format during the transition. Other languages: coordinated cutover.
3. **Replace the custom serializer** with the appropriate Confluent serializer (`KafkaAvroSerializer`, `ProtobufSerializer`, or `KafkaJsonSchemaSerializer`) and add `HeaderSchemaIdSerializer` to write schema ID to Kafka headers.
4. After all old data has been consumed or expired, replace the composite deserializer with the standard Confluent deserializer.

See detailed upgrade instructions in the "Upgrade Quick Reference — Custom Serializers" section below.

> **Minimum versions required:**
> - Java: CP client >= 8.1.1
> - C/C++: libserdes >= 0.1.0
> - Python: confluent-kafka >= 2.13.0
> - .NET: Confluent.Kafka >= 2.13.0
> - Go: confluent-kafka-go >= 2.13.0
> - Node.js: @confluentinc/kafka-javascript >= 1.8.0
>
> **Consumer side — automatic dual-read.** All Confluent client libraries on supported versions automatically check Kafka headers first (`__value_schema_id` / `__key_schema_id`) for the schema ID and fall back to the payload prefix if not found. Once consumers are on the Confluent deserializer, no further config change is needed when producers switch to `HeaderSchemaIdSerializer`.

See per-app upgrade instructions in the "Producer Upgrade Recommendations" section below.

---

## Producer Upgrade Recommendations

For producers with schemas in code but no Schema Registry integration (Category B and E):

**CRITICAL:** Each application section MUST state "Category X" explicitly in the heading. Do NOT omit this.

### {App Name} ({Language}) — **Category {B|C|E}**

**Current state:**
- **Category:** {B|C|E} — {brief reason: "Schema in code, no SR" / "Auto-register enabled" / "Custom serializer"}
- Serializer: `{current_serializer}`
- Data model: `{class/file path}`
- Topics: {topics}

**Recommended changes:**

1. **Add dependency:**
   {language-specific dependency to add}

2. **Update serializer config:**
   {language-specific config changes}

3. **Add Schema Registry config:**
   {language-specific SR URL and auth config}

**Example - CORRECT:**
```markdown
### order-processor (Python) — **Category C**

**Current state:**
- **Category:** C — Auto-registration enabled
- Serializer: AvroSerializer (with auto.register.schemas=true)
- Data model: Pydantic models (OrderCreated, OrderUpdated)
- Topics: order-events, order-updates
```

**Example - WRONG (missing category):**
```markdown
### order-processor (Python)  ← Missing "Category C"

**Current state:**
- Serializer: AvroSerializer with auto-registration ← Describes it but doesn't say "Category C"
```

(Repeat per app)

### Upgrade Quick Reference — JSON Data (Category B)

Replace the serializer with the Confluent JSON serializer + header-based schema ID.
Payload stays clean JSON. Schema ID goes to Kafka headers. **Non-breaking** for consumers.

> **Minimum versions:** Java 8.1.1+, C/C++ 0.1.0+, Python 2.13.0+, .NET 2.13.0+, Go 2.13.0+, Node 1.8.0+.

| Current State | Recommended Serializer | Config Changes |
|--------------|----------------------|----------------|
| Java `StringSerializer` + JSON | `KafkaJsonSchemaSerializer` + `HeaderSchemaIdSerializer` | Add `value.serializer`, `schema.registry.url`, `value.schema.id.serializer` |
| Java `JsonSerializer` (Spring) | `KafkaJsonSchemaSerializer` + `HeaderSchemaIdSerializer` | Add Confluent dependency, update serializer class |
| Python `kafka-python` + `json.dumps` | `confluent-kafka` `JSONSerializer` + `header_schema_id_serializer` | Replace library, use `SerializingProducer`, set `schema.id.serializer` |
| Python `confluent-kafka` + inline `json.dumps` | `confluent-kafka` `JSONSerializer` + `header_schema_id_serializer` | Remove inline serialization, set `schema.id.serializer` |
| .NET `JsonConvert` / `System.Text.Json` | `Confluent.SchemaRegistry.Serdes.Json.JsonSerializer<T>` + header mode | Add NuGet (>= 2.13.0), configure header-based schema ID |
| Go `json.Marshal` before `Produce()` | `confluent-kafka-go` JSON serializer + header mode | Remove manual marshal, add SR client, configure header-based schema ID |
| Node `kafkajs` + `JSON.stringify` | `@confluentinc/kafka-javascript` with SR + header mode | Replace library, remove inline serialization, configure header-based schema ID |

### Upgrade Quick Reference — Custom Serializers (Category E)

Replace the custom serializer with a Confluent serializer. The payload format changes, so **consumers must be upgraded first** to handle both old and new formats during the transition.

> **Rollout order: consumers first, then producers.**
> **Minimum versions:** Java 8.1.1+, C/C++ 0.1.0+, Python 2.13.0+, .NET 2.13.0+, Go 2.13.0+, Node 1.8.0+.

**Step 1 — Upgrade all consumers (before touching producers):**

*Java:*
Configure a composite deserializer that wraps both the old custom deserializer and the new Confluent deserializer. The composite deserializer inspects each message for a schema ID (in the header or payload prefix). If a schema ID is found, it delegates to the Confluent deserializer. If not, it falls back to the old custom deserializer. This lets consumers read both old-format and new-format data during the migration. See the [Confluent Schema Registry serializer/deserializer docs](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html) for the exact configuration properties.

*Python / .NET / Go / Node.js:*
These languages do not have a composite deserializer. Deploy a new consumer version that can handle both formats, or do a coordinated cutover.

**Step 2 — Upgrade all producers:**

Replace the custom serializer with the Confluent serializer for the chosen format:

| Language | Recommended Serializer | Config |
|----------|----------------------|--------|
| Java | `KafkaAvroSerializer` / `ProtobufSerializer` / `KafkaJsonSchemaSerializer` | Set `value.serializer`, `schema.registry.url`, `value.schema.id.serializer=HeaderSchemaIdSerializer` |
| Python | `confluent-kafka` `AvroSerializer` / `ProtobufSerializer` / `JSONSerializer` | Use `SerializingProducer`, set `schema.id.serializer` |
| .NET | `Confluent.SchemaRegistry.Serdes` serializer + header mode | Add NuGet, configure header-based schema ID |
| Go | `confluent-kafka-go` serializer + header mode | Add SR client, configure header-based schema ID |
| Node | `@confluentinc/kafka-javascript` with SR + header mode | Replace library, configure header-based schema ID |

Once all producers are upgraded, consumers read new data via the Confluent deserializer and old data via the custom deserializer (Java composite deserializer). After all old data has been consumed or expired, the composite deserializer can be replaced with the standard Confluent deserializer.

---

## Migration Rollout Ordering

The order you upgrade producers vs consumers depends on your starting point. Getting this wrong can cause deserialization failures.

### Scenario 1: JSON data, no SR (Category B) — Producers First

Consumers today read raw JSON and ignore Kafka headers. Safe to upgrade producers first.

1. **Upgrade all producers** — switch to Confluent serializer + `HeaderSchemaIdSerializer`. Schema ID goes to headers; payload stays clean JSON. Existing consumers keep working.
2. **Upgrade consumers** — switch to Confluent deserializer. On supported versions, it automatically finds schema ID in headers or payload.

### Scenario 2: Already on SR (Category A→Header) — Producers Only

Consumers already use Confluent deserializers. On supported versions, they automatically check headers first for the schema ID and fall back to the payload prefix. **No consumer changes needed** — just verify consumers are on supported versions.

1. **Verify consumer versions** — Java 8.1.1+, C/C++ 0.1.0+, Python 2.13.0+, .NET 2.13.0+, Go 2.13.0+, Node 1.8.0+.
2. **Upgrade producers** — add `HeaderSchemaIdSerializer`. Everything else stays the same.

### Scenario 3: Custom serdes → Confluent serdes (Category E) — Consumers First

The payload format changes when replacing custom serializers with Confluent serializers, so consumers must be upgraded first.

1. **Upgrade all consumers** — Java: configure a composite deserializer (see Category E upgrade above). Other languages: coordinated cutover.
2. **Upgrade all producers** — replace custom serializer with Confluent serializer + `HeaderSchemaIdSerializer`.

---

## Multi-Schema Topics

Topics where multiple event types are produced by different data models.
A wrapper schema with `oneOf`/union/`oneof` has been generated with `schema_reference`
blocks pointing to the individual event schemas.

| Topic | Event Types | Wrapper Schema | References |
|-------|-------------|---------------|------------|
| {topic} | {EventType1}, {EventType2} | schemas/{dir}/{topic}-value.{ext} | {event-type-1}, {event-type-2} |

> If no multi-schema topics are found, omit this section.

---

## Schemas Extracted

| # | Topic | Subject | Format | Source | Schema File |
|---|-------|---------|--------|--------|-------------|
| 1 | {topic} | {topic}-value | {format} | {code model / existing file / inferred} | schemas/{dir}/{file} |
| ... |

---

## PII Fields Detected

The following fields were identified as potential PII and tagged with `confluent:tags` in their schemas.
These tags enable Confluent Stream Governance features like field-level encryption, masking, and audit.

| # | Schema | Field | Tags | Reason |
|---|--------|-------|------|--------|
| 1 | {topic}-value | {field_name} | `PII` | Field name matches PII pattern: email |
| 2 | {topic}-value | {field_name} | `PII`, `PRIVATE` | Field name matches PII pattern: ssn |
| ... |

> **Total PII fields tagged:** N across M schemas
>
> **Action required:** Review tagged fields for accuracy. Add `PUBLIC` tag to
> fields that were incorrectly flagged. Add `PII`/`PRIVATE` tags to any fields
> that were missed (e.g., fields with non-standard names containing personal data).
>
> **Stream Governance:** These tags integrate with Confluent's Data Contracts
> feature. You can add `ruleset` blocks to the Terraform resources to enforce
> field-level masking or encryption on tagged fields.

---

## Terraform Resources Generated

| File | Resources | Status |
|------|-----------|--------|
| `terraform/schemas.tf` | N `confluent_schema` resources | Ready to apply |
| `terraform/flagged-auto-register.tf` | N `confluent_schema` resources | Commented out — review and enable after disabling auto-register |
| `terraform/import.sh` | N import commands | Run first if schemas already exist in SR |

---

## Consumer Impact Notes

Topics where serializer changes may affect consumers:

| Topic | Category | Producers Changing | Active Consumers | Rollout Order | Consumer Action |
|-------|----------|-------------------|-----------------|---------------|-----------------|
| {topic} | B | {app} | {consumers} | Producers first | None during migration. Eventually upgrade to Confluent deserializer. |
| {topic} | A→Header | {app} | {consumers} | Producers only | None — Confluent deserializers on supported versions automatically read schema ID from both headers and payload. Verify client version. |
| {topic} | E | {app} | {consumers} | Consumers first | Java: configure composite deserializer to handle both old and new formats. Other langs: coordinated cutover. |

> **Automatic dual-read behavior:** All Confluent client libraries on supported versions
> automatically check Kafka headers first for the schema ID, then fall back to the
> payload prefix. No consumer configuration change is needed when producers switch to
> `HeaderSchemaIdSerializer`.

---

## Next Steps

1. [ ] Review `schema-report.md` findings with the team
2. [ ] Review and fix all `auto.register.schemas=true` occurrences
3. [ ] Review extracted schemas in `schemas/` for accuracy
4. [ ] Configure Terraform variables (SR cluster ID, endpoint, API credentials)
5. [ ] Run `terraform plan` to preview schema registration
6. [ ] Run `terraform apply` to register schemas
7. [ ] Follow rollout ordering per category (see Migration Rollout Ordering section):
   - Category B: upgrade producers first, then consumers
   - Category A→Header: verify consumer versions, then upgrade producers
   - Category E: upgrade consumers first (composite deserializer for Java), then replace custom serializer with Confluent serializer
8. [ ] For Category E: after all old data is consumed, replace composite deserializer with standard Confluent deserializer
9. [ ] Uncomment `flagged-auto-register.tf` resources after disabling auto-register
10. [ ] Add schema lint/validate to CI/CD pipeline
```

---

---

## REPORT STRUCTURE CHECKLIST

Before completing the report, verify ALL of these elements are present:

**✓ MUST HAVE:**
1. **Applications Discovered table** - Exact markdown table format (not "Services Overview" sections)
   - Must have columns: # | App | Language | Role | Topics | Serializer | SR? | Category
   - Category column MUST show letter (A/B/C/D/E) for EVERY row
2. **Explicit category labels** throughout:
   - In table (see #1)
   - In upgrade recommendation section headers: "app-name — Category X"
   - In terraform comment blocks: `# Category: X`
   - When describing categorization: "This is **Category C** because..."
3. **Terraform comment blocks** for every schema resource:
   - Must include: Topic, App, Source file:line, Category
   - Use exact format from Phase 6.4

**✓ VERIFY:**
- Search report for "Category A", "Category B", "Category C", "Category D", or "Category E" - at least one MUST appear
- Count schema resources in terraform files - each MUST have the comment block above it
- Applications Discovered section MUST start with markdown table, not prose

---

## Execution Notes

### Tool Usage

**Core tools (always available — no prerequisites):**
- **`Glob`** — Find build files, schema files, source code
- **`Grep`** — Detect Kafka dependencies, producer/consumer patterns, serializers, risks
- **`Read`** — Read source files, data models, configs
- **`Write`** — Create schema files, Terraform configs, report

**MCP tools (optional — requires `schema-registry` MCP server):**
- **`schema_status`** — Call first to understand the project's current schema state
- **`schema_infer`** — Generate schemas from sample JSON data files
- **`schema_lint`** — Validate all extracted schemas (always fix warnings)
- **`schema_validate`** — Check backward compatibility against main branch or live SR
- **`schema_init`** — Create `schema.yaml` project configuration

If MCP tools are not available, the skill still works — it just skips automated schema validation. The report will note which steps were skipped and recommend running them manually before registering schemas.

### Output Organization

```
{project_root}/
├── schema-report.md              # Analysis report
├── schemas/
│   ├── schema.yaml               # Schema project config
│   ├── avro/
│   │   └── {topic}-value.avsc
│   ├── json/
│   │   └── {topic}-value.json
│   └── proto/
│       └── {topic}-value.proto
└── terraform/
    ├── providers.tf
    ├── variables.tf
    ├── tags.tf                    # confluent_tag resources (PII, PRIVATE, etc.)
    ├── schemas.tf                 # Active schema resources (depends_on tags)
    ├── flagged-auto-register.tf   # Commented-out flagged resources
    ├── outputs.tf
    └── import.sh                  # Import script for schemas already in SR
```

### Edge Cases

- **Monorepos:** Treat each service/module with its own Kafka dependencies as a separate app
- **Multi-topic producers:** Generate one schema resource per topic
- **Shared schemas:** If multiple producers use the same data model for different topics, create one schema file and reference it from multiple Terraform resources
- **No topics found:** If topic names are loaded from environment variables or external config and cannot be determined statically, note this in the report and use placeholder names with a TODO
- **Test code:** Skip test directories (`**/test/**`, `**/tests/**`, `**/__tests__/**`, `**/src/test/**`) unless they contain the only schema/model definitions
- **Multiple serializers per app:** If an app produces to multiple topics with different formats, create separate schema files and Terraform resources for each
