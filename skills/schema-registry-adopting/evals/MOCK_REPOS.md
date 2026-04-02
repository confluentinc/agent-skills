# Mock Repository Structures for Schema Registry Adoption Evals

This document describes the 10 mock repositories created for testing the schema-registry-adopting skill.

## 1. payment-service (Java Spring Boot with PII)
**Path:** `mock-repos/payment-service`
**Category:** B (Schema in code, no SR)
**Language:** Java (Spring Kafka)

### Structure:
```
payment-service/
├── pom.xml (Spring Kafka dependency)
├── src/main/java/com/example/payment/
│   ├── model/PaymentEvent.java (PII: email, credit card, name, address)
│   └── producer/PaymentProducer.java (produces to payment-events, payment-confirmations)
└── src/main/resources/application.properties (JsonSerializer)
```

**Expected Detection:**
- Serializer: Spring JsonSerializer (no SR)
- Topics: payment-events, payment-confirmations
- PII fields: customer_email, credit_card_number, cardholder_name, billing_address
- Schema format: JSON

---

## 2. order-processor (Python with auto-registration)
**Path:** `mock-repos/order-processor`
**Category:** C (Auto-register enabled)
**Language:** Python (confluent-kafka)

### Structure:
```
order-processor/
├── requirements.txt (confluent-kafka[avro])
├── config.yaml (auto.register.schemas: true)
└── src/
    ├── models.py (Pydantic OrderCreated, OrderUpdated)
    └── producer.py (auto.register.schemas=true)
```

**Expected Detection:**
- Risk: auto.register.schemas=true in producer.py:15 and config.yaml:4
- Serializer: AvroSerializer with auto-register
- Topics: order-events, order-updates
- Schema format: Avro (inferred from Pydantic models)

---

## 3. ecommerce-platform (Multi-language polyglot)
**Path:** `mock-repos/ecommerce-platform`
**Categories:** Mixed (A for Java, B for Python/Node)
**Languages:** Java, Python, Node.js

### Structure:
```
ecommerce-platform/
├── order-service/ (Java with Confluent Avro serializer - Category A)
│   ├── pom.xml (kafka-avro-serializer)
│   └── src/main/java/com/ecommerce/orders/
│       ├── OrderEvent.java (PII: email, phone, address)
│       └── OrderProducer.java (topic: orders)
├── analytics-service/ (Python with kafka-python - Category B)
│   ├── requirements.txt (kafka-python)
│   └── src/analytics_producer.py (PII: ip_address)
└── notification-service/ (Node.js with kafkajs - Category B)
    ├── package.json (kafkajs)
    └── src/notification-producer.js (PII: user_email, user_phone)
```

**Expected Detection:**
- 3 applications across 3 languages
- Topics: orders, analytics-events, notifications
- Java: Category A (already using SR)
- Python: Category B (json.dumps without SR)
- Node: Category B (JSON.stringify without SR)

---

## 4. legacy-event-publisher (Custom Python serializer)
**Path:** `mock-repos/legacy-event-publisher`
**Category:** E (Custom serializer without SR)
**Language:** Python (kafka-python)

### Structure:
```
legacy-event-publisher/
├── requirements.txt (kafka-python)
└── src/event_publisher.py (json.dumps inline)
```

**Expected Detection:**
- Custom serializer: lambda v: json.dumps(v).encode('utf-8')
- Also inline json.dumps before send
- Topics: user-events, product-events
- PII: ip_address, session_id
- Requires consumer-first rollout

---

## 5. streams-processor (Already using SR, wants headers)
**Path:** `mock-repos/streams-processor`
**Category:** A→Header
**Language:** Java (Kafka Streams)

### Structure:
```
streams-processor/
├── pom.xml (kafka-streams-avro-serde 7.6.0)
├── src/main/java/com/example/streams/StreamsApp.java
└── src/main/resources/application.properties
```

**Expected Detection:**
- Already using KafkaAvroSerializer/Deserializer
- schema.registry.url configured
- Recommendation: Add HeaderSchemaIdSerializer
- No schema extraction needed

---

## 6. notification-service (.NET with extensive PII)
**Path:** `mock-repos/notification-service`
**Category:** B (Schema in code, no SR)
**Language:** C# (.NET)

### Structure:
```
notification-service/
├── NotificationService.csproj (Confluent.Kafka, Newtonsoft.Json)
├── Models/UserNotification.cs (10+ PII fields)
└── NotificationProducer.cs (JsonConvert.SerializeObject)
```

**Expected Detection:**
- PII fields: email, phone_number, first_name, last_name, home_address, ssn, date_of_birth, ip_address
- Topics: user-notifications, sms-notifications
- Serializer: Newtonsoft.Json (no SR)
- Should generate extensive PII tagging

---

## 7. acme-services (Monorepo)
**Path:** `mock-repos/acme-services`
**Categories:** Mixed
**Languages:** Java, Python

### Structure:
```
acme-services/
├── user-service/ (Java Spring Kafka - Category B)
│   ├── pom.xml
│   └── src/.../UserEvent.java (PII: email)
├── inventory-service/ (Python - Category E)
│   ├── requirements.txt (confluent-kafka)
│   └── src/producer.py (json.dumps inline)
├── billing-service/ (Java with JSON Schema serializer - Category A)
│   ├── pom.xml (kafka-json-schema-serializer)
│   └── src/.../Invoice.java
└── reporting-service/ (Consumer only - no producer)
    └── src/consumer.py
```

**Expected Detection:**
- 4 services total
- 3 producers, 1 consumer-only
- Topics: user-events, inventory-updates, billing-events
- Should deduplicate schemas and show per-service categorization

---

## 8. go-kafka-producer (Go with json.Marshal)
**Path:** `mock-repos/go-kafka-producer`
**Category:** E (Custom serialization)
**Language:** Go

### Structure:
```
go-kafka-producer/
├── go.mod (github.com/IBM/sarama)
└── producer.go (json.Marshal before sending)
```

**Expected Detection:**
- Two structs: CustomerEvent, OrderEvent
- PII: email, phone_number, first_name, last_name, account_number
- Topics: customer-events, order-events
- json.Marshal without SR = Category E

---

## 9. transaction-system (Multi-schema topic)
**Path:** `mock-repos/transaction-system`
**Category:** B (but with multi-schema complexity)
**Language:** Java (Spring Kafka)

### Structure:
```
transaction-system/
├── order-service/
│   ├── pom.xml
│   └── src/.../OrderCreatedEvent.java → transaction-events
└── payment-service/
    ├── pom.xml
    └── src/.../PaymentProcessedEvent.java → transaction-events (SAME TOPIC!)
```

**Expected Detection:**
- Multi-schema topic: transaction-events
- Two different event types to same topic
- Should generate wrapper schema with oneOf/union
- Should create schema_reference blocks in Terraform

---

## 10. api-gateway (No Kafka - negative test)
**Path:** `mock-repos/api-gateway`
**Category:** N/A (No Kafka)
**Language:** Node.js (Express)

### Structure:
```
api-gateway/
├── package.json (express, axios - NO KAFKA)
├── src/server.js
├── src/middleware.js
└── .env.example
```

**Expected Detection:**
- No Kafka dependencies
- No producers or consumers
- Report should show 0 applications
- Recommendation: No Schema Registry action needed

---

## Summary Table

| # | Repo | Language | Category | Kafka Deps | Topics | PII Fields | Special Notes |
|---|------|----------|----------|------------|--------|------------|---------------|
| 1 | payment-service | Java | B | spring-kafka | 2 | 4 | Spring JsonSerializer |
| 2 | order-processor | Python | C | confluent-kafka | 2 | 0 | auto.register=true |
| 3 | ecommerce-platform | Multi | A/B | Mixed | 3 | 5 | 3 services |
| 4 | legacy-event-publisher | Python | E | kafka-python | 2 | 2 | Custom json.dumps |
| 5 | streams-processor | Java | A→H | kafka-streams | 2 | 0 | Header migration |
| 6 | notification-service | C# | B | Confluent.Kafka | 2 | 8 | Heavy PII |
| 7 | acme-services | Multi | A/B/E | Mixed | 3 | 1 | Monorepo 4 services |
| 8 | go-kafka-producer | Go | E | sarama | 2 | 5 | json.Marshal |
| 9 | transaction-system | Java | B | spring-kafka | 1 | 0 | Multi-schema topic |
| 10 | api-gateway | Node | - | None | 0 | 0 | Negative test |

## Running the Evals

All eval prompts in `evals.json` now point to the correct paths:

```bash
./mock-repos/<repo-name>
```

You can test individual evals by triggering the skill with the prompts from `evals.json`.
