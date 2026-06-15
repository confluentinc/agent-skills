package com.example.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/**
 * JSON Schema producer (async callback). Use this path instead of AvroProducer when
 * the user prefers JSON Schema. The value type is a plain POJO ({@code Transaction}
 * below) whose fields match src/main/resources/value.schema.json. The Confluent
 * KafkaJsonSchemaSerializer derives/validates against the registered schema.
 */
public class JsonSchemaProducer {

    /** Minimal POJO matching value.schema.json. Replace fields with the user's domain. */
    public static class Transaction {
        @JsonProperty("transaction_id")
        public String transactionId;
        @JsonProperty("amount")
        public double amount;
        @JsonProperty("currency")
        public String currency;
        @JsonProperty("timestamp")
        public String timestamp;
        @JsonProperty("status")
        public String status;
    }

    public static int registerSchema(SchemaRegistryClient srClient, String topic, String schemaStr)
            throws IOException, io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException {
        String subject = topic + "-value";
        int schemaId = srClient.register(subject, new JsonSchema(schemaStr));
        System.out.printf("Schema ID: %d for subject %s%n", schemaId, subject);
        return schemaId;
    }

    public static void produce(Producer<String, Transaction> producer, String topic, List<Transaction> messages) {
        for (Transaction value : messages) {
            String key = value.transactionId;
            ProducerRecord<String, Transaction> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Delivery failed: " + exception.getMessage());
                } else {
                    System.out.printf("Produced: partition=%d, offset=%d%n",
                            metadata.partition(), metadata.offset());
                }
            });
        }
        producer.flush();
    }

    public static void main(String[] args) throws Exception {
        KafkaConfig.Env env = KafkaConfig.loadEnv();
        String topic = env.get("TOPIC", "demo-topic");

        Properties props = KafkaConfig.baseProperties(env);
        if (!KafkaConfig.verifyKafkaSetup(props, topic)) {
            throw new IllegalStateException("Failed to verify Kafka setup");
        }
        String srUrl = env.get("SCHEMA_REGISTRY_URL");
        if (!KafkaConfig.verifySchemaRegistry(srUrl, env.get("SR_API_KEY"), env.get("SR_API_SECRET"))) {
            throw new IllegalStateException("Failed to connect to Schema Registry");
        }

        String schemaStr;
        try (var in = JsonSchemaProducer.class.getResourceAsStream("/value.schema.json")) {
            if (in == null) {
                throw new IllegalStateException("Missing resource: /value.schema.json");
            }
            schemaStr = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        SchemaRegistryClient srClient = new CachedSchemaRegistryClient(
                srUrl, 100, KafkaConfig.schemaRegistryConfig(env));
        registerSchema(srClient, topic, schemaStr);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        props.put("auto.register.schemas", false);
        props.put("use.latest.version", true);

        try (Producer<String, Transaction> producer = new KafkaProducer<>(props)) {
            Transaction txn = new Transaction();
            txn.transactionId = "txn-1";
            txn.amount = 42.50;
            txn.currency = "USD";
            txn.timestamp = "2026-06-12T10:00:00Z";
            txn.status = "completed";
            produce(producer, topic, List.of(txn));
        } finally {
            System.out.println("Producer closed");
        }
    }
}
