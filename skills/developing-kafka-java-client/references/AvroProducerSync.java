package com.example.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Synchronous producer using send(record).get(). Blocks until the broker acks each
 * record — strongest per-message confirmation, lowest throughput. Best for batch/ETL
 * jobs and steps that must confirm one write before the next.
 *
 * Avro value type is the generated SpecificRecord {@code Transaction} (from
 * src/main/avro/value.avsc). Replace {@code Transaction} and the sample data with
 * the user's domain.
 */
public class AvroProducerSync {

    /**
     * Register the schema explicitly as a separate step. Errors propagate immediately.
     */
    public static int registerSchema(SchemaRegistryClient srClient, String topic, org.apache.avro.Schema schema)
            throws IOException, io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException {
        String subject = topic + "-value";
        int schemaId = srClient.register(subject, new AvroSchema(schema));
        System.out.printf("Schema ID: %d for subject %s%n", schemaId, subject);
        return schemaId;
    }

    /**
     * Produce messages synchronously using an existing producer — never create one here.
     * Each send blocks via .get() until the record is acknowledged.
     */
    public static void produce(Producer<String, Transaction> producer, String topic, List<Transaction> messages)
            throws InterruptedException {
        for (Transaction value : messages) {
            String key = value.getTransactionId() == null ? null : value.getTransactionId().toString();
            ProducerRecord<String, Transaction> record = new ProducerRecord<>(topic, key, value);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Produced: partition=%d, offset=%d%n",
                        metadata.partition(), metadata.offset());
            } catch (ExecutionException e) {
                System.err.println("Delivery failed: " + e.getCause().getMessage());
            }
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
        System.out.println("Connected to Kafka (" + env.get("BOOTSTRAP_SERVER") + ")");

        String srUrl = env.get("SCHEMA_REGISTRY_URL");
        if (!KafkaConfig.verifySchemaRegistry(srUrl, env.get("SR_API_KEY"), env.get("SR_API_SECRET"))) {
            throw new IllegalStateException("Failed to connect to Schema Registry");
        }
        System.out.println("Connected to Schema Registry (" + srUrl + ")");

        SchemaRegistryClient srClient = new CachedSchemaRegistryClient(
                srUrl, 100, KafkaConfig.schemaRegistryConfig(env));
        registerSchema(srClient, topic, Transaction.getClassSchema());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("auto.register.schemas", false);
        props.put("use.latest.version", true);

        try (Producer<String, Transaction> producer = new KafkaProducer<>(props)) {
            // -- Generate sample messages here, adapted to the user's domain --
            List<Transaction> messages = List.of(
                    Transaction.newBuilder()
                            .setTransactionId("txn-1")
                            .setAmount(42.50)
                            .setCurrency("USD")
                            .setTimestamp("2026-06-12T10:00:00Z")
                            .setStatus(Status.completed)
                            .setMetadata(null)
                            .build());
            produce(producer, topic, messages);
        } finally {
            System.out.println("Producer closed");
        }
    }
}
