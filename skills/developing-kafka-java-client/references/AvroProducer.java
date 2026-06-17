package com.example.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Async producer using send(record, Callback). Non-blocking; the callback handles
 * per-record delivery reports. Recommended default for most applications.
 *
 * Avro value type is the generated SpecificRecord {@code Transaction} (from
 * src/main/avro/value.avsc). Replace {@code Transaction} and the sample data with
 * the user's domain.
 */
public class AvroProducer {

    /**
     * Register the schema explicitly as a separate step. Errors (auth, network,
     * permission) propagate immediately — never swallow them here.
     */
    public static int registerSchema(SchemaRegistryClient srClient, String topic, org.apache.avro.Schema schema)
            throws IOException, io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException {
        String subject = topic + "-value";
        int schemaId = srClient.register(subject, new AvroSchema(schema));
        System.out.printf("Schema ID: %d for subject %s%n", schemaId, subject);
        return schemaId;
    }

    /**
     * Produce messages using an existing producer instance — never create one here.
     * Can be called repeatedly with the same producer.
     *
     * The Kafka message key is the entity identifier (transactionId) so related
     * records share a partition and preserve ordering. Use a null key only when
     * ordering does not matter (e.g. WarpStream sticky partitioning).
     */
    public static void produce(Producer<String, Transaction> producer, String topic, List<Transaction> messages) {
        for (Transaction value : messages) {
            String key = value.getTransactionId() == null ? null : value.getTransactionId().toString();
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
        // Block until all in-flight records are delivered and callbacks have run.
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

        // Register the schema explicitly before producing.
        SchemaRegistryClient srClient = new CachedSchemaRegistryClient(
                srUrl, 100, KafkaConfig.schemaRegistryConfig(env));
        registerSchema(srClient, topic, Transaction.getClassSchema());

        // Serializer config: schema.registry.url + basic.auth.* are already in props
        // (set by KafkaConfig.baseProperties). Disable auto-registration; use the
        // registered latest version.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("auto.register.schemas", false);
        props.put("use.latest.version", true);

        // Create the producer ONCE and reuse it.
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
