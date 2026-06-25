package com.example.kafka;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the scaffolded Kafka Java client. They run without a live Kafka
 * cluster or Schema Registry — the producer is mocked and the schema is validated
 * structurally.
 *
 * Adapt to what was generated: drop the Producer section if only a consumer was
 * requested (and vice versa). For the SYNCHRONOUS producer (AvroProducerSync) the
 * send(...) call must be stubbed to return a completed Future before verifying, since
 * its produce() calls .get() on the result.
 */
class AppTest {

    // ---------------------------------------------------------------------
    // KafkaConfig
    // ---------------------------------------------------------------------

    @Test
    void cloudConfigUsesSaslSsl() {
        Map<String, String> vars = Map.of(
                "KAFKA_ENV", "cloud",
                "BOOTSTRAP_SERVER", "broker:9092",
                "API_KEY", "key",
                "API_SECRET", "secret",
                "CLIENT_ID", "client");
        KafkaConfig.Env env = vars::get;

        Properties props = KafkaConfig.baseProperties(env);

        assertEquals("broker:9092", props.get("bootstrap.servers"));
        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("PLAIN", props.get("sasl.mechanism"));
        assertTrue(((String) props.get("sasl.jaas.config")).contains("PlainLoginModule"));
    }

    @Test
    void localConfigUsesPlaintext() {
        Map<String, String> vars = Map.of(
                "KAFKA_ENV", "local",
                "BOOTSTRAP_SERVER", "localhost:9092",
                "CLIENT_ID", "client");
        KafkaConfig.Env env = vars::get;

        Properties props = KafkaConfig.baseProperties(env);

        assertEquals("PLAINTEXT", props.get("security.protocol"));
        assertNull(props.get("sasl.mechanism"));
        assertNull(props.get("sasl.jaas.config"));
    }

    // ---------------------------------------------------------------------
    // Producer (async AvroProducer). Omit if no producer was generated.
    // ---------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void produceSendsEachMessageThenFlushes() {
        Producer<String, Transaction> producer = mock(Producer.class);

        List<Transaction> messages = List.of(
                Transaction.newBuilder()
                        .setTransactionId("txn-1")
                        .setAmount(42.50)
                        .setCurrency("USD")
                        .setTimestamp("2026-06-12T10:00:00Z")
                        .setStatus(Status.completed)
                        .setMetadata(null)
                        .build());

        // produce() must accept an existing producer — it never constructs one.
        AvroProducer.produce(producer, "demo-topic", messages);

        verify(producer, times(1)).send(any(ProducerRecord.class), any());
        verify(producer, times(1)).flush();
    }

    @Test
    void producerSourceCreatesExactlyOneProducer() throws Exception {
        // Reusing one producer is a core principle — main() must create it once.
        Path source = Path.of("src/main/java/com/example/kafka/AvroProducer.java");
        String code = Files.readString(source);
        int count = code.split("new KafkaProducer", -1).length - 1;
        assertEquals(1, count, "Expected exactly one 'new KafkaProducer' instantiation");
    }

    // ---------------------------------------------------------------------
    // Consumer. Omit if no consumer was generated.
    // ---------------------------------------------------------------------

    @Test
    void consumerUsesSchemaRegistryAndWakeupShutdown() throws Exception {
        Path source = Path.of("src/main/java/com/example/kafka/AvroConsumer.java");
        String code = Files.readString(source);

        assertTrue(
                code.contains("KafkaAvroDeserializer")
                        || code.contains("KafkaJsonSchemaDeserializer")
                        || code.contains("KafkaProtobufDeserializer"),
                "Consumer must use a Schema Registry deserializer for values");
        assertTrue(code.contains("wakeup"), "Consumer must use the wakeup() shutdown pattern");

        int wakeupPos = code.indexOf("catch (WakeupException");
        int closePos = code.lastIndexOf(".close()");
        assertTrue(wakeupPos > 0 && closePos > wakeupPos,
                "Consumer must close() in finally after catching WakeupException");
    }

    // ---------------------------------------------------------------------
    // Avro schema. For JSON Schema, validate value.schema.json instead.
    // ---------------------------------------------------------------------

    @Test
    void avroSchemaIsRecordWithDocsOnEveryField() throws Exception {
        Schema schema = new Schema.Parser().parse(new File("src/main/avro/value.avsc"));

        assertEquals(Schema.Type.RECORD, schema.getType(), "Top-level schema must be a record");
        assertNotNull(schema.getDoc(), "Record must have a top-level doc for governance");
        assertTrue(schema.getFields().size() > 0, "Record must have at least one field");
        for (Schema.Field field : schema.getFields()) {
            assertNotNull(field.doc(), "Field '" + field.name() + "' must have a doc");
        }
    }
}
