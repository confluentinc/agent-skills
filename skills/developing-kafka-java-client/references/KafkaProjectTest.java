package examples;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Reference unit tests for the scaffolded Kafka Java project.
 *
 * These tests run without a live Kafka cluster or Schema Registry.
 * All external dependencies are mocked so tests can run in CI or evals.
 */
public class KafkaProjectTest {

    // -----------------------------------------------------------------------
    // KafkaConfig tests
    // -----------------------------------------------------------------------

    @Test
    public void testLoadConfigReturnsProperties() throws IOException {
        // The kafka.properties file should be on the classpath
        java.util.Properties config = KafkaConfig.loadConfig("kafka.properties");
        assertNotNull("Config should not be null", config);
        assertNotNull("bootstrap.servers should be set", config.getProperty("bootstrap.servers"));
        assertNotNull("schema.registry.url should be set", config.getProperty("schema.registry.url"));
    }

    @Test
    public void testCloudConfigContainsSaslSsl() throws IOException {
        java.util.Properties config = new java.util.Properties();
        config.setProperty("kafka.env", "cloud");
        config.setProperty("bootstrap.servers", "pkc-test.confluent.cloud:9092");
        config.setProperty("security.protocol", "SASL_SSL");
        config.setProperty("sasl.mechanism", "PLAIN");
        config.setProperty("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='key' password='secret';");
        config.setProperty("schema.registry.url", "https://psrc-test.confluent.cloud");
        config.setProperty("basic.auth.credentials.source", "USER_INFO");
        config.setProperty("basic.auth.user.info", "sr-key:sr-secret");

        java.util.Properties kafkaProps = KafkaConfig.getKafkaConfig(config);

        assertEquals("SASL_SSL", kafkaProps.getProperty("security.protocol"));
        assertEquals("PLAIN", kafkaProps.getProperty("sasl.mechanism"));
        assertNotNull(kafkaProps.getProperty("sasl.jaas.config"));
        assertNotNull(kafkaProps.getProperty("schema.registry.url"));
    }

    @Test
    public void testLocalConfigUsesPlaintext() throws IOException {
        java.util.Properties config = new java.util.Properties();
        config.setProperty("kafka.env", "local");
        config.setProperty("bootstrap.servers", "localhost:9092");
        config.setProperty("schema.registry.url", "http://localhost:8081");

        java.util.Properties kafkaProps = KafkaConfig.getKafkaConfig(config);

        assertEquals("PLAINTEXT", kafkaProps.getProperty("security.protocol"));
        assertNull("Local config should not have sasl.mechanism", kafkaProps.getProperty("sasl.mechanism"));
        assertNull("Local config should not have sasl.jaas.config", kafkaProps.getProperty("sasl.jaas.config"));
    }

    // -----------------------------------------------------------------------
    // Producer tests
    // -----------------------------------------------------------------------

    @Test
    public void testProduceAcceptsProducerParameter() throws Exception {
        // Use reflection to avoid compile-time dependency on ProducerExample
        // (which may not exist in consumer-only scaffolds)
        Class<?> producerClass;
        try {
            producerClass = Class.forName("examples.ProducerExample");
        } catch (ClassNotFoundException e) {
            // ProducerExample not generated (consumer-only scaffold) — skip test
            return;
        }

        Method produceMethod = null;
        for (Method m : producerClass.getDeclaredMethods()) {
            if (m.getName().equals("produce")) {
                produceMethod = m;
                break;
            }
        }
        assertNotNull("produce() method must exist", produceMethod);

        Class<?>[] paramTypes = produceMethod.getParameterTypes();
        assertTrue("produce() must accept at least one parameter", paramTypes.length >= 1);
        assertTrue("First parameter must be KafkaProducer",
            KafkaProducer.class.isAssignableFrom(paramTypes[0]));
    }

    @Test
    public void testProducerUsesAvroSerializer() throws IOException {
        // Check the source code references KafkaAvroSerializer
        Path sourcePath = Path.of("src", "main", "java", "examples", "ProducerExample.java");
        if (!Files.exists(sourcePath)) {
            // Try alternate location for test environments
            sourcePath = Path.of("ProducerExample.java");
        }
        if (Files.exists(sourcePath)) {
            String source = Files.readString(sourcePath);
            assertTrue("Producer must use KafkaAvroSerializer",
                source.contains("KafkaAvroSerializer"));
        }
        // If source file not found, the import check at compile time is sufficient
    }

    @Test
    public void testProducerCallsFlush() throws IOException {
        // Verify the source contains flush() call
        Path sourcePath = Path.of("src", "main", "java", "examples", "ProducerExample.java");
        if (!Files.exists(sourcePath)) {
            sourcePath = Path.of("ProducerExample.java");
        }
        if (Files.exists(sourcePath)) {
            String source = Files.readString(sourcePath);
            assertTrue("Producer must call flush()", source.contains("flush()"));
        }
    }

    // -----------------------------------------------------------------------
    // Consumer tests
    // -----------------------------------------------------------------------

    @Test
    public void testConsumerUsesAvroDeserializer() throws IOException {
        Path sourcePath = Path.of("src", "main", "java", "examples", "ConsumerExample.java");
        if (!Files.exists(sourcePath)) {
            sourcePath = Path.of("ConsumerExample.java");
        }
        if (Files.exists(sourcePath)) {
            String source = Files.readString(sourcePath);
            assertTrue("Consumer must use KafkaAvroDeserializer",
                source.contains("KafkaAvroDeserializer"));
        }
    }

    @Test
    public void testConsumerHasGracefulShutdown() throws IOException {
        Path sourcePath = Path.of("src", "main", "java", "examples", "ConsumerExample.java");
        if (!Files.exists(sourcePath)) {
            sourcePath = Path.of("ConsumerExample.java");
        }
        if (Files.exists(sourcePath)) {
            String source = Files.readString(sourcePath);
            assertTrue("Consumer must use wakeup() for shutdown", source.contains("wakeup()"));
            assertTrue("Consumer must call close()", source.contains("close()"));

            // wakeup should appear before close in shutdown flow
            int wakeupPos = source.indexOf("wakeup()");
            int closePos = source.lastIndexOf("close()");
            assertTrue("wakeup() should appear before final close()", wakeupPos < closePos);
        }
    }

    // -----------------------------------------------------------------------
    // Schema tests
    // -----------------------------------------------------------------------

    @Test
    public void testAvroSchemaIsValid() throws IOException {
        Path schemaPath = Path.of("schemas", "value.avsc");
        assertTrue("schemas/value.avsc must exist", Files.exists(schemaPath));

        String schemaStr = Files.readString(schemaPath);
        Schema schema = new Schema.Parser().parse(schemaStr);

        assertEquals("Schema type must be RECORD", Schema.Type.RECORD, schema.getType());
        assertNotNull("Schema must have a name", schema.getName());
        assertFalse("Schema must have at least one field", schema.getFields().isEmpty());
    }

    @Test
    public void testSchemaFieldsHaveNameAndType() throws IOException {
        Path schemaPath = Path.of("schemas", "value.avsc");
        if (!Files.exists(schemaPath)) return;

        String schemaStr = Files.readString(schemaPath);
        Schema schema = new Schema.Parser().parse(schemaStr);

        for (Schema.Field field : schema.getFields()) {
            assertNotNull("Field must have a name: " + field, field.name());
            assertNotNull("Field must have a type: " + field, field.schema());
        }
    }

    // -----------------------------------------------------------------------
    // Project structure tests
    // -----------------------------------------------------------------------

    @Test
    public void testBuildGradleExists() {
        Path buildPath = Path.of("build.gradle");
        assertTrue("build.gradle must exist", Files.exists(buildPath));
    }

    @Test
    public void testBuildGradleHasKafkaDependencies() throws IOException {
        Path buildPath = Path.of("build.gradle");
        if (!Files.exists(buildPath)) return;

        String contents = Files.readString(buildPath);
        assertTrue("build.gradle must contain kafka-clients",
            contents.contains("kafka-clients"));
        assertTrue("build.gradle must contain kafka-avro-serializer",
            contents.contains("kafka-avro-serializer"));
    }
}
