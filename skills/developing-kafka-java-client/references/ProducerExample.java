package examples;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

public class ProducerExample {

    /**
     * Load the Avro schema from the schemas/ directory.
     */
    public static Schema loadAvroSchema() throws IOException {
        Path schemaPath = Path.of("schemas", "value.avsc");
        if (Files.exists(schemaPath)) {
            return new Schema.Parser().parse(schemaPath.toFile());
        }
        // Fallback: try loading from classpath
        try (InputStream is = ProducerExample.class.getClassLoader().getResourceAsStream("schemas/value.avsc")) {
            if (is == null) {
                throw new IOException("Schema file not found: schemas/value.avsc");
            }
            return new Schema.Parser().parse(is);
        }
    }

    /**
     * Produce messages using an existing producer instance.
     *
     * The producer is passed in — never create a new producer per call.
     * This method can be called multiple times with the same producer.
     */
    public static void produce(KafkaProducer<String, GenericRecord> producer,
                               String topic,
                               List<GenericRecord> messages) {
        for (GenericRecord record : messages) {
            ProducerRecord<String, GenericRecord> producerRecord =
                new ProducerRecord<>(topic, record.get("id") != null ? record.get("id").toString() : null, record);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.err.println("Delivery failed: " + exception.getMessage());
                    } else {
                        System.out.printf("Produced: partition=%d, offset=%d%n",
                            metadata.partition(), metadata.offset());
                    }
                }
            });
        }

        // Block until all in-flight messages are delivered
        producer.flush();
    }

    public static void main(String[] args) throws Exception {
        Properties config = KafkaConfig.loadConfig("kafka.properties");
        Properties kafkaProps = KafkaConfig.getKafkaConfig(config);
        String topic = config.getProperty("topic", "demo-topic");

        // Verify connectivity
        if (!KafkaConfig.verifyKafkaSetup(kafkaProps, topic)) {
            throw new RuntimeException("Failed to verify Kafka setup");
        }
        System.out.println("Connected to Kafka (" + config.getProperty("bootstrap.servers") + ")");

        if (!KafkaConfig.verifySchemaRegistry(
                config.getProperty("schema.registry.url"),
                config.getProperty("basic.auth.user.info", "").split(":")[0],
                config.getProperty("basic.auth.user.info", ":").split(":").length > 1
                    ? config.getProperty("basic.auth.user.info", ":").split(":")[1] : "")) {
            throw new RuntimeException("Failed to connect to Schema Registry");
        }
        System.out.println("Connected to Schema Registry (" + config.getProperty("schema.registry.url") + ")");

        // Configure producer
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        Schema avroSchema = loadAvroSchema();

        // Create producer ONCE and reuse
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProps);

        // Shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down producer...");
            producer.flush();
            producer.close();
        }));

        try {
            // -- Generate sample messages here, adapted to the user's domain --
            // Example: build GenericRecord instances from the loaded schema
            // GenericRecord record = new GenericData.Record(avroSchema);
            // record.put("field_name", "value");
            // List<GenericRecord> messages = List.of(record);
            // produce(producer, topic, messages);
            List<GenericRecord> messages = List.of(); // Replace with domain-specific sample data
            produce(producer, topic, messages);
        } finally {
            producer.flush();
            producer.close();
            System.out.println("Producer closed");
        }
    }
}
