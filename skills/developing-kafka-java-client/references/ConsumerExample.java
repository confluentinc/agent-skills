package examples;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerExample {

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

        // Configure consumer
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("group.id", "java-consumer-group"));
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer ONCE
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(kafkaProps);

        // Shutdown hook: wakeup() causes poll() to throw WakeupException
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down consumer...");
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Listening on topic: " + topic);

            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("Consumed: key=%s, value=%s, partition=%d, offset=%d%n",
                        record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown — ignore
        } finally {
            consumer.close();
            System.out.println("Consumer closed");
        }
    }
}
