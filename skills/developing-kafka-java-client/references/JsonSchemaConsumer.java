package com.example.kafka;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * JSON Schema consumer. Deserializes into the {@link JsonSchemaProducer.Transaction}
 * POJO via KafkaJsonSchemaDeserializer. Uses the wakeup + close() shutdown pattern.
 */
public class JsonSchemaConsumer {

    public static void consume(Consumer<String, JsonSchemaProducer.Transaction> consumer, String topic) {
        consumer.subscribe(List.of(topic));
        System.out.println("Listening on topic: " + topic);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            while (true) {
                ConsumerRecords<String, JsonSchemaProducer.Transaction> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, JsonSchemaProducer.Transaction> record : records) {
                    System.out.printf("key=%s value=%s%n", record.key(), record.value());
                }
                // Steady state: commit asynchronously so the poll loop keeps moving.
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            // Expected on shutdown — fall through to the final commit + close().
        } finally {
            try {
                // Final commit: block until the broker acknowledges the last processed offset.
                consumer.commitSync();
            } finally {
                consumer.close();
                System.out.println("Consumer closed");
            }
        }
    }

    public static void main(String[] args) {
        KafkaConfig.Env env = KafkaConfig.loadEnv();
        String topic = env.get("TOPIC", "demo-topic");

        Properties props = KafkaConfig.baseProperties(env);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, env.get("GROUP_ID", "java-consumer-group"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Next-gen consumer rebalance protocol (KIP-848, AK 4.0+): eliminates stop-the-world
        // rebalances. Requires a 4.0+ broker; drop this line if pinned to a 3.x broker.
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        // Commit offsets explicitly after processing rather than on a timer, to tighten the
        // window for duplicate processing. See references/consumer.md.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        // Tell the deserializer which POJO to bind the JSON value to.
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonSchemaProducer.Transaction.class.getName());

        if (!KafkaConfig.verifyKafkaSetup(props, topic)) {
            throw new IllegalStateException("Failed to verify Kafka setup");
        }
        String srUrl = env.get("SCHEMA_REGISTRY_URL");
        if (!KafkaConfig.verifySchemaRegistry(srUrl, env.get("SR_API_KEY"), env.get("SR_API_SECRET"))) {
            throw new IllegalStateException("Failed to connect to Schema Registry");
        }

        Consumer<String, JsonSchemaProducer.Transaction> consumer = new KafkaConsumer<>(props);
        consume(consumer, topic);
    }
}
