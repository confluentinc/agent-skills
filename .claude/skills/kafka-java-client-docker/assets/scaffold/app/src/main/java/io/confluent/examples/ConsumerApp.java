package io.confluent.examples;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consumes {@link Order} records produced with JSON Schema (JSON_SR) and prints
 * them. Reads from the earliest offset with a fixed group id, so it sees every
 * record regardless of whether it started before or after the producer.
 *
 * The loop exits 0 once EXPECTED_COUNT records have been consumed, or exits 1
 * if POLL_TIMEOUT_SECONDS elapses first — that non-zero exit lets the demo
 * (and the evals) detect a broken end-to-end pipeline instead of hanging.
 */
public class ConsumerApp {

  public static void main(String[] args) {
    String bootstrap = env("BOOTSTRAP_SERVERS", "kafka:29092");
    String schemaRegistryUrl = env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
    String topic = env("TOPIC", "orders");
    String groupId = env("GROUP_ID", "java-consumer-group");
    int expected = Integer.parseInt(env("EXPECTED_COUNT", "10"));
    long timeoutSeconds = Long.parseLong(env("POLL_TIMEOUT_SECONDS", "60"));

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
    props.put("schema.registry.url", schemaRegistryUrl);
    // Without this, the deserializer returns a generic Map instead of an Order.
    // It must be the fully-qualified class name of the value type.
    props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, Order.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    System.out.printf("Consuming from topic '%s' at %s (expecting %d records)%n",
        topic, bootstrap, expected);

    int consumed = 0;
    long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;

    try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      while (consumed < expected && System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, Order> record : records) {
          System.out.printf("Consumed key=%s value=%s (partition=%d offset=%d)%n",
              record.key(), record.value(), record.partition(), record.offset());
          consumed++;
        }
      }
      // Leave the group cleanly so a fresh run does not wait out a session timeout.
      consumer.unsubscribe();
    }

    if (consumed >= expected) {
      System.out.printf("Success: consumed %d/%d records.%n", consumed, expected);
      System.exit(0);
    } else {
      System.err.printf("Timed out: consumed only %d/%d records.%n", consumed, expected);
      System.exit(1);
    }
  }

  private static String env(String key, String defaultValue) {
    String value = System.getenv(key);
    return (value == null || value.isBlank()) ? defaultValue : value;
  }
}
