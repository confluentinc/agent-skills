package io.confluent.examples;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Produces a fixed number of {@link Order} records to Kafka using JSON Schema
 * (JSON_SR) serialization. All connection details come from environment
 * variables so the same image runs unchanged inside docker-compose.
 *
 * The process produces MESSAGE_COUNT records, flushes, and exits 0 so it can
 * run as a one-shot container (the consumer depends on it completing).
 */
public class ProducerApp {

  public static void main(String[] args) {
    String bootstrap = env("BOOTSTRAP_SERVERS", "kafka:29092");
    String schemaRegistryUrl = env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
    String topic = env("TOPIC", "orders");
    int count = Integer.parseInt(env("MESSAGE_COUNT", "10"));

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // JSON_SR: the value serializer derives a JSON Schema from the POJO and
    // registers it with Schema Registry, prepending the schema id to the
    // payload (the Confluent wire format).
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
    props.put("schema.registry.url", schemaRegistryUrl);
    // For this self-contained demo we let the serializer auto-register the
    // schema on first send. In production you typically register schemas as a
    // separate CI/CD step and set this to false.
    props.put("auto.register.schemas", true);

    System.out.printf("Producing %d records to topic '%s' at %s%n", count, topic, bootstrap);

    try (Producer<String, Order> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < count; i++) {
        Order order = new Order("order-" + i, "widget", i + 1, 9.99 * (i + 1));
        // Keying by orderId keeps all events for one order on the same
        // partition, preserving per-order ordering for downstream consumers.
        ProducerRecord<String, Order> record =
            new ProducerRecord<>(topic, order.getOrderId(), order);
        producer.send(record, (metadata, exception) -> {
          if (exception != null) {
            exception.printStackTrace();
          } else {
            System.out.printf("Produced %s -> %s-%d@%d%n",
                order.getOrderId(), metadata.topic(), metadata.partition(), metadata.offset());
          }
        });
      }
      // flush() blocks until every buffered record is acknowledged; without it,
      // try-with-resources could close the producer before delivery completes.
      producer.flush();
      System.out.printf("Done. Produced %d records.%n", count);
    }
  }

  private static String env(String key, String defaultValue) {
    String value = System.getenv(key);
    return (value == null || value.isBlank()) ? defaultValue : value;
  }
}
