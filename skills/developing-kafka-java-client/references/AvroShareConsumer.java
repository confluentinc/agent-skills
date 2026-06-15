package com.example.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Share consumer ("Queues for Kafka", KIP-932) that deserializes via Schema Registry into the
 * generated SpecificRecord {@code Transaction}. Unlike {@link AvroConsumer}, multiple instances of
 * this app in the SAME share group can consume from the SAME partition cooperatively — the broker
 * hands a different subset of records to each member, giving queue-like semantics where you can scale
 * consumers beyond the partition count. There is NO per-partition ordering guarantee across members.
 *
 * Records must be acknowledged. This example uses EXPLICIT acknowledgement
 * (share.acknowledgement.mode=explicit): after handling each record, call
 * consumer.acknowledge(record, AcknowledgeType.ACCEPT). Use AcknowledgeType.RELEASE to redeliver a
 * record (transient failure) or AcknowledgeType.REJECT to send it to the share group's dead-letter
 * handling (poison record). The wakeup pattern provides graceful shutdown, exactly as in AvroConsumer.
 *
 * Requires Apache Kafka 4.x clients. The share feature must be enabled on the cluster and
 * share.auto.offset.reset configured on the share group — see references/share-consumer.md.
 */
public class AvroShareConsumer {

    public static void consume(ShareConsumer<String, Transaction> consumer, String topic) {
        consumer.subscribe(List.of(topic));
        System.out.println("Share consumer listening on topic: " + topic);

        // wakeup() is the expected shutdown signal — it makes poll() throw WakeupException.
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            while (true) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Transaction> record : records) {
                    try {
                        System.out.printf("key=%s value=%s%n", record.key(), record.value());
                        // Successfully processed — accept so the record is not redelivered.
                        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    } catch (Exception e) {
                        // Transient failure — release for redelivery to another member.
                        consumer.acknowledge(record, AcknowledgeType.RELEASE);
                    }
                }
                // Commit acknowledgements for this batch before the next poll.
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // Expected on shutdown — fall through to close().
        } finally {
            consumer.close();
            System.out.println("Share consumer closed");
        }
    }

    public static void main(String[] args) {
        KafkaConfig.Env env = KafkaConfig.loadEnv();
        String topic = env.get("TOPIC", "demo-topic");

        Properties props = KafkaConfig.baseProperties(env);
        // The share GROUP id — all members sharing this id cooperatively consume the topic.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, env.get("GROUP_ID", "java-share-group"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        // Deserialize into the generated SpecificRecord type rather than a GenericRecord.
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        // Explicit acknowledgement gives per-record control (ACCEPT / RELEASE / REJECT).
        // Omit this line to use implicit mode, where records are acknowledged on the next poll().
        props.put("share.acknowledgement.mode", "explicit");
        // NOTE: where consumption starts is set on the share GROUP, not the client:
        //   kafka-configs --alter --group <group> --add-config share.auto.offset.reset=earliest
        // (auto.offset.reset on the client Properties is ignored for share consumers.)

        if (!KafkaConfig.verifyKafkaSetup(props, topic)) {
            throw new IllegalStateException("Failed to verify Kafka setup");
        }
        System.out.println("Connected to Kafka (" + env.get("BOOTSTRAP_SERVER") + ")");

        String srUrl = env.get("SCHEMA_REGISTRY_URL");
        if (!KafkaConfig.verifySchemaRegistry(srUrl, env.get("SR_API_KEY"), env.get("SR_API_SECRET"))) {
            throw new IllegalStateException("Failed to connect to Schema Registry");
        }
        System.out.println("Connected to Schema Registry (" + srUrl + ")");

        ShareConsumer<String, Transaction> consumer = new KafkaShareConsumer<>(props);
        consume(consumer, topic);
    }
}
