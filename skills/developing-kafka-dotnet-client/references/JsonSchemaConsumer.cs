using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace ExampleKafka
{
    /// <summary>
    /// Deserializes via Schema Registry -- no raw string/JSON fallback. Uses a
    /// CancellationToken for graceful shutdown (the .NET idiom): Consume(token)
    /// throws OperationCanceledException when the token is cancelled, instead of
    /// Java's consumer.wakeup() + WakeupException. Commits offsets explicitly
    /// after processing (EnableAutoCommit = false).
    /// </summary>
    public static class JsonSchemaConsumer
    {
        public static async Task Main()
        {
            var env = KafkaConfig.LoadEnv();
            var consumerConfig = KafkaConfig.BaseConsumerConfig(env);
            // KIP-848 next-gen rebalance protocol (Kafka 4.0+ clients and brokers).
            // Eliminates stop-the-world rebalances. Falls back to Classic if unset.
            consumerConfig.GroupProtocol = GroupProtocol.Consumer;
            var topic = KafkaConfig.Get(env, "TOPIC", "demo-topic");

            var adminConfig = KafkaConfig.BaseAdminConfig(env);
            if (!KafkaConfig.VerifyKafkaSetup(adminConfig, topic))
            {
                throw new InvalidOperationException("Failed to verify Kafka setup");
            }

            var srUrl = KafkaConfig.Get(env, "SCHEMA_REGISTRY_URL");
            var srKey = KafkaConfig.Get(env, "SR_API_KEY");
            var srSecret = KafkaConfig.Get(env, "SR_API_SECRET");
            if (!await KafkaConfig.VerifySchemaRegistryAsync(srUrl, srKey, srSecret))
            {
                throw new InvalidOperationException("Failed to connect to Schema Registry");
            }

            using var schemaRegistry = new CachedSchemaRegistryClient(KafkaConfig.SchemaRegistryConfig(env));
            // JsonDeserializer<T> deserializes the JSON body directly (after skipping the Confluent
            // wire-format header) and does not need an ISchemaRegistryClient -- unlike AvroDeserializer,
            // it never fetches the writer schema from the registry to decode a record.
            var deserializer = new JsonDeserializer<Value>(schemaRegistry).AsSyncOverAsync();

            using var consumer = new ConsumerBuilder<string, Value>(consumerConfig)
                .SetValueDeserializer(deserializer)
                .Build();

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent immediate process termination
                cts.Cancel();
            };

            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    try
                    {
                        var record = consumer.Consume(cts.Token);
                        Process(record);
                        consumer.Commit(record);
                    }
                    catch (ConsumeException ex)
                    {
                        Console.Error.WriteLine($"Consume error: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown (Ctrl+C / CancellationToken cancelled) -- fall through.
            }
            finally
            {
                consumer.Close();
            }
        }

        private static void Process(ConsumeResult<string, Value> record)
        {
            Console.WriteLine($"Consumed {record.TopicPartitionOffset}: {record.Message.Key} -> {record.Message.Value.OrderId}");
        }
    }
}
