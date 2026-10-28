using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using ExampleKafka.Avro;

namespace ExampleKafka
{
    /// <summary>
    /// Avro alternative to JsonSchemaConsumer.cs. Deserializes into the generated
    /// Transaction class (avrogen from value.avsc). Same CancellationToken shutdown
    /// and explicit-commit pattern as the JSON consumer.
    /// </summary>
    public static class AvroConsumer
    {
        public static async Task Main()
        {
            var env = KafkaConfig.LoadEnv();
            var consumerConfig = KafkaConfig.BaseConsumerConfig(env);
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
            var deserializer = new AvroDeserializer<Transaction>(schemaRegistry).AsSyncOverAsync();

            using var consumer = new ConsumerBuilder<string, Transaction>(consumerConfig)
                .SetValueDeserializer(deserializer)
                .Build();

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
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
                        // avrogen keeps the .avsc field casing verbatim -- transactionId, not TransactionId.
                        Console.WriteLine($"Consumed {record.TopicPartitionOffset}: {record.Message.Key} -> {record.Message.Value.transactionId}");
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
                // Expected on shutdown -- fall through.
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
