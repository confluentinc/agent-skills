using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace ExampleKafka
{
    /// <summary>
    /// ProduceAsync producer: awaits per record, confirming each write before the next.
    /// Lowest throughput but strongest per-message confirmation and simplest error
    /// handling -- best for batch/ETL scripts. ProduceAsync() accepts the Schema
    /// Registry JsonSerializer&lt;T&gt; directly as an IAsyncSerializer&lt;T&gt; --
    /// do NOT wrap it with .AsSyncOverAsync() here (that wrapper is only needed for
    /// the delivery-callback Produce() method; see JsonSchemaProducer.cs).
    /// </summary>
    public static class JsonSchemaProducerSync
    {
        public static async Task Main()
        {
            var env = KafkaConfig.LoadEnv();
            var producerConfig = KafkaConfig.BaseProducerConfig(env);
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

            var schemaJson = NJsonSchema.JsonSchema.FromType<Value>().ToJson();
            var schemaId = await JsonSchemaProducer.RegisterSchemaAsync(schemaRegistry, $"{topic}-value", schemaJson);
            Console.WriteLine($"Registered schema id: {schemaId}");

            var serializerConfig = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
            };
            var serializer = new JsonSerializer<Value>(schemaRegistry, serializerConfig);

            using var producer = new ProducerBuilder<string, Value>(producerConfig)
                .SetValueSerializer(serializer)
                .Build();

            var sample = new Value
            {
                OrderId = "order-1",
                CustomerId = "cust-1",
                Total = 42.50,
                Timestamp = DateTime.UtcNow.ToString("O"),
            };

            await ProduceAsync(producer, topic, sample.OrderId, sample);

            producer.Flush(TimeSpan.FromSeconds(10));
        }

        /// <summary>
        /// Accepts an IProducer&lt;TKey,TValue&gt; parameter -- never constructs one.
        /// Awaits the delivery result for this record before returning, so callers
        /// naturally confirm one write before starting the next.
        /// </summary>
        public static async Task ProduceAsync(IProducer<string, Value> producer, string topic, string key, Value value)
        {
            try
            {
                var result = await producer.ProduceAsync(topic, new Message<string, Value> { Key = key, Value = value });
                Console.WriteLine($"Delivered to {result.TopicPartitionOffset}");
            }
            catch (ProduceException<string, Value> ex)
            {
                Console.Error.WriteLine($"Delivery failed for key '{key}': {ex.Error.Reason}");
                throw;
            }
        }
    }
}
