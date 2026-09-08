using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace ExampleKafka
{
    /// <summary>
    /// Delivery-handler callback producer (recommended default): non-blocking, best
    /// throughput. Produce() requires a synchronous ISerializer&lt;T&gt;, so the
    /// Schema Registry JsonSerializer&lt;T&gt; (which implements IAsyncSerializer&lt;T&gt;)
    /// is wrapped with .AsSyncOverAsync(). If you need ProduceAsync() instead, use the
    /// serializer directly -- do NOT wrap it there. See JsonSchemaProducerSync.cs.
    /// </summary>
    public static class JsonSchemaProducer
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

            // Register the schema explicitly, before producing. Never rely on
            // AutoRegisterSchemas -- CI/CD should own schema evolution, not the app.
            var schemaJson = NJsonSchema.JsonSchema.FromType<Value>().ToJson();
            var schemaId = await RegisterSchemaAsync(schemaRegistry, $"{topic}-value", schemaJson);
            Console.WriteLine($"Registered schema id: {schemaId}");

            var serializerConfig = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
            };
            var serializer = new JsonSerializer<Value>(schemaRegistry, serializerConfig);

            using var producer = new ProducerBuilder<string, Value>(producerConfig)
                .SetValueSerializer(serializer.AsSyncOverAsync())
                .Build();

            var sample = new Value
            {
                OrderId = "order-1",
                CustomerId = "cust-1",
                Total = 42.50,
                Timestamp = DateTime.UtcNow.ToString("O"),
            };

            Produce(producer, topic, sample.OrderId, sample);

            // Give in-flight deliveries a chance to complete before exiting.
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        /// <summary>
        /// Accepts an IProducer&lt;TKey,TValue&gt; parameter -- never constructs one.
        /// Producer instances are thread-safe and expensive to create; create exactly
        /// one and reuse it across sends.
        /// </summary>
        public static void Produce(IProducer<string, Value> producer, string topic, string key, Value value)
        {
            producer.Produce(topic, new Message<string, Value> { Key = key, Value = value }, report =>
            {
                if (report.Error.IsError)
                {
                    Console.Error.WriteLine($"Delivery failed for key '{report.Message.Key}': {report.Error.Reason}");
                }
                else
                {
                    Console.WriteLine($"Delivered to {report.TopicPartitionOffset}");
                }
            });
        }

        /// <summary>Registers the schema explicitly and returns the schema id. Errors propagate -- never swallowed.</summary>
        public static async Task<int> RegisterSchemaAsync(ISchemaRegistryClient schemaRegistry, string subject, string schemaJson)
        {
            var schema = new Schema(schemaJson, SchemaType.Json);
            return await schemaRegistry.RegisterSchemaAsync(subject, schema);
        }
    }
}
