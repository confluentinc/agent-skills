using Confluent.Kafka;
using Moq;
using Xunit;

namespace ExampleKafka.Tests
{
    /// <summary>
    /// Runs without a live Kafka cluster or Schema Registry -- IProducer&lt;TKey,TValue&gt;
    /// and IConsumer&lt;TKey,TValue&gt; are real interfaces on Confluent.Kafka's
    /// Producer&lt;TKey,TValue&gt;/Consumer&lt;TKey,TValue&gt;, so Moq can substitute for them.
    /// </summary>
    public class KafkaConfigTests
    {
        [Fact]
        public void BaseProducerConfig_UsesSaslSsl_WhenCloud()
        {
            var env = new System.Collections.Generic.Dictionary<string, string>
            {
                ["KAFKA_ENV"] = "cloud",
                ["BOOTSTRAP_SERVER"] = "pkc-xxxxx.aws.confluent.cloud:9092",
                ["API_KEY"] = "key",
                ["API_SECRET"] = "secret",
            };

            var config = KafkaConfig.BaseProducerConfig(env);

            Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
            Assert.Equal(SaslMechanism.Plain, config.SaslMechanism);
        }

        [Fact]
        public void BaseProducerConfig_UsesPlaintext_WhenLocal()
        {
            var env = new System.Collections.Generic.Dictionary<string, string>
            {
                ["KAFKA_ENV"] = "local",
                ["BOOTSTRAP_SERVER"] = "localhost:9092",
            };

            var config = KafkaConfig.BaseProducerConfig(env);

            Assert.Equal(SecurityProtocol.Plaintext, config.SecurityProtocol);
            Assert.Null(config.SaslMechanism);
        }

        [Fact]
        public void BaseConsumerConfig_DisablesAutoCommit()
        {
            var env = new System.Collections.Generic.Dictionary<string, string>
            {
                ["KAFKA_ENV"] = "local",
                ["BOOTSTRAP_SERVER"] = "localhost:9092",
            };

            var config = KafkaConfig.BaseConsumerConfig(env);

            Assert.False(config.EnableAutoCommit);
            Assert.Equal("dotnet-consumer-group", config.GroupId);
        }
    }

    public class JsonSchemaProducerTests
    {
        [Fact]
        public void Produce_SendsExactlyOnce_WithExpectedKey()
        {
            var mockProducer = new Mock<IProducer<string, Value>>();

            JsonSchemaProducer.Produce(mockProducer.Object, "demo-topic", "order-1",
                new Value { OrderId = "order-1", CustomerId = "cust-1", Total = 9.99 });

            mockProducer.Verify(p => p.Produce(
                "demo-topic",
                It.Is<Message<string, Value>>(m => m.Key == "order-1" && m.Value.OrderId == "order-1"),
                It.IsAny<Action<DeliveryReport<string, Value>>>()), Times.Once);
        }
    }

    public class JsonSchemaProducerSyncTests
    {
        [Fact]
        public async Task ProduceAsync_AwaitsDeliveryResult()
        {
            var mockProducer = new Mock<IProducer<string, Value>>();
            var expectedResult = new DeliveryResult<string, Value>
            {
                TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition("demo-topic", 0), new Offset(1)),
                Message = new Message<string, Value> { Key = "order-2", Value = new Value { OrderId = "order-2" } },
            };
            mockProducer
                .Setup(p => p.ProduceAsync("demo-topic", It.IsAny<Message<string, Value>>(), default))
                .ReturnsAsync(expectedResult);

            await JsonSchemaProducerSync.ProduceAsync(mockProducer.Object, "demo-topic", "order-2",
                new Value { OrderId = "order-2" });

            mockProducer.Verify(p => p.ProduceAsync("demo-topic", It.IsAny<Message<string, Value>>(), default), Times.Once);
        }
    }

    public class SchemaGenerationTests
    {
        [Fact]
        public void JsonSchema_HasDescriptionOnEveryProperty()
        {
            var schema = NJsonSchema.JsonSchema.FromType<Value>();

            Assert.All(schema.Properties.Values, p => Assert.False(string.IsNullOrWhiteSpace(p.Description)));
        }
    }
}
