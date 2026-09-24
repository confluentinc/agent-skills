using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace ExampleKafka
{
    /// <summary>
    /// Configuration loading and connectivity verification shared by the producer and consumer.
    /// Settings are loaded from appsettings.json (via System.Text.Json, no extra dependency),
    /// falling back to the process environment. NEVER log credential values -- reference them by name only.
    /// </summary>
    public static class KafkaConfig
    {
        private static readonly string ConfigFile =
            Environment.GetEnvironmentVariable("CONFIG_FILE") ?? "appsettings.json";

        /// <summary>
        /// Load configuration from appsettings.json if present, then fall back to process
        /// environment variables for any key absent from the file. A value in the file
        /// takes precedence over the environment.
        /// </summary>
        public static IReadOnlyDictionary<string, string> LoadEnv()
        {
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
             
            if (File.Exists(ConfigFile))
            {
                using var stream = File.OpenRead(ConfigFile);
                using var doc = JsonDocument.Parse(stream);
                foreach (var property in doc.RootElement.EnumerateObject())
                {
                    values[property.Name] = property.Value.GetString() ?? "";
                }
            }

            return values;
        }

        /// <summary>
        /// Look up a config key: a value in the dictionary returned by <see cref="LoadEnv"/> wins,
        /// otherwise fall back to the process environment, otherwise <paramref name="defaultValue"/>.
        /// Public so producer/consumer code can resolve keys (e.g. TOPIC, SCHEMA_REGISTRY_URL) the
        /// same way -- reading directly from the <c>env</c> dictionary via TryGetValue skips the
        /// environment-variable fallback and silently ignores env-var-only configuration.
        /// </summary>
        public static string Get(IReadOnlyDictionary<string, string> env, string key, string? defaultValue = null)
        {
            if (env.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value))
            {
                return value;
            }
            return Environment.GetEnvironmentVariable(key) ?? defaultValue ?? "";
        }

        /// <summary>
        /// Build the base producer config. Uses SASL_SSL + PLAIN for Confluent Cloud / WarpStream,
        /// PLAINTEXT for local Docker.
        /// </summary>
        public static ProducerConfig BaseProducerConfig(IReadOnlyDictionary<string, string> env)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = Get(env, "BOOTSTRAP_SERVER"),
                ClientId = Get(env, "CLIENT_ID", "dotnet-client"),
            };
            ApplySecurity(config, env);
            return config;
        }

        /// <summary>
        /// Build the base consumer config. EnableAutoCommit is left false -- commit explicitly
        /// after processing succeeds.
        /// </summary>
        public static ConsumerConfig BaseConsumerConfig(IReadOnlyDictionary<string, string> env)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = Get(env, "BOOTSTRAP_SERVER"),
                ClientId = Get(env, "CLIENT_ID", "dotnet-client"),
                GroupId = Get(env, "GROUP_ID", "dotnet-consumer-group"),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };
            ApplySecurity(config, env);
            return config;
        }

        public static AdminClientConfig BaseAdminConfig(IReadOnlyDictionary<string, string> env)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = Get(env, "BOOTSTRAP_SERVER"),
            };
            ApplySecurity(config, env);
            return config;
        }

        private static void ApplySecurity(ClientConfig config, IReadOnlyDictionary<string, string> env)
        {
            var kafkaEnv = Get(env, "KAFKA_ENV", "cloud");
            var apiKey = Get(env, "API_KEY");
            var apiSecret = Get(env, "API_SECRET");
            // Confluent Cloud always requires SASL_SSL. Local Docker is always Plaintext (no auth).
            // WarpStream supports either -- Plaintext for unauthenticated deployments, SASL_SSL when
            // API_KEY/API_SECRET are configured -- so branch on whether credentials were provided.
            var useSasl = kafkaEnv == "cloud" || (kafkaEnv == "warpstream" && !string.IsNullOrEmpty(apiKey));
            if (useSasl)
            {
                config.SecurityProtocol = SecurityProtocol.SaslSsl;
                config.SaslMechanism = SaslMechanism.Plain;
                config.SaslUsername = apiKey;
                config.SaslPassword = apiSecret;
            }
            else
            {
                config.SecurityProtocol = SecurityProtocol.Plaintext;
            }
        }

        /// <summary>
        /// Config for constructing a CachedSchemaRegistryClient. Serializer configs
        /// (JsonSerializerConfig / AvroSerializerConfig) set AutoRegisterSchemas = false and
        /// UseLatestVersion = true separately -- schemas are registered explicitly, never
        /// auto-registered on first produce.
        /// </summary>
        public static SchemaRegistryConfig SchemaRegistryConfig(IReadOnlyDictionary<string, string> env)
        {
            var config = new SchemaRegistryConfig
            {
                Url = Get(env, "SCHEMA_REGISTRY_URL"),
            };
            var srKey = Get(env, "SR_API_KEY");
            var srSecret = Get(env, "SR_API_SECRET");
            if (!string.IsNullOrEmpty(srKey) && !string.IsNullOrEmpty(srSecret))
            {
                config.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
                config.BasicAuthUserInfo = $"{srKey}:{srSecret}";
            }
            return config;
        }

        /// <summary>Verify broker connectivity and that the topic exists.</summary>
        public static bool VerifyKafkaSetup(AdminClientConfig adminConfig, string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                Console.Error.WriteLine("No topic specified");
                return false;
            }
            try
            {
                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                var exists = metadata.Topics.Exists(t => t.Topic == topic);
                if (!exists)
                {
                    Console.Error.WriteLine($"Topic '{topic}' not found.");
                    return false;
                }
                return true;
            }
            catch (KafkaException ex)
            {
                Console.Error.WriteLine($"Kafka connection error: {ex.Message}");
                return false;
            }
        }

        /// <summary>Verify Schema Registry connectivity with an HTTP health check against /subjects.</summary>
        public static async Task<bool> VerifySchemaRegistryAsync(string srUrl, string? srKey, string? srSecret)
        {
            if (string.IsNullOrWhiteSpace(srUrl))
            {
                Console.Error.WriteLine("No Schema Registry URL specified");
                return false;
            }
            try
            {
                using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
                using var request = new HttpRequestMessage(HttpMethod.Get, $"{srUrl}/subjects");
                if (!string.IsNullOrEmpty(srKey) && !string.IsNullOrEmpty(srSecret))
                {
                    var creds = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{srKey}:{srSecret}"));
                    request.Headers.Authorization = new AuthenticationHeaderValue("Basic", creds);
                }
                var response = await client.SendAsync(request);
                return response.IsSuccessStatusCode;
            }
            catch (HttpRequestException ex)
            {
                Console.Error.WriteLine($"Schema Registry connection error: {ex.Message}");
                return false;
            }
        }
    }
}
