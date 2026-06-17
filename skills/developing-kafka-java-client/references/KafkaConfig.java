package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Configuration loading and connectivity verification shared by the producer and consumer.
 *
 * Settings are loaded from a .properties file (via java.util.Properties), falling back to
 * the process environment. NEVER log credential values — reference them by name only.
 */
public final class KafkaConfig {

    private KafkaConfig() {
    }

    /**
     * Minimal key/value lookup so config building is testable. In production it wraps a
     * {@link Properties} loaded from a file ({@code KafkaConfig.Env env = KafkaConfig.loadEnv()});
     * in tests it wraps a Map ({@code KafkaConfig.Env env = myMap::get}).
     */
    @FunctionalInterface
    public interface Env {
        String get(String key);

        default String get(String key, String defaultValue) {
            String value = get(key);
            return value != null ? value : defaultValue;
        }
    }

    /** Path to the .properties config file; override with -Dconfig.file=... */
    private static final String CONFIG_FILE = System.getProperty("config.file", ".properties");

    /**
     * Load configuration from the {@code .properties} file if present, then fall back to
     * process environment variables for any key absent from the file. A value in the file
     * takes precedence over the environment.
     */
    public static Env loadEnv() {
        Properties fileProps = new Properties();
        Path path = Path.of(CONFIG_FILE);
        if (Files.exists(path)) {
            try (InputStream in = Files.newInputStream(path)) {
                fileProps.load(in);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read config file: " + path, e);
            }
        }
        return key -> {
            String value = fileProps.getProperty(key);
            return value != null ? value : System.getenv(key);
        };
    }

    /**
     * Build the base Kafka client {@link Properties}.
     *
     * Uses SASL_SSL + PLAIN for Confluent Cloud / WarpStream, PLAINTEXT for local Docker.
     * The Schema Registry serializer reads {@code schema.registry.url} and the
     * {@code basic.auth.*} keys from this same Properties object, so they are added here.
     */
    public static Properties baseProperties(Env env) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.get("BOOTSTRAP_SERVER"));
        props.put("client.id", env.get("CLIENT_ID", "java-client"));

        String kafkaEnv = env.get("KAFKA_ENV", "cloud");
        if ("local".equals(kafkaEnv)) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        } else {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"%s\" password=\"%s\";",
                    env.get("API_KEY"), env.get("API_SECRET")));
        }

        // Schema Registry config consumed by the (de)serializer.
        String srUrl = env.get("SCHEMA_REGISTRY_URL");
        if (srUrl != null) {
            props.put("schema.registry.url", srUrl);
        }
        String srKey = env.get("SR_API_KEY");
        String srSecret = env.get("SR_API_SECRET");
        if (srKey != null && srSecret != null) {
            props.put("basic.auth.credentials.source", "USER_INFO");
            props.put("basic.auth.user.info", srKey + ":" + srSecret);
        }
        return props;
    }

    /** Config map for constructing a CachedSchemaRegistryClient and the serializer. */
    public static Map<String, Object> schemaRegistryConfig(Env env) {
        Map<String, Object> conf = new HashMap<>();
        conf.put("schema.registry.url", env.get("SCHEMA_REGISTRY_URL"));
        String srKey = env.get("SR_API_KEY");
        String srSecret = env.get("SR_API_SECRET");
        if (srKey != null && srSecret != null) {
            conf.put("basic.auth.credentials.source", "USER_INFO");
            conf.put("basic.auth.user.info", srKey + ":" + srSecret);
        }
        // Register schemas explicitly (see registerSchema in the producer) — never auto-register.
        conf.put("auto.register.schemas", false);
        conf.put("use.latest.version", true);
        return conf;
    }

    /** Verify broker connectivity and that the topic exists. */
    public static boolean verifyKafkaSetup(Properties baseProps, String topic) {
        if (topic == null || topic.isBlank()) {
            System.err.println("No topic specified");
            return false;
        }
        Properties adminProps = new Properties();
        adminProps.putAll(baseProps);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            if (!topics.contains(topic)) {
                System.err.printf("Topic '%s' not found. Available topics: %s%n", topic, topics);
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Kafka connection interrupted: " + e.getMessage());
            return false;
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            System.err.println("Kafka connection error: " + e.getMessage());
            return false;
        }
    }

    /** Verify Schema Registry connectivity with an HTTP health check against /subjects. */
    public static boolean verifySchemaRegistry(String srUrl, String srKey, String srSecret) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(srUrl + "/subjects"))
                    .timeout(Duration.ofSeconds(5))
                    .GET();
            if (srKey != null && srSecret != null) {
                String creds = Base64.getEncoder().encodeToString(
                        (srKey + ":" + srSecret).getBytes(StandardCharsets.UTF_8));
                builder.header("Authorization", "Basic " + creds);
            }
            HttpResponse<String> response = HttpClient.newHttpClient()
                    .send(builder.build(), HttpResponse.BodyHandlers.ofString());
            return response.statusCode() >= 200 && response.statusCode() < 300;
        } catch (IOException e) {
            System.err.println("Schema Registry connection error: " + e.getMessage());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
