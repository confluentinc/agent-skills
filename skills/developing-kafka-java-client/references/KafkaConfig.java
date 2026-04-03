package examples;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaConfig {

    /**
     * Load configuration from a properties file on the classpath.
     *
     * @param resourceName the properties file name (e.g., "kafka.properties")
     * @return a Properties object with all loaded settings
     */
    public static Properties loadConfig(String resourceName) throws IOException {
        Properties props = new Properties();
        try (InputStream input = KafkaConfig.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (input == null) {
                throw new IOException("Properties file not found: " + resourceName);
            }
            props.load(input);
        }
        return props;
    }

    /**
     * Build Kafka client properties from the loaded config.
     * Adds SASL_SSL settings for cloud, PLAINTEXT for local.
     *
     * @param config the base properties loaded from file
     * @return a new Properties object ready for KafkaProducer/KafkaConsumer
     */
    public static Properties getKafkaConfig(Properties config) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", config.getProperty("bootstrap.servers"));

        String env = config.getProperty("kafka.env", "cloud");
        if ("local".equals(env)) {
            kafkaProps.put("security.protocol", "PLAINTEXT");
        } else {
            kafkaProps.put("security.protocol", config.getProperty("security.protocol", "SASL_SSL"));
            kafkaProps.put("sasl.mechanism", config.getProperty("sasl.mechanism", "PLAIN"));
            kafkaProps.put("sasl.jaas.config", config.getProperty("sasl.jaas.config"));
        }

        // Schema Registry config
        kafkaProps.put("schema.registry.url", config.getProperty("schema.registry.url"));
        if (!"local".equals(env)) {
            String srAuth = config.getProperty("basic.auth.credentials.source");
            if (srAuth != null) {
                kafkaProps.put("basic.auth.credentials.source", srAuth);
            }
            String srUserInfo = config.getProperty("basic.auth.user.info");
            if (srUserInfo != null) {
                kafkaProps.put("basic.auth.user.info", srUserInfo);
            }
        }

        return kafkaProps;
    }

    /**
     * Verify Kafka broker connectivity and topic existence.
     */
    public static boolean verifyKafkaSetup(Properties kafkaProps, String topic) {
        if (topic == null || topic.isEmpty()) {
            System.out.println("No topic specified");
            return false;
        }

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafkaProps.getProperty("bootstrap.servers"));
        adminProps.put("security.protocol", kafkaProps.getProperty("security.protocol", "PLAINTEXT"));
        if (kafkaProps.containsKey("sasl.mechanism")) {
            adminProps.put("sasl.mechanism", kafkaProps.getProperty("sasl.mechanism"));
        }
        if (kafkaProps.containsKey("sasl.jaas.config")) {
            adminProps.put("sasl.jaas.config", kafkaProps.getProperty("sasl.jaas.config"));
        }

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get(10, TimeUnit.SECONDS);
            if (!topicNames.contains(topic)) {
                System.out.println("Topic '" + topic + "' not found. Available: " + topicNames);
                return false;
            }
            return true;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.out.println("Kafka connection error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Verify Schema Registry connectivity with an HTTP health check.
     */
    public static boolean verifySchemaRegistry(String srUrl, String srKey, String srSecret) {
        try {
            URL url = new URL(srUrl + "/subjects");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            if (srKey != null && !srKey.isEmpty()) {
                String auth = srKey + ":" + srSecret;
                String encoded = Base64.getEncoder().encodeToString(auth.getBytes());
                conn.setRequestProperty("Authorization", "Basic " + encoded);
            }

            int status = conn.getResponseCode();
            conn.disconnect();
            return status >= 200 && status < 300;
        } catch (IOException e) {
            System.out.println("Schema Registry connection error: " + e.getMessage());
            return false;
        }
    }
}
