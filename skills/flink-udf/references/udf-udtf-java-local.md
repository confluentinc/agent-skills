# Java Scalar UDFs and UDTFs - Local Docker

Guide for building and deploying Java-based Scalar UDFs and UDTFs to a local Flink environment running in Docker.

## Overview

The implementation code is identical to Confluent Cloud. This guide focuses on the **local deployment** steps.

For implementation details, see `udf-udtf-java-confluent-cloud.md` sections:
- Project Setup
- Scalar UDF Implementation
- UDTF Implementation
- Build the JAR

## Local Deployment

### Step 1: Build the JAR

Maven:
```bash
mvn clean package
```

Gradle:
```bash
./gradlew shadowJar
```

### Step 2: Copy JAR to Flink SQL Client

```bash
# Copy to the container
docker cp target/my-udf-1.0.jar flink-sql-client:/opt/flink/lib/

# Or copy to /tmp for runtime loading
docker cp target/my-udf-1.0.jar flink-sql-client:/tmp/
```

### Step 3: Access Flink SQL Client

```bash
docker exec -it flink-sql-client sql-client.sh
```

### Step 4: Load and Register the UDF

#### Option A: Pre-loaded in /opt/flink/lib (Requires Restart)

If you copied to `/opt/flink/lib/`, restart the container:

```bash
docker restart flink-sql-client
docker exec -it flink-sql-client sql-client.sh
```

Then register:

```sql
CREATE FUNCTION MyFunction AS 'com.example.udf.MyScalarFunction';
```

#### Option B: Runtime Loading (Recommended)

If you copied to `/tmp/`:

```sql
-- Load JAR at runtime
ADD JAR '/tmp/my-udf-1.0.jar';

-- Register Scalar UDF
CREATE FUNCTION MyFunction AS 'com.example.udf.MyScalarFunction';

-- Or register UDTF
CREATE FUNCTION SplitString AS 'com.example.udtf.SplitFunction';
```

## Using the UDF

### With Datagen (Testing)

```sql
-- Create test table
CREATE TABLE test_input (
    id INT,
    message STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10'
);

-- Test Scalar UDF
SELECT id, message, MyFunction(message) AS result
FROM test_input;
```

### With Kafka Topics

```sql
-- Create Kafka source table
CREATE TABLE kafka_input (
    id INT,
    message STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'test-input',
    'properties.bootstrap.servers' = 'broker:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- Use UDF
SELECT id, MyFunction(message) AS processed
FROM kafka_input;
```

### UDTF with LATERAL JOIN

```sql
-- Split comma-separated values
CREATE TABLE orders (
    order_id INT,
    items STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json'
);

-- Use UDTF
SELECT order_id, item
FROM orders,
LATERAL TABLE(SplitString(items, ',')) AS T(item);
```

## Table API (Java) Local Usage

```java
import org.apache.flink.table.api.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LocalUDFExample {
    public static void main(String[] args) {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Register UDF
        tableEnv.createTemporarySystemFunction("MyUDF", MyScalarFunction.class);
        
        // Define source (connects to Docker Kafka on localhost:29092)
        tableEnv.executeSql(
            "CREATE TABLE source_table (" +
            "  id INT," +
            "  value STRING" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'test-input'," +
            "  'properties.bootstrap.servers' = 'localhost:29092'," +
            "  'scan.startup.mode' = 'earliest-offset'," +
            "  'format' = 'json'" +
            ")"
        );
        
        // Use UDF
        Table result = tableEnv.sqlQuery(
            "SELECT id, MyUDF(value) AS processed FROM source_table"
        );
        
        // Print results
        result.execute().print();
    }
}
```

**Note**: Use `localhost:29092` from Table API (external to Docker) and `broker:9092` from Flink SQL (inside Docker).

## Debugging

### View Logs

```bash
# SQL Client logs
docker logs flink-sql-client

# Job Manager logs
docker logs flink-jobmanager

# Task Manager logs
docker logs flink-taskmanager
```

### Enable UDF Logging

Add to your UDF:

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyUDF extends ScalarFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MyUDF.class);
    
    public String eval(String input) {
        LOG.info("Processing: {}", input);
        return input.toUpperCase();
    }
}
```

Check logs in Flink Web UI (http://localhost:8082) or container logs.

### Common Issues

1. **ClassNotFoundException**: Check fully qualified class name matches
2. **JAR not found**: Verify `docker cp` succeeded with `docker exec flink-sql-client ls /tmp/`
3. **Kafka connection error**: Use `broker:9092` from Flink SQL, `localhost:29092` from Table API

## Iterate and Retest

After modifying your UDF:

```bash
# 1. Rebuild JAR
mvn clean package

# 2. Copy new JAR
docker cp target/my-udf-1.0.jar flink-sql-client:/tmp/my-udf-new.jar

# 3. In Flink SQL, drop old function
DROP FUNCTION IF EXISTS MyFunction;

# 4. Load new JAR
ADD JAR '/tmp/my-udf-new.jar';

# 5. Re-register function
CREATE FUNCTION MyFunction AS 'com.example.udf.MyScalarFunction';

# 6. Test again
SELECT id, MyFunction(message) FROM test_input;
```

## Example End-to-End Workflow

```bash
# 1. Start Docker environment
docker compose -f docker/docker-compose-flinksql.yml up -d

# 2. Build your UDF
mvn clean package

# 3. Copy JAR
docker cp target/my-udf-1.0.jar flink-sql-client:/tmp/

# 4. Enter SQL client
docker exec -it flink-sql-client sql-client.sh
```

```sql
-- 5. Load JAR
ADD JAR '/tmp/my-udf-1.0.jar';

-- 6. Register function
CREATE FUNCTION ToUpper AS 'com.example.ToUpperUDF';

-- 7. Create test table
CREATE TABLE test_data (
    id INT,
    text STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

-- 8. Test UDF
SELECT id, text, ToUpper(text) AS upper_text FROM test_data;
```

## Next Steps

- Test with realistic data volumes
- Monitor performance in Flink Web UI (http://localhost:8082)
- Iterate on UDF logic
- When ready, deploy to Confluent Cloud for production use
