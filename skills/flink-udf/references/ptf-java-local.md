# Process Table Functions (PTFs) - Local Docker

Guide for deploying and testing PTFs in a local Flink environment running in Docker.

## Overview

The implementation code is identical to Confluent Cloud. This guide focuses on **local deployment and testing**.

For implementation details, see `ptf-java-confluent-cloud.md` sections:
- PTF Implementation Structure
- State Management
- Timer Support
- Examples

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

### Step 2: Copy JAR to Flink

```bash
# Copy to SQL client container

# If you built with Maven:
docker cp target/my-ptf-all.jar flink-sql-client:/tmp/my-ptf-all.jar

# If you built with Gradle:
docker cp build/libs/my-ptf-all.jar flink-sql-client:/tmp/my-ptf-all.jar

### Step 3: Access Flink SQL Client

```bash
docker exec -it flink-sql-client sql-client.sh
```

### Step 4: Load and Register PTF

```sql
-- Load JAR
ADD JAR '/tmp/my-ptf-all.jar';

-- Register PTF
CREATE FUNCTION Median AS 'com.example.ptf.Median';
```

## Testing PTFs Locally

### Example 1: Median PTF

```sql
-- Create source with watermark
CREATE TABLE temperature_readings (
    sensor_id INT,
    temperature DOUBLE,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '2',
    'fields.sensor_id.kind' = 'random',
    'fields.sensor_id.min' = '1',
    'fields.sensor_id.max' = '3',
    'fields.temperature.kind' = 'random',
    'fields.temperature.min' = '20.0',
    'fields.temperature.max' = '30.0'
);

-- Invoke PTF with 3-event trailing window
SELECT sensor_id, temperature, median
FROM Median(
    input => TABLE temperature_readings PARTITION BY sensor_id,
    numTrailing => 3
);
```

### Example 2: Event Counter PTF

```sql
-- Create user events table
CREATE TABLE user_events (
    user_id STRING,
    event_name STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.user_id.length' = '5',
    'fields.event_name.length' = '10'
);

-- Count events per user
SELECT user_id, event_name, count
FROM EventCounter(
    input => TABLE user_events PARTITION BY user_id,
    uid => 'local-event-counter-v1'
);
```

### Example 3: Deduplication PTF

```sql
-- Create events with potential duplicates
CREATE TABLE raw_events (
    user_id STRING,
    event_id STRING,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.user_id.length' = '5',
    'fields.event_id.length' = '10'
);

-- Deduplicate with 5-minute window
SELECT user_id, event_id, ts
FROM DeduplicateEvents(
    input => TABLE raw_events PARTITION BY user_id,
    uid => 'dedupe-v1',
    windowMillis => 300000
);
```

## Using Kafka Topics

### Create Kafka Topic

```bash
# From host
docker exec -it broker kafka-topics \
    --create \
    --topic sensor-readings \
    --bootstrap-server localhost:9092 \
    --partitions 3
```

### Define Kafka Source

```sql
CREATE TABLE sensor_data (
    sensor_id INT,
    temperature DOUBLE,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-readings',
    'properties.bootstrap.servers' = 'broker:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- Use PTF
SELECT sensor_id, temperature, median
FROM Median(
    input => TABLE sensor_data PARTITION BY sensor_id,
    numTrailing => 5
);
```

### Produce Test Data

```bash
# Produce JSON messages
docker exec -it broker kafka-console-producer \
    --topic sensor-readings \
    --bootstrap-server localhost:9092

# Type messages:
{"sensor_id":1,"temperature":22.5,"ts":"2024-05-15T10:00:00.000Z"}
{"sensor_id":1,"temperature":23.1,"ts":"2024-05-15T10:00:01.000Z"}
{"sensor_id":2,"temperature":21.8,"ts":"2024-05-15T10:00:00.000Z"}
# Press Ctrl+D to exit
```

## Table API (Java) with PTFs

```java
import org.apache.flink.table.api.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.*;

public class LocalPTFExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Register PTF
        tableEnv.createTemporarySystemFunction("Median", com.example.ptf.Median.class);
        
        // Define source
        tableEnv.executeSql(
            "CREATE TABLE sensor_data (" +
            "  sensor_id INT," +
            "  temperature DOUBLE," +
            "  ts TIMESTAMP(3)," +
            "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'sensor-readings'," +
            "  'properties.bootstrap.servers' = 'localhost:29092'," +
            "  'format' = 'json'" +
            ")"
        );
        
        // Invoke PTF using SQL
        Table result = tableEnv.sqlQuery(
            "SELECT sensor_id, temperature, median " +
            "FROM Median(" +
            "  input => TABLE sensor_data PARTITION BY sensor_id, " +
            "  numTrailing => 3" +
            ")"
        );
        
        result.execute().print();
    }
}
```

## Debugging PTFs

### View State in Flink Web UI

1. Open http://localhost:8082
2. Click on your running job
3. Navigate to "State" tab
4. Inspect state size per operator

### Enable Logging

Add to PTF:

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DataTypeHint("ROW<temperature DOUBLE, median DOUBLE>")
public class Median extends ProcessTableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(Median.class);
    
    public void eval(
        @StateHint TemperatureList state,
        @ArgumentHint(isSet = true) Row row,
        Integer numTrailing
    ) {
        LOG.info("State size: {}, numTrailing: {}", state.temps.size(), numTrailing);
        // ...
    }
}
```

Check logs:

```bash
docker logs flink-taskmanager
```

### Monitor Watermarks

Enable watermark logging in source:

```sql
CREATE TABLE sensor_data (
    sensor_id INT,
    temperature DOUBLE,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-readings',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json',
    'scan.watermark.alignment.group' = 'alignment-group-1',
    'scan.watermark.alignment.max-drift' = '10s'
);
```

## Testing Timers

### PTF with Timer

```java
@DataTypeHint("ROW<user_id STRING, event STRING, timeout BOOLEAN>")
public class SessionLimiter extends ProcessTableFunction<Row> {
    
    public static class SessionState {
        public long lastActivityTime = 0;
    }
    
    public void eval(
        Context ctx,
        @StateHint StayState state,
        @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input
    ) {
        String userId = input.getFieldAs("user_id");

        collect(Row.of(userId, "activity", false));
        
        TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
        timeCtx.registerOnTime(timeCtx.time().plus(Duration.ofMinutes(2)));
    }
    
    public void onTimer(
        @StateHint SessionState state,
        long timestamp
    ) {
        collect(Row.of("timeout", "session_end", true));
    }
}
```

### Test Timer Execution

```sql
-- Create source with realistic timestamps
CREATE TABLE user_activity (
    user_id STRING,
    event_name STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '0.1',  -- Slow rate to observe timers
    'fields.user_id.length' = '5'
);

-- Invoke PTF
SELECT user_id, event, timeout
FROM SessionDetector(
    input => TABLE user_activity PARTITION BY user_id,
    uid => 'session-v1'
);
```

## Iterate and Retest

```bash
# 1. Modify PTF code
# 2. Rebuild
./gradlew shadowJar

# 3. Copy new JAR
docker cp build/libs/my-ptf-all.jar flink-sql-client:/tmp/my-ptf-new.jar

# 4. In Flink SQL
DROP FUNCTION IF EXISTS Median;
ADD JAR '/tmp/my-ptf-new.jar';
CREATE FUNCTION Median AS 'com.example.ptf.Median';

# 5. Retest
SELECT * FROM Median(
    input => TABLE sensor_data PARTITION BY sensor_id,
    numTrailing => 3
);
```

## Performance Testing

### Monitor Metrics

```bash
# Check container resource usage
docker stats flink-jobmanager flink-taskmanager

# View metrics in Web UI
open http://localhost:8082
```

### Test with Higher Throughput

```sql
CREATE TABLE high_volume_data (
    sensor_id INT,
    temperature DOUBLE,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1000',  -- High rate
    'fields.sensor_id.kind' = 'random',
    'fields.sensor_id.min' = '1',
    'fields.sensor_id.max' = '100'
);

SELECT sensor_id, temperature, median
FROM Median(
    input => TABLE high_volume_data PARTITION BY sensor_id,
    numTrailing => 10
);
```

Monitor backpressure and latency in Flink Web UI.

## Common Issues

1. **Timer not firing**: Ensure watermark is progressing (check `WATERMARK FOR` clause)
2. **State growing unbounded**: Prune old entries in `eval()` or `onTimer()`
3. **High memory usage**: Reduce state size or partition cardinality
4. **Serialization errors**: Ensure state POJO fields are serializable

## Next Steps

- Test with realistic data patterns
- Monitor state size and checkpoint duration
- Validate correctness with known inputs/outputs
- When ready, deploy to Confluent Cloud for production use
