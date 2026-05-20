# Process Table Functions (PTFs) - Confluent Cloud

Guide for building and deploying stateful Process Table Functions on Confluent Cloud for Apache Flink.

## Overview

Process Table Functions (PTFs) are advanced UDFs that enable:
- **Stateful processing**: Maintain state across events within partitions
- **N-to-M semantics**: Consume multiple input rows, emit multiple output rows
- **Timer support**: Schedule future actions based on event time
- **Complex logic**: Windowing, deduplication, sessionization, state machines

**Status**: Early Access Program (evaluation/testing only, not for production)

## When to Use PTFs

Use PTFs for:
- Custom windowing logic not expressible in standard Flink SQL
- Stateful deduplication or filtering
- Session management and timeout detection
- Running aggregations with custom state
- Event pattern detection across ordered events

Use standard UDFs/UDTFs if you don't need state or timers.

## Project Setup

### Maven Dependencies

```xml
<properties>
    <flink.version>1.19.0</flink.version>
    <java.version>11</java.version>
</properties>

<dependencies>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-api-java</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-common</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    
    <!-- Third-party dependencies (example) -->
    <dependency>
        <groupId>com.google.guava</groupId>
        <artifactId>guava</artifactId>
        <version>32.1.3-jre</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.5.0</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <artifactSet>
                            <excludes>
                                <exclude>org.apache.flink:*</exclude>
                            </excludes>
                        </artifactSet>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

### Gradle Configuration

```gradle
plugins {
    id 'java'
    id 'com.github.johnrengelman.shadow' version '8.1.1'
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

dependencies {
    compileOnly 'org.apache.flink:flink-table-api-java:1.19.0'
    compileOnly 'org.apache.flink:flink-table-common:1.19.0'
    
    // Third-party dependencies (bundled in JAR)
    implementation 'com.google.guava:guava:32.1.3-jre'
}

shadowJar {
    dependencies {
        exclude(dependency('org.apache.flink:.*'))
    }
}
```

## PTF Implementation Structure

### Required Components

1. **State POJO**: Plain Java class with public fields to hold partitioned state
2. **eval() method**: Main processing logic with state and input parameters
3. **onTimer() method** (optional): Callback for timer expiration
4. **Annotations**: `@DataTypeHint`, `@StateHint`, `@ArgumentHint`

### Basic Template

```java
package com.example.ptf;

import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.types.Row;

@DataTypeHint("ROW<output_field STRING, count INT>")
public class MyPTF extends ProcessTableFunction<Row> {
    
    // State POJO
    public static class MyState {
        public int counter = 0;
        public List<String> items = new ArrayList<>();
    }
    
    // Main processing method
    public void eval(
        @StateHint MyState state,
        @ArgumentHint(isSet = true) Row input,
        @DataTypeHint("INT") Integer windowSize
    ) {
        String value = input.getFieldAs("field_name");
        state.counter++;
        state.items.add(value);
        
        // Emit output
        collect(Row.of(value, state.counter));
    }
}
```

## Example 1: Event Counter

Simple PTF that counts events per partition:

```java
package com.example.ptf;

import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.annotation.*;
import org.apache.flink.types.Row;

@DataTypeHint("ROW<user_id STRING, event_name STRING, count INT>")
public class EventCounter extends ProcessTableFunction<Row> {
    
    public static class CountState {
        public int eventCount = 0;
    }
    
    public void eval(
        @StateHint CountState state,
        @ArgumentHint(isSet = true) Row input
    ) {
        String userId = input.getFieldAs("user_id");
        String eventName = input.getFieldAs("event_name");
        
        state.eventCount++;
        
        collect(Row.of(userId, eventName, state.eventCount));
    }
}
```

## Example 2: Deduplication with Time Window

Remove duplicate events within a time window per user:

```java
package com.example.ptf;

import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.annotation.*;
import org.apache.flink.types.Row;
import java.util.HashMap;
import java.util.Map;

@DataTypeHint("ROW<user_id STRING, event_id STRING, ts TIMESTAMP(3)>")
public class DeduplicateEvents extends ProcessTableFunction<Row> {
    
    public static class DedupeState {
        public Map<String, Long> seenEvents = new HashMap<>();
    }
    
    public void eval(
        @StateHint DedupeState state,
        @ArgumentHint(isSet = true) Row input,
        @DataTypeHint("BIGINT") Long windowMillis
    ) {
        String userId = input.getFieldAs("user_id");
        String eventId = input.getFieldAs("event_id");
        Long timestamp = input.getFieldAs("ts");
        
        Long lastSeen = state.seenEvents.get(eventId);
        
        if (lastSeen == null || (timestamp - lastSeen) > windowMillis) {
            // Not a duplicate or outside window
            state.seenEvents.put(eventId, timestamp);
            collect(Row.of(userId, eventId, timestamp));
        }
        // else: duplicate, don't emit
    }
}
```

## Example 3: Median Calculation (from tutorial)

Calculate rolling median over trailing N events:

```java
package com.example.ptf;

import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.annotation.*;
import org.apache.flink.types.Row;
import com.google.common.math.Quantiles;
import java.util.ArrayList;
import java.util.List;

@DataTypeHint("ROW<temperature DOUBLE, median DOUBLE>")
public class Median extends ProcessTableFunction<Row> {
    
    public static class TemperatureList {
        public List<Double> temps = new ArrayList<>();
    }
    
    public void eval(
        @StateHint TemperatureList trailingTemps,
        @ArgumentHint(isSet = true) Row row,
        @DataTypeHint("INT") Integer numTrailing
    ) {
        Double temperature = row.getFieldAs("temperature");
        
        trailingTemps.temps.add(temperature);
        
        // Keep only the trailing N temperatures
        while (trailingTemps.temps.size() > numTrailing) {
            trailingTemps.temps.remove(0);
        }
        
        // Calculate and emit median
        double median = Quantiles.median().compute(trailingTemps.temps);
        collect(Row.of(temperature, median));
    }
}
```

## Timer Support

### Event-Time Timers

Schedule callbacks when watermark passes a threshold:

```java
@DataTypeHint("ROW<user_id STRING, event STRING, timeout BOOLEAN>")
public class SessionDetector extends ProcessTableFunction<Row> {
    
    public static class SessionState {
        public long lastActivityTime = 0;
        public int eventCount = 0;
    }
    
    public void eval(
        @StateHint SessionState state,
        @ArgumentHint(isSet = true) Row input
    ) {
        String userId = input.getFieldAs("user_id");
        String event = input.getFieldAs("event");
        Long eventTime = input.getFieldAs("event_time");
        
        state.lastActivityTime = eventTime;
        state.eventCount++;
        
        // Schedule timeout 5 minutes after this event
        long timeoutTime = eventTime + (5 * 60 * 1000);
        registerTimer(timeoutTime);
        
        collect(Row.of(userId, event, false));
    }
    
    public void onTimer(
        @StateHint SessionState state,
        long timestamp
    ) {
        // Timer fired - session timed out
        if (timestamp > state.lastActivityTime) {
            collect(Row.of("session_timeout", "", true));
            state.eventCount = 0; // Reset for new session
        }
    }
}
```

## State Management Best Practices

### Use POJOs for State

```java
// ✅ Good: Simple POJO with public fields
public static class MyState {
    public int count = 0;
    public String lastValue = "";
    public List<Double> values = new ArrayList<>();
    public Map<String, Integer> lookup = new HashMap<>();
}

// ❌ Bad: Private fields, getters/setters
public static class BadState {
    private int count;
    public int getCount() { return count; }
    public void setCount(int c) { count = c; }
}
```

### Supported Collection Types

- `List<T>` and `ArrayList<T>` ✅
- `Map<K, V>` and `HashMap<K, V>` ✅
- **NOT** `ListView<T>` or `MapView<K, V>` ❌ (not supported in Early Access)

## Build and Package

### Maven
```bash
mvn clean package
```

Output: `target/my-ptf-1.0.jar`

### Gradle
```bash
./gradlew shadowJar
```

Output: `build/libs/my-ptf-all.jar`

## Deploy to Confluent Cloud

### Step 1: Upload Artifact

```bash
confluent flink artifact create my-ptf \
    --artifact-file build/libs/my-ptf-all.jar \
    --cloud aws \
    --region us-east-1 \
    --environment <env-id>
```

Note the artifact ID (e.g., `cfa-xyz789`).

### Step 2: Register PTF

```sql
CREATE FUNCTION EventCounter
AS 'com.example.ptf.EventCounter'
USING JAR 'confluent-artifact://cfa-xyz789';
```

## Invoke the PTF

### SQL Invocation

The key requirements:
- Use `TABLE <table-name>` to specify input
- Include `PARTITION BY` clause to define state partitioning
- Pass `uid` parameter for state recovery

```sql
-- Example: Count events per user
SELECT user_id, event_name, count
FROM EventCounter(
    input => TABLE user_events PARTITION BY user_id,
    uid => 'event-counter-v1'
);
```

With additional parameters:

```sql
-- Example: Deduplication with 5-minute window
SELECT user_id, event_id, ts
FROM DeduplicateEvents(
    input => TABLE raw_events PARTITION BY user_id,
    uid => 'dedupe-v1',
    windowMillis => 300000
);
```

### Table API Invocation

```java
import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;

TableEnvironment tableEnv = TableEnvironment.create(
    EnvironmentSettings.inStreamingMode()
);

// Register PTF
tableEnv.createTemporarySystemFunction("EventCounter", EventCounter.class);

// Invoke with partitioning
Table result = tableEnv.from("user_events")
    .partitionBy($("user_id"))
    .process(call("EventCounter").withUID("event-counter-v1"))
    .as("user_id", "event_name", "count");
```

## Testing

### Create Test Data

```sql
CREATE TABLE test_events (
    user_id STRING,
    event_name STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.user_id.kind' = 'random',
    'fields.user_id.length' = '5',
    'fields.event_name.kind' = 'random',
    'fields.event_name.length' = '10'
);
```

### Run PTF

```sql
SELECT user_id, event_name, count
FROM EventCounter(
    input => TABLE test_events PARTITION BY user_id,
    uid => 'test-counter-v1'
);
```

## Important Constraints

### UID Parameter

The `uid` parameter is **required** for PTFs and must be:
- Stable across deployments (don't change it)
- Unique per PTF instance
- Versioned when making breaking changes (e.g., `my-ptf-v2`)

Without a stable UID, state cannot be recovered after restarts.

### Partitioning

- `PARTITION BY` clause is required for PTFs
- Each partition gets isolated state
- Choose partition keys that match your business logic (e.g., user_id, device_id, session_id)
- Skewed partitions (uneven key distribution) can cause performance issues

### State Size

- State is kept in memory (with checkpointing to object storage)
- Keep state POJOs memory-efficient
- Prune old data from collections (lists, maps) to prevent unbounded growth
- Monitor state size in Confluent Cloud console

## Troubleshooting

### Common Issues

1. **Missing uid parameter**: Error `"uid is required for PTFs"`
   - Solution: Add `uid => 'my-unique-id'` to the invocation

2. **Missing PARTITION BY**: Error `"PARTITION BY required for set semantics"`
   - Solution: Add `PARTITION BY <key>` after table name

3. **State serialization errors**: State POJO fields must be serializable
   - Solution: Use standard Java types and avoid complex nested objects

4. **Timer not firing**: Ensure watermarks are progressing
   - Solution: Check watermark strategy on source table

5. **High latency**: State size too large or skewed partitions
   - Solution: Reduce state size, choose better partition key

### Enable Logging

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyPTF extends ProcessTableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(MyPTF.class);
    
    public void eval(@StateHint MyState state, @ArgumentHint(isSet = true) Row input) {
        LOG.info("Processing row: {}, current state: {}", input, state.counter);
        // ...
    }
}
```

Check logs in Confluent Cloud Flink console.

## Best Practices

1. **State Management**:
   - Keep state small and pruned
   - Use `HashMap` for lookups, `ArrayList` for ordered data
   - Clear expired entries in `onTimer`

2. **Partitioning**:
   - Choose partition keys with reasonable cardinality
   - Avoid highly skewed keys (one key with 90% of data)

3. **UIDs**:
   - Use descriptive, versioned UIDs: `dedupe-user-events-v1`
   - Change UID only when making incompatible state schema changes

4. **Timers**:
   - Use timers for timeout/expiration logic
   - Remember timers fire only when watermark advances

5. **Testing**:
   - Test with realistic data volumes locally before deploying
   - Monitor state size and latency in Confluent Cloud

6. **Early Access Limitations**:
   - Do not use in production workloads
   - Expect potential breaking changes
   - Report bugs to Confluent support
