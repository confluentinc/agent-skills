# Local Docker Setup for Flink UDFs

Guide for setting up a local Flink + Kafka environment using Docker Compose to develop and test UDFs.

## Prerequisites

- Docker Desktop installed (https://www.docker.com/products/docker-desktop)
- Docker Compose (included with Docker Desktop)
- At least 8GB RAM allocated to Docker
- Git (to clone tutorial repository)

## Quick Start

### Step 1: Get Docker Compose File

```bash
# Clone the tutorials repository
git clone https://github.com/confluentinc/tutorials.git
cd tutorials

# Or download just the docker-compose file
curl -O https://raw.githubusercontent.com/confluentinc/tutorials/master/docker/docker-compose-flinksql.yml
```

### Step 2: Start Services

```bash
docker compose -f docker/docker-compose-flinksql.yml up -d
```

This starts:
- **Kafka broker** (KRaft mode, no ZooKeeper)
- **Schema Registry** (for Avro/Protobuf support)
- **Flink Job Manager** (orchestrates jobs)
- **Flink Task Manager** (executes tasks)
- **Flink SQL Client** (interactive SQL shell)

### Step 3: Verify Services

```bash
# Check all services are running
docker compose -f docker/docker-compose-flinksql.yml ps

# Expected output: 5 containers running
```

### Step 4: Access Flink SQL Client

```bash
docker exec -it flink-sql-client sql-client.sh
```

You should see:

```
Flink SQL>
```

## Docker Compose Configuration Details

### Services

#### 1. Kafka Broker
- **Port**: 29092 (host access)
- **Internal Port**: 9092 (container access)
- **Mode**: KRaft (no ZooKeeper required)
- **Auto-create topics**: Enabled

#### 2. Schema Registry
- **Port**: 8081
- **Purpose**: Schema management for Avro/Protobuf

#### 3. Flink Job Manager
- **Web UI Port**: 8082 (http://localhost:8082)
- **Role**: Coordinates job execution
- **Image**: `cnfldemos/flink-kafka:2.2.0-scala_2.12-java17`

#### 4. Flink Task Manager
- **Role**: Executes tasks
- **Parallelism**: 1 (scalable with `--scale taskmanager=3`)

#### 5. Flink SQL Client
- **Purpose**: Interactive SQL shell
- **Pre-configured** with Kafka and Schema Registry connectors

## Using the Environment

### Access Flink Web UI

Open http://localhost:8082 in your browser to:
- Monitor running jobs
- View task manager metrics
- Check job execution plans
- Debug failures

### Create Test Topics

From your host machine:

```bash
# Create topic using Kafka broker
docker exec -it broker kafka-topics \
    --create \
    --topic test-input \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
```

Or from Flink SQL:

```sql
CREATE TABLE test_input (
    id INT,
    message STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'test-input',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json'
);
```

### Produce Test Data

```bash
# Produce JSON messages
docker exec -it broker kafka-console-producer \
    --topic test-input \
    --bootstrap-server localhost:9092

# Then type messages:
{"id": 1, "message": "hello"}
{"id": 2, "message": "world"}
# Press Ctrl+D to exit
```

Or use Flink SQL datagen:

```sql
CREATE TABLE test_data (
    id INT,
    message STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '100'
);
```

### Deploy UDF JAR to Flink

#### Method 1: Copy to Flink Lib Directory

```bash
# Copy JAR to flink-sql-client container
docker cp my-udf.jar flink-sql-client:/opt/flink/lib/

# Restart SQL client to load the JAR
docker restart flink-sql-client

# Re-enter SQL client
docker exec -it flink-sql-client sql-client.sh
```

#### Method 2: Load JAR at Runtime (Recommended)

```bash
# Copy JAR to SQL client
docker cp my-udf.jar flink-sql-client:/tmp/

# In Flink SQL, load the JAR
Flink SQL> ADD JAR '/tmp/my-udf.jar';

# Register function
Flink SQL> CREATE FUNCTION MyFunction AS 'com.example.MyUDF';
```

## Working with UDFs

### Register and Test a UDF

```sql
-- Load JAR
ADD JAR '/tmp/my-udf.jar';

-- Register function
CREATE FUNCTION ToUpper AS 'com.example.ToUpperUDF';

-- Test with datagen
CREATE TABLE test_input (
    id INT,
    text STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

-- Use the UDF
SELECT id, text, ToUpper(text) AS upper_text
FROM test_input;
```

### Register and Test a PTF

```sql
-- Load JAR
ADD JAR '/tmp/my-ptf.jar';

-- Register PTF
CREATE FUNCTION Median AS 'com.example.ptf.Median';

-- Create input table with watermark
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
    'fields.sensor_id.max' = '5',
    'fields.temperature.kind' = 'random',
    'fields.temperature.min' = '20.0',
    'fields.temperature.max' = '30.0'
);

-- Invoke PTF
SELECT sensor_id, temperature, median
FROM Median(
    TABLE temperature_readings PARTITION BY sensor_id,
    3
);
```

## Table API (Java) Development

### Project Setup

Add Flink dependencies to `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-api-java-bridge</artifactId>
        <version>1.19.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>1.19.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>3.1.0-1.19</version>
    </dependency>
</dependencies>
```

### Table API Example

```java
import org.apache.flink.table.api.*;

public class FlinkUDFExample {
    public static void main(String[] args) {
        // Create table environment
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();
        
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        
        // Configure Kafka connection
        tableEnv.getConfig().set("pipeline.jars", "file:///path/to/my-udf.jar");
        
        // Register UDF
        tableEnv.createTemporarySystemFunction("MyUDF", MyUDFClass.class);
        
        // Define source table
        tableEnv.executeSql(
            "CREATE TABLE source_table (" +
            "  id INT," +
            "  value STRING" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'test-input'," +
            "  'properties.bootstrap.servers' = 'localhost:29092'," +
            "  'format' = 'json'" +
            ")"
        );
        
        // Use UDF in query
        Table result = tableEnv.sqlQuery(
            "SELECT id, MyUDF(value) AS processed FROM source_table"
        );
        
        result.execute().print();
    }
}
```

Run from your IDE or command line (Flink will connect to `localhost:29092`).

## Scaling Task Managers

Increase parallelism for testing:

```bash
docker compose -f docker/docker-compose-flinksql.yml up -d --scale taskmanager=3
```

This creates 3 task managers for higher throughput.

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker compose -f docker/docker-compose-flinksql.yml logs

# Common issue: port conflicts
# Ensure ports 8081, 8082, 29092 are not in use
lsof -i :8082
```

### UDF ClassNotFoundException

```bash
# Ensure JAR is in the correct location
docker exec -it flink-sql-client ls /opt/flink/lib/

# Or use ADD JAR in SQL client
ADD JAR '/tmp/my-udf.jar';
```

### Kafka Connection Refused

From Flink SQL, use `broker:9092` (internal hostname).  
From host machine, use `localhost:29092`.

### Out of Memory

```bash
# Increase Docker memory allocation
# Docker Desktop → Settings → Resources → Memory
# Allocate at least 8GB
```

## Stopping and Cleanup

### Stop Services

```bash
docker compose -f docker/docker-compose-flinksql.yml down
```

### Remove Volumes (Clean Slate)

```bash
docker compose -f docker/docker-compose-flinksql.yml down -v
```

This deletes all Kafka topics and Flink checkpoints.

## Next Steps

Once the environment is running:
1. Build your UDF/PTF JAR or ZIP
2. Copy it to the Flink SQL client container
3. Register the function with `CREATE FUNCTION`
4. Test with sample data
5. Iterate and refine

Refer to the UDF-specific guides for implementation details.
