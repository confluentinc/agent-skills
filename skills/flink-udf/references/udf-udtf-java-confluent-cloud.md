# Java Scalar UDFs and UDTFs - Confluent Cloud

Guide for building and deploying Java-based Scalar UDFs and User-Defined Table Functions (UDTFs) on Confluent Cloud for Apache Flink.

## Overview

- **Scalar UDF**: Transform input values to a single output value
- **UDTF**: Transform input values to multiple output rows (table function)
- **Language**: Java 11 or later
- **Deployment**: JAR artifact uploaded to Confluent Cloud
- **Invocation**: Flink SQL or Table API

## Project Setup

See `dependencies-confluent-cloud.md` for the Maven and Gradle build configuration.

## Scalar UDF Implementation

### Basic Structure

```java
package com.example.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;

public class MyScalarFunction extends ScalarFunction {
    
    // The eval method contains the business logic
    public String eval(String input) {
        if (input == null) {
            return null;
        }
        return input.toUpperCase();
    }
}
```

### Advanced: Multiple Eval Methods (Overloading)

You can provide multiple `eval` methods for different input types:

```java
public class FlexibleHash extends ScalarFunction {
    
    public int eval(String input) {
        return input == null ? 0 : input.hashCode();
    }
    
    public int eval(Integer input) {
        return input == null ? 0 : input;
    }
    
    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object input) {
        return input == null ? 0 : input.hashCode();
    }
}
```

### Example: Real-World Scalar UDF

```java
package com.example.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;

/**
 * Calculate tax based on price and tax rate
 */
public class CalculateTax extends ScalarFunction {
    
    public @DataTypeHint("DECIMAL(10, 2)") Double eval(
        @DataTypeHint("DECIMAL(10, 2)") Double price,
        @DataTypeHint("DECIMAL(5, 4)") Double taxRate
    ) {
        if (price == null || taxRate == null) {
            return 0.0;
        }
        return price * taxRate;
    }
}
```

## UDTF Implementation

### Basic Structure

```java
package com.example.udtf;

import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class SplitFunction extends TableFunction<Row> {
    
    public void eval(String str, String delimiter) {
        if (str == null || delimiter == null) {
            return;
        }
        for (String part : str.split(delimiter)) {
            collect(Row.of(part));
        }
    }
}
```

### Example: Real-World UDTF

```java
package com.example.udtf;

import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.types.Row;

/**
 * Parse JSON array and emit each element as a row
 */
@FunctionHint(output = @DataTypeHint("ROW<value STRING>"))
public class ExplodeJsonArray extends TableFunction<Row> {
    
    public void eval(String jsonArray) {
        if (jsonArray == null || jsonArray.isEmpty()) {
            return;
        }
        
        // Simple parsing (use Jackson or Gson for production)
        String trimmed = jsonArray.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            String content = trimmed.substring(1, trimmed.length() - 1);
            String[] elements = content.split(",");
            
            for (String element : elements) {
                collect(Row.of(element.trim().replaceAll("\"", "")));
            }
        }
    }
}
```

## Build the JAR

### Maven
```bash
mvn clean package
```

Output: `target/<artifact-id>-<version>.jar`

### Gradle
```bash
./gradlew shadowJar
```

Output: `build/libs/<project-name>-all.jar`

## Deploy to Confluent Cloud

### Step 1: Upload Artifact

Using Confluent CLI:

```bash
confluent flink artifact create my-udf \
    --artifact-file target/my-udf-1.0.jar \
    --cloud aws \
    --region us-east-1 \
    --environment <env-id>
```

Output will include an artifact ID like `cfa-abc123`.

### Step 2: Register Function in Flink SQL

For **Scalar UDF**:

```sql
CREATE FUNCTION MyFunction
AS 'com.example.udf.MyScalarFunction'
USING JAR 'confluent-artifact://cfa-abc123';
```

For **UDTF**:

```sql
CREATE FUNCTION SplitString
AS 'com.example.udtf.SplitFunction'
USING JAR 'confluent-artifact://cfa-abc123';
```

## Invoke the Function

### Scalar UDF Usage (Flink SQL)

```sql
-- Example: Apply tax calculation
SELECT 
    product_id,
    price,
    CalculateTax(price, 0.0825) AS tax_amount,
    price + CalculateTax(price, 0.0825) AS total_price
FROM products;
```

### UDTF Usage (Flink SQL)

```sql
-- Example: Split comma-separated values
SELECT order_id, item
FROM orders,
LATERAL TABLE(SplitString(items, ',')) AS T(item);
```

### Table API Usage (Java)

For Scalar UDF:

```java
import org.apache.flink.table.api.*;

TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

// Register function programmatically
tableEnv.createTemporarySystemFunction("MyFunction", MyScalarFunction.class);

// Use in query
Table result = tableEnv.sqlQuery(
    "SELECT id, MyFunction(name) AS upper_name FROM my_table"
);
```

For UDTF:

```java
Table result = tableEnv.from("orders")
    .joinLateral(call("SplitString", $("items"), ",").as("item"))
    .select($("order_id"), $("item"));
```

## Testing

Create a test table and verify the function:

```sql
-- Create test table
CREATE TABLE test_input (
    id INT,
    value STRING
) WITH (
    'connector' = 'datagen',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10',
    'fields.value.kind' = 'random'
);

-- Test the UDF
SELECT id, MyFunction(value) AS result
FROM test_input;
```

## Troubleshooting

### Common Issues

1. **ClassNotFoundException**: Ensure the fully qualified class name in `CREATE FUNCTION` matches your Java package and class
2. **NoSuchMethodException**: Verify you have a public `eval()` method with correct signature
3. **Serialization errors**: Ensure all classes used in the UDF are serializable
4. **Dependency conflicts**: Use `provided` scope for Flink dependencies, `compile` for third-party libraries

### Enable Logging

Add logging to your UDF for debugging:

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyFunction extends ScalarFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MyFunction.class);
    
    public String eval(String input) {
        LOG.info("Processing input: {}", input);
        return input.toUpperCase();
    }
}
```

View logs in the Confluent Cloud Flink console under your statement's execution details.

## Best Practices

1. **Null handling**: Always check for null inputs and handle gracefully
2. **Performance**: Avoid heavy operations in UDFs; keep logic simple
3. **Type hints**: Use `@DataTypeHint` for complex types or when type inference fails
4. **Versioning**: Use artifact version numbers and function name versioning (e.g., `CalculateTax_v2`)
5. **Testing**: Test locally before deploying to Confluent Cloud
6. **Dependencies**: Only include necessary third-party libraries in the JAR
