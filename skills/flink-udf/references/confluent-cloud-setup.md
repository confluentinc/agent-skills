# Confluent Cloud Setup for Flink UDFs

Guide for setting up Confluent Cloud infrastructure to develop and deploy Flink UDFs.

## Prerequisites

- Confluent Cloud account (sign up at https://confluent.cloud)
- Confluent CLI installed (https://docs.confluent.io/confluent-cli/current/install.md)
- Credit card for billing (or free trial credits)

## Quick Setup with confluent-quickstart Plugin

The fastest way to get started:

```bash
# Install the quickstart plugin
confluent plugin install confluent-quickstart

# Create environment with Flink enabled
confluent quickstart \
    --region us-east-1 \
    --cloud aws \
    --create-flink-key \
    --flink-properties-file ./flink-config.properties
```

This creates:
- Confluent Cloud environment
- Kafka cluster in the specified region
- Flink compute pool
- API keys for Flink access
- Properties file for Table API clients

**Output**: Environment ID, Cluster ID, Flink compute pool ID, and API credentials.

## Manual Setup

If you prefer manual control:

### Step 1: Create Environment

```bash
# Login
confluent login

# Create environment
confluent environment create my-flink-env

# List environments to get ID
confluent environment list

# Set as active
confluent environment use <env-id>
```

### Step 2: Create Kafka Cluster

```bash
# Create Basic cluster (for development)
confluent kafka cluster create my-cluster \
    --cloud aws \
    --region us-east-1 \
    --type basic

# Or create Standard cluster (for production)
confluent kafka cluster create my-cluster \
    --cloud aws \
    --region us-east-1 \
    --type standard

# Get cluster ID
confluent kafka cluster list

# Set as active
confluent kafka cluster use <cluster-id>
```

### Step 3: Enable Flink

Flink is automatically available in regions that support it. No separate enablement needed.

### Step 4: Create API Keys

For Kafka:

```bash
# Create Kafka API key
confluent api-key create --resource <cluster-id>

# Store the key and secret (shown only once!)
```

For Flink:

```bash
# Create Flink API key
confluent api-key create --resource <flink-compute-pool-id>

# Or use the quickstart plugin flag --create-flink-key
```

### Step 5: Create Schema Registry (Optional)

If using Avro/Protobuf schemas:

```bash
# Enable Schema Registry for the environment
confluent schema-registry cluster enable --cloud aws --geo us

# Get Schema Registry endpoint
confluent schema-registry cluster describe
```

## Configure Confluent CLI

Set default environment and cluster:

```bash
# Set default environment
confluent environment use <env-id>

# Set default cluster
confluent kafka cluster use <cluster-id>
```

## Create Topics for Testing

```bash
# Create a test input topic
confluent kafka topic create test-input \
    --partitions 3

# Create a test output topic
confluent kafka topic create test-output \
    --partitions 3
```

## Verify Flink Access

### Using Confluent Cloud Console

1. Go to https://confluent.cloud
2. Select your environment
3. Click "Flink" in the left nav
4. Click "Open SQL workspace"

You should see the Flink SQL editor.

### Using Confluent CLI

```bash
# List Flink compute pools
confluent flink compute-pool list

# Open Flink shell (if available in your region)
confluent flink shell
```

## Table API Configuration

For Java Table API clients, you need connection properties:

### Generate Properties File

```bash
# If you used quickstart, this is already done
# Otherwise, create flink-config.properties manually:
cat > flink-config.properties <<EOF
client.flink.rest-endpoint=<YOUR_FLINK_ENDPOINT>
client.flink.rest.port=443
client.organization-id=<YOUR_ORG_ID>
client.environment-id=<YOUR_ENV_ID>
client.compute-pool-id=<YOUR_COMPUTE_POOL_ID>
client.api-key=<YOUR_FLINK_API_KEY>
client.api-secret=<YOUR_FLINK_API_SECRET>
EOF
```

Get these values from:

```bash
# Organization ID
confluent organization list

# Environment ID
confluent environment list

# Compute pool ID
confluent flink compute-pool list

# Flink endpoint
confluent flink compute-pool describe <pool-id>
```

### Java Table API Example

```java
import org.apache.flink.table.api.*;

Map<String, String> properties = new HashMap<>();
properties.put("client.flink.rest-endpoint", "xxx.flink.us-east-1.aws.confluent.cloud");
properties.put("client.flink.rest.port", "443");
properties.put("client.organization-id", "your-org-id");
properties.put("client.environment-id", "your-env-id");
properties.put("client.compute-pool-id", "your-pool-id");
properties.put("client.api-key", "your-api-key");
properties.put("client.api-secret", "your-api-secret");

TableEnvironment tableEnv = TableEnvironment.create(
    EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(properties))
        .build()
);
```

## Permissions

Ensure your user has the FlinkDeveloper role:

```bash
# View current role assignments
confluent iam rbac role-binding list

# Grant FlinkDeveloper role (if needed)
confluent iam rbac role-binding create \
    --principal User:<user-id> \
    --role FlinkDeveloper \
    --environment <env-id>
```

## Cost Management

### Estimate Costs

- **Kafka cluster**: ~$1-2/hour for Basic, ~$2-5/hour for Standard
- **Flink compute pool**: ~$0.50-2/hour depending on size
- **Data transfer**: Varies by egress

Use free trial credits for development.

### Minimize Costs

1. **Pause Flink compute pool** when not in use (via console)
2. **Delete resources** after testing:
   ```bash
   confluent kafka cluster delete <cluster-id>
   confluent environment delete <env-id>
   ```
3. **Use Basic cluster** for development (cheaper than Standard)

## Cleanup

When done testing:

```bash
# Delete Flink compute pool (if applicable)
confluent flink compute-pool delete <pool-id>

# Delete Kafka cluster
confluent kafka cluster delete <cluster-id>

# Delete environment
confluent environment delete <env-id>

# Delete API keys
confluent api-key delete <key-id>
```

## Troubleshooting

### "Flink not available in this region"

Not all regions support Flink. Try:
- us-east-1 (AWS)
- us-west-2 (AWS)
- eu-west-1 (AWS)

Check current availability: https://docs.confluent.io/cloud/current/flink/reference/cloud-regions.md

### API Key Permission Errors

Ensure you created an API key specifically for the **Flink compute pool**, not just the Kafka cluster.

### Table API Connection Errors

Verify:
- Endpoint URL is correct (no `https://` prefix in properties)
- Port is 443
- API key and secret are valid
- Organization/environment/pool IDs match

## Next Steps

Once infrastructure is set up:
- Create and upload your UDF artifact
- Register functions with `CREATE FUNCTION`
- Test with sample data
- Build production pipelines

Refer to the UDF-specific guides for implementation details.
