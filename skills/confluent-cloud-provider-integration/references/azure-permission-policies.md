# Azure Permission Policies — Provider Integrations catalog

> **Source:** Confluent Cloud Provider Integrations wizard — `azure_integration_policies`
> payload, **captured verbatim from the live portal on 2026-11-05**. Do not
> hand-edit the role lists; re-capture from the wizard to refresh.
> This is the per-resource companion to `references/azure.md`.
>
> ⚠️ **Staleness caveat.** These roles are a point-in-time snapshot. Confluent can
> change the permissions it requests as connectors are added or scopes are
> tightened. **A Confluent employee should re-verify this catalog against the live
> wizard roughly every couple of months** (re-run the capture above) and update the
> roles + the `captured` date if anything differs.

## How to use

1. Find the user's connector/resource in the **Resource index** below.
2. Assign the service principal **exactly** the `required_roles` the portal
   prescribes — no broader role — via an **Azure RBAC role assignment** scoped to
   the **target resource** (storage account/container, Cosmos DB account, Event
   Hubs namespace, Key Vault, etc.). Prefer the narrowest scope.
3. Some entries are **not** Azure RBAC roles:
   - **"Microsoft Entra admin"** (SQL Server / MySQL / Postgres) means the service
     principal must be set as an **Entra admin / database user** on the database,
     not an RBAC role — follow the connector's DB-auth docs.
   - **"Default access policy"** (Azure Managed Redis) and **Cosmos DB Built-in**
     roles are service-specific data-plane roles, assigned on that service.
4. This is **in addition to** creating the service principal with
   `az ad sp create --id <app-id>` as described in `references/azure.md`.

Request the **same** roles the portal does — this catalog is the source of truth;
the inline examples in `references/azure.md` are teaching aids.

## Resource index

| Resource (portal label) | template_id | Required role(s) |
|---|---|---|
| Azure Event Hubs Source Connector | `AzureEventHubsSource` | Azure Event Hubs Data Receiver |
| Azure Cosmos DB Source V2 Connector | `CosmosDbSourceV2` | Cosmos DB Built-in Data Reader |
| Azure Cosmos DB Sink V2 Connector | `CosmosDbSinkV2` | Cosmos DB Built-in Data Contributor |
| Azure Data Lake Storage Gen2 Sink Connector | `AzureDataLakeGen2Sink` | Storage Blob Data Contributor |
| Azure Data Lake Storage Gen2 Tableflow | — | Storage Blob Data Contributor, Reader |
| Azure Blob Storage Sink Connector | `AzureBlobSink` | Storage Blob Data Contributor |
| Microsoft SQL Server CDC Source V2 (Debezium) Connector | `SqlServerCdcSourceV2` | Microsoft Entra admin |
| Azure Key Vault Integration | — | Key Vault Secrets User |
| Azure Blob Storage Source Connector | `AzureBlobSource` | Storage Blob Data Contributor |
| Microsoft SQL Server Sink | `MicrosoftSqlServerSink` | Microsoft Entra admin |
| Microsoft SQL Server Source | `MicrosoftSqlServerSource` | Microsoft Entra admin |
| MySQL Sink | `MySqlSink` | Microsoft Entra admin |
| MySQL Source | `MySqlSource` | Microsoft Entra admin |
| Postgres Sink | `PostgresSink` | Microsoft Entra admin |
| Postgres Source | `PostgresSource` | Microsoft Entra admin |
| Azure Data Explorer (Kusto) Sink Connector | `AzureDataExplorerKustoSink` | Database Ingestor, Database Viewer |
| Redis Kafka Sink Connector (Azure Managed Redis) | `RedisKafkaSink` | Default access policy |
| Redis Kafka Source Connector (Azure Managed Redis) | `RedisKafkaSource` | Default access policy |

## Verbatim payload

```json
{
  "azure_integration_policies": [
    {
      "confluent_resource": "Azure Event Hubs Source Connector",
      "template_id": "AzureEventHubsSource",
      "required_roles": [
        "Azure Event Hubs Data Receiver"
      ]
    },
    {
      "confluent_resource": "Azure Cosmos DB Source V2 Connector",
      "template_id": "CosmosDbSourceV2",
      "required_roles": [
        "Cosmos DB Built-in Data Reader"
      ]
    },
    {
      "confluent_resource": "Azure Cosmos DB Sink V2 Connector",
      "template_id": "CosmosDbSinkV2",
      "required_roles": [
        "Cosmos DB Built-in Data Contributor"
      ]
    },
    {
      "confluent_resource": "Azure Data Lake Storage Gen2 Sink Connector",
      "template_id": "AzureDataLakeGen2Sink",
      "required_roles": [
        "Storage Blob Data Contributor"
      ]
    },
    {
      "confluent_resource": "Azure Data Lake Storage Gen2 Tableflow",
      "required_roles": [
        "Storage Blob Data Contributor",
        "Reader"
      ]
    },
    {
      "confluent_resource": "Azure Blob Storage Sink Connector",
      "template_id": "AzureBlobSink",
      "required_roles": [
        "Storage Blob Data Contributor"
      ]
    },
    {
      "confluent_resource": "Microsoft SQL Server CDC Source V2 (Debezium) Connector",
      "template_id": "SqlServerCdcSourceV2",
      "required_roles": [
        "Microsoft Entra admin"
      ]
    },
    {
      "confluent_resource": "Azure Key Vault Integration",
      "required_roles": [
        "Key Vault Secrets User"
      ]
    },
    {
      "confluent_resource": "Azure Blob Storage Source Connector",
      "template_id": "AzureBlobSource",
      "required_roles": [
        "Storage Blob Data Contributor"
      ]
    },
    {
      "confluent_resource": "Microsoft SQL Server Sink",
      "template_id": "MicrosoftSqlServerSink",
      "required_roles": [
        "Microsoft Entra admin"
      ]
    },
    {
      "confluent_resource": "Microsoft SQL Server Source",
      "template_id": "MicrosoftSqlServerSource",
      "required_roles": [
        "Microsoft Entra admin"
      ]
    },
    {
      "confluent_resource": "MySQL Sink",
      "template_id": "MySqlSink",
      "required_roles": [
        "Microsoft Entra admin"
      ]
    },
    {
      "confluent_resource": "MySQL Source",
      "template_id": "MySqlSource",
      "required_roles": [
        "Microsoft Entra admin"
      ]
    },
    {
      "confluent_resource": "Postgres Sink",
      "template_id": "PostgresSink",
      "required_roles": [
        "Microsoft Entra admin"
      ]
    },
    {
      "confluent_resource": "Postgres Source",
      "template_id": "PostgresSource",
      "required_roles": [
        "Microsoft Entra admin"
      ]
    },
    {
      "confluent_resource": "Azure Data Explorer (Kusto) Sink Connector",
      "template_id": "AzureDataExplorerKustoSink",
      "required_roles": [
        "Database Ingestor",
        "Database Viewer"
      ]
    },
    {
      "confluent_resource": "Redis Kafka Sink Connector (Azure Managed Redis)",
      "template_id": "RedisKafkaSink",
      "required_roles": [
        "Default access policy"
      ]
    },
    {
      "confluent_resource": "Redis Kafka Source Connector (Azure Managed Redis)",
      "template_id": "RedisKafkaSource",
      "required_roles": [
        "Default access policy"
      ]
    }
  ]
}
```
