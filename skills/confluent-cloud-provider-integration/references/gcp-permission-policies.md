# GCP Permission Policies — Provider Integrations catalog

> **Source:** Confluent Cloud Provider Integrations wizard — `gcp_integration_policies`
> payload, **captured verbatim from the live portal on 2026-11-05**. Do not
> hand-edit the permission lists; re-capture from the wizard to refresh.
> This is the per-resource companion to `references/gcp.md`.
>
> ⚠️ **Staleness caveat.** These permissions are a point-in-time snapshot. Confluent
> can change what it requests as connectors are added or scopes are tightened.
> **A Confluent employee should re-verify this catalog against the live wizard
> roughly every couple of months** (re-run the capture above) and update the
> permissions + the `captured` date if anything differs.

## How to use

1. Find the user's connector/resource in the **Resource index** below.
2. Grant the service account **exactly** the `required_permissions` the portal
   prescribes for that resource — no more — via a **custom IAM role** (or the
   equivalent predefined role) bound on the **target resource** (bucket, Cloud
   SQL instance, BigQuery dataset, Pub/Sub subscription, secret).
3. A few entries list **predefined roles** (e.g. `roles/biglake.admin`) instead
   of granular permissions — grant those roles as shown.
4. This is **in addition to** the impersonation binding
   (`roles/iam.serviceAccountTokenCreator` on your own service account for
   Confluent's service account) described in `references/gcp.md`.

Request the **same** permissions the portal does — this catalog is the source of
truth; the inline examples in `references/gcp.md` are teaching aids.

## Resource index

| Resource (portal label) | template_id | Grants |
|---|---|---|
| Google BigQuery Sink V2 Connector | `BigQueryStorageSink` | 7 permissions |
| Google Cloud Storage Sink Connector | `GcsSink` | 5 permissions |
| PostgreSQL CDC Source V2 (Debezium) Connector | `PostgresCdcSourceV2` | 4 permissions |
| Google Cloud Storage Source Connector | `GcsSource` | 3 permissions |
| Google Cloud Pub/Sub Source Connector | `PubSubSource` | 2 permissions |
| MySQL Sink | `MySqlSink` | 4 permissions |
| MySQL Source | `MySqlSource` | 4 permissions |
| Postgres Sink | `PostgresSink` | 4 permissions |
| Postgres Source | `PostgresSource` | 4 permissions |
| GCP Secret Manager | — | 1 permission |
| Tableflow GCS Bucket | — | 5 permissions |
| Tableflow BigLake Metastore | — | 2 predefined roles |

## Verbatim payload

```json
{
  "gcp_integration_policies": [
    {
      "confluent_resource": "Google BigQuery Sink V2 Connector",
      "template_id": "BigQueryStorageSink",
      "required_permissions": [
        "bigquery.datasets.get",
        "bigquery.tables.create",
        "bigquery.tables.get",
        "bigquery.tables.getData",
        "bigquery.tables.list",
        "bigquery.tables.update",
        "bigquery.tables.updateData"
      ]
    },
    {
      "confluent_resource": "Google Cloud Storage Sink Connector",
      "template_id": "GcsSink",
      "required_permissions": [
        "storage.objects.get",
        "storage.objects.create",
        "storage.objects.list",
        "storage.objects.delete",
        "storage.buckets.get"
      ]
    },
    {
      "confluent_resource": "PostgreSQL CDC Source V2 (Debezium) Connector",
      "template_id": "PostgresCdcSourceV2",
      "required_permissions": [
        "cloudsql.instances.connect",
        "cloudsql.instances.get",
        "cloudsql.instances.login",
        "iam.serviceAccounts.getAccessToken"
      ]
    },
    {
      "confluent_resource": "Google Cloud Storage Source Connector",
      "template_id": "GcsSource",
      "required_permissions": [
        "storage.buckets.get",
        "storage.objects.get",
        "storage.objects.list"
      ]
    },
    {
      "confluent_resource": "Google Cloud Pub/Sub Source Connector",
      "template_id": "PubSubSource",
      "required_permissions": [
        "pubsub.subscriber",
        "pubsub.viewer"
      ]
    },
    {
      "confluent_resource": "MySQL Sink",
      "template_id": "MySqlSink",
      "required_permissions": [
        "cloudsql.instances.connect",
        "cloudsql.instances.get",
        "cloudsql.instances.login",
        "iam.serviceAccounts.getAccessToken"
      ]
    },
    {
      "confluent_resource": "MySQL Source",
      "template_id": "MySqlSource",
      "required_permissions": [
        "cloudsql.instances.connect",
        "cloudsql.instances.get",
        "cloudsql.instances.login",
        "iam.serviceAccounts.getAccessToken"
      ]
    },
    {
      "confluent_resource": "Postgres Sink",
      "template_id": "PostgresSink",
      "required_permissions": [
        "cloudsql.instances.connect",
        "cloudsql.instances.get",
        "cloudsql.instances.login",
        "iam.serviceAccounts.getAccessToken"
      ]
    },
    {
      "confluent_resource": "Postgres Source",
      "template_id": "PostgresSource",
      "required_permissions": [
        "cloudsql.instances.connect",
        "cloudsql.instances.get",
        "cloudsql.instances.login",
        "iam.serviceAccounts.getAccessToken"
      ]
    },
    {
      "confluent_resource": "GCP Secret Manager",
      "required_permissions": [
        "secretmanager.versions.access"
      ]
    },
    {
      "confluent_resource": "Tableflow GCS Bucket",
      "required_permissions": [
        "storage.objects.get",
        "storage.objects.list",
        "storage.objects.create",
        "storage.objects.delete",
        "storage.folders.create"
      ]
    },
    {
      "confluent_resource": "Tableflow BigLake Metastore",
      "required_permissions": [
        "BigLake Admin (roles/biglake.admin)",
        "Storage Object Viewer (roles/storage.objectViewer)"
      ]
    }
  ]
}
```
