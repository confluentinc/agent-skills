# AWS Permission Policies — Provider Integrations catalog

> **Source:** Confluent Cloud Provider Integrations wizard — `GET …/policies?provider=aws`
> (`aws_integration_policies` payload), **captured verbatim from the live portal on
> 2026-11-05**. Do not hand-edit the policy documents; re-capture from the wizard
> to refresh. This is the per-resource companion to `references/aws.md`.
>
> ⚠️ **Staleness caveat.** These policies are a point-in-time snapshot. Confluent
> can change the permissions it requests as connectors are added or scopes are
> tightened. **A Confluent employee should re-verify this catalog against the live
> wizard roughly every couple of months** (re-run the capture above) and update the
> JSON + the `captured` date if anything differs.

## How to use

1. Find the user's connector/resource in the **Resource index** below.
2. Create the IAM **permission policy** using **exactly** the `policy_document`
   the portal prescribes for that resource — same actions, same resource ARNs.
3. Replace the `<…>` placeholders with the user's real values (e.g.
   `<bucket-name>`, `<region>`, `<account-id>`, `<function-name>`,
   `<db-username>`). Keep the scoping as-is; do not broaden it.
4. Attach the policy to the IAM role, then finalize the **trust policy**
   (placeholder Deny → Allow with `sts:ExternalId` + `sts:TagSession`) per
   `references/aws.md`. The trust policy is generic across resources — see
   `references/aws.md` for its JSON; it is **not** part of this per-resource catalog.

Request the **same** permissions the portal does — this catalog is the source of
truth; the inline examples in `references/aws.md` are teaching aids kept in sync
with these entries.

## Applying these templates — ARN rendering notes

The catalog JSON below is **verbatim from the portal** and is not edited. But for
three resource families the portal's placeholder is **not** simply the friendly
name — dropping in the console name produces an ARN that looks right yet silently
fails to authorize (`AccessDenied` at connect time). Render these correctly when
you substitute placeholders:

- **Redshift** (`redshift:GetClusterCredentials`). The `dbuser`/`dbname`
  placeholders are **cluster-scoped**, not bare names. Render:
  - `arn:aws:redshift:<region>:<account-id>:dbuser:<cluster-name>/<db-user>`
  - `arn:aws:redshift:<region>:<account-id>:dbname:<cluster-name>/<db-name>`
  - (and `…:dbgroup:<cluster-name>/<group>` if a group is used). Omitting
    `<cluster-name>/` means the action authorizes against nothing and is denied.

- **RDS / Aurora** (`rds-db:connect`). The `<instance-identifier>` in the resource
  ARN is the **immutable resource ID**, not the friendly instance name:
  - DB instance: `DbiResourceId` (looks like `db-ABCDEF1234567890`)
  - Aurora cluster: `DbClusterResourceId` (looks like `cluster-ABCDEF123…`)
  - ARN form: `arn:aws:rds-db:<region>:<account-id>:dbuser:<DbiResourceId>/<db-username>`
  - Look it up — don't guess:
    ```bash
    aws rds describe-db-instances \
      --query 'DBInstances[].DbiResourceId' --output text
    # Aurora:
    aws rds describe-db-clusters \
      --query 'DBClusters[].DbClusterResourceId' --output text
    ```
    The friendly name will not authorize `rds-db:connect`. Covers all six RDS/Aurora
    entries (MySQL/PostgreSQL CDC V2, MySQL/PostgreSQL JDBC Sink/Source).

- **Secrets Manager** (`secretsmanager:GetSecretValue`). A secret's ARN ends in a
  **random 6-character suffix** (`-aB3xYz`); the friendly name alone won't match.
  Use the full ARN, or a trailing wildcard:
  - `aws secretsmanager describe-secret --secret-id <name> --query ARN --output text`
  - or `arn:aws:secretsmanager:<region>:<account-id>:secret:<secret-name>-*`
    (the `-*` tolerates the generated suffix without pinning it).

These are *rendering* corrections for the apply path — the catalog payloads stay
exactly as captured.

## Resource index

| Resource (portal label) | template_id | Scope highlight |
|---|---|---|
| DynamoDB CDC Source Connector | `DynamoDbCdcSource` | table/*, checkpoint-table prefix, list/tag on * |
| S3 Sink Connector | `S3Sink` | bucket + bucket/* + `s3:ListAllMyBuckets` |
| Tableflow S3 Bucket | — | bucket + bucket/* (read/write/delete) |
| Tableflow Glue Catalog Sync | — | `glue:*` CRUD on `glue:<region>:<account-id>:*` |
| Lambda Sink Connector | `LambdaSink` | `lambda:GetFunction`/`InvokeFunction` on function |
| S3 Source Connector | `S3Source` | bucket + bucket/* (read) + `s3:ListAllMyBuckets` |
| MySQL CDC Source V2 (Debezium) Connector | `MySqlCdcSourceV2` | `rds-db:connect` on dbuser |
| PostgreSQL CDC Source V2 (Debezium) Connector | `PostgresCdcSourceV2` | `rds-db:connect` on dbuser |
| MySQL Sink (JDBC) Connector | `MySqlSink` | `rds-db:connect` on dbuser |
| MySQL Source (JDBC) Connector | `MySqlSource` | `rds-db:connect` on dbuser |
| PostgreSQL Sink (JDBC) Connector | `PostgresSink` | `rds-db:connect` on dbuser |
| PostgreSQL Source (JDBC) Connector | `PostgresSource` | `rds-db:connect` on dbuser |
| SQS Source Connector | `SqsSource` | `sqs:ReceiveMessage`/`DeleteMessage` on queue |
| Kinesis Source Connector | `KinesisSource` | `kinesis:*` read on stream |
| DynamoDB Sink Connector | `DynamoDbSink` | CRUD on table |
| CloudWatch Logs Connector | `CloudWatchLogsSource` | `logs:DescribeLogStreams`/`GetLogEvents` |
| Redshift Sink Connector | `RedshiftSink` | `redshift:GetClusterCredentials` |
| Redis Kafka Sink/Source Connector (MemoryDB) | — | `memorydb:Connect` on cluster/user |
| Redis Kafka Sink/Source Connector (ElastiCache) | — | `elasticache:Connect` on cache/user/group |
| AWS Secrets Manager | — | `secretsmanager:GetSecretValue` on secret |
| OpenSearch Sink Connector | `OpenSearchSink` | `es:ESHttp*` on domain |

## Verbatim payload

```json
{
  "aws_integration_policies": [
    {
      "confluent_resource": "DynamoDB CDC Source Connector",
      "template_id": "DynamoDbCdcSource",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "dynamodb:DescribeTable",
              "dynamodb:DescribeStream",
              "dynamodb:ListTagsOfResource",
              "dynamodb:DescribeLimits",
              "dynamodb:GetRecords",
              "dynamodb:GetShardIterator",
              "dynamodb:Scan"
            ],
            "Resource": [
              "arn:aws:dynamodb:*:*:table/*"
            ]
          },
          {
            "Effect": "Allow",
            "Action": [
              "dynamodb:CreateTable",
              "dynamodb:DescribeTable",
              "dynamodb:GetItem",
              "dynamodb:PutItem",
              "dynamodb:Scan",
              "dynamodb:UpdateItem",
              "dynamodb:DeleteItem"
            ],
            "Resource": [
              "arn:aws:dynamodb:*:*:table/<dynamodb.cdc.checkpointing.table.prefix>-*"
            ]
          },
          {
            "Effect": "Allow",
            "Action": [
              "dynamodb:ListStreams",
              "dynamodb:ListTables",
              "dynamodb:ListGlobalTables",
              "tag:GetResources"
            ],
            "Resource": [
              "*"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "S3 Sink Connector",
      "template_id": "S3Sink",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "s3:ListAllMyBuckets"
            ],
            "Resource": [
              "arn:aws:s3:::*"
            ]
          },
          {
            "Effect": "Allow",
            "Action": [
              "s3:ListBucket",
              "s3:GetBucketLocation",
              "s3:ListBucketMultipartUploads"
            ],
            "Resource": [
              "arn:aws:s3:::<bucket-name>"
            ]
          },
          {
            "Effect": "Allow",
            "Action": [
              "s3:PutObject",
              "s3:PutObjectTagging",
              "s3:GetObject",
              "s3:AbortMultipartUpload",
              "s3:ListMultipartUploadParts"
            ],
            "Resource": [
              "arn:aws:s3:::<bucket-name>/*"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "Tableflow S3 Bucket",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "s3:GetBucketLocation",
              "s3:ListBucketMultipartUploads",
              "s3:ListBucket"
            ],
            "Resource": [
              "arn:aws:s3:::<bucket-name>"
            ]
          },
          {
            "Effect": "Allow",
            "Action": [
              "s3:PutObject",
              "s3:PutObjectTagging",
              "s3:GetObject",
              "s3:DeleteObject",
              "s3:AbortMultipartUpload",
              "s3:ListMultipartUploadParts"
            ],
            "Resource": [
              "arn:aws:s3:::<bucket-name>/*"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "Tableflow Glue Catalog Sync",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "glue:GetTable",
              "glue:GetDatabase",
              "glue:DeleteTable",
              "glue:DeleteDatabase",
              "glue:CreateTable",
              "glue:CreateDatabase",
              "glue:UpdateTable",
              "glue:UpdateDatabase"
            ],
            "Resource": [
              "arn:aws:glue:<region>:<account-id>:*"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "Lambda Sink Connector",
      "template_id": "LambdaSink",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "lambda:GetFunction",
              "lambda:InvokeFunction"
            ],
            "Resource": [
              "arn:aws:lambda:*:*:function:<function-name>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "S3 Source Connector",
      "template_id": "S3Source",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "s3:ListAllMyBuckets"
            ],
            "Resource": [
              "arn:aws:s3:::*"
            ]
          },
          {
            "Effect": "Allow",
            "Action": [
              "s3:ListBucket"
            ],
            "Resource": [
              "arn:aws:s3:::<bucket-name>"
            ]
          },
          {
            "Effect": "Allow",
            "Action": [
              "s3:GetObject"
            ],
            "Resource": [
              "arn:aws:s3:::<bucket-name>/*"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "MySQL CDC Source V2 (Debezium) Connector",
      "template_id": "MySqlCdcSourceV2",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "rds-db:connect"
            ],
            "Resource": [
              "arn:aws:rds-db:<region>:<account-id>:dbuser:<instance-identifier>/<db-username>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "PostgreSQL CDC Source V2 (Debezium) Connector",
      "template_id": "PostgresCdcSourceV2",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "rds-db:connect"
            ],
            "Resource": [
              "arn:aws:rds-db:<region>:<account-id>:dbuser:<instance-identifier>/<db-username>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "MySQL Sink (JDBC) Connector",
      "template_id": "MySqlSink",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "rds-db:connect"
            ],
            "Resource": [
              "arn:aws:rds-db:<region>:<account-id>:dbuser:<instance-identifier>/<db-username>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "MySQL Source (JDBC) Connector",
      "template_id": "MySqlSource",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "rds-db:connect"
            ],
            "Resource": [
              "arn:aws:rds-db:<region>:<account-id>:dbuser:<instance-identifier>/<db-username>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "PostgreSQL Sink (JDBC) Connector",
      "template_id": "PostgresSink",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "rds-db:connect"
            ],
            "Resource": [
              "arn:aws:rds-db:<region>:<account-id>:dbuser:<instance-identifier>/<db-username>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "PostgreSQL Source (JDBC) Connector",
      "template_id": "PostgresSource",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "rds-db:connect"
            ],
            "Resource": [
              "arn:aws:rds-db:<region>:<account-id>:dbuser:<instance-identifier>/<db-username>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "SQS Source Connector",
      "template_id": "SqsSource",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "sqs:ReceiveMessage",
              "sqs:DeleteMessage"
            ],
            "Resource": [
              "arn:aws:sqs:*:*:<queue-name>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "Kinesis Source Connector",
      "template_id": "KinesisSource",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "kinesis:DescribeStream",
              "kinesis:GetRecords",
              "kinesis:GetShardIterator"
            ],
            "Resource": [
              "arn:aws:kinesis:*:*:stream/<stream-name>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "DynamoDB Sink Connector",
      "template_id": "DynamoDbSink",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "dynamodb:Query",
              "dynamodb:DescribeTable",
              "dynamodb:CreateTable",
              "dynamodb:GetItem",
              "dynamodb:PutItem",
              "dynamodb:Scan",
              "dynamodb:ListTables",
              "dynamodb:UpdateItem",
              "dynamodb:DeleteItem",
              "dynamodb:BatchWriteItem",
              "dynamodb:BatchGetItem"
            ],
            "Resource": [
              "arn:aws:dynamodb:*:*:table/<table-name>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "CloudWatch Logs Connector",
      "template_id": "CloudWatchLogsSource",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "logs:DescribeLogStreams",
              "logs:GetLogEvents"
            ],
            "Resource": [
              "arn:aws:logs:*:*:*"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "Redshift Sink Connector",
      "template_id": "RedshiftSink",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "redshift:GetClusterCredentials"
            ],
            "Resource": [
              "arn:aws:redshift:*:*:dbuser:<db-user>/<db-name>",
              "arn:aws:redshift:*:*:dbname:<db-user>/<db-name>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "Redis Kafka Sink/Source Connector (MemoryDB)",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "memorydb:Connect"
            ],
            "Resource": [
              "arn:aws:memorydb:*:*:cluster/<cluster-name>",
              "arn:aws:memorydb:*:*:user/<username>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "Redis Kafka Sink/Source Connector (ElastiCache)",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "elasticache:Connect"
            ],
            "Resource": [
              "arn:aws:elasticache:*:*:serverlesscache:<cache-name>",
              "arn:aws:elasticache:*:*:user:<username>",
              "arn:aws:elasticache:*:*:replicationgroup:<replication-group>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "AWS Secrets Manager",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "secretsmanager:GetSecretValue"
            ],
            "Resource": [
              "arn:aws:secretsmanager:<region>:<account-id>:secret:<secret-name>"
            ]
          }
        ]
      }
    },
    {
      "confluent_resource": "OpenSearch Sink Connector",
      "template_id": "OpenSearchSink",
      "policy_document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "es:ESHttpGet",
              "es:ESHttpPost",
              "es:ESHttpPut",
              "es:ESHttpDelete"
            ],
            "Resource": [
              "arn:aws:es:*:*:domain/<domain-name>/*"
            ]
          }
        ]
      }
    }
  ]
}
```
