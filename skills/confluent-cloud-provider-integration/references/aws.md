# AWS Provider Integration (IAM AssumeRole)

Read this file when the target CSP is **AWS**. It fills in the exact steps, JSON,
and commands for the four-phase flow in `SKILL.md`.

## Contents
- [Paradigm](#paradigm)
- [Prerequisites](#prerequisites)
- [Ordered flow](#ordered-flow-the-ping-pong)
- [Permission policy templates](#permission-policy-templates)
- [Trust policy JSON](#trust-policy-json)
- [Confluent CLI / UI / API](#confluent-side-create--map)
- [Validate](#validate)
- [Troubleshooting](#troubleshooting)
- [AWS documentation](#aws-documentation)

## Paradigm

Confluent assumes an **IAM role** in your AWS account via `sts:AssumeRole`,
scoped by an **External ID** that Confluent generates. You create a role with a
permission policy (what it can touch) and a **trust policy** (who may assume it).
The trust policy is finalized **last**, with the Confluent role ARN + External ID
returned when you map the role in Confluent.

> AWS provider integrations are only supported for AWS-based Confluent Cloud
> clusters. Default quota: 100 integrations per organization.

## Prerequisites

- Confluent Cloud user with **OrganizationAdmin** or **EnvironmentAdmin**.
- AWS permissions to create IAM policies and roles.
- The Confluent **environment ID** (`env-...`).
- Access to the AWS IAM console (https://console.aws.amazon.com/iam/) or the
  `aws` CLI, and — for the API path — a Confluent Cloud API key plus `curl`/`jq`.

## Ordered flow (the ping-pong)

Track this checklist and restate it after every step:

```
[ ] 1. AWS: create IAM permission policy (scoped to the target resource)
[ ] 2. AWS: create IAM role with the PLACEHOLDER DENY trust policy; attach policy
[ ] 3. AWS: copy the role ARN
[ ] 4. Confluent: create integration mapping that ARN → get Confluent role ARN + External ID
[ ] 5. AWS: replace the trust policy with the ALLOW policy using step-4 values
[ ] 6. Confluent: validate (describe; confirm no AssumeRole AccessDenied)
```

The reason for the ping-pong: in step 2 you cannot yet know who to trust, so you
start with an explicit deny; step 4 is where Confluent gives you the principal
ARN and External ID; step 5 is where you actually grant the trust.

## Permission policy templates

Attach a **least-privilege** policy for the target resource. Replace every
placeholder (bucket name, table name, region, account) with the user's real
values. Examples use synthetic placeholders.

> For the **exact per-resource policy** the portal prescribes (all 21 resources —
> S3 Sink/Source, DynamoDB CDC/Sink, Lambda, Kinesis, SQS, Redshift, RDS
> CDC/JDBC, Redis, OpenSearch, Secrets Manager, Tableflow S3/Glue, …), see
> `references/aws-permission-policies.md` — a verbatim catalog captured from the
> wizard. The examples below are the same as the catalog's entries; **request the
> same permissions the portal does.**

**S3 Sink Connector** (portal-exact `S3Sink`):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:ListAllMyBuckets"],
      "Resource": ["arn:aws:s3:::*"] },
    { "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
      "Resource": ["arn:aws:s3:::<bucket-name>"] },
    { "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:PutObjectTagging", "s3:GetObject",
                 "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      "Resource": ["arn:aws:s3:::<bucket-name>/*"] }
  ]
}
```

**DynamoDB CDC Source Connector** (portal-exact `DynamoDbCdcSource` — note the
separate statement for the checkpointing table and the account-wide list/tag
actions). The `DynamoDbSink` policy is a single CRUD statement on one table — see
the catalog:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["dynamodb:DescribeTable", "dynamodb:DescribeStream",
                 "dynamodb:ListTagsOfResource", "dynamodb:DescribeLimits",
                 "dynamodb:GetRecords", "dynamodb:GetShardIterator", "dynamodb:Scan"],
      "Resource": ["arn:aws:dynamodb:*:*:table/*"] },
    { "Effect": "Allow",
      "Action": ["dynamodb:CreateTable", "dynamodb:DescribeTable", "dynamodb:GetItem",
                 "dynamodb:PutItem", "dynamodb:Scan", "dynamodb:UpdateItem",
                 "dynamodb:DeleteItem"],
      "Resource": ["arn:aws:dynamodb:*:*:table/<dynamodb.cdc.checkpointing.table.prefix>-*"] },
    { "Effect": "Allow",
      "Action": ["dynamodb:ListStreams", "dynamodb:ListTables",
                 "dynamodb:ListGlobalTables", "tag:GetResources"],
      "Resource": ["*"] }
  ]
}
```

**Tableflow with AWS Glue Data Catalog.** The portal uses **two separate
policies**, not one combined policy: a **Glue Catalog Sync** policy (metadata) and
a **Tableflow S3 Bucket** policy (the Iceberg/Delta data, when you bring your own
bucket). Attach both to the role.

*Glue Catalog Sync* (portal-exact — 8 actions on a single account-level Glue ARN;
no partition actions, no S3):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["glue:GetTable", "glue:GetDatabase", "glue:DeleteTable",
                 "glue:DeleteDatabase", "glue:CreateTable", "glue:CreateDatabase",
                 "glue:UpdateTable", "glue:UpdateDatabase"],
      "Resource": ["arn:aws:glue:<region>:<account-id>:*"] }
  ]
}
```
*Tableflow S3 Bucket* (portal-exact — includes `s3:DeleteObject`, unlike the S3
Sink policy):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["s3:GetBucketLocation", "s3:ListBucketMultipartUploads", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::<bucket-name>"] },
    { "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:PutObjectTagging", "s3:GetObject",
                 "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      "Resource": ["arn:aws:s3:::<bucket-name>/*"] }
  ]
}
```
> If Tableflow storage is Confluent-managed (not BYOB), only the Glue Catalog Sync
> policy is needed.

For any resource not shown here, use the portal-exact entry from
`references/aws-permission-policies.md` (or the connector's own docs via
`references/common.md` → Connector hand-off) — same actions, same scope.

## Trust policy JSON

**Step 2 — placeholder trust policy** (blocks all assumption until finalized):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Deny", "Principal": { "AWS": "*" }, "Action": "sts:AssumeRole" }
  ]
}
```

**Step 5 — finalized trust policy** (replace both `Principal.AWS` values with the
Confluent role ARN, and `sts:ExternalId` with the External ID from step 4):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::CONFLUENT_ACCT:role/cspi-XXXXX" },
      "Action": "sts:AssumeRole",
      "Condition": { "StringEquals": { "sts:ExternalId": "EXTERNAL_ID_FROM_CONFLUENT" } }
    },
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::CONFLUENT_ACCT:role/cspi-XXXXX" },
      "Action": "sts:TagSession"
    }
  ]
}
```

The `sts:TagSession` statement is required — omitting it causes intermittent
AssumeRole failures.

## Confluent side: create + map

> **"Is my role ARN a secret? Am I being hacked?"** No — and the caution is a
> healthy instinct worth affirming. An **ARN (Amazon Resource Name) is an
> identifier**, like a postal address or an account number, *not* a credential.
> Knowing it grants no access: a caller can only assume the role if your **trust
> policy** explicitly allows their principal **and** they present the matching
> **External ID** — that is exactly the mechanism AWS prescribes for granting a
> third party like Confluent access. So pasting the role ARN into Confluent (or
> echoing it in a terminal) is safe and expected. What must **never** be pasted
> or shared are real credentials: an AWS **access key ID**, **secret access
> key**, or **session token**. If the setup ever asks for those, stop — this
> flow is designed specifically so you don't hand over keys. See the AWS ARN and
> external-ID references under [AWS documentation](#aws-documentation).

**CLI (default, hybrid).** After creating the AWS role (steps 1–3), create the
integration mapping and read back the Confluent values:
```bash
confluent provider-integration create <name> \
  --cloud aws \
  --customer-role-arn arn:aws:iam::000000000000:role/EXAMPLE_ROLE \
  --environment env-abc123
```
The output includes `ID` (`cspi-...`), `IAM Role ARN` (the Confluent principal to
trust), and `External ID` — use these in the step-5 trust policy.

**UI.** Integrations → Provider Integrations → **Add integration** →
**AWS IAM role**. The UI walks: view/copy the prescribed `Permission-policy.json`,
create the AWS policy, create the AWS role with the shown `Trust-policy.json`,
paste the role ARN + name back, then copy the **updated** trust policy and paste
it into AWS.

**REST API.**
```bash
curl --request POST \
  --url https://api.confluent.cloud/pim/v1/integrations \
  --header 'Authorization: Basic <base64-encoded-key-and-secret>' \
  --header 'content-type: application/json' \
  --data '{
    "display_name":"example_provider_integration",
    "provider":"AWS",
    "config":{ "customer_iam_role_arn":"arn:aws:iam::000000000000:role/EXAMPLE_ROLE",
               "kind":"AwsIntegrationConfig" },
    "environment":{ "id":"env-abc123" }
  }'
```
Record `iam_role_arn` and `external_id` from the response for the trust policy.

## Validate

```bash
confluent provider-integration describe --id cspi-XXXXX --environment env-abc123
```
Then confirm the AWS role's **Trust relationships** tab shows the Allow +
TagSession statements with the correct principal and External ID. A quick way to
prove trust end-to-end is to create the connector and confirm it provisions
without an AssumeRole error; connector creation itself is out of scope here.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `AccessDenied` on AssumeRole | Trust policy not finalized, wrong principal ARN, or `TagSession` missing | Re-apply the step-5 trust policy exactly; include the `sts:TagSession` statement |
| `AccessDenied` mentioning External ID / condition failed | External ID mismatch | Copy the External ID from `describe` verbatim into `sts:ExternalId` |
| Works intermittently | IP-conditional policy, or External ID changed | Check for conditional policies; verify External ID; inspect CloudTrail `AssumeRole` events |
| Connector can read metadata but not objects | Permission policy too narrow | Add the missing S3/DynamoDB/Glue actions from the connector's docs |
| Tableflow: `AccessDenied` on `glue:CreateTable`/`UpdateTable` | Glue policy scope or action missing | Use the portal-exact Glue Catalog Sync policy: the 8 `glue:*` actions on `arn:aws:glue:<region>:<account-id>:*` |
| `describe` fails though it's listed | Role deleted/modified after creation | Verify the IAM role still exists and its trust policy still allows Confluent |

Check **AWS CloudTrail** for `AssumeRole` failure events to see the exact reason.

## AWS documentation

- Amazon Resource Names (ARNs) — what an ARN is (an identifier, not a secret): https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
- Granting a third party access with an External ID (why sharing your role ARN is safe): https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
- Creating IAM policies: https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_create.html
- Roles with custom trust policies: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-custom.html
- IAM policy simulator: https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_testing-policies.html
- Confluent: Create an AWS provider integration: https://docs.confluent.io/cloud/current/connectors/provider-integration/cloud-console/aws.html
- Confluent: Tableflow → AWS Glue Data Catalog integration: https://docs.confluent.io/cloud/current/topics/tableflow/how-to/integrate-catalogs/integrate-aws-glue-catalog.html
- AWS Glue resource ARNs & actions: https://docs.aws.amazon.com/service-authorization/latest/reference/list_awsglue.html
