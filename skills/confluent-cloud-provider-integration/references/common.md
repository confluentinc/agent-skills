# Common: Roles, Management, and Connector Hand-off

Read this file for cross-provider concerns: required roles, quotas, listing /
describing / deleting integrations, the "update = recreate" rule, and how a
connector consumes a finished integration. It is **not** CSP-specific — for the
identity setup itself use `references/aws.md`, `references/azure.md`, or
`references/gcp.md`.

## Contents
- [Roles & prerequisites](#roles--prerequisites)
- [Quota](#quota)
- [List / describe](#list--describe)
- [Update (delete + recreate)](#update-delete--recreate)
- [Delete](#delete)
- [Cross-provider troubleshooting](#cross-provider-troubleshooting)
- [Connector hand-off](#connector-hand-off)

## Roles & prerequisites

- **Create/manage an integration:** Confluent Cloud **OrganizationAdmin** or
  **EnvironmentAdmin**. Confirm the user has one of these before starting.
- **Create a connector that *uses* an integration:** **ResourceOwner** or
  **Assigner** on the integration is sufficient (they don't need admin).
- Access to the target Confluent **environment**, and CSP permissions to manage
  the relevant identity primitives (see each CSP reference file).

## Quota

Default quota is **100 provider integrations per organization** across all CSPs.
Contact Confluent Support to raise it.

## List / describe

```bash
# List all, or filter by cloud (lowercase aws|azure|gcp, flag is --cloud)
confluent provider-integration list --environment env-abc123
confluent provider-integration list --cloud aws --environment env-abc123
confluent provider-integration list --cloud gcp --environment env-abc123

# Describe one — the ID is POSITIONAL (there is no --id flag)
confluent provider-integration describe cspi-XXXXX --environment env-abc123
```

REST equivalents:
```bash
curl --request GET \
  'https://api.confluent.cloud/pim/v1/integrations?environment=env-abc123' \
  --header 'Authorization: Basic <base64-encoded-key-and-secret>' | jq

curl --request GET \
  'https://api.confluent.cloud/pim/v1/integrations/cspi-XXXXX?environment=env-abc123' \
  --header 'Authorization: Basic <base64-encoded-key-and-secret>' | jq
```

UI: **Integrations → Provider Integrations**, then select an integration to
see its details.

## Update (delete + recreate)

**Updating a provider integration is not supported.** To change configuration,
delete and recreate it. Cloud-specific notes when recreating:
- **AWS:** you can reuse the same IAM role, but must update the trust policy with
  the **new** Confluent role ARN + External ID.
- **Google Cloud:** you can reuse the same service account, but must update the
  impersonation binding with the **new** Confluent service-account email.
- **Azure:** re-establish the tenant trust for the (new) integration.

## Delete

```bash
# delete takes the ID(s) POSITIONALLY (no --id flag); multiple IDs are allowed
confluent provider-integration delete cspi-XXXXX --environment env-abc123
```
```bash
curl --request DELETE \
  'https://api.confluent.cloud/pim/v1/integrations/cspi-XXXXX?environment=env-abc123' \
  --header 'Authorization: Basic <base64-encoded-key-and-secret>' | jq
```

> A delete request **fails if any Confluent workload still uses the integration**.
> Deleting the integration does **not** remove the CSP-side role / service
> principal / service account — clean those up separately (remove the IAM role or
> its trust policy, the SP, or the impersonation binding) to prevent future access.

## Cross-provider troubleshooting

| Symptom | Where to look |
|---|---|
| Integration listed but `describe` fails | CSP-side identity changed/deleted after creation — verify it still exists (see CSP reference file) |
| Delete fails: "integration in use" | A connector still references it — `confluent connect cluster list`, find the one using `cspi-...`, delete/reconfigure it, then retry |
| Works intermittently | CSP-side conditional access, changed External ID (AWS) / SA email (GCP), or propagation lag — see the CSP reference file's troubleshooting table |

## Connector hand-off

Once the integration is **validated**, this skill's scope is complete. To use it:

- A connector references the integration by its ID via **`provider.integration.id`**
  (`cspi-...`), and sets **`authentication.method`** to the assumed-identity mode
  the connector documents (e.g. an IAM-role/assumed-identity option) rather than
  supplying access keys.
- One integration can be reused by **multiple connectors** in the same
  environment.
- **Tableflow BYOB** (bring-your-own-bucket) also consumes a provider integration
  ID for its storage — the same `cspi-...` can back it.

Point the user to their specific connector's documentation for the exact config
keys and to `confluent-cloud-cdc-tableflow` for full pipeline builds. Do not
create the connector as part of this skill.

## Reference

- Manage provider integrations: https://docs.confluent.io/cloud/current/connectors/provider-integration/cloud-console/manage.html
- CLI reference: https://docs.confluent.io/confluent-cli/current/command-reference/provider-integration/index.html
- API reference: https://docs.confluent.io/cloud/current/ccloud/integrations-pim-v-1/
