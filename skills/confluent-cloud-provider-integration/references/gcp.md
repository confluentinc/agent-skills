# Google Cloud Provider Integration (Service Account Impersonation)

Read this file when the target CSP is **Google Cloud**. It fills in the exact
steps and commands for the four-phase flow in `SKILL.md`.

## Contents
- [Paradigm](#paradigm)
- [Prerequisites](#prerequisites)
- [Ordered flow](#ordered-flow-the-ping-pong)
- [Confluent side (UI-driven)](#confluent-side-ui-driven)
- [Google Cloud side: role, SA, impersonation](#google-cloud-side-role-service-account-impersonation)
- [Establish trust + validate](#establish-trust--validate)
- [Troubleshooting](#troubleshooting)
- [Google Cloud documentation](#google-cloud-documentation)

## Paradigm

Confluent generates a **service account in Confluent's Google Cloud project**.
You create **your own** service account (with a role granting the needed
permissions), then grant Confluent's service account the **Service Account Token
Creator** role on *your* service account. That token-creator binding is what lets
Confluent **impersonate** your service account — no long-term key is exchanged.

> The Confluent side of the GCP flow is **UI-driven**: Confluent must
> generate its own service account for you to grant impersonation on.

## Prerequisites

- Confluent Cloud user with **OrganizationAdmin** or **EnvironmentAdmin**.
- Access to Google Cloud Console → IAM & Admin → Service Accounts
  (https://console.cloud.google.com/iam-admin/serviceaccounts) or the `gcloud`
  CLI, with permission to create roles, service accounts, and IAM bindings.

## Ordered flow (the ping-pong)

```
[ ] 1. Confluent: Add integration → Google service account → name it → Continue
[ ] 2. Confluent: Create service account → COPY Confluent's SA email
[ ] 3. Confluent: select Confluent resources → COPY the list of permissions needed
[ ] 4. GCP: create a custom role with those permissions
[ ] 5. GCP: create YOUR service account and assign that role
[ ] 6. GCP: grant Confluent's SA (step 2 email) the Service Account Token Creator role on YOUR SA
[ ] 7. Confluent: enter YOUR SA email → Validate (retry after a few min) → Continue
```

If you stop after step 2–3 but before mapping your SA, the integration shows as
**Draft** — resume it later with **Complete**.

## Confluent side (UI-driven)

1. Go to the target environment → **Integrations** → **Provider Integrations** →
   **Add integration**.
2. Select **Google service account**, enter a unique integration name, **Continue**.
3. Click **Create service account** (creates the SA in Confluent's GCP project).
   **Copy Confluent's service account email** — it has the form
   `cspi-<id>@cflt-cspi-prod.iam.gserviceaccount.com`.
4. Under **Confluent resources**, choose the resource(s). In **List of
   permissions needed**, **copy the permissions**.

## Google Cloud side: role, service account, impersonation

> For the **exact per-resource permission set** the portal prescribes (BigQuery
> Sink, GCS Sink/Source, Cloud SQL CDC, Pub/Sub, Secret Manager, Tableflow GCS /
> BigLake, etc.), see `references/gcp-permission-policies.md` — a verbatim catalog
> captured from the wizard. Use those permissions in the custom role below.

```bash
# 4. Create a custom role with the copied permissions
gcloud iam roles create ConfluentConnectorRole \
  --project=<YOUR_PROJECT_ID> \
  --title="Confluent Connector Role" \
  --permissions=<COMMA_SEPARATED_PERMISSIONS_FROM_CONFLUENT>

# 5. Create your service account and grant it that role
gcloud iam service-accounts create confluent-connector-sa \
  --project=<YOUR_PROJECT_ID> --display-name="Confluent connector SA"

gcloud projects add-iam-policy-binding <YOUR_PROJECT_ID> \
  --member="serviceAccount:confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
  --role="projects/<YOUR_PROJECT_ID>/roles/ConfluentConnectorRole"

# 6. Let Confluent's SA impersonate YOUR SA (the trust binding)
gcloud iam service-accounts add-iam-policy-binding \
  confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com \
  --member="serviceAccount:<CONFLUENT_SA_EMAIL_FROM_STEP_2>" \
  --role="roles/iam.serviceAccountTokenCreator"
```

For resource-scoped grants (e.g. a specific GCS bucket or BigQuery dataset), bind
the role at that resource scope instead of project-wide where possible, following
least privilege. You can also do all of this via the Google Cloud Console (Create
role → Create service account → grant Token Creator on it).

## Establish trust + validate

1. Back in Confluent, in **Google Cloud service account**, enter **your** service
   account email (the one you created in step 5).
2. Click **Validate** — confirms Confluent can impersonate your SA.
   - Success → "Successfully validated."
   - **Failure right after granting Token Creator is often just propagation
     lag** — GCP needs a few minutes to sync IAM bindings. Wait and retry before
     assuming misconfiguration.
3. Click **Continue**. The integration shows status **Created**.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Validation fails immediately after granting Token Creator | IAM propagation lag | Wait a few minutes and retry — this is expected |
| Validation keeps failing | Token Creator granted on the wrong SA, or wrong Confluent SA email | Grant `roles/iam.serviceAccountTokenCreator` to **Confluent's** SA email **on your** SA; re-copy the email from Confluent |
| Impersonation works but connector lacks access | Custom role missing permissions, or bound at too-narrow scope | Add the copied permissions; bind at the resource the connector uses |
| `describe` fails though listed | Your SA disabled/deleted after creation | Verify your SA still exists and the impersonation binding is intact |

Test impersonation manually:
```bash
gcloud auth print-access-token \
  --impersonate-service-account=confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com
```
Check **Google Cloud Audit Logs** for impersonation failures.

## Google Cloud documentation

- Creating and managing service accounts: https://cloud.google.com/iam/docs/creating-managing-service-accounts
- Creating custom roles: https://cloud.google.com/iam/docs/creating-custom-roles
- Service account impersonation: https://cloud.google.com/iam/docs/impersonating-service-accounts
- Google Cloud Audit Logs: https://cloud.google.com/logging/docs/audit
- Confluent: Create a Google Cloud provider integration: https://docs.confluent.io/cloud/current/connectors/provider-integration/cloud-console/gcp.html
