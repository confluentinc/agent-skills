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

> **Some of the copied values are predefined roles, not permissions.** A few
> connectors (notably **Pub/Sub**) list `roles/...`-style two-segment names
> (`pubsub.subscriber`, `pubsub.viewer`) in the portal. Those are **predefined
> role short names**, not IAM permissions — do **not** feed them to
> `gcloud iam roles create --permissions` (it fails). Grant them directly as
> roles instead (`--role=roles/pubsub.subscriber`), skipping the custom role for
> those. See `references/gcp-permission-policies.md` for which entries are roles.

```bash
# 4. Create a custom role with the copied PERMISSIONS (skip this for entries that
#    are predefined roles — grant those directly in step 5b)
gcloud iam roles create ConfluentConnectorRole \
  --project=<YOUR_PROJECT_ID> \
  --title="Confluent Connector Role" \
  --permissions=<COMMA_SEPARATED_PERMISSIONS_FROM_CONFLUENT>

# 5. Create your service account
gcloud iam service-accounts create confluent-connector-sa \
  --project=<YOUR_PROJECT_ID> --display-name="Confluent connector SA"
```

**5b. Grant the role — scoped to the target resource (recommended).** Bind the
role at the specific resource the connector touches, *not* project-wide, so the SA
can only reach what it needs:

```bash
# GCS bucket:
gsutil iam ch \
  serviceAccount:confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com:projects/<YOUR_PROJECT_ID>/roles/ConfluentConnectorRole \
  gs://<bucket-name>

# BigQuery dataset (members[].role in the dataset's IAM):
bq add-iam-policy-binding \
  --member="serviceAccount:confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
  --role="projects/<YOUR_PROJECT_ID>/roles/ConfluentConnectorRole" \
  <YOUR_PROJECT_ID>:<dataset>

# Pub/Sub subscription (predefined roles — no custom role):
gcloud pubsub subscriptions add-iam-policy-binding <subscription> \
  --member="serviceAccount:confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

# Secret Manager secret, Cloud SQL instance, etc. — bind on that resource similarly.
```

*Fallback — project-wide* (only if the resource-scoped binding isn't practical,
e.g. the connector discovers resources dynamically). This grants the role across
the **whole project**:
```bash
gcloud projects add-iam-policy-binding <YOUR_PROJECT_ID> \
  --member="serviceAccount:confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
  --role="projects/<YOUR_PROJECT_ID>/roles/ConfluentConnectorRole"
```

```bash
# 6. Let Confluent's SA impersonate YOUR SA (the trust binding).
#    Bind Token Creator on YOUR specific SA — NEVER project-wide (see warning).
gcloud iam service-accounts add-iam-policy-binding \
  confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com \
  --member="serviceAccount:<CONFLUENT_SA_EMAIL_FROM_STEP_2>" \
  --role="roles/iam.serviceAccountTokenCreator"
```

> ⚠️ **Never grant Token Creator / `iam.serviceAccounts.getAccessToken`
> project-wide.** The step-6 binding is applied with
> `gcloud iam service-accounts add-iam-policy-binding` on **one named SA** on
> purpose. Granting `roles/iam.serviceAccountTokenCreator` (or a custom role
> containing `iam.serviceAccounts.getAccessToken`) at the **project** level lets
> Confluent's SA impersonate **every** service account in your project — a real
> privilege-escalation path, not just over-broad data access. This one is a hard
> "don't," unlike the data-plane scope nudges above. Keep impersonation scoped to
> the single SA you created.

You can also do all of this via the Google Cloud Console (Create role → Create
service account → grant the role on the target resource → grant Token Creator on
the SA).

## Establish trust + validate

1. Back in Confluent, in **Google Cloud service account**, enter **your** service
   account email (the one you created in step 5).
2. **Validate — this is the authoritative check.** It verifies that *Confluent's*
   SA can impersonate *your* SA (the real trust binding), which no local command
   can confirm. Either:
   - Click **Validate** in the UI, or
   - Run the CLI: `confluent provider-integration v2 validate <cspi-...> \
     --gcp-service-account <your-sa-email> --environment env-abc123`
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

**Optional local sanity check (NON-authoritative — do not rely on it):**
```bash
gcloud auth print-access-token \
  --impersonate-service-account=confluent-connector-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com
```
> ⚠️ This tests whether **your own** identity can impersonate your SA — it does
> **not** test whether *Confluent's* SA can. Both false passes (you have Token
> Creator but Confluent doesn't) and false fails (Confluent is bound but you
> aren't) are possible. Use it only to confirm the SA exists and is impersonable in
> principle; the **Confluent Validate step above is the source of truth** for the
> trust binding.

Check **Google Cloud Audit Logs** for impersonation failures.

## Google Cloud documentation

- Creating and managing service accounts: https://cloud.google.com/iam/docs/creating-managing-service-accounts
- Creating custom roles: https://cloud.google.com/iam/docs/creating-custom-roles
- Service account impersonation: https://cloud.google.com/iam/docs/impersonating-service-accounts
- Google Cloud Audit Logs: https://cloud.google.com/logging/docs/audit
- Confluent: Create a Google Cloud provider integration: https://docs.confluent.io/cloud/current/connectors/provider-integration/cloud-console/gcp.html
