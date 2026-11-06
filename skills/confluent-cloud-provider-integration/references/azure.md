# Azure Provider Integration (Microsoft Entra ID)

Read this file when the target CSP is **Azure**. It fills in the exact steps and
commands for the four-phase flow in `SKILL.md`.

## Contents
- [Paradigm](#paradigm)
- [Prerequisites](#prerequisites)
- [Ordered flow](#ordered-flow-the-ping-pong)
- [Confluent side (UI-driven)](#confluent-side-ui-driven)
- [Azure side: service principal + permissions](#azure-side-service-principal--permissions)
- [Establish trust + validate](#establish-trust--validate)
- [Troubleshooting](#troubleshooting)
- [Azure documentation](#azure-documentation)

## Paradigm

Confluent generates a **multi-tenant Microsoft Entra ID application** in
Confluent's Azure account. You create a **service principal** for that app *in
your own tenant* using the Azure CLI, grant it the permissions the connector
needs, and then map your **tenant ID** in Confluent to establish trust. There is
no long-term secret — trust is anchored on the app + your tenant.

> The Confluent side of the Azure flow is **UI-driven**: Confluent must
> generate the multi-tenant Entra app for you. There is no `confluent
> provider-integration create` shortcut for Azure like there is for AWS.

## Prerequisites

- Confluent Cloud user with **OrganizationAdmin** or **EnvironmentAdmin**.
- An Entra ID admin role in your tenant: **Cloud Application Administrator** or
  **Application Administrator** (required to create the service principal).
- The **Azure CLI** (`az`) installed and logged in (`az login`). The Azure
  **Portal cannot create a service principal for a multi-tenant app** — the CLI
  is mandatory for that one step. No local install allowed? **Azure Cloud Shell**
  (in the Portal) has `az` preinstalled, or have a user with the required Entra
  admin role run `az ad sp create` for you.
- Your Azure **tenant ID**, and permissions to assign Azure RBAC roles on the
  target resource.

## Ordered flow (the ping-pong)

```
[ ] 1. Confluent: Add integration → Microsoft Entra ID → name it → Continue
[ ] 2. Confluent: Create Entra ID app → COPY the application (client) ID
[ ] 3. Confluent: select Confluent resources → COPY the list of permissions needed
[ ] 4. Azure (CLI): az ad sp create --id <APPLICATION_ID>
[ ] 5. Azure: assign the copied permissions (RBAC) to that service principal on the resource
[ ] 6. Confluent: enter your tenant ID → Validate → Continue
```

If you stop after step 2–3 but before entering the tenant ID, the integration
shows as **Draft** on the Provider Integrations list — resume it later with
**Complete**.

## Confluent side (UI-driven)

1. Go to the target environment → **Integrations** → **Provider Integrations** →
   **Add integration**.
2. Select **Microsoft Entra ID**, enter a unique integration name, **Continue**.
3. Click **Create Entra ID app**. This creates the multi-tenant application in
   Confluent's Azure account. **Copy the application (client) ID** (looks like
   `a8198fb1-845g-4f54-973b-86fg742b5a38`) — you need it for `az ad sp create`.
4. Under **Confluent resources**, choose the resource(s) this integration will
   serve. In **List of permissions needed**, **copy the permissions** — you'll
   assign them to the service principal.

## Azure side: service principal + permissions

Create the service principal for the multi-tenant app **in your tenant**:
```bash
az ad sp create --id <APPLICATION_ID>
```
`<APPLICATION_ID>` is the client ID copied in the previous step. This must be run
by a user with Cloud Application Administrator or Application Administrator.

> For the **exact per-resource role(s)** the portal prescribes (Blob/ADLS Gen2,
> Event Hubs, Cosmos DB, Key Vault, Kusto, Redis, the SQL/MySQL/Postgres
> "Microsoft Entra admin" cases, Tableflow, etc.), see
> `references/azure-permission-policies.md` — a verbatim catalog captured from the
> wizard. Assign exactly those roles.

Then assign the copied permissions to that service principal on the target
resource. For most managed connectors this is an Azure **RBAC role assignment**
on the specific resource scope (for example, a Storage Blob Data role on a
storage account/container). Example shape:
```bash
az role assignment create \
  --assignee <APPLICATION_ID> \
  --role "<Built-in role from the copied permissions>" \
  --scope "/subscriptions/<SUB_ID>/resourceGroups/<RG>/providers/<RESOURCE_PATH>"
```
Use exactly the role(s)/permissions the Confluent screen listed; prefer built-in
roles scoped to the narrowest resource.

## Establish trust + validate

1. Back in Confluent, in **Microsoft tenant ID account**, enter your Azure
   **tenant ID**.
2. Click **Validate** — this checks that the service principal exists in your
   tenant for Confluent's multi-tenant app.
   - Success → "the Microsoft tenant ID is valid."
   - Failure → the message states why; verify the tenant ID and that
     `az ad sp create` actually completed, then retry.
3. Click **Continue**. The integration shows status **Created**.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `az ad sp create` fails with authorization error | Missing Entra admin role | Have a Cloud Application Administrator / Application Administrator run it |
| Validation fails: SP not found | Service principal not created in *your* tenant, or wrong app ID | Re-run `az ad sp create --id <APPLICATION_ID>` with the exact copied client ID |
| Validation succeeds but connector can't access resource | RBAC role not assigned or wrong scope | Assign the copied permissions to the SP on the correct resource scope |
| Integration stuck in **Draft** | Left before entering tenant ID | Click **Complete** next to it and finish step 6 |
| Access works then stops | Conditional access / role assignment removed | Check Entra sign-in logs and the resource's role assignments |

Check **Entra ID sign-in logs** and the Azure **Activity Log** for the failing
principal to see the exact denial.

## Azure documentation

- Register an app / create a service principal (portal reference): https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal
- Create an enterprise app from a multitenant app: https://learn.microsoft.com/entra/identity/enterprise-apps/create-service-principal-cross-tenant
- Assign Azure roles (RBAC) in the portal: https://learn.microsoft.com/azure/role-based-access-control/role-assignments-portal
- Azure built-in roles: https://learn.microsoft.com/azure/role-based-access-control/built-in-roles
- Entra sign-in & audit logs: https://learn.microsoft.com/entra/identity/monitoring-health/concept-sign-ins
- Confluent: Create an Azure provider integration: https://docs.confluent.io/cloud/current/connectors/provider-integration/cloud-console/azure.html
