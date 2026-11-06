---
name: confluent-cloud-provider-integration
description: >-
  Guide a user through setting up and validating a Confluent Cloud provider
  integration, so fully-managed connectors and Tableflow reach cloud provider
  (CSP) resources via a short-lived assumed identity, not long-term access keys.
  Covers AWS (IAM AssumeRole), Azure (Entra ID multi-tenant app plus service
  principal), and Google Cloud (service account impersonation), with state
  tracking across the Confluent and CSP UIs/CLIs. Use when a user says
  "set up/create a provider integration", "provider integration for an S3 /
  DynamoDB / GCS / BigQuery connector", "connect a connector without access
  keys", "IAM role for a Confluent connector", "Entra ID service principal for
  Confluent", or asks about provider.integration.id or authentication.method. Do
  NOT trigger for configuring a connector that already has a working integration,
  for Confluent RBAC or API-key management, for CDC or Tableflow pipelines
  end-to-end, or for authoring an agent skill; this skill stops once the
  integration is created and validated.
metadata:
  author: confluent
  version: "1.0.0"
  last_updated: "2026-11-05"
compatibility: >-
  Requires the Confluent CLI (`confluent`) logged in to Confluent Cloud, and a
  Confluent Cloud user with the OrganizationAdmin or EnvironmentAdmin role.
  Cloud-side steps use the AWS Console or AWS CLI, Azure Portal or Azure CLI
  (`az`), or Google Cloud Console or gcloud CLI depending on the target CSP.
---

# Confluent Cloud Provider Integration

Create and validate a Confluent Cloud **provider integration** — the resource
that lets fully-managed connectors and Tableflow "act-as" a trusted identity in
your cloud service provider (CSP) account, so they use short-lived assumed
credentials instead of long-term access keys.

This skill is a **guided, stateful walkthrough**. The setup deliberately
ping-pongs between two UIs (Confluent Cloud and your CSP), and the correct
ordering matters — the trust relationship can only be finalized *after*
Confluent hands you an identity to trust. Your job is to keep the user oriented
at every step and never let them lose their place.

## When this skill applies

| The user wants to… | This skill? |
|---|---|
| Create a provider integration for a connector/Tableflow that needs CSP access | ✅ Yes |
| Validate / troubleshoot an existing provider integration's trust | ✅ Yes |
| Configure a connector that *already has* a working integration | ❌ No — connector setup |
| Manage Confluent RBAC or Cloud API keys | ❌ No |
| Build a full CDC or Tableflow pipeline | ❌ No — see `confluent-cloud-cdc-tableflow` |

Provider integrations are supported for three CSPs, each with a different
identity paradigm:

| CSP | Identity paradigm | Reference file |
|---|---|---|
| **AWS** | IAM role + `sts:AssumeRole` with an External ID | `references/aws.md` |
| **Azure** | Microsoft Entra ID multi-tenant app + service principal + tenant trust | `references/azure.md` |
| **Google Cloud** | Service account impersonation (Token Creator) | `references/gcp.md` |

> ⚠️ **Do NOT read all reference files.** Read only the one matching the user's
> CSP once you have detected it (Step 1). `references/common.md` is read only for
> management/list/describe/delete or the connector hand-off.

## Golden rules

1. **Plan, then get explicit confirmation, before any resource-modifying step.**
   Never create/modify a Confluent integration or a CSP role/policy/service
   principal without showing the exact command or UI action first and
   waiting for the user to say go. See "Plan-before-execute" below.
2. **Track state out loud.** After every step, restate the running checklist and
   what comes next (see "State tracking"). This is the core value of the skill.
3. **Respect the ordering (it differs per CSP).** Apply the trust/impersonation
   binding only once you hold the values it references, and confirm/validate it
   last. The exact step order is **not** the same across CSPs — follow the
   selected reference file's ordered checklist; do not impose one global sequence
   (e.g. AWS needs the IAM role to exist *before* the Confluent create call,
   whereas Azure/GCP have Confluent generate the identity first). Applying the
   binding before you hold the values it needs is the #1 cause of failures.
4. **Never read, display, or log credential files.** No `cat`/`Read`/`head`/
   `grep` on `.env` or `credentials.yaml`. Reference variables by name only.
   Use synthetic placeholders in all examples.
5. **Ground explanations in the docs, not just recall.** When the user asks a
   conceptual or "what is…" question (e.g. "what's a service principal?"), read
   the relevant reference file and anchor the answer on the official CSP /
   Confluent documentation it links — and cite that source — rather than
   answering purely from model memory. Never invent commands, role names, or doc
   URLs; if the reference file doesn't cover it, say so and point to the docs.
6. **Least privilege — scope by default, refuse only true admin grants.** Scope
   every statement to the specific target resource ARN(s) and only the actions the
   connector needs, and *recommend* that scoped form. Two cases where `Resource: *`
   is legitimate, not a violation: (a) actions the provider won't let you
   resource-scope — account-level discovery such as `dynamodb:ListTables`,
   `dynamodb:ListStreams`, `s3:ListAllMyBuckets`, `tag:GetResources` — which
   *require* `*`; (b) a user who knowingly declines to scope — recommend the
   scoped policy, note that a wildcard resource on a **data-reading** action
   (`GetSecretValue`, `GetObject`, `Scan`, …) grants account-wide access to that
   data, but **proceed** if they still want to. Enforcing least privilege is not
   this skill's job — nudging is. **Always decline `Action: *` / administrator-level
   access** — no connector needs it; offer the scoped policy instead even if the
   user insists.

## Step 1 — Detect the CSP and gather requirements

Determine the target CSP first (it selects the reference file and the entire
paradigm). If the user hasn't said, ask directly: "Which cloud is the resource
in — AWS, Azure, or Google Cloud?"

Then gather (ask only for what's missing; confirm back what you infer):

| Requirement | Why it's needed | Example |
|---|---|---|
| Target CSP | Selects paradigm + reference file | `AWS` |
| Confluent environment ID | Integration is scoped to one environment | `env-abc123` |
| Target resource + connector | Determines the CSP permission policy | S3 bucket for an S3 Sink |
| Confluent role | Must be OrganizationAdmin/EnvironmentAdmin to create | — |
| CSP permissions | Must be able to create roles/policies/service principals | — |
| Interface preference | CLI vs UI for each side (see Step 2) | "CLI for Confluent, Console for AWS" |

Confirm the user holds the required roles before doing anything else — see
`references/common.md` (Roles & prerequisites). If they don't, stop and tell
them what to request.

**Then read the CSP reference file** (`references/aws.md`, `references/azure.md`,
or `references/gcp.md`) and follow its ordered steps. The sections below apply
to all three.

## Step 2 — Choose the interaction method (hybrid)

This skill is **hybrid**: prefer running commands the user approves, and fall
back to precise UI click-through where no CLI exists.

**Confluent side** (creating/mapping the integration), in order of preference:
1. **Confluent MCP tools** if available — check for `mcp__confluent__*` provider
   integration tools; use them if present.
2. **Confluent CLI** — `confluent provider-integration create|list|describe|delete`.
   This is the reliable default. Verify login with `confluent environment list`.
3. **REST API** — `POST https://api.confluent.cloud/pim/v1/integrations` (see
   the reference file) when scripting without the CLI.
4. **Confluent Cloud UI** — Integrations → Provider Integrations →
   Add integration. Use when the user prefers clicking, or for Azure/GCP where the
   UI drives the multi-tenant app / service-account creation.

> **Note:** The Azure and Google Cloud flows are **UI-driven on the
> Confluent side** — Confluent must generate the multi-tenant Entra app (Azure)
> or the Confluent-owned service account (GCP) for you. The CLI create command
> is primarily an AWS convenience. The reference files call this out per CSP.

**CSP side** (creating the identity + trust): AWS Console or AWS CLI; Azure
Portal or `az` CLI (the service principal for a multi-tenant app **must** be
created with `az ad sp create --id <app-id>` — the Portal cannot); Google Cloud
Console or `gcloud`.

Before running any command, confirm the relevant CLI is installed and
authenticated. If it is not and the user can't install it, switch that side to
the UI walkthrough.

## Plan-before-execute (mandatory)

Before the first resource-modifying action, present a numbered plan and wait for
explicit approval. Keep it concise but exact.

> ⚠️ **Take the step *order* from the selected CSP reference file, not from this
> template.** The sequence is **not** the same across CSPs — AWS needs the IAM
> role to exist *before* the `confluent provider-integration create` call (create
> takes `--customer-role-arn`), whereas Azure/GCP have Confluent generate the
> identity first and you grant trust afterward. The skeleton below is
> *representative*; fill the real ordered checklist from `references/<csp>.md`.

```
Plan: <CSP> provider integration for <resource/connector> in <env-id>

Interaction: Confluent = <CLI/UI/MCP>, <CSP> = <CLI/Console>

Steps (in the order given by references/<csp>.md — representative skeleton):
  - <CSP> create permission policy + identity (role / SP / service account)
  - <Confluent> create/map the integration (AWS: pass the existing role ARN;
    Azure/GCP: Confluent generates the app/SA for you)
  - exchange values: obtain <ARN+ExternalID / app-id+tenant / SA email>
  - <CSP> finalize trust/impersonation using those values
  - <Confluent/CSP> validate (per-CSP — see the reference file)

Resources created:
  - Confluent: provider integration "<name>"
  - <CSP>: <policy>, <role/SP/SA>, <trust binding>

Proceed? [yes/no]
```

Never execute a step that creates or modifies a resource without a yes. Read-only
discovery (`list`/`describe`) may run without a plan but still narrate it.

## State tracking (the core value)

The setup crosses UIs and cannot always be completed in one sitting (an
integration left mid-way shows as **Draft** and can be resumed via **Complete**
in the UI). The user may pause for minutes or days between steps — especially
while waiting on CSP permissions or IAM propagation — so **persist progress to a
local file** that survives across sessions. Keep the user oriented:

- **Save progress to a visible local file** in the working directory:
  `provider-integration-progress.md`. Create it when you present the first plan,
  and **rewrite it after every step** so it always reflects the latest state.
- **Restate the checklist after every step** (from the file), marking
  `[x]` done, `[→]` current, `[ ]` pending.
- **Tell the user, periodically, that progress is saved and they can stop** — at
  minimum after each step and whenever you hand off to the other UI. For example:
  "Progress saved to `provider-integration-progress.md`. You can pause here and
  later say *resume the provider integration* to pick up at step N."
- On resume, **read `provider-integration-progress.md` first** to recover where
  you left off, then continue from the `[→]` step.
- When you hand the user off to the other UI, tell them exactly what value
  to copy, where to paste it, and what to bring back.
- Before finalizing trust, explicitly confirm you have both sides' values in
  hand. **Confluent-generated** (returned when you create/map the integration):
  AWS — Confluent's role ARN + External ID; Azure — the multi-tenant
  application/client ID; GCP — Confluent's service-account email. **Yours** (you
  already hold these, Confluent does not issue them): AWS — your target role ARN;
  Azure — your Entra **tenant ID**; GCP — the service account to be impersonated.
  Don't mislabel your tenant ID as a Confluent-generated value.

> ⚠️ **Never write secrets to the progress file** (Golden rule 4). Record only
> non-secret state: step status, environment ID, integration ID (`cspi-...`),
> resource/role names, and the chosen interaction method. Do **not** write access
> keys, client secrets, or service-account JSON keys. Values like the External ID
> can be re-fetched with `confluent provider-integration describe` — note where to
> get them, don't persist them. Suggest the user add the file to their `.gitignore`.

The file is both the running checklist and the resume point. Example contents:

```
# Provider integration — AWS (env-abc123)
Resource: S3 Sink → s3://example-bucket
Interaction: Confluent = CLI, AWS = Console
Integration ID: cspi-xxxxx      # filled in after step 3 (non-secret)

  [x] 1. Create IAM permission policy (AWS)
  [x] 2. Create IAM role w/ placeholder deny trust (AWS)
  [→] 3. Map role in Confluent → get Confluent ARN + External ID
  [ ] 4. Finalize trust policy in AWS (Allow AssumeRole + ExternalId + TagSession)
  [ ] 5. Validate
```

## Universal setup flow

Every CSP moves through the same four *phases*, but **the phase order differs per
CSP** — the reference file gives the authoritative ordered checklist plus the
exact commands, JSON, and UI clicks. The phases:

1. **Create the CSP permission policy + identity** — the role (AWS), service
   principal (Azure), or service account + custom role (GCP), scoped to the
   target resource with least privilege.
2. **Create/map the integration in Confluent** — **AWS:** the IAM role must
   already exist, because `confluent provider-integration create` takes
   `--customer-role-arn`; the create call returns Confluent's External ID.
   **Azure/GCP:** Confluent *generates* the identity for you here (Entra
   multi-tenant app / Confluent-owned service account) via the UI.
3. **Exchange the trust values** — collect what the other side must trust: AWS —
   Confluent's role ARN + External ID; Azure — the generated app/client ID (plus
   your tenant ID); GCP — Confluent's service-account email.
4. **Finalize trust + validate** — apply the trust/impersonation binding on the
   CSP using the phase-3 values, then validate. **Validation is not the same on
   all three (see note below):**
   - AWS: **no live self-validation is possible.** `confluent
     provider-integration describe <cspi-...>` and the IAM **Trust relationships**
     tab confirm the *configuration*, not that an AssumeRole actually succeeds.
     Report status as **"trust configured; live validation pending"** until a real
     workload assumes the role (first connector/Tableflow run, or a CloudTrail
     `AssumeRole` event). Do not claim the trust is proven from `describe` alone.
   - Azure: click **Validate** after entering your tenant ID. Note this confirms
     the **service principal exists in your tenant** — it does **not** verify the
     data-plane RBAC role assignments; those are proven only when a connector runs.
   - GCP: click **Validate** after granting Token Creator (retry after a few
     minutes — IAM propagation lag is normal). This one does exercise the
     impersonation binding.

> **Note — validation differs by CSP (by design).** Azure and GCP expose an
> *optional but recommended* **Validate** button in the Confluent Cloud UI — and a
> matching CLI command, `confluent provider-integration v2 validate <cspi-...>`
> (with `--azure-tenant-id` for Azure or `--gcp-service-account` for GCP) — that
> checks the trust before you finish. **AWS has neither a Validate button nor a
> `v2 validate` path today** — `v2 validate` supports only Azure and GCP. AWS trust
> is proven implicitly when a connector first assumes the role, so confirm the
> *configuration* with `confluent provider-integration describe` plus the IAM
> **Trust relationships** tab, and report live validation as pending until a
> workload runs. A Validate option for AWS is on the roadmap; when one appears, use
> it the same way as Azure/GCP. **Also:** Azure's Validate confirms the service
> principal exists in your tenant, not that the data-plane RBAC assignments are
> correct; GCP's Validate does exercise the impersonation binding.
>
> **Who clicks Validate:** by default, **ask the user** to click **Validate** in
> the UI (the agent should not assume it can reach the browser). *Optionally*, if
> the agent has computer-use / browser-automation capability **and the user
> explicitly consents**, it may perform the Validate click itself. Even then:
> operate only on the Confluent Cloud **Validate** action, never type credentials,
> secrets, or keys into any field, and report the result back to the user.

## Troubleshooting

On any error, do **not** start with a generic web search. Instead:
1. Read the **Troubleshooting** section of the active CSP reference file — it
   maps common symptoms (AssumeRole AccessDenied, External ID mismatch, Entra
   admin-role errors, GCP impersonation propagation lag) to fixes.
2. Follow the **curated CSP documentation links** in that file (they point at
   the exact IAM/Entra/IAM-impersonation pages). Anchor on the official CSP docs
   first — they are authoritative for trust-policy and permission details.
3. Only then search, and keep it **targeted**: query the **specific error string**
   (not a generic description). Community forums — Stack Overflow, the AWS
   re:Post / Microsoft Q&A / Google Cloud community, the Confluent Community
   forum/Slack — are often the fastest source for a concrete error, so include
   them. Avoid broad, unscoped searches.

Cross-provider management issues (integration "in use" on delete, describe fails
after CSP-side changes, update = delete-and-recreate) are in
`references/common.md`.

## After the integration is validated (hand-off)

Scope ends here. Tell the user how to consume the integration and point them on:

- The integration ID (`cspi-...`) is what connectors reference via
  `provider.integration.id`, with `authentication.method` set to the
  IAM-role/assumed-identity mode for that connector.
- For the exact connector config keys and Tableflow BYOB usage, see
  `references/common.md` (Connector hand-off) and the connector's own docs.

## Common mistakes

| Thought | Reality |
|---|---|
| "I'll just paste my AWS access key / SA JSON key into the connector." | Provider integrations exist to *eliminate* long-term keys — the connector assumes a role / impersonates a service account. Guide them to the integration. |
| "Add the Confluent trust policy first, then map the role." | Backwards. You must map the role in Confluent **first** to obtain the Confluent role ARN + External ID, then finalize the AWS trust policy with those values. |
| "The AWS role's trust policy can start empty." | Start it with the explicit placeholder **deny** trust policy, then replace it after mapping — don't leave it open or omit it. |
| "Create the Azure service principal in the Portal." | A multi-tenant app's SP must be created with `az ad sp create --id <app-id>`; the Portal can't do it. Requires Cloud/Application Administrator. |
| "GCP validation failed, so the setup is wrong." | GCP needs time to propagate the Token Creator binding. Retry validation after a few minutes before assuming misconfiguration. |
| "Deleting the integration cleans up my cloud role." | It does not. The IAM role / service principal / service account remains in your CSP; remove it separately. And delete fails while any workload still uses the integration. |

## Quality gates

Before telling the user setup is complete, confirm:
- [ ] The integration exists and `describe` returns it with the expected CSP.
      (`describe` proves *configuration*, not a working AssumeRole — see below.)
- [ ] The CSP trust/impersonation binding uses the Confluent-generated values.
- [ ] Validation state is reported **honestly per CSP**:
      - Azure/GCP: **Validate** (UI button or `v2 validate`) returned success.
      - AWS: there is no live validation — state **"trust configured; live
        validation pending until a workload assumes the role"**, and don't claim
        it's proven from `describe` alone.
- [ ] The user knows the `cspi-...` ID and how to reference it from a connector.
- [ ] No credential file was read; all examples used synthetic placeholders.
- [ ] Progress was tracked in `provider-integration-progress.md`, and it holds
      no secrets (only step status + non-secret IDs/names).

## References

- `references/aws.md` — AWS IAM AssumeRole flow, policies, trust JSON, CLI/API, troubleshooting, AWS docs.
- `references/azure.md` — Entra ID multi-tenant app + service principal + tenant trust, `az` CLI, troubleshooting, Azure docs.
- `references/gcp.md` — service account impersonation + Token Creator, `gcloud`, troubleshooting, GCP docs.
- `references/common.md` — roles/prerequisites, quota, list/describe/delete, update=recreate, connector hand-off.
- Provider Integrations overview: https://docs.confluent.io/cloud/current/connectors/provider-integration/index.html
- Provider Integration CLI reference: https://docs.confluent.io/confluent-cli/current/command-reference/provider-integration/index.html
- Provider Integration API reference: https://docs.confluent.io/cloud/current/ccloud/integrations-pim-v-1/
