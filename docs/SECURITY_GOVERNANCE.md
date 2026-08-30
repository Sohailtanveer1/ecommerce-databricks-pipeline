# Security &amp; Governance

The controls in this project and where each is implemented.

## Identity &amp; secrets

| Concern | Control | Where |
|---|---|---|
| No credentials in code/state | **Azure Key Vault** is the only secret store | `infra/terraform/modules/foundation/keyvault.tf` |
| ADF → secrets | Managed identity + `ls_keyvault` linked service; secrets referenced, never inlined | `data_factory.tf` |
| Databricks → secrets | Key Vault-backed **secret scope** | `platform/secret_scope.tf` |
| Databricks → ADLS | **Managed identity** (Access Connector) + UC storage credential; no keys/SAS | `storage.tf`, `platform/unity_catalog.tf` |
| ADF → ADLS | ADF **managed identity** + RBAC (Storage Blob Data Contributor) | `storage.tf` |
| Source DB | `ecom_reader` role, **SELECT-only** | `docker/seed/02_data.sql` |
| Key Vault authz | **RBAC** (not legacy access policies); you get Secrets Officer, services get Secrets User | `keyvault.tf` |

## Data protection (Unity Catalog)

- **Schema-level least privilege.** Analysts: `USE_CATALOG` + read on **Gold only**.
  Bronze/Silver have no analyst grant → default-deny. Engineers operate all layers.
  (`infra/terraform/modules/platform/grants.tf`)
- **Column masking.** `email`, `phone` masked unless the caller is in
  `pii-authorized`. (`sql/governance/unity_catalog_masks.sql`)
- **Row-level security.** `fact_orders` filtered by region unless `global-access`.
- **Lineage &amp; audit.** Automatic in Unity Catalog (`system.access.audit`,
  table/column lineage) — no extra setup.

## Network (trial vs prod)

- **Trial:** storage reachable over public endpoint with TLS 1.2 + RBAC; keys off
  the data path. Adequate for a demo, cheap.
- **Prod hardening (documented, not applied):** Private Endpoints for ADLS/Key
  Vault/Databricks, VNet-injected workspace, storage firewall default-deny,
  no-public-blob, purge protection on Key Vault, ZRS/GRS replication.

## Observability &amp; audit

- Diagnostic settings ship ADF logs/metrics and **Key Vault audit events** to Log
  Analytics (`monitoring.tf`).
- Data-quality outcomes are logged per run; the `*_quarantine` tables are a
  queryable data-health signal.

## Secrets hygiene checklist

- `*.tfvars` and `*.tfstate` are gitignored; only `*.tfvars.example` is committed.
- No secret is printed in logs; `shir_primary_key` is a `sensitive` output.
- CI uses **OIDC** federated identity — no long-lived cloud secret in GitHub.
