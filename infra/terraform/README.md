# Terraform — modular, per-environment

Reusable **modules** called by thin, per-environment **roots**. Each environment
has its own state and its own config, so dev/UAT/prod are fully isolated and can
differ in sizing.

```
infra/terraform/
├── modules/
│   ├── foundation/    # RG, ADLS, Key Vault, Databricks workspace, ADF + SHIR,
│   │                  #   monitoring, alerts, RBAC, UC access connector
│   └── platform/      # UC storage credential, external locations, catalog,
│                      #   schemas, grants, KV-backed secret scope, cluster policy
└── environments/
    ├── dev/
    │   ├── foundation/   # root: providers + backend(dev) + module "foundation"
    │   └── platform/     # root: providers + backend(dev) + remote_state -> module "platform"
    ├── uat/  { foundation, platform }
    └── prod/ { foundation, platform }
```

## What differs per environment

Roots pass env-specific inputs to the same modules:

| Input | dev | uat | prod |
|---|---|---|---|
| `storage_replication_type` | LRS | LRS | **GRS** |
| `log_retention_days` | 30 | 60 | 90 |
| catalog | `ecommerce_dev` | `ecommerce_uat` | `ecommerce_prod` |
| state key | `dev/*` | `uat/*` | `prod/*` |

Add prod-only hardening (private endpoints, storage firewall, `prevent_destroy`)
by extending the module with flags toggled on in the prod root.

## Apply order (per environment)

```bash
cd environments/dev/foundation
cp terraform.tfvars.example terraform.tfvars   # fill my_object_id, postgres_password
terraform init
terraform apply

cd ../platform      # reads ../foundation state, builds Unity Catalog
terraform init
terraform apply
```

Destroy in reverse (`platform` then `foundation`). Swap `dev` → `uat`/`prod` for
other environments. CI does this automatically (see `.github/workflows/terraform.yml`).

## State

Local per-folder state by default (each root keeps its own `terraform.tfstate`).
For shared/CI use, enable the **azurerm backend** in each root's `versions.tf`
with a per-env key (`<env>/foundation.tfstate`, `<env>/platform.tfstate`) after
creating a versioned, locked backend storage account. `*.tfstate` and
`*.tfvars` are gitignored.

## Notes

- `terraform validate` passes for the module roots. A full `plan`/`apply` needs
  your Azure subscription and a UC metastore assigned to the workspace (see
  `docs/RUNBOOK.md`).
- The Postgres linked service is a custom ADF service so it can reference a Key
  Vault secret *and* connect via the Self-Hosted IR; its `type` may not round-trip
  in state (a harmless perpetual re-create on plan).
