# Terraform

Two layers, applied in order, with **separate state** (separate blast radius).

```
infra/terraform/
├── foundation/   # Azure resources: RG, ADLS, Key Vault, Databricks workspace,
│                 #   ADF + Self-Hosted IR + linked services, Log Analytics, RBAC
└── platform/     # Databricks/Unity Catalog objects: storage credential,
                  #   external locations, catalog, schemas, grants, secret scope,
                  #   cluster policy. Reads foundation outputs via remote state.
```

## Apply order

```bash
# 1) foundation
cd foundation
cp dev.tfvars.example dev.tfvars     # fill in my_object_id, postgres_password
terraform init
terraform apply -var-file=dev.tfvars

# 2) platform (after the workspace exists + a UC metastore is assigned)
cd ../platform
terraform init
terraform apply
```

Destroy in reverse: `platform` then `foundation`.

## Prerequisites

- `az login` (Terraform uses Azure CLI auth).
- A **Unity Catalog metastore** in your region assigned to the workspace, and you
  as metastore admin (see `docs/RUNBOOK.md` step 4).
- The account groups referenced in `platform/variables.tf` must exist, or override
  them with groups you have.

## State

Local by default (trial). For shared/CI use, uncomment the `azurerm` backend in
`foundation/versions.tf` (and add one to `platform`) after creating a versioned,
locked backend storage account. `*.tfstate` and `*.tfvars` are gitignored.

## Multi-environment

Promote to prod by adding `prod.tfvars` + a separate state (or a Terraform
workspace) — no code fork. Kept dev-only here to stay within free-trial limits.

## Notes / caveats

- `terraform validate` passes for both layers. A full `plan`/`apply` needs your
  Azure subscription and the UC metastore prerequisite.
- The Postgres linked service is a custom service (`PostgreSqlV2`) so it can
  reference a Key Vault secret **and** connect via the Self-Hosted IR. Connector
  property names occasionally shift between ADF connector versions — if a field is
  rejected, verify in ADF Studio and adjust `type_properties_json`.
