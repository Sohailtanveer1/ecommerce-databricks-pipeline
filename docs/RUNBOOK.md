# Deployment Runbook (free-trial, dev)

End-to-end steps to stand up the whole project on your Azure + Databricks trial.
Budget ~1–2 hours the first time. Everything tears down cleanly at the end.

> **Cost guard:** the only meaningful cost is Databricks DBUs while a cluster
> runs. The cluster policy forces spot + 20-min auto-terminate, and all jobs use
> job clusters (spin up → run → die). Idle cost ≈ storage cents/day.

## 0. Prerequisites (install once)

- Azure subscription (trial) and **Azure CLI** (`az`)
- **Terraform** ≥ 1.6
- **Docker Desktop**
- **Databricks CLI** (`databricks`) — for the Asset Bundle
- This repo cloned locally

```bash
az login
az account show          # confirm the right subscription
az account set --subscription "<your-sub-id>"
```

## 1. Start the local Postgres source (Source #2)

```bash
docker compose -f docker/docker-compose.yml up -d
docker exec -it ecommerce-postgres psql -U postgres -d ecommerce -c "\dt"   # verify tables
```

Set a real password for `ecom_reader` (used by ADF) — edit `docker/seed/02_data.sql`
before first `up`, or `ALTER ROLE ecom_reader PASSWORD '...';` after. Use the same
value for `postgres_password` in step 2.

## 2. Deploy the Azure foundation (Terraform)

```bash
cd infra/terraform/foundation
cp dev.tfvars.example dev.tfvars
# edit dev.tfvars: my_object_id, postgres_password, location
#   my_object_id: az ad signed-in-user show --query id -o tsv
terraform init
terraform apply -var-file=dev.tfvars
```

Creates: resource group, ADLS Gen2 + containers, Key Vault (+ secrets), Databricks
workspace, ADF + Self-Hosted IR + linked services, Log Analytics, RBAC, UC access
connector.

## 3. Register the Self-Hosted Integration Runtime (reach local Postgres)

The SHIR runs on **your machine** and is how ADF (in the cloud) reads your local
Docker DB — a cloud IR cannot see `localhost`.

1. Get the key: `terraform output -raw shir_primary_key`
2. Download **Microsoft Integration Runtime** (MSI) from the ADF Studio
   (Manage → Integration runtimes → `shir-local-docker`), install it.
3. Paste the key to register. Wait for status **Running**.
4. The SHIR connects to Postgres at `localhost:5432` (the published Docker port).

## 4. Unity Catalog prerequisites (one-time, account level)

Unity Catalog needs a **metastore** in your region assigned to the workspace.
Azure usually auto-creates one for new workspaces; if not:

- In the **Databricks Account Console** → Data → create a metastore in your
  region → assign it to `dbw-ecomlakedev`.
- Make yourself **metastore admin**.
- Create (or reuse) the AAD/account groups referenced by grants:
  `data-engineers`, `data-analysts`, `pii-authorized` — or change the defaults in
  `infra/terraform/platform/variables.tf` to groups you already have. Grants to a
  non-existent principal will error.

## 5. Deploy the platform layer (Unity Catalog + governance)

```bash
cd ../platform
terraform init
terraform apply
```

Creates: storage credential (managed identity), external locations per container,
catalog `ecommerce_dev`, schemas, least-privilege grants, Key Vault-backed secret
scope, cost-capped cluster policy.

## 6. Create the tables + governance

Open a Databricks notebook (or SQL editor) on a small cluster and run, in order:

1. `sql/ddl_all_layers.sql` (prefix a `USE CATALOG ecommerce_dev;`)
2. `sql/governance/unity_catalog_masks.sql` (column masks + row filters)

## 7. Deploy the pipeline code (Databricks Asset Bundle)

```bash
cd ../../..            # repo root
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

This syncs `src/` into the workspace so ADF's `pythonFile` paths resolve. (Confirm
the `/Workspace/...` paths in `adf/pipeline/pl_ecommerce_batch.json` match your
bundle target root; adjust if needed.)

## 8. Deploy ADF pipeline artifacts

Linked services + SHIR already exist (Terraform). Add the pipeline/datasets/trigger:

- **Preferred:** connect ADF Studio → Manage → Git configuration to this repo,
  root folder `adf/`, then **Publish**.
- **Or** import `adf/pipeline`, `adf/dataset`, `adf/trigger` via
  `az datafactory pipeline create ...` (see `adf/README.md`).

## 9. Land the CSV source (Source #1)

Upload the sample CSV to the landing container:

```bash
az storage fs file upload \
  --account-name <adls_account_name> \
  --file-system landing \
  --source data/sample/marketing_ad_spend_2026-08.csv \
  --path marketing/marketing_ad_spend_2026-08.csv --auth-mode login
```

## 10. Run + validate

- Trigger `pl_ecommerce_batch` in ADF (Debug or the daily trigger).
- Watch: ADF monitor → activities green; Databricks job runs; then query:

```sql
SELECT COUNT(*) FROM ecommerce_dev.bronze.orders;
SELECT * FROM ecommerce_dev.silver.orders LIMIT 20;
SELECT * FROM ecommerce_dev.silver.orders_quarantine;   -- DQ rejects, if any
SELECT * FROM ecommerce_dev.gold.mv_revenue_usd_by_category_month;
```

## 11. Teardown (stop all cost)

```bash
cd infra/terraform/platform  && terraform destroy
cd ../foundation             && terraform destroy -var-file=dev.tfvars
docker compose -f ../../../docker/docker-compose.yml down -v
```

---

### Known wire-up items (do these as you deploy)

These are intentional seams — the infra/code is written, but a few values are
environment-specific and get set on first deploy:

1. **`REPLACE_ME` ADLS account name** in `src/bronze/bronze_csv_autoloader.py` and
   `src/ingestion/rest_api_fx.py` — set the `CSV_*` / `FX_*` env vars (or job
   params) to your `terraform output adls_account_name`. Don't hardcode.
2. **Bronze orders format.** ADF lands the JDBC copy as **Parquet** in
   `landing/orders/<date>`. `src/bronze/bronze_loader.py` currently reads JSON from
   the legacy path — point it at the landing Parquet (or switch the ADF sink to
   JSON) so the two agree. One-line change; left explicit so you make the call.
3. **ADF `pythonFile` paths** must match your bundle deploy root (`adf/pipeline/pl_ecommerce_batch.json`).
4. **UC three-level names.** DDL uses two-level names (`bronze.orders`); run it with
   `USE CATALOG ecommerce_dev;` first, or fully-qualify.

### Quick troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| ADF Postgres connection fails | SHIR not Running, or wrong password in Key Vault; test the linked service in ADF Studio. |
| `terraform apply` platform fails on grants | AAD groups don't exist yet — create them or edit `platform/variables.tf`. |
| UC storage credential / external location error | Metastore not assigned to the workspace (step 4). |
| Databricks job "file not found" | Bundle not deployed, or `pythonFile` path in the ADF pipeline doesn't match the deploy root. |
| Auto Loader finds no CSV | File not under `landing/marketing/`, or checkpoint already consumed it. |
