# Azure Data Factory artifacts

ADF is the **batch orchestrator**. The factory, Self-Hosted IR, and linked
services (which carry the security model — managed identity + Key Vault) are
provisioned in **Terraform** (`infra/terraform/foundation/data_factory.tf`). The
data-movement artifacts here are deployed on top.

```
adf/
├── dataset/    ds_pg_orders.json, ds_adls_landing_parquet.json
├── pipeline/   pl_ecommerce_batch.json   (Copy JDBC + land, then trigger Databricks)
└── trigger/    tr_daily_0300.json         (daily tumbling window, passes bounds)
```

## The pipeline

`pl_ecommerce_batch` runs, per day:

1. **Copy_Orders_ToLanding** — Postgres (via SHIR) → `landing/orders/<date>` as
   Parquet, incremental on the tumbling-window bounds.
2. **Ingest_FX_API** — Databricks runs `rest_api_fx.py` (parallel).
3. **Bronze_Orders** / **Bronze_CSV_Marketing** — Databricks bronze loaders.
4. **Silver_Orders** → **Gold_Dim_Product** → **Gold_Fact_Orders** — the medallion
   transforms, chained by success dependencies.

Incremental strategy is **tumbling-window bounds** (`windowStart`/`windowEnd` from
the trigger) — no watermark table required, and every window is independently
re-runnable.

## Deploying these artifacts

**Preferred — ADF Git integration:** ADF Studio → Manage → Git configuration →
point at this repo, root folder `adf/` → author in a feature branch → **Publish**
to the factory. This gives ADF-native CI/CD with the `adf_publish` branch.

**Or — CLI import:**

```bash
az datafactory pipeline create   --resource-group <rg> --factory-name <adf> \
  --name pl_ecommerce_batch --pipeline @adf/pipeline/pl_ecommerce_batch.json
# repeat for datasets and the trigger
```

## Notes

- `pythonFile` paths (`/Workspace/Repos/ecommerce/...`) must match where the
  Databricks Asset Bundle deploys `src/`. Adjust if your target root differs.
- These JSONs follow the ADF resource schema but are not validated against a live
  factory here — confirm connector fields in ADF Studio on first import.
