# E-Commerce Lakehouse Pipeline — Databricks / Delta Lake / Azure or AWS

An end-to-end Medallion Architecture data pipeline for a small e-commerce company,
built on Databricks and Delta Lake. Designed to be right-sized for a small
company's scale and budget — not over-engineered for enterprise volume.

## Architecture Overview

```
Sources                Ingestion              Bronze          Silver           Gold                    Serving
--------                ---------              ------          ------           ----                    -------
Transactional DB   →   Scheduled batch    →   bronze.orders  → silver.orders  → gold.fact_orders    →  Materialized
(orders, customers)    (incremental pull)                                        gold.dim_product        Views
                                                                                   (SCD Type 2)
Clickstream events →   Auto Loader        →   bronze.events  → silver.events  → gold.fact_clickstream →  Power BI /
(unpredictable                                                                                            Databricks
 arrival)                                                                                                 SQL Warehouse

Payment API        →   Scheduled batch    →   bronze.payments → silver.payments
Marketing Ads API  →   Scheduled batch    →   bronze.ad_spend → silver.ad_spend → gold.fact_marketing_spend
```

## Design Decisions (and why)

| Decision | Reasoning |
|---|---|
| Batch extract for transactional DB, not CDC | CDC (Debezium/Fivetran) adds real operational and cost overhead. Batch is simpler and sufficient unless near-real-time order visibility is a genuine business requirement. |
| Auto Loader for clickstream only | Clickstream arrives continuously/unpredictably — Auto Loader's incremental file detection is worth its overhead here, unlike the predictable DB extract. |
| Job clusters (spin-up/terminate), not always-on | Small data volume doesn't justify standing compute cost. |
| Databricks Workflows, not a separate Airflow deployment | Avoids the operational overhead of maintaining a second orchestration system for this scale. |
| SCD Type 2 on dim_product / dim_customer | Historical reporting (revenue by category) must stay point-in-time accurate even after reclassification. |
| ROW_NUMBER() dedup, not dropDuplicates() | dropDuplicates() keeps an arbitrary row; ROW_NUMBER() ordered by a timestamp guarantees the latest version survives. |
| Materialized Views for high-traffic dashboards only | Balances query speed against refresh/storage cost — not applied blindly to every table. |
| Unity Catalog governance | Schema-level isolation (Bronze/Silver restricted, Gold open), column masking for PII, row-level security by region, automatic lineage + audit logging. |

## Repo Structure

```
├── src/
│   ├── ingestion/       # Batch extract + Auto Loader ingestion scripts
│   ├── bronze/          # Bronze layer loaders
│   ├── silver/          # Cleaning, dedup, validation
│   ├── gold/             # Dimensional model: SCD2 dimensions, fact merges
│   └── governance/       # Unity Catalog setup: grants, masks, row filters
├── sql/                  # DDL for all layers + materialized views
├── config/               # Pipeline configuration (paths, watermarks, etc.)
├── workflows/            # Databricks Workflow job definition
├── docs/                 # Architecture notes, data dictionary
└── tests/                # Unit tests for dedup / transformation logic
```

## Pipeline Flow

1. **Ingest** — `src/ingestion/` pulls from sources into raw landing storage (S3/ADLS).
2. **Bronze** — `src/bronze/` loads raw files into Bronze Delta tables, append-only, minimal transformation.
3. **Silver** — `src/silver/` deduplicates (latest-wins via `ROW_NUMBER()`), validates, standardizes.
4. **Gold** — `src/gold/` merges into dimensional fact/dimension tables, with SCD Type 2 on dimensions.
5. **Serve** — `sql/materialized_views.sql` defines pre-aggregated views for Power BI, refreshed incrementally.
6. **Govern** — `src/governance/` applies Unity Catalog grants, column masks, and row filters.

## Requirements

- Databricks Workspace (AWS or Azure)
- Unity Catalog enabled
- Delta Lake (bundled with Databricks Runtime)
- Source credentials stored in a Key Vault / Secrets Manager–backed secret scope

## Running the Pipeline

See `workflows/databricks_workflow.json` for the full task DAG (Bronze → Silver → Gold → Refresh Views),
schedulable as a Databricks Workflow.
