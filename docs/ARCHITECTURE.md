# Architecture Notes

## Why Medallion Architecture

Bronze/Silver/Gold separation means:
- Bronze preserves raw history -- if a downstream bug is found, we can
  always replay from the untouched original data.
- Silver isolates cleaning/validation logic in one place, so Gold models
  never have to re-implement dedup or null handling.
- Gold is purpose-built for consumption -- dimensional modeling, ready
  for BI tools, without exposing raw/PII data.

## Ingestion strategy: batch vs. Auto Loader

Two ingestion patterns are used deliberately, matched to each source's
actual arrival behavior:

- **Orders (transactional DB):** scheduled incremental batch pull on an
  `updated_at` watermark. Predictable, low operational overhead, no
  standing infrastructure cost. CDC would be over-engineering at this
  data volume.
- **Clickstream events:** Auto Loader, since arrival is genuinely
  continuous and unpredictable throughout the day. Uses cloud-native
  notification services (Event Grid on Azure, S3 event notifications
  on AWS) rather than repeated directory listing.

## Deduplication: ROW_NUMBER(), not dropDuplicates()

`dropDuplicates()` keeps whichever row Spark encounters first during a
distributed scan -- effectively arbitrary. When a record can appear
more than once in an incremental batch (e.g. an update landing near an
original insert), this risks silently keeping a stale version.

The fix used throughout this repo: `ROW_NUMBER()` partitioned by the
business key, ordered by a recency timestamp descending, filtered to
rank 1. This deterministically keeps the latest version.

## SCD Type 2 on dimensions

Dimension attributes that can change over time (product category,
customer segment) are versioned, not overwritten. Each version carries
`effective_start_date` / `effective_end_date` / `is_current`. Fact
tables join to the dimension version that was active on the fact's
own date -- so historical reports don't silently change when a
dimension value is updated later.

## Incremental Bronze -> Silver

Bronze is read as a Delta structured stream (`spark.readStream.table(...)`)
with `foreachBatch`, rather than a full table read on every run. The
streaming checkpoint tracks progress automatically, so re-running never
reprocesses already-handled data. `trigger(availableNow=True)` makes
this behave like a scheduled batch job rather than an always-on stream,
keeping compute cost bounded to actual run time.

## Governance: Unity Catalog

- Bronze/Silver access restricted to the ETL service principal only.
- Gold access granted to analysts/data scientists/Power BI.
- PII columns (email, phone) masked via Unity Catalog column masks --
  unauthorized queries transparently see a masked value.
- Row-level security scopes fact table access by region where needed.
- Lineage and audit logging are automatic across the whole pipeline.

## Cost-conscious choices for a small company's scale

- Job clusters (spin up, run, terminate) instead of always-on clusters.
- Serverless SQL Warehouse with auto-suspend for BI serving.
- Materialized views only on the highest-traffic dashboard queries,
  not applied blanket across every table.
- Databricks Workflows for orchestration -- no separate Airflow/Composer
  deployment needed at this scale.
- Weekly (not continuous) `OPTIMIZE`/`VACUUM` maintenance, matched to
  the data volume actually being written.

## What would change at 10x scale

- Revisit batch extract for orders -- may need CDC if extract windows
  start conflicting with the production DB's own load, or if near-
  real-time order visibility becomes a requirement.
- Introduce more granular partitioning / Z-ordering strategy on
  fact tables as row counts grow.
- Consider a dedicated orchestration tool (Airflow/Composer) if
  pipeline complexity grows beyond what Workflows comfortably expresses.
- Re-evaluate Auto Loader trigger cadence -- may move from periodic
  `availableNow` sweeps to a continuous trigger if latency requirements
  tighten.
