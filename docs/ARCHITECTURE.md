# Architecture Notes

## Azure platform &amp; orchestration

The pipeline runs on Azure: **ADLS Gen2** (lakehouse storage), **Databricks**
(compute + Delta), **Azure Data Factory** (batch orchestration), **Key Vault**
(secrets), **Unity Catalog** (governance), all provisioned by **Terraform**.

Three sources, each a distinct ingestion pattern:

1. **CSV files** land in the ADLS `landing` container → **Auto Loader** →
   `bronze.marketing_ad_spend`.
2. **PostgreSQL** (local Docker "ERP") → ADF **Copy** over a **Self-Hosted
   Integration Runtime** → Parquet in `landing/orders` → `bronze.orders`. The
   SHIR is the bridge from cloud ADF to a database on the developer's machine
   (a cloud IR can't reach `localhost`) — the same mechanism used for real
   on-prem sources.
3. **REST API** (FX rates, keyless) → a Databricks job (`rest_api_fx.py`) →
   `bronze.fx_rates`. FX exists to normalize multi-currency orders to USD in Gold.

**ADF is the control plane:** it copies the JDBC source and lands files, then
triggers the Databricks Bronze→Silver→Gold jobs with success-dependency chaining,
retries, and failure alerts. Incremental extraction uses **tumbling-window
bounds** from the trigger (no watermark table needed; every window re-runnable).
Databricks-native orchestration via the Asset Bundle job is kept as a portable
alternative.

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

## Data quality / validation

Validation is centralized in a config-driven framework (`src/common/data_quality.py`,
rules in `config/pipeline_config.yaml`), not hand-coded per script. See
[DATA_QUALITY.md](DATA_QUALITY.md) for the full rule set. Key properties:

- **Single-pass evaluation.** All row-level rules compile into one `_dq_errors`
  column evaluated in a single job, replacing the old pattern of one
  `df.filter(...).count()` action per rule (each a separate full scan).
- **Three failure modes per dataset:** `fail` (stop the pipeline), `warn`
  (log, pass through), `quarantine` (route bad rows to a `*_quarantine` side
  table, publish the rest). Orders/customers quarantine; clickstream warns.
- **Rule types:** not-null, range, allowed-values (domain), regex, uniqueness
  (business key), referential integrity (broadcast anti-join), row-count
  floor, and freshness (max timestamp age).

## Performance &amp; cost optimization

See [OPTIMIZATION.md](OPTIMIZATION.md). Highlights:

- **Self-optimizing tables:** every Delta table sets `optimizeWrite` +
  `autoCompact` (no small-file problem between maintenance runs),
  `tuneFileSizesForRewrites` on MERGE-heavy tables, and deletion vectors.
- **Liquid clustering** on fact tables (`CLUSTER BY`) instead of static date
  partitioning + separate ZORDER — avoids low-volume-partition small files.
- **Tuned Spark session** (`src/common/spark_session.py`): AQE + skew-join +
  partition coalescing, dynamic partition pruning, auto-broadcast for small
  dimension joins.
- **Removed `.count()` anti-patterns:** ingestion/bronze cache their single
  read so count/write/watermark don't re-execute the source; gold merges rely
  on Delta's own MERGE metrics; SCD2 uses `isEmpty()` not `count()`.
- **Weekly maintenance job** (`src/maintenance/table_maintenance.py`):
  OPTIMIZE, ANALYZE (CBO statistics), VACUUM — config-driven, decoupled from
  daily ETL.

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
