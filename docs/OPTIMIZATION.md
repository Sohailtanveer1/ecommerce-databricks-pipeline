# Performance &amp; Cost Optimization

Optimizations applied across the pipeline, and why each fits a small-company
scale (the repo's guiding principle — right-sized, not enterprise-maximal).

## 1. Self-optimizing tables (write-time)

Set once in DDL (`sql/ddl_all_layers.sql`) so every write stays healthy between
weekly maintenance:

| Property | Effect |
|---|---|
| `delta.autoOptimize.optimizeWrite` | Compacts small files as they're written. |
| `delta.autoOptimize.autoCompact` | Background compaction of tiny files. |
| `delta.tuneFileSizesForRewrites` | Right-sizes files on MERGE-heavy tables (silver/fact). |
| `delta.enableDeletionVectors` | MERGE/UPDATE/DELETE rewrite far fewer files. |
| `delta.enableChangeDataFeed` | Only where a downstream stream reads changes. |

## 2. Data layout: liquid clustering

Fact tables use `CLUSTER BY` (liquid clustering) instead of static
`PARTITIONED BY (order_date)` + a separate `ZORDER`:

- Date partitioning at low volume creates many tiny partitions → small-file
  problem. Liquid clustering keeps files right-sized and skippable.
- Clustering keys can change later **without** rewriting the table.
- `gold.fact_orders` clusters by `(order_date, customer_id)`;
  `gold.fact_clickstream` by `(event_timestamp, customer_id)`.

Runtimes without liquid clustering fall back to `OPTIMIZE ... ZORDER BY`
automatically (`src/common/optimize.py`).

## 3. Tuned Spark session

`src/common/spark_session.py` applies to every job:

- **AQE** + skew-join handling + shuffle-partition coalescing.
- **Dynamic partition pruning** for star-schema joins.
- **Auto-broadcast** (≤32MB) so small dimension joins skip the shuffle.
- Modest `shuffle.partitions=64` (AQE coalesces further) — avoids the 200-tiny-file default.

## 4. Removing `.count()` anti-patterns

`df.count()` forces a full job; several scripts triggered the source read
multiple times.

| Script | Before | After |
|---|---|---|
| `batch_extract_orders` | JDBC executed 3× (count, write, max-watermark) | cache once; count+max in one agg; 1 read |
| `bronze_loader` | raw files read 2× (count, write) | cache once; count served from cache |
| `gold_fact_orders` | full `.count()` after merge | rely on Delta MERGE metrics |
| `gold_dim_product_scd2` | `.count()` guard before append | `.isEmpty()` short-circuit + broadcast anti-join |

## 5. Auto Loader throughput

`maxFilesPerTrigger` bounds each micro-batch (no single giant skewed batch on
backlog); `schemaEvolutionMode=addNewColumns` evolves schema instead of failing.

## 6. Weekly maintenance job

`src/maintenance/table_maintenance.py` (config-driven, `optimization` section):
`OPTIMIZE` → `ANALYZE` (CBO statistics) → `VACUUM`. Runs Sundays 04:00,
decoupled from daily ETL so compaction never blocks ingestion.

## Deliberately NOT done (would be over-engineering here)

- Bloom-filter indexes, per-table custom file-size targets, Z-order on every
  column — unnecessary at this volume; add when a table's scan cost proves it.
- Always-on streaming — the `availableNow` sweep model is cheaper at this scale.
- Photon is a **cluster** setting (enable on the job cluster), not code; enable
  it if CPU-bound query cost justifies the DBU premium.
