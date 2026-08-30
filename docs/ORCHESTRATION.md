# Orchestration & Retry Design

## One pipeline or many? → **Many, in one ADF, metadata-driven**

We use **one reusable pipeline per ingestion *pattern*** plus a **master
orchestrator** — not one mega-pipeline, and not one-pipeline-per-table.

```
pl_master  (schedule trigger)
├── Sync_Control_Registry        (sources.yaml -> control tables)
├── pl_ingest_watermark          (Lookup -> ForEach N SQL Server tables -> Copy+SHIR -> Bronze)
├── pl_ingest_cdc                (Debezium landing -> MERGE Bronze, all CDC tables)
├── pl_ingest_rest              (all REST endpoints, pagination)
├── pl_ingest_csv              (Auto Loader, all CSV objects)
├── Build_Medallion            (Bronze -> Silver -> Gold -> Views)
└── Dispatch_Alerts            (on Succeeded OR Failed)
```

**Why this shape**

| Option | Verdict |
|---|---|
| One mega-pipeline | ✗ unmaintainable, can't retry per-source, one failure blocks all, huge blast radius |
| One pipeline per table | ✗ dozens of near-identical pipelines; adding a table = new pipeline |
| **Per-pattern + master (chosen)** | ✓ reusable, parameterized, isolated failure, scales objects via metadata, clear dependencies |

Each per-pattern pipeline is **parameterized and loops over N objects** from
`control.source_objects` — so `watermark` handles 7 tables, `csv` handles 3 files,
etc., with zero new pipelines when you add more.

## Retry — at both levels (the interview answer)

**Activity level** (inside a run):
- Each ADF activity has `retry` + `retryIntervalInSeconds` (e.g. Copy 3×/60s,
  Databricks 2×/90s) for transient blips (SHIR hiccup, storage throttle).
- In code, `framework/retry.with_retry` adds exponential backoff around HTTP/JDBC,
  **transient-only** (fatal errors fail fast — no hammering on a real bug).

**Pipeline level** (across runs):
- The **trigger** carries a retry policy for the whole run.
- Failure is *surgical*: every object writes STARTED/SUCCEEDED/FAILED to
  `control.pipeline_runs`, and a rerun **reprocesses only the objects marked
  FAILED** — not the whole batch. One poison object never blocks the other 20.
- Per-object `try/except` in each ingestion job isolates failures and raises an
  alert instead of aborting the batch.

## Idempotency (so retries are safe)

- **watermark**: watermark advances only after a successful Bronze write; Auto
  Loader checkpoint prevents re-reading landed files.
- **cdc**: MERGE on PK, latest-per-`ts_ms` — replays converge.
- **rest**: raw JSON overwritten per date; Silver dedups on PK.
- **csv**: Auto Loader exactly-once file discovery.

## Parallelism (per-object)

Objects within a source are processed **in parallel**, not sequentially, to cut
run time — without changing error handling or resume behaviour.

- **How:** `framework.runner.run_objects` submits each object on a driver-side
  thread pool; Spark runs the independent jobs concurrently on the **shared**
  cluster under the **FAIR** scheduler (`spark.scheduler.mode=FAIR`). N objects
  finish in ~`max(object_time)` instead of `sum(object_time)`. Degree of
  parallelism = `MAX_PARALLEL` (default 4).
- **Two levels, same as retry:** ADF `ForEach` runs `batchCount` objects in
  parallel *across* activities (watermark Copy); `run_objects` parallelizes
  objects *inside* the Databricks job (cdc/rest/csv/silver).
- **Unchanged by design:**
  - *isolation* — each object has its own try/except; one failure is logged
    FAILED + alerted and never stops the others;
  - *idempotency/exactly-once* — each object keeps its own Auto Loader checkpoint,
    watermark, and MERGE target, so correctness is identical to the sequential run;
  - *resume* — failures land in `control.pipeline_runs`, so pipeline retry still
    reprocesses only the FAILED objects.
- **Concurrency safety:** per-object writes go to different Bronze/Silver tables
  (no conflict); the one shared MERGE (`control.watermarks`) is wrapped in retry
  for Delta optimistic-concurrency conflicts.

## Alerting & monitoring

- `control.alerts` is a **decoupled outbox** — jobs append, `dispatch_alerts`
  delivers (webhook for WARN/CRITICAL), so a flaky notification never breaks
  ingestion and no alert is lost on failure.
- **Azure Monitor** alert rules (`infra/terraform/modules/foundation/alerts.tf`) fire on
  ADF pipeline failures → action group (email/webhook).
- **ADF native** on-failure email is the backstop at the pipeline level.
- `pipeline_runs` + `dq_results` power a monitoring dashboard (rows in/out,
  duration, failure rate, freshness per object).
