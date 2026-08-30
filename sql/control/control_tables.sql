-- ============================================================================
-- CONTROL PLANE (metadata-driven orchestration state). Schema: control.
-- Run once under `USE CATALOG ecommerce_dev`. Populated from config/sources.yaml
-- by framework/load_control.py; read by ADF (Lookup -> ForEach) and Databricks.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS control;
-- Dead-letter schema: per-object quarantine tables (bad rows) are created here on
-- first write by framework.quarantine.
CREATE SCHEMA IF NOT EXISTS quarantine;

-- The object registry — one row per thing we ingest (mirrors sources.yaml).
CREATE TABLE IF NOT EXISTS control.source_objects (
    object_id        STRING NOT NULL,         -- <source_system>.<name>  (stable key)
    source_system    STRING NOT NULL,
    pattern          STRING NOT NULL,         -- watermark | cdc | rest | csv
    connection       STRING,                  -- linked service / secret scope key
    object_name      STRING NOT NULL,
    source_schema    STRING,
    source_table     STRING,
    watermark_column STRING,                  -- watermark pattern only
    primary_keys     ARRAY<STRING>,
    load_type        STRING,                  -- full | incremental
    target_bronze    STRING NOT NULL,         -- catalog.bronze.<table>
    options          MAP<STRING, STRING>,     -- endpoint, path, params, etc.
    enabled          BOOLEAN NOT NULL,
    updated_at       TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

-- Per-object high-watermark (watermark pattern) + last status.
CREATE TABLE IF NOT EXISTS control.watermarks (
    object_id            STRING NOT NULL,
    watermark_column     STRING,              -- the per-table column this value came from
    last_watermark_value STRING,              -- string form of the max watermark loaded
    last_run_id          STRING,
    last_status          STRING,              -- SUCCEEDED | FAILED | RUNNING
    last_run_ts          TIMESTAMP,
    updated_at           TIMESTAMP
) USING DELTA;

-- Run log — one row per (run, object, layer). Powers monitoring + pipeline-level
-- retry that reprocesses only the objects that FAILED.
CREATE TABLE IF NOT EXISTS control.pipeline_runs (
    run_id        STRING NOT NULL,            -- ADF pipeline run id (or job run id)
    parent_run_id STRING,                     -- master pipeline run id
    pipeline      STRING NOT NULL,
    object_id     STRING,
    layer         STRING,                     -- landing | bronze | silver | gold
    status        STRING NOT NULL,            -- STARTED | SUCCEEDED | FAILED | SKIPPED
    attempt       INT,                        -- retry attempt number
    rows_read     BIGINT,
    rows_written  BIGINT,
    rows_quarantined BIGINT,
    error_message STRING,
    started_at    TIMESTAMP,
    ended_at      TIMESTAMP,
    duration_sec  DOUBLE
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

-- Data-quality results per run/object (feeds alerts + the quarantine story).
CREATE TABLE IF NOT EXISTS control.dq_results (
    run_id     STRING NOT NULL,
    object_id  STRING NOT NULL,
    check_name STRING NOT NULL,
    passed     BOOLEAN,
    violations BIGINT,
    detail     STRING,
    checked_at TIMESTAMP
) USING DELTA;

-- Alert outbox — the alerting job reads unsent rows and dispatches them
-- (webhook / email / Log Analytics), so alerting is decoupled and retryable.
CREATE TABLE IF NOT EXISTS control.alerts (
    alert_id   STRING NOT NULL,
    severity   STRING,                        -- INFO | WARN | CRITICAL
    source     STRING,                        -- pipeline / object / check
    title      STRING,
    body       STRING,
    created_at TIMESTAMP,
    sent       BOOLEAN,
    sent_at    TIMESTAMP
) USING DELTA;
