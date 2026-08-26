-- ============================================================
-- Delta table DDL for all layers.
--
-- Every table sets self-optimizing properties so ordinary writes stay healthy
-- between the weekly OPTIMIZE/VACUUM maintenance job:
--   * autoOptimize.optimizeWrite  -> compact small files at write time
--   * autoOptimize.autoCompact    -> background compaction of tiny files
--   * tuneFileSizesForRewrites    -> right-size files for MERGE-heavy tables
--   * enableDeletionVectors       -> MERGE/UPDATE/DELETE rewrite fewer files
--   * enableChangeDataFeed        -> only where a downstream stream reads changes
--
-- Fact tables use LIQUID CLUSTERING (CLUSTER BY) instead of static partitioning
-- + a separate ZORDER: it avoids small-file skew from low-volume date
-- partitions and lets the clustering keys evolve without a rewrite.
-- ============================================================


-- ============================================================
-- BRONZE: raw, append-only, minimal transformation
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    order_date STRING,      -- kept as raw string in Bronze; cast happens in Silver
    amount DOUBLE,
    order_status STRING,
    updated_at TIMESTAMP,
    source_file_path STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableDeletionVectors = true
);

CREATE TABLE IF NOT EXISTS bronze.clickstream_events (
    event_id STRING,
    customer_id STRING,
    event_type STRING,       -- page_view, add_to_cart, checkout, etc.
    product_id STRING,
    event_timestamp TIMESTAMP,
    source_file_path STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableDeletionVectors = true
);

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id STRING,
    name STRING,
    email STRING,
    phone STRING,
    region STRING,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableDeletionVectors = true
);

-- Source #3 (REST API): daily FX rates, base USD. Powers currency-normalized
-- revenue in Gold.
CREATE TABLE IF NOT EXISTS bronze.fx_rates (
    as_of_date DATE,
    base_currency STRING,
    quote_currency STRING,
    rate DOUBLE,
    source_api STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true
);

-- Source #1 (CSV in ADLS): marketing ad spend, ingested via Auto Loader.
CREATE TABLE IF NOT EXISTS bronze.marketing_ad_spend (
    spend_date STRING,
    channel STRING,
    campaign STRING,
    region STRING,
    impressions STRING,
    clicks STRING,
    spend STRING,
    currency STRING,
    _rescued_data STRING,
    source_file_path STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true
);


-- ============================================================
-- SILVER: cleaned, deduplicated, validated
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    product_id STRING,
    order_date DATE NOT NULL,
    amount DOUBLE,
    currency STRING,
    order_status STRING,
    updated_at TIMESTAMP,
    processed_timestamp TIMESTAMP
) USING DELTA
CLUSTER BY (order_id)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.tuneFileSizesForRewrites = true,
    delta.enableDeletionVectors = true
);

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id STRING NOT NULL,
    name STRING,
    email STRING,
    phone STRING,
    region STRING,
    updated_at TIMESTAMP,
    processed_timestamp TIMESTAMP
) USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.tuneFileSizesForRewrites = true,
    delta.enableDeletionVectors = true
);

-- Side tables for rows that fail Silver data-quality checks (quarantine mode).
CREATE TABLE IF NOT EXISTS silver.orders_quarantine (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    order_date DATE,
    amount DOUBLE,
    order_status STRING,
    updated_at TIMESTAMP,
    processed_timestamp TIMESTAMP,
    _dq_errors ARRAY<STRING>,
    _dq_dataset STRING,
    _dq_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS silver.customers_quarantine (
    customer_id STRING,
    name STRING,
    email STRING,
    phone STRING,
    region STRING,
    updated_at TIMESTAMP,
    processed_timestamp TIMESTAMP,
    _dq_errors ARRAY<STRING>,
    _dq_dataset STRING,
    _dq_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);


-- ============================================================
-- GOLD: dimensional model
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_id STRING NOT NULL,
    product_name STRING,
    category STRING,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,          -- NULL = currently active version
    is_current BOOLEAN NOT NULL
) USING DELTA
CLUSTER BY (product_id)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableDeletionVectors = true
);

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_id STRING NOT NULL,
    name STRING,
    region STRING,
    customer_segment STRING,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BOOLEAN NOT NULL
) USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableDeletionVectors = true
);

CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    product_id STRING,
    order_date DATE NOT NULL,
    amount DOUBLE,
    currency STRING,
    order_status STRING,
    region STRING
) USING DELTA
CLUSTER BY (order_date, customer_id)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.tuneFileSizesForRewrites = true,
    delta.enableDeletionVectors = true
);

-- Curated FX dimension (from bronze.fx_rates), used to normalize revenue to USD.
CREATE TABLE IF NOT EXISTS gold.dim_fx_rates (
    as_of_date DATE NOT NULL,
    base_currency STRING NOT NULL,
    quote_currency STRING NOT NULL,
    rate DOUBLE NOT NULL
) USING DELTA
CLUSTER BY (as_of_date)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true
);

CREATE TABLE IF NOT EXISTS gold.fact_clickstream (
    event_id STRING NOT NULL,
    customer_id STRING,
    event_type STRING,
    product_id STRING,
    event_timestamp TIMESTAMP
) USING DELTA
CLUSTER BY (event_timestamp, customer_id)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableDeletionVectors = true
);
