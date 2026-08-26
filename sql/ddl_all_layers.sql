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
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS bronze.clickstream_events (
    event_id STRING,
    customer_id STRING,
    event_type STRING,       -- page_view, add_to_cart, checkout, etc.
    product_id STRING,
    event_timestamp TIMESTAMP,
    source_file_path STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id STRING,
    name STRING,
    email STRING,
    phone STRING,
    region STRING,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);


-- ============================================================
-- SILVER: cleaned, deduplicated, validated
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    product_id STRING,
    order_date DATE NOT NULL,
    amount DOUBLE,
    order_status STRING,
    updated_at TIMESTAMP,
    processed_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id STRING NOT NULL,
    name STRING,
    email STRING,
    phone STRING,
    region STRING,
    updated_at TIMESTAMP,
    processed_timestamp TIMESTAMP
) USING DELTA;


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
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_id STRING NOT NULL,
    name STRING,
    region STRING,
    customer_segment STRING,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BOOLEAN NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    product_id STRING,
    order_date DATE NOT NULL,
    amount DOUBLE,
    order_status STRING,
    region STRING
) USING DELTA
PARTITIONED BY (order_date);

CREATE TABLE IF NOT EXISTS gold.fact_clickstream (
    event_id STRING NOT NULL,
    customer_id STRING,
    event_type STRING,
    product_id STRING,
    event_timestamp TIMESTAMP
) USING DELTA
PARTITIONED BY (DATE(event_timestamp));
