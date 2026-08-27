-- ============================================================
-- Delta table DDL for all layers (Unity Catalog: run under `USE CATALOG ecommerce_dev`).
--
-- Sources: orders + customers (Postgres/JDBC), marketing ad spend (CSV in ADLS),
-- fx rates (REST API). Self-optimizing table properties on every table; fact
-- tables use liquid clustering (CLUSTER BY) instead of static partitioning.
-- ============================================================


-- ============================================================
-- BRONZE: raw, append-only, minimal transformation
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DOUBLE,
    discount DOUBLE,
    amount DOUBLE,
    currency STRING,
    payment_method STRING,
    order_channel STRING,
    order_status STRING,
    order_date STRING,          -- raw string in Bronze; cast to DATE in Silver
    shipping_country STRING,
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

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    country STRING,
    city STRING,
    region STRING,
    customer_segment STRING,
    loyalty_tier STRING,
    signup_date STRING,
    marketing_opt_in BOOLEAN,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableDeletionVectors = true
);

-- Source #3 (REST API): daily FX rates, base USD. Powers USD-normalized revenue.
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

-- Source #1 (CSV in ADLS): marketing ad spend, ingested via Auto Loader / COPY INTO.
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
    quantity INT,
    unit_price DOUBLE,
    discount DOUBLE,
    amount DOUBLE,
    currency STRING,
    payment_method STRING,
    order_channel STRING,
    order_status STRING,
    order_date DATE NOT NULL,
    shipping_country STRING,
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
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    country STRING,
    city STRING,
    region STRING,
    customer_segment STRING,
    loyalty_tier STRING,
    signup_date DATE,
    marketing_opt_in BOOLEAN,
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

-- Quarantine side tables (rows failing Silver data-quality checks).
CREATE TABLE IF NOT EXISTS silver.orders_quarantine (
    order_id STRING, customer_id STRING, product_id STRING, quantity INT,
    unit_price DOUBLE, discount DOUBLE, amount DOUBLE, currency STRING,
    payment_method STRING, order_channel STRING, order_status STRING,
    order_date DATE, shipping_country STRING, updated_at TIMESTAMP,
    processed_timestamp TIMESTAMP,
    _dq_errors ARRAY<STRING>, _dq_dataset STRING, _dq_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS silver.customers_quarantine (
    customer_id STRING, first_name STRING, last_name STRING, email STRING,
    phone STRING, country STRING, city STRING, region STRING,
    customer_segment STRING, loyalty_tier STRING, signup_date DATE,
    marketing_opt_in BOOLEAN, updated_at TIMESTAMP, processed_timestamp TIMESTAMP,
    _dq_errors ARRAY<STRING>, _dq_dataset STRING, _dq_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);


-- ============================================================
-- GOLD: dimensional model
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_id STRING NOT NULL,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    unit_cost DOUBLE,
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
    first_name STRING,
    last_name STRING,
    country STRING,
    city STRING,
    region STRING,
    customer_segment STRING,
    loyalty_tier STRING,
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
    quantity INT,
    unit_price DOUBLE,
    discount DOUBLE,
    amount DOUBLE,
    currency STRING,
    payment_method STRING,
    order_channel STRING,
    order_status STRING,
    region STRING,
    shipping_country STRING
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
