-- Materialized Views for Power BI
--
-- Databricks incrementally refreshes these under the hood (via Lakeflow
-- Declarative Pipelines), so Power BI queries hit a small, pre-aggregated
-- table through a Databricks SQL Warehouse -- rather than re-running the
-- full fact/dimension join and aggregation on every filter change.
--
-- The fact-to-dimension join uses an effective-date range match against
-- dim_product's SCD Type 2 versions, so historical reporting stays
-- point-in-time correct even after a product is reclassified.

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_revenue_by_category_month
AS
SELECT
    d.category,
    DATE_TRUNC('month', f.order_date) AS month,
    SUM(f.amount) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS order_count
FROM gold.fact_orders f
JOIN gold.dim_product d
    ON f.product_id = d.product_id
    AND f.order_date BETWEEN d.effective_start_date AND COALESCE(d.effective_end_date, current_date())
GROUP BY d.category, DATE_TRUNC('month', f.order_date);


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_revenue_by_region_month
AS
SELECT
    f.region,
    DATE_TRUNC('month', f.order_date) AS month,
    SUM(f.amount) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(DISTINCT f.customer_id) AS unique_customers
FROM gold.fact_orders f
GROUP BY f.region, DATE_TRUNC('month', f.order_date);


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_cart_funnel_daily
AS
SELECT
    DATE(event_timestamp) AS event_date,
    event_type,
    COUNT(DISTINCT customer_id) AS unique_users,
    COUNT(*) AS event_count
FROM gold.fact_clickstream
GROUP BY DATE(event_timestamp), event_type;
