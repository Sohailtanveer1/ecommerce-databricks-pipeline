-- Unity Catalog governance setup
-- Run once during environment setup, or as part of an infra-as-code pipeline.

-- ============================================================
-- 1. Schema-level isolation: Bronze/Silver restricted, Gold open
-- ============================================================

-- Data scientists / BI users get read access ONLY on gold
GRANT USE CATALOG ON CATALOG lakehouse_catalog TO `data-scientists-group`;
GRANT USE SCHEMA ON SCHEMA lakehouse_catalog.gold TO `data-scientists-group`;
GRANT SELECT ON SCHEMA lakehouse_catalog.gold TO `data-scientists-group`;

-- No grants issued on bronze/silver for this group -- access denied by default,
-- not merely discouraged.

-- ETL service principal gets full access across all layers
GRANT ALL PRIVILEGES ON CATALOG lakehouse_catalog TO `etl-service-principal`;


-- ============================================================
-- 2. Column-level security: mask PII for unauthorized users
-- ============================================================

CREATE OR REPLACE FUNCTION lakehouse_catalog.governance.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('pii-authorized') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@masked.com')
END;

ALTER TABLE silver.customers
ALTER COLUMN email SET MASK lakehouse_catalog.governance.mask_email;

CREATE OR REPLACE FUNCTION lakehouse_catalog.governance.mask_phone(phone STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('pii-authorized') THEN phone
    ELSE '***-***-****'
END;

ALTER TABLE silver.customers
ALTER COLUMN phone SET MASK lakehouse_catalog.governance.mask_phone;


-- ============================================================
-- 3. Row-level security: scope access by region
-- ============================================================

CREATE OR REPLACE FUNCTION lakehouse_catalog.governance.region_filter(region STRING)
RETURNS BOOLEAN
RETURN region = current_user_region()  -- lookup against a user-region mapping table
    OR is_account_group_member('global-access');

ALTER TABLE gold.fact_orders
SET ROW FILTER lakehouse_catalog.governance.region_filter ON (region);


-- ============================================================
-- 4. Audit logging is automatic in Unity Catalog -- no setup
-- required here. Query system.access.audit for access history.
-- ============================================================
