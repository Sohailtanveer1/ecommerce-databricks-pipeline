-- ============================================================
-- Unity Catalog data protection for the ecommerce_dev catalog:
--   * Column masks on PII (email, phone) — unauthorized users see masked values.
--   * Row-level security on fact_orders — users see only their region.
-- Grants themselves are managed in Terraform (infra/terraform/platform/grants.tf).
-- Run after DDL, as a user with MODIFY on the governance schema.
-- ============================================================
USE CATALOG ecommerce_dev;

-- ---- Column masking functions ----
CREATE OR REPLACE FUNCTION governance.mask_email(email STRING)
RETURN CASE
    WHEN is_account_group_member('pii-authorized') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@masked.com')
END;

CREATE OR REPLACE FUNCTION governance.mask_phone(phone STRING)
RETURN CASE
    WHEN is_account_group_member('pii-authorized') THEN phone
    ELSE '***-***-****'
END;

ALTER TABLE silver.customers ALTER COLUMN email SET MASK governance.mask_email;
ALTER TABLE silver.customers ALTER COLUMN phone SET MASK governance.mask_phone;

-- ---- Row-level security on fact_orders by region ----
-- Maps a user to their allowed region(s). In prod this reads a mapping table;
-- here we allow a 'global-access' group to see everything.
CREATE OR REPLACE FUNCTION governance.region_filter(region STRING)
RETURN is_account_group_member('global-access')
    OR region = current_user_region();   -- replace with a lookup in prod

ALTER TABLE gold.fact_orders SET ROW FILTER governance.region_filter ON (region);

-- Audit history is automatic in Unity Catalog:
--   SELECT * FROM system.access.audit WHERE ... ;
