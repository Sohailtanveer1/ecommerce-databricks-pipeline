# Naming Conventions & Data-Type Standards (Silver + Gold)

Conventions are **enforced by code** (`src/framework/standardize.py`, applied in
`silver_generic`), not left to discipline. Bronze stays raw (source spelling);
**Silver onward is canonical**.

## Columns

- **snake_case** always. `CustomerID` → `customer_id`, `order-amt` → `order_amt`.
- **Strings trimmed** (leading/trailing whitespace removed).
- **A column's name determines its type** (so types are consistent by construction):

| Name pattern | Canonical type | Example |
|---|---|---|
| `*_id` | `STRING` | `customer_id` (never lose leading zeros) |
| `*_sk` | `BIGINT` | `customer_sk` (surrogate key) |
| `*_date` | `DATE` | `order_date` |
| `*_at`, `*_ts`, `*_timestamp` | `TIMESTAMP` (store **UTC**) | `updated_at`, `ingestion_timestamp` |
| `*_amount`, `*_price`, `*_cost` | `DECIMAL(18,2)` | `order_amount` (never FLOAT for money) |
| `*_rate` | `DECIMAL(18,6)` | `fx_rate` |
| `*_pct` | `DECIMAL(9,4)` | `discount_pct` |
| `*_qty`, `*_quantity` | `INT` | `order_qty` |
| `*_count` | `BIGINT` | `event_count` |
| `is_*`, `has_*` | `BOOLEAN` | `is_current`, `has_returned` |

Explicit overrides live per object in `sources.yaml → standardize.cast`; semantic
renames (source spelling → business name) in `standardize.rename`.

```yaml
standardize:
  rename: { CustID: customer_id, OrderAmt: order_amount }
  cast:   { impressions: bigint, spend: "decimal(18,2)" }
```

## A worked example — one audit column across many tables

The SQL Server sources spell their audit column differently per table
(`lastupdatedate`, `last_updated_at`, `updated_ts`, `last_change_at`,
`modified_at`). Bronze preserves each. **Silver canonicalizes them all to a single
`updated_at`** automatically (silver_generic maps each object's `watermark_column`
→ `updated_at`), so every Silver table has the same audit column.

## Audit / lineage columns (standard on every table)

`ingestion_timestamp` (Bronze write time), `processed_timestamp` (Silver write
time), `source_file_path` (where applicable), `updated_at` (source change time).
Quarantine adds `_dq_*` (see `docs/QUARANTINE.md`).

## Tables

| Layer | Pattern | Example |
|---|---|---|
| Bronze | `bronze.<system>__<object>` (raw, source names) | `bronze.sqlserver_erp__orders` |
| Silver | `silver.<system>__<object>` (canonical) | `silver.sqlserver_erp__orders` |
| Gold — dimension | `gold.dim_<entity>` | `gold.dim_customer` |
| Gold — fact | `gold.fact_<grain>` | `gold.fact_orders` |
| Gold — view | `gold.mv_<subject>` / `gold.vw_<subject>` | `gold.mv_revenue_by_category_month` |
| Quarantine | `quarantine.<system>__<object>` | `quarantine.partner_files__supplier_costs` |

## Gold (dimensional) conventions

- **Surrogate key** `<entity>_sk BIGINT` (generated), plus the **natural key**
  `<entity>_id STRING` from source.
- **Facts** hold measures + FK `<entity>_id`/`<entity>_sk`, at a stated grain.
- **SCD Type 2** columns are standard: `effective_start_date DATE`,
  `effective_end_date DATE` (NULL = current), `is_current BOOLEAN`.
- **Money** always `DECIMAL(18,2)`; **rates** `DECIMAL(18,6)`; never FLOAT/DOUBLE.
- No source/raw or PII columns leak into Gold that aren't modeled deliberately.

## Why enforce by code

Type/name drift is the most common cause of broken joins and wrong aggregates
(`amount` as STRING in one table, DOUBLE in another; `custid` vs `customer_id`).
Deriving type from the name makes a whole class of bugs impossible and keeps the
model self-consistent as new sources are added — a one-line `sources.yaml` change,
no per-table code. Bad values that can't cast become NULL and are **quarantined**
by the DQ step that runs right after standardization.

Enforced by: `src/framework/standardize.py` · tested in `tests/test_standardize.py`.
