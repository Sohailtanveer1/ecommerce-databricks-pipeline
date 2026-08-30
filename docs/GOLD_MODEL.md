# Gold — Metadata-Driven Dimensional Model

Gold is declared in `config/gold_model.yaml` and built by `src/gold/gold_generic.py`.
Add a dimension or fact in YAML — no code. Everything obeys the naming/type
conventions (see `NAMING_CONVENTIONS.md`).

## Dimensions (SCD Type 2)

```yaml
- name: dim_customer
  source: silver.sqlserver_erp__customers
  natural_key: customer_id      # -> customer_id (natural) + customer_sk (surrogate)
  scd2: [segment, region]       # a change here opens a new version
  attributes: [name, email, segment, region]
```

Each dimension gets:
- **`<entity>_sk` (BIGINT)** — generated surrogate key, deterministic per version
  (`xxhash64(dim, natural_key, effective_start_date)`).
- **`<entity>_id` (STRING)** — the source natural key.
- **SCD2 columns** — `effective_start_date`, `effective_end_date` (NULL = current),
  `is_current`.

Versioning: a null-safe compare on the `scd2` attributes closes the current row
(`is_current=false`, sets `effective_end_date`) and inserts a fresh version;
unchanged rows are left alone; new naturals are inserted.

## Facts (point-in-time keys)

```yaml
- name: fact_orders
  source: silver.sqlserver_erp__orders
  grain: [order_id]
  event_date: order_date         # used to pick the dim version active then
  measures: [amount]
  degenerate: [order_status, currency, order_date]
  dimensions:
    - { dim: dim_customer, natural_key: customer_id }
```

For each dimension the builder resolves the **surrogate key that was active on the
`event_date`** — a point-in-time join on `event_date BETWEEN effective_start_date
AND COALESCE(effective_end_date, current_date())`. That's the payoff of SCD2:
`fact_orders` links to the customer/product *as they were when the order happened*,
so historical reports don't change when a dimension is later updated. The fact
keeps both the surrogate FK (`customer_sk`) and the natural key (`customer_id`),
the measures, and any degenerate dimensions, MERGEd at its grain.

## Why metadata-driven

The SCD2 mechanics, surrogate-key generation, and point-in-time joins are identical
for every dimension/fact — so they live once in `gold_generic.py`, parameterized by
YAML. A new star (e.g. `dim_supplier` + `fact_purchases`) is a config change with
zero new transform code, and it inherits the conventions and lineage for free.

Tested: `tests/test_gold_generic.py`.
