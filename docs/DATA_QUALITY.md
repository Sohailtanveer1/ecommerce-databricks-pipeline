# Data Quality / Validation

Validation lives in one config-driven framework — `src/common/data_quality.py`,
with rules declared in `config/pipeline_config.yaml` under `data_quality`.
Analysts tune thresholds in YAML; no code change needed.

## Failure modes (per dataset)

| Mode | Behavior | Used by |
|---|---|---|
| `fail` | Raise `DataQualityError`, stop the pipeline. | (opt-in for critical feeds) |
| `warn` | Log every violation, let all rows through. | `clickstream_events` |
| `quarantine` | Route violating rows to `<dataset>_quarantine`, publish the rest. | `orders`, `customers` |

## Supported checks

**Row-level** (per record, evaluated in a single pass):
- `not_null: [cols]`
- `ranges: { col: { min, max } }` (inclusive) / `non_negative`
- `allowed_values: { col: [values] }` (domain / enum)
- `regex: { col: pattern }` (e.g. email format)

**Dataset-level:**
- `unique: [keys]` — business/primary-key uniqueness
- `min_row_count: N` — batch-size floor (catches upstream drops)
- `freshness: { column, max_age_hours }` — newest record not too stale
- `foreign_key(col, ref_df, ref_col)` — referential integrity (broadcast anti-join, code-level)

## How it runs

```python
from common.config import dq_rules
from common.data_quality import from_config, write_quarantine

outcome = from_config("orders", dq_rules("orders")).validate(cleaned_df)
write_quarantine(outcome, "silver.orders_quarantine")   # no-op if nothing bad
publish(outcome.valid)
```

`validate()`:
1. caches the batch and counts it **once**;
2. builds one `_dq_errors` array column (rule name per failed rule) — a single
   Spark job instead of N `filter().count()` scans;
3. splits valid / quarantined (quarantine mode only);
4. runs dataset-level checks;
5. prints a per-rule summary and enforces the failure mode.

Quarantined rows keep the original columns plus `_dq_errors`, `_dq_dataset`,
`_dq_ts` for triage (see the `*_quarantine` DDL in `sql/ddl_all_layers.sql`).

## Why not just `dropDuplicates` / silent filters

Silently dropping bad rows hides upstream breakage. Quarantine keeps a durable,
queryable record of *what* failed and *why*, so a spike in `silver.orders_quarantine`
is an alert signal, not invisible data loss.

Tests: `tests/test_data_quality.py`.
