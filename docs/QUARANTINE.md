# Quarantine & Remediation (Dead-Letter) — bad records don't break the batch

Bad rows — whether a **malformed line in a file** or a row **failing data-quality
rules from any source** — are neither dropped nor allowed to fail the run. They're
routed to a per-object **quarantine table**, alerted on, and can be **remediated**
and promoted back to Silver. Metadata-driven, so it applies to every object with
zero per-table code.

## Where it runs — capture at Bronze, quarantine at Silver

Two kinds of "bad" fail at two different stages, so they're handled in two places:

| Kind | Fails at | Handled by |
|---|---|---|
| **Structurally corrupt** (unparseable row, wrong type, extra column, bad encoding) | **Bronze read** (parse time) | **PERMISSIVE reader + `_rescued_data`** so the load never fails and nothing drops; the row lands in Bronze *flagged* |
| **Rule violation** (parseable but `null`/negative/invalid value) | not at read — it's valid data | **Silver** DQ rules |

So the mechanism that stops a corrupt record from crashing the Bronze load lives
at the **Bronze reader** (all four loaders use `PERMISSIVE`/`rescuedDataColumn` —
never `FAILFAST` or `DROPMALFORMED`). The **routing** of bad rows to a quarantine
table happens once, at **Bronze → Silver** (`src/silver/silver_generic.py` →
`framework.quarantine.apply()`), where both the `_rescued_data`-flagged rows and
the rule failures are split off.

**Why not quarantine at Bronze?** Keep Bronze a faithful, append-only, replayable
copy of the source — bad rows included but flagged. Making the cleansing decision
at the Silver boundary preserves "replay everything from Bronze" and keeps one
quarantine step instead of two.

## What gets quarantined

| Reason (`_dq_reason`) | Trigger | Source of "bad" |
|---|---|---|
| `rescued` | Auto Loader `_rescued_data` is non-null | CSV/JSON parse issue, extra/typed-wrong column, encoding |
| `dq` | fails a rule in the object's `dq:` block | not-null, ranges, allowed-values, regex, uniqueness |

DQ rules live per-object in `config/sources.yaml`:

```yaml
- name: supplier_costs
  path: "landing/partner_files/supplier_costs/"
  primary_keys: [supplier_id, product_id, effective_date]
  dq:
    not_null: [supplier_id, product_id, unit_cost]
    ranges: { unit_cost: { min: 0 } }
```

## The quarantine record

Original columns **plus** metadata so you know what/when/why and can remediate:

`_dq_errors ARRAY<STRING>` (which rules), `_dq_reason` (rescued|dq), `_dq_dataset`
(object_id), `_dq_run_id`, `_dq_ts`, `_dq_resolved BOOLEAN`, `_dq_resolved_ts`.

```sql
-- triage: what's failing, and why
SELECT _dq_reason, _dq_errors, COUNT(*) 
FROM ecommerce_dev.quarantine.partner_files__supplier_costs
WHERE NOT _dq_resolved GROUP BY 1,2 ORDER BY 3 DESC;
```

## Remediation loop (the "mitigate" part)

1. **Detect** — a WARN alert fires when the quarantine ratio exceeds
   `QUARANTINE_ALERT_RATIO` (default 10%); counts land in `control.dq_results`
   and `control.pipeline_runs.rows_quarantined`.
2. **Fix** — the other team re-drops a corrected file, the source is patched, or a
   too-strict rule is widened in `sources.yaml`.
3. **Reprocess** — run `framework/reprocess_quarantine.py [object_id]`. It
   re-validates unresolved rows against the **current** rules, appends the
   now-valid ones to Silver, and marks them `_dq_resolved = true`. Still-bad rows
   stay for the next pass. Idempotent.

```bash
# after re-dropping the fixed CSV:
python src/framework/reprocess_quarantine.py partner_files.supplier_costs
```

## Why this design

- **No data loss** — bad rows are preserved with full context, not dropped.
- **No batch failure** — one bad row/file never blocks the good 99%.
- **Auditable & measurable** — quarantine tables + `dq_results` are a data-health
  signal; a spike is an alert, not a silent problem.
- **Remediable** — a first-class reprocess path, not a manual re-run.

Tests: `tests/test_quarantine.py` (the pure `classify` split). See also
`docs/DATA_QUALITY.md` (rule engine) and `docs/PRODUCTION_ISSUES.md` (#16–#19).
