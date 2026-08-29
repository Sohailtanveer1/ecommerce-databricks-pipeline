# Production Issues Log (things that break in real life)

A curated catalogue of failures this pipeline is designed to survive — what
causes them, how the design absorbs them, and **how to reproduce each one** so you
can demo it. This is the "I've operated pipelines, not just built them" evidence.

Legend: **Mitigation** = built-in handling · **Repro** = how to trigger it.

---

## A. Source: SQL Server watermark (SHIR)

1. **Late-arriving / clock-skew rows** — a row with `modified_at` *earlier* than the
   last watermark (backdated correction) is missed by `> watermark`.
   - **Mitigation:** overlap window — pull `> watermark - lookback` and dedup in
     Silver (ROW_NUMBER latest-wins). Configurable per object.
   - **Repro:** `UPDATE orders SET modified_at = DATEADD(day,-3,modified_at) WHERE order_id='O1003'` after a load.

2. **Watermark advanced but load failed downstream** → data loss (gap).
   - **Mitigation:** watermark is advanced *only after* the Bronze write succeeds
     (`watermark_to_bronze` sets it inside the successful `foreachBatch`). Run log
     records STARTED/SUCCEEDED so a crash between them is visible.
   - **Repro:** kill the Databricks job mid-run; confirm watermark unchanged.

3. **Schema drift** — a new column appears in a source table.
   - **Mitigation:** Bronze append uses `mergeSchema`; Auto Loader `addNewColumns`.
   - **Repro:** `ALTER TABLE dbo.orders ADD promo_code VARCHAR(20)`.

4. **Duplicate load on retry** — pipeline retry re-copies the same slice.
   - **Mitigation:** Bronze is append (dups tolerated); Silver dedups on PK. Auto
     Loader checkpoint prevents re-reading the same landed file.
   - **Repro:** re-run `pl_ingest_watermark` without changing source.

5. **SHIR offline / unreachable DB** — the agent host is down.
   - **Mitigation:** activity retry (3×, 60s) on the Copy; CRITICAL alert;
     pipeline continues other sources (isolated ForEach failure).
   - **Repro:** stop the SHIR service, run the pipeline.

6. **Huge table / long Copy** blows the activity timeout.
   - **Mitigation:** per-object `load_type`, partitioned Copy, and timeout tuned
     per object; big tables flagged for a separate schedule.

## B. Source: Postgres CDC (Debezium)

7. **Replication slot bloat** — connector down while WAL accumulates → disk fills
   on Postgres.
   - **Mitigation:** monitor slot lag; alert on `pg_replication_slots` retained
     bytes; heartbeat table to advance the slot on quiet tables.
   - **Repro:** stop `connect`, generate changes, watch `SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots`.

8. **Out-of-order / duplicate events** on connector restart (at-least-once).
   - **Mitigation:** MERGE keyed on PK, keeping the latest by `ts_ms` per batch
     (see `cdc_to_bronze._apply_changes`). Idempotent.
   - **Repro:** restart `connect`; re-delivered events must not double-apply.

9. **Deletes not captured** — default REPLICA IDENTITY drops the before-image.
   - **Mitigation:** `REPLICA IDENTITY FULL` on all CDC tables (in the seed);
     `op='d'` handled via `before` image → tombstone (`__deleted=true`).
   - **Repro:** `DELETE FROM orders WHERE order_id='O1005'`; confirm bronze row flips `__deleted`.

10. **Snapshot vs streaming overlap** — initial snapshot (`op='r'`) races live changes.
    - **Mitigation:** Debezium `snapshot.mode=initial` handles the handoff; MERGE
      makes replays safe.

11. **Schema change on a CDC table** (add/drop column, type change).
    - **Mitigation:** Auto Loader schema evolution on the landing JSON; MERGE with
      `mergeSchema`. Breaking type changes raise a WARN alert for review.

## C. Source: REST API

12. **403 without a User-Agent** (CDN bot-blocking) — *actually hit this.*
    - **Mitigation:** fixed `User-Agent` header in `rest_to_bronze`.
    - **Repro:** remove the header; call `api.frankfurter.app`.

13. **Rate limiting (429) / transient 503.**
    - **Mitigation:** `with_retry` (exp backoff, transient-only) around the HTTP call.
    - **Repro:** tighten a paginated pull to hammer the endpoint.

14. **Pagination drift / partial pages** — API changes page size or returns fewer.
    - **Mitigation:** loop until a short page; PK dedup in Silver.

15. **Endpoint schema change** — a field renamed/removed.
    - **Mitigation:** schema-on-read (`from_json` to map) + `mergeSchema`; Silver
      selects explicitly and quarantines rows missing required fields.

## D. Source: CSV files in ADLS

16. **Late / missing scheduled file** — the other team's drop doesn't arrive.
    - **Mitigation:** freshness DQ check (max ingest age) → WARN/CRITICAL alert;
      run log shows zero rows for the object.
    - **Repro:** don't upload today's file; run the freshness check.

17. **Partial / still-uploading file** — Auto Loader reads it half-written.
    - **Mitigation:** land to a `.tmp` then atomic rename (producer contract);
      Auto Loader only picks completed files; rescued-data captures malformed rows.

18. **Duplicate file re-drop** (same file re-sent).
    - **Mitigation:** Auto Loader checkpoint = exactly-once file discovery.
    - **Repro:** re-upload the same CSV; row count must not double.

19. **Malformed rows / extra columns / bad encoding.**
    - **Mitigation:** `rescuedDataColumn` keeps bad data instead of dropping;
      `addNewColumns` absorbs new columns.
    - **Repro:** add a junk column / a row with too many fields.

## E. Cross-cutting

20. **Poison object fails the whole batch.**
    - **Mitigation:** per-object try/except in every ingestion job — one object's
      failure is logged + alerted, others continue; pipeline-level retry
      reprocesses only FAILED objects from `control.pipeline_runs`.

21. **Cost blow-up on a trial** — a job launches a big/always-on cluster.
    - **Mitigation:** cost-capped cluster policy (single-node, spot, 10-min
      auto-terminate); serverless SQL for SQL-only steps.

22. **Secret leakage** — a plan/state file committed.
    - **Mitigation:** gitleaks pre-commit + CI; `.gitignore` covers tfstate/tfvars/
      tfplan (see `SECURITY.md`). *(We actually caught and purged one.)*

23. **Small-file explosion** from many tiny CDC/CSV batches.
    - **Mitigation:** `optimizeWrite` + `autoCompact` + weekly OPTIMIZE; liquid
      clustering on facts.

24. **Timezone / currency inconsistency** — mixed TZ timestamps, multi-currency amounts.
    - **Mitigation:** normalize to UTC in Silver; FX dimension normalizes revenue
      to USD in Gold.

25. **Backfill vs incremental collision** — a manual backfill runs during the
    scheduled load.
    - **Mitigation:** run log + object-level status; backfill uses a separate
      watermark reset procedure, not the live one.

---

## How to demo failure handling quickly

```bash
# CDC delete + update + insert
docker exec -i v2-postgres psql -U postgres -d ecommerce < docker/generate_changes.sql
# REST 403: temporarily drop the UA header and re-run rest_to_bronze
# CSV dup: re-upload the same file; watch bronze count stay flat
# Poison object: set a bad endpoint in sources.yaml for one rest object; others still load
```

> As you implement, append new issues here with date + root cause + fix. This log
> is the most interview-valuable artifact in the repo.
