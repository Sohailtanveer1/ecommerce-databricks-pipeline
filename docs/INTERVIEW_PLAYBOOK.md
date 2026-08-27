# Interview Playbook — Defending Every Decision

Questions an interviewer is likely to ask about this project, with tight,
defensible answers. Grouped by theme. Skim the **bold** claim, read the *why*.

---

## Architecture & design

**Q: Walk me through the architecture.**
Three sources feed a Medallion (Bronze→Silver→Gold) lakehouse on Databricks/Delta.
ADF is the batch orchestrator: it copies the JDBC source and lands files, then
triggers Databricks jobs for the transformations. Governance is Unity Catalog;
infra is Terraform; CI/CD is GitHub Actions. Serving is materialized views over
Gold for BI.

**Q: Why Medallion / why three layers?**
Separation of concerns and replayability. Bronze is the immutable raw record — if
a Silver bug is found, I replay from Bronze without re-pulling sources. Silver
centralizes cleaning/dedup/validation so Gold never re-implements it. Gold is
consumption-shaped (dimensional model), no raw/PII leakage.

**Q: Why ADF *and* Databricks — isn't that redundant?**
Different jobs. ADF is the control plane and the connector layer — it reaches the
on-prem/local DB via the Self-Hosted IR, does managed Copy activities, and
orchestrates with retries/monitoring/alerting. Databricks is the compute/transform
engine. Using ADF to *copy* and Databricks to *transform* is a very common,
defensible split. (I can also run it Databricks-native via the Asset Bundle job —
kept for portability.)

**Q: Why three sources, and why those three?**
To prove I can handle the three real ingestion shapes: (1) **files** landing in a
lake (CSV → Auto Loader), (2) a **transactional DB** (Postgres via JDBC/Copy), and
(3) an **API** (REST FX rates). They're not arbitrary — FX normalizes the
multi-currency orders to USD in Gold, so each source has a business reason.

## Ingestion

**Q: How do you reach a database on your laptop from cloud ADF?**
A **Self-Hosted Integration Runtime**. The cloud IR can't see `localhost`; the SHIR
is an agent on the DB host that makes the outbound connection, so no inbound
firewall hole. This is exactly how you'd reach an on-prem ERP in production.

**Q: Batch vs CDC for the DB?**
Batch incremental on an `updated_at` watermark (tumbling-window bounds from the
trigger). CDC (Debezium/Fivetran) adds real cost/ops overhead; unjustified at a
small company's volume unless near-real-time order visibility is a hard
requirement. I documented CDC as the 10x-scale upgrade path.

**Q: Why Auto Loader for CSV instead of a plain read?**
Incremental file discovery via checkpoint (exactly-once, no "which files did I
load?" bookkeeping), schema inference + evolution, and rescued-data capture so a
malformed column never silently drops rows. `availableNow` trigger makes it a
cheap scheduled sweep, not an always-on stream.

**Q: How is the API ingestion incremental / idempotent?**
Fetch by date; land raw JSON to `landing/fx/<date>/` (immutable, replayable);
append typed rows to Bronze with ingestion metadata. Re-running a date overwrites
the same raw path and the Gold join is by (currency, date), so it's idempotent at
the reporting layer.

## Delta Lake & modeling

**Q: How do you dedup, and why not `dropDuplicates()`?**
`ROW_NUMBER()` partitioned by the business key, ordered by a recency timestamp,
filtered to rank 1 — deterministically keeps the latest version.
`dropDuplicates()` keeps an arbitrary row, so a stale update can win.

**Q: SCD Type 1 vs Type 2?**
Type 2 on dimensions whose history matters (product category, customer segment):
each version carries effective start/end + `is_current`, so "revenue by category,
June" stays correct even after a September re-classification. Facts join to the
dimension version active on the order date.

**Q: Partitioning strategy?**
Liquid clustering (`CLUSTER BY`) on fact tables instead of static date
partitioning + a separate ZORDER. Low-volume date partitions create a small-file
problem; liquid clustering keeps files right-sized and skippable, and the keys can
change without a rewrite.

**Q: How do you keep file sizes healthy / tables fast?**
Write-time: `optimizeWrite` + `autoCompact` + `tuneFileSizesForRewrites` +
deletion vectors on every table. Scheduled: a weekly OPTIMIZE/ANALYZE/VACUUM job.
Session: AQE, skew-join handling, dynamic partition pruning, auto-broadcast for
dim joins. See `docs/OPTIMIZATION.md`.

## Data quality

**Q: How do you validate data?**
A config-driven framework (`src/common/data_quality.py`): not-null, range,
allowed-values, regex, uniqueness, row-count floor, freshness, referential
integrity — declared in YAML. Single-pass evaluation (one `_dq_errors` column),
not one scan per rule.

**Q: What happens to bad rows?**
Per-dataset mode: `fail` (stop), `warn` (log, pass), or `quarantine` (route to a
`*_quarantine` side table, publish the rest). Orders quarantine — so a spike in
`silver.orders_quarantine` is an alert, not silent data loss.

## Security & governance

**Q: Where do secrets live?**
Azure Key Vault, only. ADF linked services reference KV secrets; Databricks reads
them via a KV-backed secret scope. No credential in code, linked-service bodies,
or Terraform state where avoidable. Postgres password, JDBC URL, API keys — all KV.

**Q: How does Databricks access the lake without keys?**
A managed identity (Databricks Access Connector) granted Storage Blob Data
Contributor; Unity Catalog uses it via a storage credential + external locations.
No account keys or SAS on the data path. ADF uses its own managed identity the
same way.

**Q: How do you protect PII / restrict access?**
Unity Catalog: schema-level grants (analysts get Gold only; Bronze/Silver default-
deny), column masks on email/phone (masked unless in `pii-authorized`), and a row
filter on `fact_orders` by region. Lineage and `system.access.audit` are automatic.

**Q: Least privilege in the DB?**
The pipeline connects as `ecom_reader` — SELECT-only, no write/DDL on the source.

## Infrastructure as Code

**Q: Why two Terraform layers (foundation vs platform)?**
Separate state = separate blast radius. Azure resources (foundation) change rarely;
Unity Catalog/workspace objects (platform) change more often. Splitting them means
a platform mistake can't corrupt the foundation state, and each can be planned/
applied independently.

**Q: Terraform state — where and why?**
Local for the trial; an azurerm backend (versioned, locked blob) for anything
shared — commented in `versions.tf`. State can contain secrets, so it's gitignored
and would be encrypted-at-rest in the backend.

**Q: How is this multi-environment?**
One parameterized root + `dev.tfvars`; prod is a `prod.tfvars` + a separate state,
no code fork. I kept it dev-only to stay in trial limits while showing the
promotion path.

**Q: How do you change a live resource safely?**
Change a variable in `tfvars` (not the resource block), open a PR, CI runs
`terraform plan`, a human reads the diff, `apply` runs on merge. I always
`plan -out` then `apply` that saved plan. I distinguish **update-in-place** (`~`)
from **replacement** (`-/+ ... forces replacement`) — the latter is a stop-and-
think on any stateful resource. Full worked example (retention change, adding a
container, handling a replacement, rollback): `docs/TERRAFORM_CHANGE_EXERCISE.md`.

## CI/CD

**Q: Describe the pipelines.**
GitHub Actions: (1) **CI** — ruff/black/pytest+coverage and `databricks bundle
validate` on every PR; (2) **Terraform** — fmt/validate/plan on PR, apply on main
via OIDC (no stored cloud secret); (3) **Deploy** — bundle to staging on main,
prod on a `vX.Y.Z` tag. ADF artifacts publish via ADF Git integration.

**Q: How do you avoid storing cloud credentials in CI?**
OIDC federated identity — GitHub gets a short-lived token from Entra ID per run;
nothing long-lived is stored.

## Cost & scale

**Q: How do you keep this cheap?**
Job clusters (spin up → run → terminate), a cluster policy forcing spot + 20-min
auto-terminate, LRS storage, serverless-friendly patterns, weekly (not continuous)
maintenance, materialized views only on hot dashboards. Idle cost is ~storage cents.

**Q: What changes at 10x / 100x?**
CDC for orders if extract windows collide with prod load; finer clustering; a
dedicated orchestrator only if DAG complexity outgrows ADF; streaming trigger if
latency tightens; ZRS/GRS + private endpoints for prod hardening. Documented in
`docs/ARCHITECTURE.md`.

## Reliability & operations

**Q: Idempotency / re-runs?**
MERGE upserts on business keys (re-running a batch converges, no dupes); Auto
Loader + streaming checkpoints never reprocess consumed files; watermark bounds
make the JDBC copy re-runnable for a window.

**Q: How do you monitor and alert?**
ADF diagnostics + Key Vault audit → Log Analytics; ADF activity retries +
on-failure email; DQ summaries per run; the quarantine table as a data-health
signal. Delta history/`DESCRIBE HISTORY` for row-level lineage of writes.

**Q: A downstream bug corrupted Silver — recovery?**
Fix the logic, then replay from Bronze (immutable) — or Delta time-travel /
`RESTORE` Silver to a prior version. No source re-pull needed.
