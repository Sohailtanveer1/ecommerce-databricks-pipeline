# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Azure industrialization
- Three ingestion sources: CSV in ADLS Gen2 (Auto Loader, `bronze_csv_autoloader.py`),
  PostgreSQL on Docker via ADF Copy over a Self-Hosted IR, and a keyless REST API
  (FX rates, `rest_api_fx.py`) that normalizes multi-currency revenue to USD in Gold.
- Azure Data Factory as batch orchestrator: datasets, `pl_ecommerce_batch` pipeline,
  daily tumbling-window trigger (`adf/`), with linked services/SHIR in Terraform.
- Terraform IaC in two layers: `foundation/` (RG, ADLS Gen2, Key Vault, Databricks
  workspace, ADF + SHIR + linked services, Log Analytics, RBAC, UC access connector)
  and `platform/` (UC storage credential, external locations, catalog/schemas,
  least-privilege grants, KV-backed secret scope, cost-capped cluster policy).
  `terraform validate` + `fmt` pass for both layers.
- Local Postgres source: `docker/` compose + seed schema/data + read-only role.
- Security/governance: managed-identity data access (no keys), Key Vault secrets,
  UC column masks + row filters (`sql/governance/unity_catalog_masks.sql`).
- CI: `terraform.yml` (fmt/validate/plan via OIDC).
- Docs: `RUNBOOK.md` (trial deploy), `INTERVIEW_PLAYBOOK.md`, `SECURITY_GOVERNANCE.md`,
  `TERRAFORM_CHANGE_EXERCISE.md` (update-in-place vs add vs replacement walkthrough),
  `infra/terraform/README.md`, `adf/README.md`; README rebuilt with architecture diagram.
- Terraform tunables (`log_retention_days`, `extra_containers`) so a routine change
  is a one-line tfvars edit for the change-management exercise.
- Model: `currency` flows Silver→Gold; `gold.dim_fx_rates` + USD-normalized MV.

### Added — validation & optimization
- Config-driven data-quality framework (`src/common/data_quality.py`): single-pass
  row-level + dataset-level checks (not-null, range, allowed-values, regex, unique,
  min-row-count, freshness, referential integrity) with fail/warn/quarantine modes;
  rules declared in `config/pipeline_config.yaml`; quarantine side tables in DDL.
- Optimization layer: tuned Spark session (`src/common/spark_session.py`: AQE,
  skew join, dynamic partition pruning, auto-broadcast); Delta self-optimizing
  table properties + liquid clustering on fact tables (`sql/ddl_all_layers.sql`);
  weekly maintenance job (`src/maintenance/table_maintenance.py`: OPTIMIZE/ANALYZE/VACUUM)
  wired into the bundle (`resources/maintenance_job.yml`).
- Removed `.count()` anti-patterns in ingestion/bronze/gold (cache-once, MERGE
  metrics, `isEmpty()`, broadcast anti-join).
- Docs: `docs/DATA_QUALITY.md`, `docs/OPTIMIZATION.md`; tests: `tests/test_data_quality.py`.
- CI: `setup-java` so PySpark tests run.

### Added
- Dependency management: `requirements.txt` / `requirements-dev.txt`, `pyproject.toml`.
- Linting & formatting: Ruff + Black config, `.pre-commit-config.yaml`.
- CI (`.github/workflows/ci.yml`): lint, format check, unit tests + coverage, bundle validation.
- CD (`.github/workflows/deploy.yml`): staging deploy on `main`, prod deploy on `vX.Y.Z` tags.
- Databricks Asset Bundle (`databricks.yml` + `resources/`) with dev / staging / prod targets.
- Repo hygiene: `.gitignore`, `LICENSE`, `CONTRIBUTING.md`, PR template, `Makefile`, `VERSION`.
- Tests: data-quality-gate coverage (negative amount rejected, clean batch passes).

### Fixed
- Silver: removed the misleading `cust_id`→`customer_id` rename (a no-op against
  the actual Bronze schema, which already uses `customer_id`); tests now match
  the real schema.
- Ingestion: hardened the orders batch extract against SQL injection —
  parameterized the watermark lookup predicate and validate the watermark is a
  real timestamp before it is pushed into the JDBC query.

## [0.1.0] - 2026-08-26

### Added
- Initial Medallion pipeline: batch + Auto Loader ingestion, Bronze/Silver/Gold layers,
  SCD Type 2 dimensions, materialized views, Unity Catalog governance, dedup unit tests.

[Unreleased]: https://example.com/compare/v0.1.0...HEAD
[0.1.0]: https://example.com/releases/tag/v0.1.0
