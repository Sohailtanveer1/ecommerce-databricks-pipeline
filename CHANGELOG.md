# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
