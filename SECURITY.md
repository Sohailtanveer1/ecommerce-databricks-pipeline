# Security

## Secrets handling — the rules

**No credential is ever committed to this repository.** Secrets live only in:

- **Azure Key Vault** — Postgres username/password/JDBC URL, API keys. ADF reads
  them via managed identity + Key Vault-referenced linked services; Databricks
  reads them via a Key Vault-backed secret scope. (`infra/terraform/foundation/keyvault.tf`)
- **`*.tfvars`** (e.g. `dev.tfvars`) — local only, **gitignored**. Copy from
  `dev.tfvars.example` and fill in. Never commit it.
- **Environment variables / CI secrets** — CI auth uses GitHub OIDC (no stored
  long-lived cloud credential); the deploy workflows read `DATABRICKS_*` /
  `AZURE_*` from GitHub **Environment secrets**.

Data-plane access uses **managed identities** (Databricks Access Connector, ADF
system-assigned identity) with RBAC — **no account keys or SAS tokens** on the
data path. The pipeline's DB user (`ecom_reader`) is **SELECT-only**.

## What is gitignored (never commit)

`*.tfvars` (except `*.example`), `*.tfstate*`, `*.tfplan*`, `.terraform/`,
`.env*`, `*.pem`/`*.key`/`*.pfx`, `.databrickscfg`, `secrets.*`. See `.gitignore`.

> Terraform **state** and **plan** files can contain secrets in plaintext — they
> are gitignored. For shared/CI use, store state in the azurerm remote backend
> (stubbed in `infra/terraform/*/versions.tf`), which encrypts and locks it.

## Automated protection

- **pre-commit**: `gitleaks` + `detect-private-key` block secrets before a commit
  is created. Enable with `pre-commit install` (see `CONTRIBUTING.md`).
- **CI**: `.github/workflows/secret-scan.yml` runs gitleaks on every PR/push over
  the full history as a backstop.
- Config/allowlist for known-safe placeholders: `.gitleaks.toml`.

## If a secret is ever exposed

1. **Rotate it immediately** (Key Vault secret, DB password, API key, token).
2. Purge it from history (`git filter-repo` / BFG) and force-push.
3. Revoke any Databricks PAT: workspace → Settings → Developer → Access tokens.

## Reporting

Found a vulnerability? Open a private security advisory on the GitHub repo, or
contact the maintainer directly — please don't file a public issue.
