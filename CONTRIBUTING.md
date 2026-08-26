# Contributing

## Local setup

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pre-commit install
```

## Branching & versioning

- Branch off `main`: `feat/<short-name>`, `fix/<short-name>`, `docs/<short-name>`.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`).
- The project uses [Semantic Versioning](https://semver.org/). Bump `VERSION`
  and update `CHANGELOG.md` in the same PR as any user-facing change.

## Before opening a PR

```bash
make lint    # ruff + black --check
make test    # pytest with coverage
make validate  # databricks bundle validate -t dev
```

CI runs the same checks; PRs must be green to merge.

## Releasing

1. Merge to `main` — CI auto-deploys the bundle to **staging**.
2. Tag the release: `git tag v0.2.0 && git push --tags`.
3. The tag triggers the **prod** deploy (gated by required reviewers on the
   `prod` GitHub Environment).

## Secrets

Never commit credentials. Source DB / API secrets live in a Key Vault– or
Secrets Manager–backed Databricks secret scope and are read at runtime via
`dbutils.secrets.get(...)`. CI/CD auth uses GitHub Environment secrets.
