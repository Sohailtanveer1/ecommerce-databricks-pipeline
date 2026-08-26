.PHONY: help install lint format test validate deploy-dev clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install:  ## Install dev dependencies + git hooks
	pip install -r requirements-dev.txt
	pre-commit install

lint:  ## Ruff lint + Black format check
	ruff check .
	black --check .

format:  ## Auto-fix lint + format
	ruff check --fix .
	black .

test:  ## Run unit tests with coverage
	pytest --cov --cov-report=term-missing

validate:  ## Validate the Databricks Asset Bundle (dev target)
	databricks bundle validate -t dev

deploy-dev:  ## Deploy the bundle to your dev workspace
	databricks bundle deploy -t dev

clean:  ## Remove caches and local Spark artifacts
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov spark-warehouse metastore_db derby.log
	find . -type d -name __pycache__ -exec rm -rf {} +
