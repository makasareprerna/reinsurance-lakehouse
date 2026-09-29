# Shortcuts. Run `make help` to list them.
.PHONY: help setup test lint simulate dbt status docs clean

help:
	@grep -E '^[a-z]+:.*## ' Makefile | sed 's/:.*## /\t/'

setup: ## install the project and dev tools
	pip install -e . -r requirements-dev.txt

lint: ## check code style
	ruff check src tests

test: ## run unit and end-to-end tests
	pytest

simulate: ## generate and load 14 days, running dbt after each day
	relake simulate --days 14 --with-dbt

dbt: ## build gold models and run dbt tests
	cd dbt && dbt build --profiles-dir .

status: ## row counts per table and recent runs
	relake status

docs: ## generate and serve dbt docs (lineage graph)
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

clean: ## delete all generated data
	rm -rf data dbt/target dbt/logs spark-warehouse metastore_db
