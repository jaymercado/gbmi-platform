# Developer and operator commands (blueprint 9.1). Written for GNU Make 3.81, which is what
# macOS ships. Targets are added by the work package that makes them real.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Laptop, image and CI use uv-managed CPython; release binaries live in ~/.local/bin.
export UV_PYTHON_PREFERENCE := only-managed
export PATH := $(HOME)/.local/bin:$(PATH)

SQL_PATHS := dbt/models tests/integration/dbt/project/models

# Compose files live in compose/, so the project directory and the env file are always passed
# explicitly; otherwise relative paths and .env would resolve from compose/.
COMPOSE := docker compose --project-directory . --env-file .env -f compose/compose.yaml
COMPOSE_BI := $(COMPOSE) -f compose/compose.bi.yaml

# On Linux, containers write bind-mounted files as the calling user (CI runners included).
ifeq ($(shell uname -s),Linux)
export AIRFLOW_UID := $(shell id -u)
endif

# LIMITS=off removes every memory limit (used once, for the unconstrained idle measurement).
ifeq ($(LIMITS),off)
export MEM_AIRFLOW_APISERVER := 0
export MEM_AIRFLOW_SCHEDULER := 0
export MEM_AIRFLOW_DAG_PROCESSOR := 0
export MEM_AIRFLOW_TRIGGERER := 0
export MEM_POSTGRES := 0
export MEM_CLICKHOUSE := 0
export MEM_METABASE := 0
export CH_MAX_SERVER_MEMORY_USAGE := 0
endif

# The Glue/S3 halves of the integration tests run only on request:
#   eval "$(aws configure export-credentials --profile gbmi --format env)"
#   GBMI_TEST_AWS=1 GBMI_TEST_AWS_LAKE_BUCKET=<lake bucket> make integration
INTEGRATION_MARK := integration and not aws
ifeq ($(GBMI_TEST_AWS),1)
INTEGRATION_MARK := integration
endif

# The fixture hour of the demo: four export slots, 2026-11-25 08:00 to 08:45 UTC.
DEMO_HOUR := 2026-11-25T08
TOOLBOX := $(COMPOSE_BI) --profile tools run --rm toolbox

.PHONY: help setup fmt lint test env images up up-bi down reset ps logs smoke toolbox integration \
	dbt-build demo e2e logs-dump

help: ## List the available targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-14s %s\n", $$1, $$2}'

setup: ## Create the virtual environments and install the Git hooks
	uv sync
	uv sync --project dbt
	uv run python compose/make_env.py
	uv run pre-commit install

fmt: ## Format and auto-fix Python code
	uv run ruff format .
	uv run ruff check --fix .

lint: ## ruff, mypy, sqlfluff and lock-file checks
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy
	@paths=""; for p in $(SQL_PATHS); do if [ -d "$$p" ]; then paths="$$paths $$p"; fi; done; \
	if [ -n "$$paths" ]; then echo "uv run --project dbt sqlfluff lint$$paths"; uv run --project dbt sqlfluff lint $$paths; \
	else echo "sqlfluff: no SQL models yet"; fi
	uv lock --check
	uv lock --check --project dbt

test: ## Unit and transformation tests with coverage
	uv run pytest tests/unit tests/transforms --cov --cov-report=term-missing

env: ## Create or complete .env with generated secrets
	@command -v uv >/dev/null && uv run python compose/make_env.py || python3 compose/make_env.py

images: ## Build both image targets (dev for Compose, runtime for deployment)
	docker build --file docker/airflow/Dockerfile --target dev --tag gbmi-airflow:dev .
	docker build --file docker/airflow/Dockerfile --target runtime --tag gbmi-airflow:runtime \
		--build-arg GIT_SHA=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown) .

up: ## Start the core profile and wait until it is healthy
	@test -f .env || $(MAKE) env
	@mkdir -p data/lake tests/fixtures
	$(COMPOSE) up --detach --build --wait

up-bi: ## Start the core profile plus Metabase
	@test -f .env || $(MAKE) env
	@mkdir -p data/lake tests/fixtures
	$(COMPOSE_BI) up --detach --build --wait

down: ## Stop the stack and keep its volumes
	$(COMPOSE_BI) --profile tools down --remove-orphans

reset: ## Stop the stack and delete its volumes and data/
	$(COMPOSE_BI) --profile tools down --volumes --remove-orphans
	rm -rf data dbt/target dbt/logs

ps: ## Show the state of the services
	$(COMPOSE_BI) ps --all

logs: ## Follow the logs (S=<service> for one service)
	$(COMPOSE_BI) logs --follow --tail 100 $(S)

logs-dump: ## Print the last lines of every service log (used by CI when a run fails)
	$(COMPOSE_BI) ps --all
	$(COMPOSE_BI) logs --no-color --tail 200

smoke: ## Check the running stack (health, pools, versions, shared lake)
	bash compose/smoke.sh

toolbox: ## Run a command on the Compose network: make toolbox CMD="gbmi config show"
	$(COMPOSE) --profile tools run --rm toolbox $(CMD)

integration: ## Integration tests on the Compose network (spikes SK1 and SK2); needs make up
	$(COMPOSE) --profile tools run --rm toolbox \
		pytest tests/integration -m "$(INTEGRATION_MARK)" -p no:cacheprovider $(PYTEST_ARGS)

dbt-build: ## Build and test every dbt model against the Compose ClickHouse (full refresh)
	$(TOOLBOX) dbt build --project-dir dbt --profiles-dir dbt/profiles --full-refresh

demo: ## The whole pipeline on committed fixtures, with one chart; needs no cloud account
	$(MAKE) up-bi
	$(MAKE) smoke
	@echo "== tracer DAG on the fixture hour $(DEMO_HOUR)"
	$(TOOLBOX) python -m tests.e2e.airflow --hour $(DEMO_HOUR) --source fixture
	@echo "== Metabase: connection, question and dashboard"
	$(TOOLBOX) python -m monitoring.metabase.provision
	@echo "== end-to-end assertions"
	$(TOOLBOX) pytest tests/e2e -m e2e -p no:cacheprovider
	@echo
	@echo "Demo passed. Dashboard: http://127.0.0.1:3000  (sign in with METABASE_ADMIN_EMAIL"
	@echo "and METABASE_ADMIN_PASSWORD from .env). Airflow: http://127.0.0.1:8080"
	@if [ -t 1 ] && [ -z "$CI" ] && command -v open >/dev/null; then open http://127.0.0.1:3000; fi

e2e: ## End-to-end assertions against a stack on which the demo has run
	$(TOOLBOX) pytest tests/e2e -m e2e -p no:cacheprovider $(PYTEST_ARGS)

