# Developer and operator commands (blueprint 9.1). Written for GNU Make 3.81, which is what
# macOS ships. Targets are added by the work package that makes them real.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Laptop, image and CI use uv-managed CPython; release binaries live in ~/.local/bin.
export UV_PYTHON_PREFERENCE := only-managed
export PATH := $(HOME)/.local/bin:$(PATH)

SQL_PATHS := dbt/models dbt/tests tests/integration/dbt/project/models

# Two stacks on one laptop (week 2 plan 3.3). The working stack (Compose project gbmi, lake
# data/lake) holds real data. The demo stack (project gbmi-demo, lake data/demo/lake, volumes of
# its own) runs make demo and the end-to-end tests on the committed fixtures. Both stacks publish
# the same ports, so one stack runs at a time. STACK selects the stack; make demo and make e2e
# always use the demo stack.
WORKING_STACK := gbmi
DEMO_STACK := gbmi-demo
STACK ?= $(WORKING_STACK)
ifeq ($(STACK),$(WORKING_STACK))
LAKE_DIR := data/lake
OTHER_STACK := $(DEMO_STACK)
DAG_SCHEDULES := on
# The working stack runs the schedules on live data: a task which fails is tried again, three
# times, as the blueprint says (8.1). A transient error must not fail a day of a backfill.
TASK_RETRIES := 3
# The working stack asks the live SEC, with the declared user agent of .env, until the first
# deployment: then the AWS host is the only place with the source "live" (week 3 plan 3.3).
SEC_SOURCE := live
# The pipelines read the first 50 rows of the seed of the watchlist (week 3 plan 3.3).
WATCHLIST_SIZE := 50
else ifeq ($(STACK),$(DEMO_STACK))
LAKE_DIR := data/demo/lake
OTHER_STACK := $(WORKING_STACK)
# The demo stack runs its DAGs by hand only, on the recorded files and with a fixed clock. An
# unpaused DAG must not start a run against the live source by the clock (dags/common).
DAG_SCHEDULES := off
# On the demo stack and in CI a failing task shows at once.
TASK_RETRIES := 0
# The demo stack and CI read the recorded SEC files: no network and no user agent.
SEC_SOURCE := fixture
# The recorded files hold the documents of the five demo companies: ranks 1 to 5 of the seed.
WATCHLIST_SIZE := 5
else
$(error STACK is "$(STACK)"; use $(WORKING_STACK) for the working stack or $(DEMO_STACK) for the demo stack)
endif
# compose/compose.yaml mounts the lake from GBMI_LAKE_DIR. Scripts which build a Compose command
# of their own (compose/smoke.sh) read STACK.
export STACK
export COMPOSE_PROJECT_NAME := $(STACK)
export GBMI_LAKE_DIR := ./$(LAKE_DIR)
export GBMI_DAG_SCHEDULES := $(DAG_SCHEDULES)
export GBMI_TASK_RETRIES := $(TASK_RETRIES)
# The stack alone decides the source of the SEC client: neither a value of the shell nor a
# value on the command line of make changes the source. Live requests leave from the working
# stack only, so one database remembers a block by the SEC (review 1 of week 3, finding B2).
override GBMI_SEC_SOURCE := $(SEC_SOURCE)
export GBMI_SEC_SOURCE
export GBMI_WATCHLIST_SIZE := $(WATCHLIST_SIZE)

# Compose files live in compose/, so the project directory and the env file are always passed
# explicitly; otherwise relative paths and .env would resolve from compose/.
COMPOSE := docker compose --project-name $(STACK) --project-directory . --env-file .env -f compose/compose.yaml
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

# The profile of the AWS CLI which hands out the short-lived credentials of "aws login".
AWS_PROFILE ?= gbmi
TF := infra/terraform
TF_ROOTS := $(TF)/bootstrap $(TF)/envs/prod
TF_MODULES := $(TF)/modules/lake_s3 $(TF)/modules/glue_catalog $(TF)/modules/budgets \
	$(TF)/modules/network $(TF)/modules/iam $(TF)/modules/ec2_host $(TF)/modules/monitoring

# The Glue/S3 halves of the integration tests run only on request. Credentials from `aws login`
# last 15 minutes, less than the AWS halves take, so they run one test at a time:
#   GBMI_TEST_AWS_LAKE_BUCKET=<lake bucket> make integration-aws
# With longer-lived credentials in the environment everything can run at once:
#   GBMI_TEST_AWS=1 GBMI_TEST_AWS_LAKE_BUCKET=<lake bucket> make integration
INTEGRATION_MARK := integration and not aws
ifeq ($(GBMI_TEST_AWS),1)
INTEGRATION_MARK := integration
endif

# The fixture hour of the demo: four export slots, 2026-11-25 08:00 to 08:45 UTC.
DEMO_HOUR := 2026-11-25T08
TOOLBOX := $(COMPOSE_BI) --profile tools run --rm toolbox

.PHONY: help setup fmt lint test test-dags env images up up-bi down reset ps logs smoke toolbox psql integration \
	integration-aws dbt-build demo e2e drill bench logs-dump tf-check plan redact-check host-secrets host-sec-source \
	host-rehearsal host-status host-deploy-log host-shell host-forward deploy _other-stack-stopped _lake-dir _demo _e2e

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

# The ledger tests work in throwaway databases of the PostgreSQL which listens on 127.0.0.1:5432:
# the running stack on the laptop, with the administrator password from .env, or the service
# container of the workflow ci, which sets GBMI_TEST_PG_ADMIN_DSN itself. The coverage gate
# covers the parsers, the ledger and the SEC source (week 2 plan 3.3; week 3 plan 3.3).
COVERAGE_GATE := src/gbmi/ledger/*,src/gbmi/core/*,src/gbmi/transforms/duckdb/*,src/gbmi/sources/sec/*

test: ## Unit, ledger, transformation and contract tests with the coverage gate; needs a reachable PostgreSQL
	@export GBMI_TEST_PG_ADMIN_DSN="$${GBMI_TEST_PG_ADMIN_DSN:-postgresql://postgres:$$(sed -n 's/^POSTGRES_PASSWORD=//p' .env 2>/dev/null)@127.0.0.1:5432/postgres}"; \
	echo "uv run pytest tests/unit tests/ledger tests/transforms tests/contract --cov --cov-report=term-missing"; \
	uv run pytest tests/unit tests/ledger tests/transforms tests/contract --cov --cov-report=term-missing
	@echo "== coverage gate: gbmi.ledger, gbmi.core, gbmi.transforms.duckdb and gbmi.sources.sec together, at least 80%"
	uv run coverage report --include='$(COVERAGE_GATE)' --fail-under=80

redact-check: ## Search the repository for the AWS account number and the addresses of the local files
	@python3 tools/redact_check.py

# --- the AWS host (week 3 plan, WP-12) ---------------------------------------------------------
# Every target needs a sign-in (aws login --remote --profile gbmi-login; see
# infra/terraform/README.md) and uses the profile gbmi. No target prints a value of a secret.
HOST_AWS := AWS_PROFILE=$(AWS_PROFILE) AWS_DEFAULT_REGION=us-east-1

host-secrets: ## Create the missing parameters of the AWS host below /gbmi/prod/ (never overwrites; prints names only)
	@$(HOST_AWS) uv run python tools/host_secrets.py create
	@$(HOST_AWS) uv run python tools/host_secrets.py list

host-sec-source: ## Set the SEC source of the host: make host-sec-source SOURCE=live (the laptop has to be off first) or SOURCE=off
	@test -n "$(SOURCE)" || { echo 'make host-sec-source SOURCE=off  or  SOURCE=live'; exit 1; }
	@$(HOST_AWS) uv run python tools/host_secrets.py sec-source $(SOURCE)

# The one target of the block without a sign-in: the first boot and the deployments of the
# host in a container of Amazon Linux 2023 on the laptop (week 3 plan, change C16 b). Run it
# before a change of the user data or of a host script reaches an instance.
host-rehearsal: ## Rehearse the user data and the host scripts in an Amazon Linux 2023 arm64 container (no AWS access)
	@bash tools/host_rehearsal/run.sh

host-status: ## The AWS host: instance, agent, deployed release, services
	@AWS_PROFILE=$(AWS_PROFILE) tools/host.sh status

host-deploy-log: ## The status and the output of the newest deployment on the AWS host
	@AWS_PROFILE=$(AWS_PROFILE) tools/host.sh deploy-log

host-shell: ## A shell on the AWS host through Session Manager
	@AWS_PROFILE=$(AWS_PROFILE) tools/host.sh shell

host-forward: ## Forward a port of the host to the laptop: make host-forward (Airflow, 8080 to 18080) or PORT=3000 (Metabase)
	@AWS_PROFILE=$(AWS_PROFILE) tools/host.sh forward $(or $(PORT),8080)

deploy: ## Start the workflow deploy for the head of main; Jay approves the deployment in GitHub
	@git fetch origin main --quiet
	gh workflow run deploy.yml --ref main -f commit=$$(git rev-parse origin/main)
	@echo "The workflow waits for the approval of the environment prod (Jay, in GitHub)."
	@echo "Follow the run:  gh run list --workflow deploy.yml --limit 1"

env: ## Create or complete .env with generated secrets
	@command -v uv >/dev/null && uv run python compose/make_env.py || python3 compose/make_env.py

images: ## Build both image targets (dev for Compose, runtime for deployment) and check the runtime image
	docker build --file docker/airflow/Dockerfile --target dev --tag gbmi-airflow:dev .
	@# The image carries the 40 characters of the commit as its pipeline version (week 3 plan,
	@# exit-gate row 13). The workflow "images" builds and pushes the image of a commit of main.
	commit=$$(git rev-parse HEAD 2>/dev/null || echo unknown); \
	docker build --file docker/airflow/Dockerfile --target runtime --tag gbmi-airflow:runtime \
		--build-arg GIT_SHA=$$commit . \
	&& docker/airflow/check_runtime.sh gbmi-airflow:runtime $$commit

# A target which starts containers stops here while the other stack runs: both stacks publish
# the same ports, and fixtures must never reach the ledger and the lake of the working stack.
_other-stack-stopped:
	@if [ -n "$$(docker ps --quiet --filter label=com.docker.compose.project=$(OTHER_STACK))" ]; then \
		echo "The stack $(OTHER_STACK) is running, and both stacks publish the same ports."; \
		echo "Stop the stack $(OTHER_STACK) first:  make down STACK=$(OTHER_STACK)"; \
		exit 1; \
	fi

# The caller creates the lake directory: on Linux, Docker would create a missing bind-mount
# source as root, and the containers could not write to the directory.
_lake-dir:
	@mkdir -p $(LAKE_DIR) tests/fixtures

up: _other-stack-stopped _lake-dir ## Start the core profile of the stack STACK and wait until it is healthy
	@test -f .env || $(MAKE) env
	$(COMPOSE) up --detach --build --wait

up-bi: _other-stack-stopped _lake-dir ## Start the core profile plus Metabase
	@test -f .env || $(MAKE) env
	$(COMPOSE_BI) up --detach --build --wait

down: ## Stop the stack STACK and keep its volumes and its lake
	$(COMPOSE_BI) --profile tools down --remove-orphans

reset: ## Stop the stack STACK and delete its volumes and its lake (the working stack asks for CONFIRM=yes)
ifeq ($(STACK),$(WORKING_STACK))
	@if [ "$(CONFIRM)" != "yes" ]; then \
		echo "make reset deletes the volumes and the lake of the working stack: the ledger and every"; \
		echo "downloaded and built file under $(LAKE_DIR)."; \
		echo "To delete the working stack:  make reset CONFIRM=yes"; \
		echo "To delete the demo stack:     make reset STACK=$(DEMO_STACK)"; \
		exit 1; \
	fi
	@bash compose/circuit_guard.sh
endif
	$(COMPOSE_BI) --profile tools down --volumes --remove-orphans
	rm -rf $(LAKE_DIR)
	@if [ -z "$$(docker ps --quiet --filter label=com.docker.compose.project=$(OTHER_STACK))" ]; then \
		echo "rm -rf dbt/target dbt/logs"; rm -rf dbt/target dbt/logs; \
	fi

ps: ## Show the state of the services
	$(COMPOSE_BI) ps --all

logs: ## Follow the logs (S=<service> for one service)
	$(COMPOSE_BI) logs --follow --tail 100 $(S)

logs-dump: ## Print the last lines of every service log (used by CI when a run fails)
	$(COMPOSE_BI) ps --all
	$(COMPOSE_BI) logs --no-color --tail 200

smoke: ## Check the running stack (health, pools, versions, shared lake)
	bash compose/smoke.sh

toolbox: _other-stack-stopped _lake-dir ## Run a command on the Compose network: make toolbox CMD="gbmi config show"
	$(TOOLBOX) $(CMD)

# The runbooks read the ledger with SQL. The statement goes through an environment variable, so
# double quotes and dollar signs in the statement reach psql unchanged.
psql: ## Run one SQL statement on the operational database of the stack: make psql SQL="SELECT ..."
	@if [ -z "$$SQL" ]; then echo 'make psql SQL="SELECT ...": the statement is missing'; exit 1; fi
	@$(COMPOSE) exec -T postgres psql --username postgres --dbname gbmi --command "$$SQL"

# A drill injects a fault, runs the documented recovery and checks the outcome (blueprint 9.8).
# Each drill works in a throwaway database and in a lake directory of its own under _test/.
DRILL_2 := dr2_crash_silver
DRILL_4 := dr4_corrupt_file
DRILL_5 := dr5_sec_throttling
DRILL_8 := dr8_rerun_silver
drill: _other-stack-stopped _lake-dir ## Run a failure drill on the recorded files: make drill DR=2 (2, 4, 5 or 8); needs make up
	@if [ -z "$(DRILL_$(DR))" ]; then echo "make drill DR=<n>: n is 2 (crash), 4 (corrupt file), 5 (SEC throttling) or 8 (rerun)"; exit 1; fi
	$(TOOLBOX) python -m drills.$(DRILL_$(DR)) $(ARGS)

# The benchmarks of blueprint 15.6 which phase P1 measures. A benchmark needs the real days of
# the working stack; B1 and B2 also need the three GDELT DAGs paused. Each script writes the
# raw result beside the script, under benchmarks/. More scripts and the method: the report
# docs/reports/benchmark-v1.md.
BENCH_DAY ?= 2026-11-04
BENCH_1 := benchmarks.b1_ingest.throughput --day $(BENCH_DAY) --from-hour 12
BENCH_2 := benchmarks.b2_transform.rebuild_day --day $(BENCH_DAY) --runs 3
BENCH_4 := benchmarks.b4_storage.storage --day $(BENCH_DAY)

bench: _other-stack-stopped _lake-dir ## Run a benchmark on the working stack: make bench B=4 (1 ingestion, 2 transformation, 4 storage)
	@if [ -z "$(BENCH_$(B))" ]; then echo "make bench B=<n>: n is 1 (ingestion), 2 (transformation) or 4 (storage)"; exit 1; fi
	$(TOOLBOX) python -m $(BENCH_$(B)) $(ARGS)

# The Airflow environment of the image has no pytest, and no package is added to the Airflow
# environment without need: the DAG tests use unittest and the interpreter of Airflow.
test-dags: _other-stack-stopped _lake-dir ## DAG tests with the interpreter of Airflow inside the image; needs make up
	$(TOOLBOX) /home/airflow/.local/bin/python -m unittest discover --start-directory tests/dags --top-level-directory .

integration: _other-stack-stopped _lake-dir ## Integration tests on the Compose network (spikes SK1 to SK3, ingestion, silver build); needs make up
	$(TOOLBOX) pytest tests/integration -m "$(INTEGRATION_MARK)" -p no:cacheprovider $(PYTEST_ARGS)

integration-aws: ## The Glue/S3 halves of the spikes, one test at a time with fresh credentials; needs make up and aws login
	tests/integration/run_aws.sh

dbt-build: _other-stack-stopped _lake-dir ## Build and test every dbt model against the Compose ClickHouse (full refresh)
	$(TOOLBOX) dbt build --project-dir dbt --profiles-dir dbt/profiles --full-refresh

demo: ## The whole pipeline on committed fixtures, with one chart, on the demo stack; needs no cloud account
	@$(MAKE) --no-print-directory STACK=$(DEMO_STACK) _demo

_demo:
	$(MAKE) up-bi
	$(MAKE) smoke
	@echo "== tracer DAG on the fixture hour $(DEMO_HOUR)"
	$(TOOLBOX) python -m tests.e2e.airflow --hour $(DEMO_HOUR) --source fixture
	@echo "== operational models: slot status and completeness from the ledger"
	$(TOOLBOX) dbt build --project-dir dbt --profiles-dir dbt/profiles --select +ops_slot_status_daily
	@echo "== Metabase: connection, question and dashboard"
	$(TOOLBOX) python -m monitoring.metabase.provision
	@echo "== end-to-end assertions"
	$(TOOLBOX) pytest tests/e2e -m e2e -p no:cacheprovider
	@echo
	@echo "Demo passed. Dashboard: http://127.0.0.1:3000  (sign in with METABASE_ADMIN_EMAIL"
	@echo "and METABASE_ADMIN_PASSWORD from .env). Airflow: http://127.0.0.1:8080"
	@if [ -t 1 ] && [ -z "$CI" ] && command -v open >/dev/null; then open http://127.0.0.1:3000; fi

e2e: ## End-to-end assertions against the demo stack, on which the demo has run
	@$(MAKE) --no-print-directory STACK=$(DEMO_STACK) _e2e

_e2e: _other-stack-stopped _lake-dir
	$(TOOLBOX) pytest tests/e2e -m e2e -p no:cacheprovider $(PYTEST_ARGS)

# A root that is already initialised keeps its S3 backend, and `init -backend=false` would then
# ask for credentials. `validate` needs only the installed providers, so init runs only in a
# root that has never been initialised (a fresh clone, CI).
tf-check: ## Terraform checks that need no AWS access: fmt, validate, tflint, checkov
	terraform fmt -check -recursive $(TF)
	@for root in $(TF_ROOTS); do \
		echo "== validate $$root"; \
		test -d $$root/.terraform || terraform -chdir=$$root init -backend=false -input=false >/dev/null || exit 1; \
		terraform -chdir=$$root validate || exit 1; \
	done
	@tflint --init --config "$(CURDIR)/$(TF)/.tflint.hcl" >/dev/null
	@for dir in $(TF_ROOTS) $(TF_MODULES); do \
		echo "== tflint $$dir"; \
		tflint --config "$(CURDIR)/$(TF)/.tflint.hcl" --chdir $$dir || exit 1; \
	done
	uvx checkov==3.4.11 --config-file .checkov.yaml

plan: ## terraform plan for bootstrap and envs/prod (needs AWS credentials; see infra/terraform/README.md)
	@test -f $(TF)/local.env || { echo "create $(TF)/local.env from $(TF)/local.env.example first"; exit 1; }
	@set -a; . $(TF)/local.env; set +a; \
	for root in $(TF_ROOTS); do \
		echo "== plan $$root"; \
		terraform -chdir=$$root init -input=false -backend-config=backend.hcl >/dev/null || exit 1; \
		terraform -chdir=$$root plan -input=false -lock-timeout=60s || exit 1; \
	done
