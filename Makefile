# Developer and operator commands (blueprint 9.1). Written for GNU Make 3.81, which is what
# macOS ships. Targets are added by the work package that makes them real.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Laptop, image and CI use uv-managed CPython; release binaries live in ~/.local/bin.
export UV_PYTHON_PREFERENCE := only-managed
export PATH := $(HOME)/.local/bin:$(PATH)

SQL_PATHS := dbt/models tests/integration/dbt/project/models

.PHONY: help setup fmt lint test

help: ## List the available targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-14s %s\n", $$1, $$2}'

setup: ## Create the virtual environments and install the Git hooks
	uv sync
	uv sync --project dbt
	@test -f .env || { cp .env.example .env && echo "created .env from .env.example"; }
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

test: ## Unit tests with coverage
	uv run pytest tests/unit --cov --cov-report=term-missing
