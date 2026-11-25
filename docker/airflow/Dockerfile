# syntax=docker/dockerfile:1.7
#
# One image, three isolated Python environments (blueprint 5.5 and 9.3):
#
#   /home/airflow/.local   Airflow itself, plus requirements.txt under the Airflow constraints file
#   /opt/venvs/gbmi        the gbmi package, from uv.lock
#   /opt/venvs/dbt         dbt-core and dbt-clickhouse, from dbt/uv.lock
#
# Tasks reach gbmi through @task.external_python and dbt through its own executable, so the three
# dependency sets never have to agree with each other.
#
# Targets:
#   dev      Used by Compose on the laptop and in CI. gbmi is installed in editable mode together
#            with the development group; src/, dags/ and dbt/ are bind-mounted under /opt/gbmi.
#   runtime  The default. For deployment: the gbmi wheel, dags/ and dbt/ (with its manifest) baked in.
#
# Base images are pinned by digest; the tags are kept for readability.

ARG AIRFLOW_IMAGE=apache/airflow:3.4.0-python3.12@sha256:818f94affc5bd7ce0be0cc04f2ace09aa8382e65190a73a08881229ff32700cb
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.13.10@sha256:a3ea1357342c1a4bf9a0c53a3deb1b55379aefa037338d8113d5771efa01774f

FROM ${UV_IMAGE} AS uv

# ---------------------------------------------------------------------------------------------
FROM ${AIRFLOW_IMAGE} AS base

ARG AIRFLOW_VERSION=3.4.0
ARG AIRFLOW_PYTHON=3.12

USER root
# The base image ships its own uv; the pinned one is installed beside it under another name so
# that the lock files are always read by the uv version that wrote them.
COPY --from=uv /uv /usr/local/bin/uvp
RUN mkdir -p /opt/venvs /opt/uv/python /opt/build /opt/gbmi \
 && chown -R airflow:0 /opt/venvs /opt/uv /opt/build /opt/gbmi
USER airflow

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_CACHE=1 \
    UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_PYTHON_PREFERENCE=only-managed \
    GBMI_HOME=/opt/gbmi \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/gbmi/dags \
    DBT_PROJECT_DIR=/opt/gbmi/dbt \
    DBT_PROFILES_DIR=/opt/gbmi/dbt/profiles

# 1. Airflow environment: requirements.txt only, under the constraints of this Airflow version.
#    pip check must stay clean (spike SK3, question B1). A conflict here is solved by moving the
#    package into its own virtual environment, never by loosening the constraints.
COPY --chown=airflow:0 docker/airflow/requirements.txt /opt/build/airflow-requirements.txt
RUN pip install --no-cache-dir -r /opt/build/airflow-requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON}.txt" \
 && pip check

# 2. The interpreter for the two project environments: the same uv-managed CPython that the
#    laptop and CI use (.python-version).
COPY --chown=airflow:0 .python-version /opt/build/.python-version
RUN uvp python install "$(cat /opt/build/.python-version)"

# 3. dbt environment, from dbt/uv.lock.
COPY --chown=airflow:0 dbt/pyproject.toml dbt/uv.lock /opt/build/dbt/
RUN cd /opt/build/dbt \
 && UV_PROJECT_ENVIRONMENT=/opt/venvs/dbt uvp sync --frozen --no-dev \
 && /opt/venvs/dbt/bin/dbt --version

# 4. Dependencies of gbmi, from uv.lock. The package itself is added by each target.
COPY --chown=airflow:0 pyproject.toml uv.lock README.md LICENSE NOTICE /opt/build/gbmi/
RUN cd /opt/build/gbmi \
 && UV_PROJECT_ENVIRONMENT=/opt/venvs/gbmi uvp sync --frozen --no-dev --no-install-project

# ---------------------------------------------------------------------------------------------
FROM base AS dev

# An editable install records the path /opt/gbmi/src. At run time Compose mounts the working
# tree there, so code changes need no rebuild.
COPY --chown=airflow:0 .python-version pyproject.toml uv.lock README.md LICENSE NOTICE /opt/gbmi/
COPY --chown=airflow:0 src /opt/gbmi/src
RUN cd /opt/gbmi \
 && UV_PROJECT_ENVIRONMENT=/opt/venvs/gbmi uvp sync --frozen \
 && /opt/venvs/gbmi/bin/gbmi version

# ---------------------------------------------------------------------------------------------
FROM base AS runtime

ARG GIT_SHA=unknown
ENV GBMI_PIPELINE_VERSION=${GIT_SHA}

COPY --chown=airflow:0 src /opt/build/gbmi/src
RUN cd /opt/build/gbmi \
 && UV_PROJECT_ENVIRONMENT=/opt/venvs/gbmi uvp sync --frozen --no-dev --no-editable \
 && /opt/venvs/gbmi/bin/gbmi version

COPY --chown=airflow:0 dags /opt/gbmi/dags
COPY --chown=airflow:0 dbt /opt/gbmi/dbt
# The manifest is built here so that DAG files never call dbt when they are parsed (blueprint 5.5).
RUN cd /opt/gbmi/dbt \
 && /opt/venvs/dbt/bin/dbt parse --profiles-dir profiles --target local \
 && test -s target/manifest.json
