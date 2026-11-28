# ADR-010 — Airflow packaging: one image, three isolated environments, dbt through Cosmos

| | |
|---|---|
| **Date** | 2026-11-28 |
| **Status** | **Provisional**: the evidence of the laptop and of CI is complete; the evidence on the AWS host follows with the first deployment (work package WP-12) |
| **Sources** | Blueprint §5.5, §8, §9.3, technology decisions TD-02 and TD-15; week 1 plan, spike SK3 (question B); week 3 plan §3.3, WP-08 and WP-10 |
| **Related** | Exit-gate rows 8, 12 and 13 of week 3; the memory report of phase P0 (`docs/reports/p0-memory.md`) |

## Context

Three sets of Python packages have to run on one machine with 8 GB of memory.

- **Airflow 3.4** installs under a constraints file: a list of exact versions which the Airflow project has tested together. A package outside the list can break the scheduler.
- **The package `gbmi`** holds the pipelines. It needs PyIceberg, DuckDB, PyArrow, psycopg and boto3 in versions which the project chooses and locks in `uv.lock`.
- **dbt** with the ClickHouse adapter has a third set of versions, locked in `dbt/uv.lock`.

The three sets do not agree with each other, and each one changes on its own schedule. A task of a DAG also has to run the same code on the laptop, in CI and on the AWS host, and a row which a task writes has to name the version of the code which wrote it.

Four points need a decision: how many images; how a task reaches the pipeline code; how a DAG runs dbt; and what "the version of the code" is.

## Decision

**1. One image for every Airflow service.** `docker/airflow/Dockerfile` builds one image, and the API server, the scheduler, the DAG processor, the triggerer, the one-shot services and the toolbox all run it. The two base images are pinned by digest.

**2. Three isolated Python environments inside the image.**

| Environment | Content | Installed from |
|---|---|---|
| `/home/airflow/.local` | Airflow itself and its providers, with Cosmos | `docker/airflow/requirements.txt` under the constraints file of the Airflow version; `pip check` has to pass in the build |
| `/opt/venvs/gbmi` | The package `gbmi` | `uv.lock` |
| `/opt/venvs/dbt` | dbt Core and the ClickHouse adapter | `dbt/uv.lock` |

A conflict in the first environment is solved by moving the package into an environment of its own, never by loosening the constraints.

**3. A task reaches the pipeline code through `@task.external_python`.** The task function runs in a new process with the interpreter `/opt/venvs/gbmi/bin/python`. A DAG file imports nothing of `gbmi`: the file holds the schedule, the order of the tasks and the arguments. A task receives its values as arguments and returns a small summary.

**4. A DAG runs dbt through Cosmos, from a manifest which the image holds.** Cosmos turns the dbt project into Airflow tasks: for each model one task which builds the model and one task which runs the tests of the model. Cosmos reads `dbt/target/manifest.json` and calls the dbt program of the third environment. The image builds the manifest once, so a DAG file never calls dbt while it is parsed. The unit tests of dbt are no tasks of a DAG: they run in the demo and in CI.

**5. Two build targets.** `dev` serves the laptop and CI: the package is installed in editable mode, and Compose mounts `src/`, `dags/` and `dbt/`. `runtime` serves the host: the wheel of the package, the DAG files, the dbt project with its manifest and the migration files are part of the image, and nothing is mounted.

**6. The Git commit is the version.** The workflow `images` builds the target `runtime` for each commit of `main` and pushes the image as `ghcr.io/<owner>/gbmi-airflow:<the 40 characters of the commit>`. The image carries the commit as `GBMI_PIPELINE_VERSION` and as a label. Every row, run record and log line names that version.

## Alternatives considered

| Alternative | Why it lost |
|---|---|
| One Python environment for Airflow, the pipelines and dbt | The constraints file of Airflow fixes versions which the pipelines and dbt need in other versions. The build of the first environment runs `pip check`, and a single environment would fail it or force old versions on the pipelines |
| One image for each component, with a container for each task (`DockerOperator`, or Kubernetes pods) | A container for each task needs the Docker socket inside the scheduler, which gives every task the rights of the machine, or a cluster, which the platform does not have (TD-15). Three images also triple the builds and the checks |
| A virtual environment which a task creates at run time (`PythonVirtualenvOperator`) | Each task would download and install packages: slow, and a task would depend on a package index at run time |
| dbt in one task (`BashOperator` with `dbt build`) | One task for the whole project hides which model failed, cannot retry one model and shows no lineage in Airflow. It stays the fallback when the tasks of Cosmos become too many for the host |
| Cosmos parsing the dbt project at each parse of a DAG file | The DAG processor would start dbt each time it reads the file. A manifest in the image costs nothing at parse time |
| A mutable tag such as `latest` | A row could not name the code which wrote it, and a way back would not know what to pull. A commit names exactly one image |

## Consequences

- **The image is large:** the target `runtime` takes 955 MB on the disk of the laptop (arm64, unpacked, with the layers of the Airflow base image; local build of 2026-11-28).
- **Each task costs a new process.** `@task.external_python` starts an interpreter, and Cosmos starts dbt once for each task. The mart build of phase P2 has 43 tasks and took about one minute on the laptop.
- **A task cannot use the Airflow context inside its function.** The function runs outside the environment of Airflow. Dates and settings travel as arguments, which also keeps the pipeline code testable without Airflow.
- **A new dbt model needs a new manifest.** On the laptop `make up` builds it; on the host each image brings its own.
- **Three lock files are kept current**, each with its own checks. A new version of Airflow changes only the first environment.
- **The tasks run inside the scheduler container** (the local executor). The memory of every task counts against the limit of that container, which the measurements of the host have to confirm.

## Evidence

| Claim | Evidence |
|---|---|
| The three environments install and work together | Spike SK3, question B, of phase P0 (passed). The build runs `pip check` for the Airflow environment. `docker/airflow/check_runtime.sh` checks a built image without any mount and refuses an image of another commit; `make images` and the workflow `images` run the check |
| A DAG runs the pipeline code and dbt | `make demo` on the laptop and in the workflow `e2e` on every pull request: the tracer DAG, three GDELT DAGs, three SEC DAGs and `marts_build` with 43 tasks |
| One image for each commit of `main` | The workflow `images`: the image of the commit `630a377` was pushed on 2026-11-28, and the registry serves it without a login |
| The image runs on the host, and the version on the host is the commit | **Open.** The smoke test of the first deployment compares the version of the running scheduler with the commit of the release (exit-gate rows 8 and 13 of week 3) |
| The tasks fit the memory of the host | **Open.** The measurements of work package WP-13 (exit-gate row 12) |

## Status

**Provisional.** The decision is in force since phase P0. The record becomes *accepted* when the first deployment has run the image on the AWS host and the measurements of the host are recorded.
