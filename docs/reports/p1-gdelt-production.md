# P1 status report — GDELT at production quality

| | |
|---|---|
| Phase | P1 — GDELT at production quality (blueprint §11.2), as specified by the week 2 plan |
| State | **In progress.** The report grows with each work package. The verdict, the exit gate with evidence, the numbers of the backfill, the open items and the hand-off follow in WP-12 |
| Run | Started on 2026-11-26 at 16:09 UTC, on one laptop: MacBook Pro M2 Pro, 16 GB, Docker Desktop with a virtual machine of 7.65 GiB and 12 CPUs. Claude, the AI model which works in the repository, carries out the work packages. Jay gave the go-ahead for the Git work of the week (week 2 plan, §3.5) |

## Work packages

The order follows change C1 of the week 2 plan (§3.5). Every time is UTC. The working time of a work package excludes the waits for the GitHub checks (change C9).

| WP | Subject | Start | End | Pull request | State |
|---|---|---|---|---|---|
| 01 | Start: checks, pins, plan in the repository | 2026-11-26 16:09 | | | In progress |

## Version pins

Checked on 2026-11-26 between 16:10 and 16:13 UTC, the start date, against PyPI, Docker Hub, the GitHub container registry, the GitHub releases and the Terraform registry. Rule of the week 2 plan (WP-01): a newer patch release of the same minor version is taken; any other change stays on the pin.

| Component | Pin of phase P0 | Found on 2026-11-26 | Pin of phase P1 |
|---|---|---|---|
| Apache Airflow; task SDK | 3.4.0; 1.4.0 | Newest releases; the digest of the image is unchanged | Unchanged |
| astronomer-cosmos | 1.16.0 | Newest release | Unchanged |
| DuckDB | 2.0.1 | Newest release | Unchanged |
| PyIceberg; PyArrow; SQLAlchemy | 0.12.0; 26.0.1; 2.1.9 | Newest releases | Unchanged |
| dbt Core; dbt-clickhouse; sqlfluff | 1.12.7; 1.10.4; 4.5.0 | Newest releases | Unchanged |
| ClickHouse | 26.8.46.6 (image tag `26.8`, by digest) | **26.8.47.5** (2026-11-25): the tag `26.8` moved to a newer patch release | **26.8.47.5**: the tag `26.8` by the new digest, in `compose/compose.yaml` and in the workflow `dbt` |
| PostgreSQL | 17.12 (image tag `17`, by digest) | 17.12 is the newest release of version 17; the digest of the tag is unchanged | Unchanged |
| Metabase | 0.65.7.1 | **0.65.7.2** (2026-11-25): a patch release | **0.65.7.2**, by digest |
| Terraform; AWS provider | 1.17.2; 6.74.0 | Newest releases | Unchanged |
| tflint; AWS rules of tflint; checkov | 0.66.0; 0.50.0; 3.4.11 | Newest releases | Unchanged |
| Python | CPython 3.12.16, managed by uv | The newest 3.12 release which uv 0.13.10 offers | Unchanged |
| uv; ruff; mypy; pytest; pytest-cov; pre-commit | 0.13.10; 0.17.6; 2.4.1; 9.2.0; 7.1.0; 4.7.0 | Newest releases; the digest of the uv image is unchanged | Unchanged |
| typer | 0.29.2 | **0.29.3** (2026-11-26): a patch release | **0.29.3** |
| httpx | 0.28.1 | **0.29.0** (2026-11-26): a new minor version, not a patch release | **0.28.1.** The lock file holds the pin. An upgrade needs a decision record (blueprint §6.1, risk R7) |
| Other libraries | clickhouse-connect 1.13.0, psycopg 3.3.9, pydantic 2.14.4, pydantic-settings 2.16.3, tenacity 9.2.1, structlog 26.1.0, tzdata 2026.6 | Newest releases | Unchanged |
| hypothesis | Not installed | 6.170.17 (2026-11-20) | **6.170.17**, new in the development group (`hypothesis>=6.170,<7`), with sortedcontainers 2.4.0 |
| GitHub Actions | checkout 7.0.1, setup-uv 10.4.0, setup-buildx 4.4.1, build-push 7.4.0, setup-terraform 4.0.1, setup-tflint 6.3.2; gitleaks 8.30.1 | Newest releases | Unchanged |

## Choices made during the run

| # | Work package | Choice | Reason |
|---|---|---|---|
| 1 | WP-01 | The pin check also took ClickHouse 26.8.47.5 and Metabase 0.65.7.2 | The week 2 plan (§4.2) had not compared the image tags with Docker Hub and had not checked Metabase again. Both changes are patch releases, so the rule of WP-01 applies |
| 2 | WP-01 | The status report starts in WP-01 and grows with each pull request. The week 2 plan lists the report under WP-12 | The pin table and the times of each work package are evidence which exists from the first day. WP-12 completes the report |
| 3 | WP-01 | `.gitignore` names the directory `.hypothesis/` | hypothesis stores examples in the directory during a test run |
