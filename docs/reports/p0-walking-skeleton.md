# P0 status report — the walking skeleton

| | |
|---|---|
| Phase | P0 — walking skeleton and de-risking (blueprint §11.2), as specified by the week 1 plan |
| Run | 2026-11-25 (UTC), on one laptop: MacBook Pro M2 Pro, 16 GB, macOS 27, Docker Desktop 4.42.0 with a 7.65 GiB VM. Closed on 2026-11-26 with the repository on GitHub and an AWS account |
| Verdict | **P0 is complete.** Every row of the exit gate holds; row 4 was removed. The tag `p0` marks the merge commit of the pull request which closed row 10, the last open row |
| Verified at | Laptop checks: commit `a19dee0`. GitHub: the four workflows on `main` at `6780771`. AWS: the baseline as applied on 2026-11-26 and the Glue/S3 test halves, run from the branch `p0-close-out`. Labels: the sheet at the commit with the tag `p0` |

**Scope.** The first run, on 2026-11-25, was local-only by choice: no AWS account and no Git remote were connected. On 2026-11-26 the repository was pushed to GitHub, an AWS account was opened, the Terraform baseline was applied and the open rows were closed; the section "Closing the gate on 2026-11-26" records that work. The labelling sheet was labelled on the same day (row 10). Every number below was measured in one of the two runs; anything not run is labelled *not run*.

## Exit gate

The rows are those of the week 1 plan, §2.

| # | Criterion | State | Evidence, or what is missing |
|---|---|---|---|
| 1 | `make demo` passes on the laptop and in CI | **True** | Laptop: passes from nothing in 99 seconds, from a fresh clone, and in a Linux rehearsal of the `e2e` workflow (see Verification). CI: `e2e` passed on `main` at `6780771` on 2026-11-26, in 10 min 12 s, on its first run |
| 2 | The chart's total equals source rows minus quarantined rows minus duplicate rows | **True** | `tests/e2e/test_demo.py::test_chart_total_equals_raw_minus_quarantined_minus_duplicates`. The Metabase card returns 4,023; the test counts 4,023 lines, 0 invalid rows and 0 repeated keys straight from the fixture zips, with code that shares nothing with the pipeline. Both subtracted terms are zero for this hour; the non-zero paths are tested with constructed rows in `tests/transforms/test_gdelt_event_parse.py` |
| 3 | Each spike has a recorded pass or fallback | **True** | SK1 passed, SK2 passed, SK3 A took its fallback (ADR-006), SK3 B passed — [`p0-spikes.md`](p0-spikes.md); the tests stay in the suite. The AWS halves, SK1 Q8 and SK2 Q6, passed on Glue and S3 on 2026-11-26 |
| 4 | ~~A budget alert email has been received~~ | **Removed** | Removed from the gate on 2026-11-26 (week 1 plan, D12). See "Decision of 2026-11-26" below |
| 5 | CI is green | **True** | `ci`, `dbt`, `e2e` and `terraform` passed on `main` at `6780771` on 2026-11-26, on their first runs on GitHub, and on the pull request that carries this report. `main` now requires the four checks |
| 6 | The Terraform baseline is applied | **True** | Applied on 2026-11-26 in `us-east-1`: 13 resources in `bootstrap`, 16 in `envs/prod`. `make plan` reports no changes for both |
| 7 | Memory is measured | **True** | [`p0-memory.md`](p0-memory.md): idle, the demo and the GKG day probe, beside the blueprint's estimates |
| 8 | ADR-001 and ADR-004 are drafted | **True** | [`docs/adr/`](../adr/). ADR-004 was accepted on 2026-11-26, when row 3 closed |
| 9 | Version pins are re-verified on the start date | **True** | The pin table below. No pin moved |
| 10 | Labelling has started | **True** | All 200 strings of `config/labelling/gold_sheet_v0.csv` have a label since 2026-11-26; the row asks for 100. Claude, the AI model which works in the repository, labelled the strings after Jay handed the task over (week 1 plan, D14). No person has reviewed the labels. Method, results and limits: [`config/labelling/README.md`](../../config/labelling/README.md). `tests/unit/test_gold_sheet.py` checks the format |

### Decision of 2026-11-26: an alert email is not a condition of P0

Jay removed exit-gate row 4 on 2026-11-26. The forced alert test (lower the limit, create about USD 0.03 of spend, wait up to a day for one email) is no longer needed for P0 or for the tag `p0`.

- **What stays:** the budget `gbmi-prod-monthly` with notifications at 50, 80 and 100% of USD 30, and the hand-made zero-spend budget.
- **What the decision gives up:** proof, before P1, that an alert email is delivered. The first real alert gives the same proof later.
- **What still needs one look:** the status of the alert address on the budget's page in the Billing console. AWS Budgets sends nothing to an unverified address.
- **Where the test went:** the procedure is optional and stays in [`infra/terraform/README.md`](../../infra/terraform/README.md), §3.

The blueprint (§11.2) lists the alert email among the acceptance criteria of P0. The week 1 plan records the difference as D12.

## Closing the gate on 2026-11-26

Jay did the account work by hand, from a step-by-step guide; the checks after it were then run from the laptop.

| What | Result |
|---|---|
| GitHub | The repository was pushed with its whole history, 18 commits. `ci`, `dbt`, `e2e` and `terraform` ran for the first time and passed without a change, in 35 s, 3 min 18 s, 10 min 12 s and 56 s. `main` is protected by the ruleset `protect-main`: changes arrive by pull request, the four checks are required, and force pushes and deletion are blocked. The environment `prod` has a required reviewer |
| AWS account and access | A new AWS account on the paid plan, used in `us-east-1`. The root user does all AWS work (week 1 plan, D13): it has MFA, no access key exists, and there is no IAM user. `aws login` gives the CLI credentials that last 15 minutes and are renewed for up to 12 hours |
| Terraform | `bootstrap` (13 resources) and `envs/prod` (16 resources) were applied by hand, in that order, with the state moved into the state bucket in between. `make plan` reports no changes for both |
| Budget | `gbmi-prod-monthly` (USD 30, notifications at 50, 80 and 100%) and the hand-made zero-spend budget exist. No alert was forced (row 4) |
| Spikes on AWS | `make integration-aws`: the 19 tests that run on AWS passed — 13 for SK1 and 6 for SK2 — and the 3 stretch tests of SK2 skipped themselves, as written. The run took 53 minutes, 18 of them spent waiting for new credentials |
| Clean-up | The tests dropped the Glue database `gbmi_ci_silver` themselves. 52 objects were left under `ci/` and were deleted: 43 zero-byte directory markers and the 9 files of the snapshots that SK1 Q7 had expired. No current object remains under `ci/`; about a thousand non-current versions expire by the bucket's lifecycle rule |
| Labelling | Jay handed the labelling of the sheet to Claude (week 1 plan, D14). Claude labelled all 200 strings in one session, with no matching system to look at: 25 `company_in_universe`, 25 `company_not_in_universe`, 149 `not_a_company`, 1 `ambiguous`. The universe and the CIKs come from a copy of the SEC ticker list dated 2026-10-22; no request went to the SEC (D7). Twelve names were checked with web searches |

### What the first real runs showed

| Finding | What was done |
|---|---|
| `envs/prod` never reported "No changes": the budget output read the limit back from AWS, which returns `30.0` | The output reads the variable (`da2cf16`). One apply with no resource change stored the new value |
| `make tf-check` asked for credentials once a root had a real backend, because `init -backend=false` still opens it | Init runs only in a root that has never been initialised (`bb38c81`) |
| Credentials from `aws login` last 15 minutes and cannot be renewed inside a container. The first attempt to run the AWS halves in one go lost its credentials after eight tests | `make integration-aws` runs them one test at a time, each with fresh credentials (`tests/integration/run_aws.sh`) |
| On S3, `purge_table` leaves directory markers and the files of expired snapshots behind | Recorded in the spike report and in ADR-004; the file clean-up of phase P4 has to cover both |
| On S3, from the laptop, the test of the hour-by-hour day (SK1 Q5, pattern a) took 471 s and the streamed day (pattern b) 194 s | Supports the choice of pattern (b) |

Two things in this section rest on documentation and have not been exercised. No workflow has assumed the roles `gbmi-ci-plan` or `gbmi-ci-deploy`, so the format of the OIDC subject is confirmed only by the trust policies that Terraform created; its first real test is the first workflow that needs AWS, in phase P2. And no budget alert has been delivered (row 4): the status of the alert address is checked by hand in the Billing console.

## Work packages

| WP | Subject | State | Branch (local commit) |
|---|---|---|---|
| 01 | Preflight | Done; the AWS and GitHub accounts followed on 2026-11-26 | — |
| 02 | Repository, tooling, first workflow, ADR-001 | Done; pushed on 2026-11-26, `main` protected | `wp-02-scaffold` (`9ae6641`) |
| 03 | Terraform baseline and budget alert | Done: applied on 2026-11-26. No alert was forced (D12) | `wp-03-terraform-baseline` (`d5b9f0c`) |
| 04 | Labelling sheet | Done; all 200 strings labelled on 2026-11-26 (D14) | `wp-04-labelling-sheet` (`c6c97f3`); `p0-labels` |
| 05 | Images and Compose core profile | Done | `wp-05-compose-stack` (`4902e47`, `fb0eba1`) |
| 06 | Spike SK1 — PyIceberg contract | Done: passed on both backends | `wp-06-sk1-pyiceberg` (`156964b`) |
| 07 | Spike SK2 — ClickHouse pinned read; dialect checks | Done: passed locally and on S3 | `wp-07-sk2-pinned-read` (`a1af61b`) |
| 08 | Spike SK3 — partition replace; Cosmos | Done: A took its fallback, B passed | `wp-08-sk3-partition-replace-cosmos` (`bbb9ffc`) |
| 09 | Tracer DAG and `make demo` | Done; the `e2e` workflow is green | `wp-09-tracer-demo` (`f2a949f`) |
| 10 | Memory under load | Done, including the GKG probe | `wp-10-memory` (`33f7179`, `3fabfab`) |
| 11 | ADR-004, reports, close-out | Done, with the AWS clean-up and the tag `p0` | `wp-11-close-out` (`a712cca`, `513736b`) |
| — | Found by the Linux rehearsal: the time-zone database is now a declared dependency | Done | `rehearsal-linux-fixes` (`1adcc69`) |
| — | Stretch items: SK2 Q7 and Q8; ClickHouse reads PostgreSQL in the smoke check | Done, local backend only | `stretch-sk2-q7-q8-smoke` (`4da21d1`) |
| — | The ClickHouse init container left an anonymous volume at every `make down` | Fixed, with a smoke check | `fix-init-volume-leak` (`a19dee0`) |
| — | Close-out on GitHub and AWS: exit-gate row 4 removed, two fixes found by the first real runs, the runner for the AWS test halves, and this report | Done on 2026-11-26 | `p0-close-out` (`f217be4`, `da2cf16`, `bb38c81` and the commits after them) |

Each branch was cut from `main`, and `main` was fast-forwarded when the package's "Done when" had been shown, so the history is linear and can be replayed as one pull request per package. WP-03 was moved behind the spikes, the reordering the plan allows for missing AWS access (§10). The close-out of 2026-11-26 is the first change that reached `main` through a pull request.

## Verification

Run on 2026-11-25 against local commit `a19dee0`, in three places: the working checkout on macOS, a fresh clone on macOS, and a fresh clone on Linux. The last four rows were run on 2026-11-26, on GitHub and against the AWS account.

| Check | Command | Result |
|---|---|---|
| Tooling | `make lint`, `make test` | Clean. 91 tests pass; the unit and transformation tests cover 72% of `src/gbmi` |
| Stack | `make up-bi && make smoke` | 13 of 13 checks pass, with the memory limits in force |
| Spikes, local | `make integration` | 50 passed and 4 strict expected failures in 97 s; the 22 AWS variants were deselected |
| Incremental contract | `pytest tests/integration/dbt` against a throwaway ClickHouse 26.8 container, as the `dbt` workflow runs it | 20 passed, 2 strict expected failures, 97 s. The workflow's own health command reported the container healthy |
| From nothing | `make reset && make demo` | Passes in **99 s** with the image already built. The plan's bound is ten minutes |
| End to end, twice | `make demo` again | Passes in 65 s. The second run wrote no file to bronze, committed `delete` and `append` to silver, and left the content checksums of silver and of the mart unchanged |
| Live source | The tracer DAG with `source=live` for 2026-11-25 14:00 UTC | Passes in 21 s: 4 files (397,293 bytes) from the GDELT servers, 5,991 rows, none quarantined; silver, the pinned table and the mart then agree on 10,014 events for the month |
| Images | `make images` | Both targets build. The runtime image reports Airflow 3.4.0, dbt 1.12.7 with dbt-clickhouse 1.10.4, a baked manifest and a clean `pip check` |
| Terraform, offline | `make tf-check` | `fmt` clean; `validate` succeeds for `bootstrap` and `envs/prod`; tflint clean in five directories; checkov: 137 passed, 0 failed, with four checks disabled in `.checkov.yaml`, each with its reason |
| Workflow syntax | actionlint on the four files | Clean |
| Fresh clone, macOS | `git clone`, then `make setup lint test` and the commands of the `e2e` workflow | Clean, from 176 tracked files and nothing else: 91 tests; stack up in 43 s; `make integration` (50 passed) and `make demo` (78 s) pass |
| Fresh clone, Linux | The `run` steps of all four workflows, replayed in order (see below) | All four jobs pass: `ci` (lint, 91 tests, the secret scan over 17 commits), `terraform` (27 s), `dbt` (95 s) and `e2e` (`make integration` 100 s, `make demo` 82 s) |
| Spikes, AWS | `make integration-aws` | 19 passed, 3 skipped by design, 0 failed, in 53 minutes. Afterwards `gbmi_ci_silver` is gone and no current object remains under `ci/` |
| CI | The four workflows on `main` at `6780771` | All four passed on their first run: `ci` 35 s, `terraform` 56 s, `dbt` 3 min 18 s, `e2e` 10 min 12 s |
| Infrastructure | `make plan` | "No changes" for `bootstrap` and for `envs/prod` |
| Budget | `aws budgets describe-budgets` | `gbmi-prod-monthly` with a limit of 30.0, and the zero-spend budget |

### The Linux rehearsal

Before the repository had a remote the workflows could not run, and Docker Desktop for macOS hides the file-ownership rules of Linux. So their steps were rehearsed on Linux: an Ubuntu 24.04 container stood in for the runner, with a user of uid 1001 as on GitHub's runners and the tools the setup actions would install, and it drove a second, nested Docker daemon that started with no images. A script read each workflow file and executed its `run` steps in order with the declared environment; it started the `dbt` job's service container with the options the workflow gives, and built the image as the build action would.

| | |
|---|---|
| Found | Five transformation tests failed on a system without a time-zone database: PyArrow needs one to return `timestamptz` values. `tzdata` is now a declared dependency, and two tests hide the system's database to keep it that way. GitHub's runner images are full Ubuntu installations, which normally carry such a database, so the workflow would probably have passed there — that was not checked. A minimal image would not |
| Confirmed | Files that the stack writes into the bind-mounted lake belong to the runner's uid, and ClickHouse reads them through its read-only mount. The secret-scan step downloads, verifies and runs as written. The service container becomes healthy with the workflow's health command |
| From nothing | On the daemon without images, in the first rehearsal (three commits before `a19dee0`): building the image, base image included, took 63 s; the first start of the stack, pulling three more images, 72 s; `make demo` 88 s. Under four minutes in all, on a fast connection |
| Not shown | The pinned actions themselves — checkout, setup-uv, the build action with its cache, setup-terraform, setup-tflint — did not run. Neither did GitHub's hardware, its preinstalled software or its network |

The rehearsal script is not part of the repository. On 2026-11-26 the four workflows ran on GitHub for the first time and passed without a change.

## The demo in numbers

One hour of GDELT Events: the four `export` slots of 2026-11-25 08:00 to 08:45 UTC, committed under `tests/fixtures/gdelt/v2/`.

| Stage | Rows or bytes | Notes |
|---|---|---|
| Source files | 4 zips; 263,180 bytes compressed, 1,639,055 bytes raw; 4,023 lines | Each verified against the size and MD5 of the committed master-list excerpt |
| Bronze | 4 objects, bytes unchanged | The second run writes nothing |
| Silver `gdelt_event` | 4,023 rows; 0 quarantined; 0 duplicates | Content checksum `f6ed187b0184f5be`, the same after every run. First run: one `append` snapshot. Rerun: `delete` and `append` in one commit |
| ClickHouse `lake.gdelt_event` | 4,023 rows | Pinned to one metadata file before each build |
| Mart `fct_gdelt_event_daily` | 1,545 rows; 4,023 events | Partition 202611 replaced as a whole |
| Chart | 19 bars; total 4,023 | Read back through Metabase's API by the test |

| Duration on the laptop | Seconds |
|---|---|
| `make reset && make demo`, from nothing with the image built | 99 to 107 |
| `make demo` on a running stack | 62 to 78 |
| Stack start, `make up-bi` | 43 to 50 |
| One run of the tracer DAG, eight tasks | 15 to 24 |
| The end-to-end tests, including a second run of the DAG | 16 to 20 |

## Version pins

Checked against PyPI, Docker Hub and GitHub on 2026-11-25, the start date. Every pin was still the latest release; none moved.

| Component | Pin | How it is fixed |
|---|---|---|
| Apache Airflow | 3.4.0 | Image `apache/airflow:3.4.0-python3.12` by digest |
| astronomer-cosmos | 1.16.0 | `docker/airflow/requirements.txt`, installed under Airflow's 3.4.0 constraints; `pip check` runs in the build |
| DuckDB | 2.0.1 | `uv.lock` |
| PyIceberg | 0.12.0, with PyArrow 26.0.1 and SQLAlchemy 2.1.9 | `uv.lock` |
| dbt Core, dbt-clickhouse, sqlfluff | 1.12.7, 1.10.4, 4.5.0 | `dbt/uv.lock` |
| ClickHouse | 26.8.46.6 | Image tag `26.8` by digest |
| PostgreSQL | 17.12 | Image tag `17` by digest |
| Metabase | 0.65.7.1 | Image by digest |
| Terraform; AWS provider | 1.17.2; 6.74.0 | `required_version`, `required_providers` and the lock files |
| tflint; its AWS rules; checkov | 0.66.0; 0.50.0; 3.4.11 | Workflow, `.tflint.hcl`, Makefile |
| Python | CPython 3.12.16, managed by uv | `.python-version`; the same interpreter on the laptop, in the image and in the workflows |
| uv, ruff, mypy, pytest, pre-commit | 0.13.10, 0.17.6, 2.4.1, 9.2.0, 4.7.0 | `uv.lock`, image, workflows |
| Libraries | clickhouse-connect 1.13.0, psycopg 3.3.9, pydantic 2.14.4, pydantic-settings 2.16.3, httpx 0.28.1, tenacity 9.2.1, typer 0.29.2, structlog 26.1.0, tzdata 2026.6 | `uv.lock` |
| GitHub Actions | checkout 7.0.1, setup-uv 10.4.0, setup-buildx 4.4.1, build-push 7.4.0, setup-terraform 4.0.1, setup-tflint 6.3.2 | Each by commit hash; gitleaks 8.30.1 by checksum |

All four images have an arm64 variant.

## Choices made during execution

Where the plan was silent, or where the run did something other than what it said.

| What | Why |
|---|---|
| Terraform and tflint are official release binaries in `~/.local/bin`, verified by checksum; the Makefile puts that directory on its `PATH` | Homebrew could install neither on this machine |
| PostgreSQL is initialised by a `.sql` file, and ClickHouse by a one-shot `clickhouse-init` service on every start; no service restarts automatically | On Docker Desktop for macOS a non-executable init script on a bind mount tests as executable and then fails to run; an automatic restart then hid the failed initialisation |
| The DAG processor and the triggerer are health-checked through the API server's health endpoint | The reference check, `airflow jobs check`, added 115–150 MiB every 30 seconds |
| A third ClickHouse role, `pipeline_rw`, creates the pinned tables; ClickHouse's `user_files_path` is the lake mount and its entrypoint does not change ownership | Found in spike SK2: what `IcebergLocal` needs. The dbt role cannot re-pin a table |
| Memory limits: API server 1.0 GB, scheduler 2.6 GB, Metabase 1.5 GB with a 640 MiB heap | Measured; see the memory report |
| Partition-replaced marts use the custom `partition_replace` materialisation | Spike SK3: the adapter's strategy cannot empty a partition (ADR-006) |
| Tasks do not retry in the local stack (`GBMI_TASK_RETRIES=0`); the code's default stays 3 | A failing demo should fail at once, not after three attempts |
| The DAG processor looks for new files every 30 seconds, and the demo unpauses the tracer | Airflow 3 defaults to 300 seconds and to paused DAGs |
| The demo triggers the DAG through Airflow's REST API and waits for the run | It exercises the scheduler and the task processes, which `airflow dags test` would bypass |
| `.python-version` names the patch release | The laptop, the image and the workflows then run the identical interpreter |
| `make setup` builds no image | It needs no Docker; `make up` builds on first use |
| A logger created at import now follows a later configuration | Found by the GKG probe: task logs were not JSON and went to standard output |
| `tzdata` is a dependency of the package | Found by the Linux rehearsal |
| The smoke check that reads PostgreSQL from ClickHouse passes the password as a query parameter | A failing query is printed with its text; the first version would have printed the password with it |
| The ClickHouse init container mounts a tmpfs where its image declares a volume | Otherwise every `make down` left an empty anonymous volume behind |
| The root user does all AWS work, signed in with `aws login`; there is no IAM Identity Center, no organization and no second user. An AWS Organization and an IAM Identity Center instance, created at 08:18 UTC on 2026-11-26 while the setup guide still used IAM Identity Center, were never used and were deleted at 13:22 UTC the same day | Decision of the owner on 2026-11-26 (week 1 plan, D13) |
| An alert email is not a condition of P0 | Decision of the owner on 2026-11-26 (week 1 plan, D12) |
| Claude labelled the 200 strings of the labelling sheet; the plan gave the task to Jay, at about 20 strings a day | Decision of the owner on 2026-11-26 (week 1 plan, D14) |
| The AWS test halves run one test at a time (`make integration-aws`) | Credentials from `aws login` last 15 minutes, less than the halves take |
| The budget output reads the variable; `make tf-check` initialises only a fresh root | Found by the first real plan and by the first check after a real `init` |

## What the evidence corrects in the blueprint

The blueprint was not edited. These are the places where a measurement or a test now says something different.

| Blueprint | Finding | Evidence |
|---|---|---|
| §7.7: prefer the adapter's `insert_overwrite` | It keeps the old rows of a partition whose rebuild returns nothing | SK3, test T4 |
| §8.4: pass record batches to the Iceberg writer | Not implemented for partitioned tables in PyIceberg 0.12.0. A day is streamed into one Parquet file and added with `add_files` | SK1, Q5 |
| §5.6, §8.4: one snapshot per overwrite | One commit, up to two snapshots; rollback, expiry and `_silver_snapshot_id` follow from that | SK1, Q2; ADR-004 |
| §7.9: keep the last five snapshots | Keep the last five commits; expiry removes no files | SK1, Q7 |
| §13, risk R1: fall back to a query-level setting in each staging model | `iceberg_metadata_file_path` is not a query-level setting. The table function takes it, at the price of a file-read grant for dbt; `iceberg_snapshot_id` on an unpinned table needs no grant and would be the first fallback | SK2, Q7 |
| §7.10: `toDate('9999-12-31')` as the open end of an interval | It silently becomes 2149-06-06. `toDate32('9999-12-31')` holds the value | Dialect checks |
| §7.1: dbt reads the operational tables through `postgresql()` | It works for the `gbmi` role. A relation with a nullable array column cannot be read at all ("PostgreSQL array cannot be NULL"), so the tables of phase P1 should declare arrays `NOT NULL` or avoid them | Smoke check |
| §9.1: Metabase in 1.2 GB on the laptop and 1.0 GB on the host | It needs about 1.1 GB with a 640 MiB heap | Memory report |
| §9.1: the API server in 0.8 GB | It peaked at 778 MiB of 800; the limit is now 1.0 GB | Memory report |
| §4.6: a GKG day is about 0.43 GB compressed and 1.3 GB raw | 2026-11-24 was 0.47 GB and 1.46 GB | GKG probe |
| §11.2: the chart equals source rows minus quarantined rows | Minus duplicate rows as well | Plan difference D9; the end-to-end test |

## Open items

**Carried to later phases**

| Item | Phase |
|---|---|
| The DAG test suite, which will also guard spike answer B3 | P1 |
| The cost of an hourly overwrite inside a compacted day; skipping commits for a unit that stays empty; a flag where "no values" must differ from "values missing" | P1 |
| The real GKG parser's memory, and a silver build beside the hourly jobs | P1 |
| A named collection for the PostgreSQL connection, so that no model carries a password; no nullable arrays in the operational tables | P1 |
| SEC fixtures and the five demo companies | P2 |
| The host column of the memory budget, including CPU credits; Metabase's allocation on the host; a ClickHouse role on AWS that cannot write to the lake | P2 |
| A workflow that assumes the two CI roles, the first real test of the OIDC subject | P2 |
| Commit latency on S3, measured on the host | P1 |
| Unreferenced files on the local backend and on S3, with the directory markers that `purge_table` leaves there; orphaned dbt staging tables; cost-allocation tags | P4 |
| The CIKs of the labelling sheet compared with the SEC reference data, once the SEC reference data is loaded | P2 |
| A review of the labels by a person, before the precision gate is treated as binding | P3 |

**Stretch items of the plan (§6).** Taken after the reports were written: SK2 questions Q7 and Q8, and the `postgresql()` read in `make smoke`. Not taken: a `plan` job in CI, and the Dependabot and `security` configuration. They needed a remote, which exists since 2026-11-26, and they stay stretch items.

**Optional, from the plan's hand-off (§11):** saving `company_tickers_exchange.json` once a day to preserve ticker history ahead of P2 has not been started. It needs the declared SEC user agent, which is Jay's to supply.

## Hand-off to week 2

By the plan's own rule (§11), P1 starts when the gate holds. The gate holds since 2026-11-26, and the tag `p0` marks the commit. **P1 starts from the tag `p0`.**

When P1 starts, it inherits the catalog factory and the commit helper (`gbmi.lake`), the pin step (`gbmi.warehouse.pin`), the `partition_replace` materialisation, the Compose stack, the fixtures and the `export` schema. It should read SK1 questions Q2 and Q5 and the conclusions of the memory report first.
